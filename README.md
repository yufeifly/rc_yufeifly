# Reliable HTTP Notifier

Reliable HTTP Notifier 是一个使用 Go 实现的可靠 HTTP 通知服务。它先把通知任务持久化，再异步调用外部供应商 API，从而把业务主链路与外部系统的超时、限流和故障隔离开。

## 一句话本质

**SQLite 保存不可丢失的任务事实，调度器只领取当前可执行的任务，Worker 在事务外调用供应商，并通过 Lease Epoch 与 Version CAS 防止过期 Worker 覆盖新结果。**

可以把它理解成一个带编号取件牌的快递柜：

1. 通知先入柜，数据库事务提交后才返回 `202 Accepted`；
2. Worker 领取任务时获得带期限和代次的取件牌；
3. 外部 HTTP 调用不占用数据库事务；
4. 只有持有当前取件牌的 Worker 能写回结果；
5. Worker 崩溃后，租约到期，任务可被重新领取；
6. 外部调用仍可能重复，因此供应商应使用稳定的通知 ID 去重。

## 设计说明要求

原始 PDF 设计说明中的需求和约束，已经在方案阶段结构化整理到：

- [产品需求文档](docs/PRD.md)：目标、范围、功能要求、验收标准；
- [技术方案](docs/TECHNICAL_DESIGN.md)：架构、数据模型、事务边界、故障恢复与安全设计；
- [实施计划](docs/IMPLEMENTATION_PLAN.md)：按 TDD 拆分的开发步骤和质量门禁。

设计说明要求的核心不是“成功发送一次 HTTP 请求”，而是构建一个可证明、可恢复、资源有界的投递闭环。

| 设计要求 | 实现方式 | 验证方式 |
|---|---|---|
| 持久化后确认 | `CreateOrGet` 事务提交成功后才返回 `202` | 存储关闭时接口不得返回 `202` |
| 幂等受理 | `caller_id + idempotency_key` 唯一约束，规范化 JSON 后计算版本化 SHA-256 | 重复内容返回原 ID，不同内容返回 `409` |
| 异步投递 | 接入链路只写 SQLite，通过 Wake Signal 唤醒调度器 | 接口不等待供应商响应 |
| 崩溃恢复 | `DELIVERING` 任务持有有限租约，过期后重新入队 | `SIGKILL` 后使用原数据库重启 |
| 防止旧 Worker 覆盖 | `lease_owner + lease_epoch + version` 条件写回 | 旧 epoch 完成请求返回 `ErrStaleLease` |
| 至少一次投递 | 未知结果在租约恢复后重试，通知 ID 始终不变 | 恢复演示中同一 ID 被再次投递 |
| 有限重试 | 错误分类、指数退避、抖动、`Retry-After` 和最大尝试次数 | 503 两次后 200，最终成功 |
| 死信与人工恢复 | 永久错误或重试耗尽进入 `DEAD`，管理员 Replay 开启新轮次 | 400 进入死信，修复后 Replay 成功 |
| 供应商差异 | 支持 `envelope`、`passthrough`、Bearer Token、API Key Header | Provider 契约测试 |
| 优先级 | 服务端映射 `HIGH/NORMAL/LOW`，等待 Aging 防止饥饿 | HIGH 优先，长期等待 LOW 最终执行 |
| 有界资源 | 全局 Worker 和 Provider 两级并发许可，连接与请求均有超时 | Mock Provider 记录最大并发 |
| 地址安全 | URL 只能来自配置；禁代理和重定向；连接时检查解析后的 IP | 生产模式拒绝 HTTP、私网及环回地址 |
| 权限隔离 | 静态 Token 映射 `producer/admin`，Token 常量时间比较 | producer 不能读取他人任务或 Replay |
| 可观测性 | JSON 日志、Prometheus 计数器/直方图/仪表盘指标 | 指标标签检查和 `/metrics` 验证 |

## 范围与边界

### 本次 MVP

- 创建、查询和 Replay API；
- SQLite WAL 持久队列；
- 幂等提交与内容冲突检测；
- Provider 静态配置和两种 Body 模式；
- HIGH/NORMAL/LOW 优先级及 Aging；
- Lease、Epoch、Version CAS；
- 自动重试、死信、崩溃恢复；
- 全局及 Provider 级并发保护；
- 静态身份认证、健康检查、指标；
- Mock Provider、自动化测试、故障演示和压测。

### 明确不保证

- 不保证严格 Exactly-once。网络超时时无法判断供应商是否已执行，请依赖 `notification_id` 做外部幂等；
- 不保证任务严格有序；
- SQLite 版本只支持单实例，不支持多个进程共享同一个数据库文件；
- 不提供动态 Provider 管理后台；
- 不允许调用方传入任意 URL；
- 不提供无限重试；
- P1 的调用方/Provider 令牌桶限流以及 P2 的熔断、多实例、MQ、跨地域容灾尚未实现。

## 核心流程

```text
调用方
  -> 认证、限长、严格 JSON 校验
  -> SQLite CreateOrGet
  -> COMMIT
  <- 202 + notification_id

Scheduler
  -> 恢复过期 Lease
  -> 计算有效优先级
  -> 预留全局和 Provider 并发许可
  -> Claim + 创建 STARTED Attempt
  -> Worker 调用外部 HTTP
  -> 使用 Epoch + Version CAS 写回
```

外部 HTTP 调用永远不在数据库事务内执行。否则慢供应商会长期持有 SQLite 写锁，使任务受理和其他 Worker 一起阻塞。

## 状态机

```text
PENDING ----claim----> DELIVERING ----成功----> SUCCEEDED
   ^                       |
   |                       +----可重试失败----> RETRY_WAIT
   |                       |                       |
   |                       +----永久/耗尽失败----> DEAD
   |                                               |
   +-------------租约恢复 / 管理员 Replay-----------+
```

状态含义：

| 状态 | 含义 | 普通调度器可领取 |
|---|---|---|
| `PENDING` | 等待首次投递或恢复后重投 | 是 |
| `DELIVERING` | 已领取且租约有效 | 否 |
| `RETRY_WAIT` | 等待下一次重试 | 到期后可以 |
| `SUCCEEDED` | 已收到配置认可的成功响应 | 否 |
| `DEAD` | 自动处理结束，等待人工判断 | 否 |

## 优先级设计

调用方不能提交 `priority`，优先级由服务端按 `event_type` 映射，并在创建任务时固化。

```text
wait = max(0, now - next_attempt_at)
promotion = floor(wait / aging_interval)
effective_priority = min(HIGH, original_priority + promotion)
```

排序顺序是：

1. 有效优先级降序；
2. 到期时间升序；
3. 创建时间升序。

重试任务从 `next_attempt_at` 开始计算等待时间，而不是从首次创建时间开始。这样既能让长期等待的 LOW 任务逐步晋升，也不会让刚结束退避的旧任务立即压过新的关键任务。

## 一致性与故障恢复

每次领取任务都会在同一个短事务中：

1. 把任务改为 `DELIVERING`；
2. 增加尝试次数；
3. 增加 `lease_epoch` 和 `version`；
4. 写入一条 `STARTED` Attempt。

Worker 完成时必须携带领取时看到的：

```text
notification_id + worker_id + lease_epoch + version
```

只要其中任何一个不匹配，写回就被拒绝。这相当于门锁已经换代，旧钥匙即使后来恢复运行，也无法修改当前任务。

如果进程在 HTTP 已发出、结果写回前崩溃，本地无法知道供应商是否执行成功。恢复器会把该 Attempt 标记为 `ABANDONED/LEASE_EXPIRED_UNKNOWN` 并重新投递。这是 At-least-once 的必然重复窗口，不应伪装成 Exactly-once。

## Provider 与网络安全

Provider URL 只能由服务端 YAML 配置，调用方只能提交 Provider ID。每个 Provider 拥有独立的：

- HTTP Transport 和连接池；
- 最大并发 Semaphore；
- 请求总超时与连接超时；
- 成功状态码；
- Body 和鉴权规则。

请求构造顺序保证静态 Header 不能覆盖系统保留字段。HTTP Client 禁用系统代理、禁止自动跟随重定向、要求 TLS 1.2+，并在每次连接时检查 DNS 解析结果，避免 DNS Rebinding 绕过启动检查。

凭证只从环境变量解析，不写入通知、Attempt、日志或查询响应；外部响应体也不会持久化。

## 配置

本地配置见 [configs/config.example.yaml](configs/config.example.yaml)，主要配置项如下：

| 配置 | 说明 | 示例值 |
|---|---|---|
| `database.path` | SQLite 文件路径，必须位于可靠的单机磁盘 | `./data/notifier.db` |
| `workers.concurrency` | 全局最大在途 Worker 数 | `10` |
| `scheduler.poll_interval` | 持久队列扫描间隔 | `500ms` |
| `scheduler.lease_duration` | Worker 对任务的临时所有权时长 | `30s` |
| `priority.aging_interval` | 等待多久提升一级有效优先级 | `30s` |
| `retry.initial_delay` | 首次重试基础间隔 | `5s` |
| `retry.max_delay` | 最大退避间隔 | `6h` |
| `retry.jitter_ratio` | 抖动比例 | `0.2` |
| `provider.max_concurrency` | 单 Provider 最大并发 | 按供应商配置 |

配置采用严格 YAML，未知字段会导致启动失败。生产环境必须启用静态 Token、使用 HTTPS，并禁止私网目标。关闭认证只允许监听 Loopback。

## 本地运行

依赖：

- Go 1.25 或更高版本；
- Python 3，仅验收和压测脚本需要；
- `curl`；
- 可选 Docker。

终端一，启动 Mock Provider：

```bash
go run ./cmd/mock-provider -address 127.0.0.1:18080
```

终端二，设置本地 Provider 凭证并启动服务：

```bash
export CRM_BEARER_TOKEN=local-crm-token
export INVENTORY_API_KEY=local-inventory-key
go run ./cmd/notifier -config configs/config.example.yaml
```

配置示例允许 HTTP 和私网地址，仅适用于本地开发。

### Docker Compose

```bash
docker compose up --build
```

Compose 只将服务绑定到宿主机 `127.0.0.1:8080`，SQLite 数据保存在命名卷中。`docker-compose.yml` 中的 Token 是本地示例值，不能用于生产。

## API 示例

### 创建通知

```bash
curl -X POST http://127.0.0.1:8080/v1/notifications \
  -H 'Authorization: Bearer local-producer-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotency_key": "order-20260927-1",
    "provider": "local",
    "event_type": "payment.succeeded",
    "payload": {
      "order_id": "20260927-1",
      "amount": 19900
    }
  }'
```

成功返回：

```json
{
  "notification_id": "ntf_01K...",
  "status": "PENDING",
  "duplicate": false,
  "status_url": "/v1/notifications/ntf_01K..."
}
```

### 查询通知

```bash
curl http://127.0.0.1:8080/v1/notifications/ntf_ID \
  -H 'Authorization: Bearer local-producer-token'
```

`producer` 只能查询自己的任务，`admin` 可以查询全部任务。

### Replay 死信

```bash
curl -X POST http://127.0.0.1:8080/v1/notifications/ntf_ID/replay \
  -H 'Authorization: Bearer local-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"reason":"供应商凭证已更新并验证"}'
```

Replay 只接受 `DEAD` 任务，必须填写原因。它保留原通知 ID、Payload、累计尝试次数和历史记录，并增加 `delivery_round`。

### 健康检查

```bash
curl http://127.0.0.1:8080/health/live
curl http://127.0.0.1:8080/health/ready
```

`live` 只表示进程可响应；`ready` 还要求服务未进入停机流程且 SQLite 可用。单个 Provider 故障不会让服务失去存活状态。

## 指标与告警

静态认证开启时，`GET /metrics` 需要管理员 Token：

```bash
curl http://127.0.0.1:8080/metrics \
  -H 'Authorization: Bearer local-admin-token'
```

指标只使用 Provider、路由、状态、结果和错误分类等低基数标签，不使用通知 ID、调用方 ID、URL 或原始错误文本。

API 5xx 失败率：

```promql
sum(rate(notifier_api_requests_total{status=~"5.."}[5m]))
/
clamp_min(sum(rate(notifier_api_requests_total[5m])), 1)
```

Provider 投递失败率：

```promql
sum by (provider) (
  rate(notifier_delivery_attempts_total{
    outcome=~"retryable_failure|permanent_failure|unknown"
  }[5m])
)
/
clamp_min(
  sum by (provider) (rate(notifier_delivery_attempts_total[5m])),
  1
)
```

告警应同时设置失败率、最小请求量和持续时间，避免低流量下单次失败造成噪声。

## 测试与验收

```bash
make test
make race
make vet
make build
make demo
make recovery-demo
make benchmark
```

也可以一次执行全部质量门：

```bash
make verify
```

验收脚本覆盖：

- `demo`：正常投递、幂等、优先级、LOW Aging、503 重试、400 死信、Replay、指标和 Provider 并发；
- `recovery-demo`：请求发出后 `SIGKILL`，使用同一 SQLite 重启，验证通知 ID 不变且 Epoch 增加；
- `benchmark`：500 请求、并发 20，要求全部返回 `202`、吞吐不低于 50 req/s、P95 不高于 200 ms。

性能数字是本地验收基线，不是生产 SLO，也不能替代目标机器、真实磁盘和真实 Provider 延迟下的容量测试。

## 技术取舍

### 为什么 MVP 使用 SQLite

SQLite WAL 足以验证事务提交、唯一约束、状态机、Lease 和 CAS，并且不引入额外服务。代价是单写者和单实例边界。出现多实例高可用、持续写锁竞争或在线扩容需求时，应迁移 PostgreSQL。

### 为什么没有引入 MQ

MQ 不会自动消除可靠性问题。“写数据库 + 发消息”会产生双写窗口；正确方案还需要 Outbox、Relay、发布确认、消费幂等以及两套重试状态协调。当前吞吐和单实例目标下，这些复杂度没有对应收益。

### 为什么没有采用通用任务框架

设计阶段评估过 Asynq、River、Backlite 和 Machinery。它们分别引入 Redis、PostgreSQL、额外状态模型或超出需求的工作流能力，同时仍不能直接替代本项目的 Provider 公平性、Aging、Replay 审计和 Epoch Fencing，因此 MVP 使用显式 SQLite 状态机。

## 其他说明

- SQLite 文件必须放在本地磁盘或具备可靠锁语义的单机持久卷，不能通过共享文件实现多实例；
- Provider 配置首版静态加载，修改后需要重启；
- 供应商成功但本地写回失败时，系统只重试本地写回，不会立即重复发送 HTTP；
- 如果写回最终仍失败，租约恢复可能再次投递，因此供应商幂等仍是必要条件；
- `delivery_attempts` 和 `replay_audits` 用于保留投递证据与人工操作审计；
- AI 辅助范围、采用和拒绝的建议、验证方法见 [AI_USAGE.md](AI_USAGE.md)。
