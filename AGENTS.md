# AGENTS.md

本文件适用于整个仓库。任何自动化编码代理在修改代码前都必须先阅读本文件。

## 项目目标

本项目是一个单进程 Reliable HTTP Notifier：

- 接收内部通知请求；
- 在返回 `202 Accepted` 前持久化任务；
- 按服务端优先级异步调用外部 Provider；
- 通过有限重试、死信和 Replay 处理失败；
- 通过 Lease、Epoch 和 Version CAS 处理崩溃恢复；
- 提供查询、认证、健康检查、指标及可执行验收脚本。

核心语义是 **At-least-once**，不是 Exactly-once。外部副作用无法由 SQLite 事务回滚，Provider 必须使用稳定 `notification_id` 做幂等。

## 文档优先级

遇到需求或实现歧义时，按以下顺序判断：

1. `docs/PRD.md`：产品范围、P0/P1/P2 和验收标准；
2. `docs/TECHNICAL_DESIGN.md`：架构、不变量、事务和故障语义；
3. `docs/IMPLEMENTATION_PLAN.md`：任务拆分和验证步骤；
4. `README.md`：运行手册和设计摘要；
5. 当前代码与测试：已经落地的接口行为。

如果文档之间冲突，不要静默选择。先确认冲突是否来自实现演进；无法从 Git 历史判断时，再向用户说明。

## 开发环境

- Go 版本：`1.25+`
- 主包：`cmd/notifier`
- Mock Provider：`cmd/mock-provider`
- 核心包：`internal/notifier`
- 持久化：SQLite，驱动为 `modernc.org/sqlite`
- 配置：严格 YAML
- 指标：Prometheus Client

本机 shell 的 `GOROOT` 可能指向错误版本。执行 Go 命令时统一使用：

```bash
env -u GOROOT go test ./...
env -u GOROOT go test -race ./...
env -u GOROOT go vet ./...
env -u GOROOT go build ./cmd/...
```

也可以使用：

```bash
make test
make race
make vet
make build
make verify
```

## 修改流程

功能开发和 Bug 修复遵循 RED -> GREEN -> REFACTOR：

1. 先写能表达行为和边界的失败测试；
2. 运行最小范围测试并确认失败原因正确；
3. 实现最小改动；
4. 运行聚焦测试；
5. 运行受影响包的完整测试；
6. 最后运行 race、vet 和 build；
7. 修改运行链路时执行对应的 Demo。

不要为了让测试通过而削弱断言、跳过测试或扩大重试时间掩盖竞态。

## 质量门禁

普通 Go 代码变更至少执行：

```bash
gofmt -w $(find cmd internal -type f -name '*.go')
env -u GOROOT go test ./...
env -u GOROOT go test -race ./...
env -u GOROOT go vet ./...
env -u GOROOT go build ./cmd/...
```

按变更范围追加：

| 变更范围 | 额外验证 |
|---|---|
| API、Service、调度、Provider | `make demo` |
| Lease、恢复、SQLite 状态机 | `make recovery-demo` |
| 接入链路或数据库写入性能 | `make benchmark` |
| Docker 或 Compose | `docker build .`、`docker compose config` |
| Shell/Python 脚本 | `bash -n scripts/*.sh`，Python 编译检查 |

`make verify` 是最终完整门禁。Benchmark 基线为吞吐不低于 `50 req/s`、P95 不高于 `200 ms`，且请求全部返回 `202`。

如果受环境限制无法执行某项检查，必须在最终说明中明确写出命令和原因。

## 目录职责

| 路径 | 职责 |
|---|---|
| `internal/notifier/model.go` | 状态、优先级、错误类别和跨模块数据结构 |
| `internal/notifier/config.go` | YAML 解析、密钥注入和启动校验 |
| `internal/notifier/store.go` | Schema、事务、查询、Claim、CAS、Recovery、Replay |
| `internal/notifier/service.go` | 创建、查询、Replay 用例编排 |
| `internal/notifier/auth.go` | Token 认证和角色映射 |
| `internal/notifier/httpapi.go` | HTTP 协议、路由、输入限制和错误映射 |
| `internal/notifier/provider.go` | Provider Registry、请求构造、连接安全和并发许可 |
| `internal/notifier/delivery.go` | 响应分类、错误分类和重试时间计算 |
| `internal/notifier/scheduler.go` | 到期扫描、公平调度、Worker 生命周期 |
| `internal/notifier/metrics.go` | 低基数 Prometheus 指标 |
| `cmd/notifier` | 依赖装配、监听、信号和关闭顺序 |
| `cmd/mock-provider` | 确定性验收服务 |
| `scripts` | 端到端、崩溃恢复和性能验收 |

保持这些边界。不要把 SQL 放进 HTTP Handler，不要让 Store 构造供应商请求，也不要让 Provider 直接修改任务状态。

## 必须保持的正确性不变量

1. 只有 `CreateOrGet` 事务提交成功后才能返回 `202`。
2. 同一 `caller_id + idempotency_key` 只能对应一个逻辑任务。
3. 相同幂等键但有效内容不同必须返回冲突。
4. 只有到期的 `PENDING` 或 `RETRY_WAIT` 任务可被普通调度领取。
5. Claim 与 `STARTED` Attempt 必须在同一事务提交。
6. 外部 HTTP 调用不得发生在数据库事务中。
7. `DELIVERING` 必须同时具有 owner、epoch 和 deadline。
8. 完成写回必须校验通知 ID、owner、epoch 和 version。
9. `SUCCEEDED` 与 `DEAD` 不得被普通调度器再次领取。
10. 每次真实 HTTP 调用必须对应一条持久化 Attempt。
11. 过期租约只能恢复或转死信，不能删除任务。
12. 凭证、完整响应体和敏感 Payload 不得进入日志、指标或 Attempt。
13. 在途请求不能超过全局和 Provider 并发上限。
14. 优先级只能由服务端映射，调用方不能自行指定。
15. Aging 必须从当前 `next_attempt_at` 开始，不能从最初创建时间计算。

修改状态机或 SQL 时，先证明这些不变量仍成立。

## SQLite 与事务规则

- SQLite 是任务事实源，内存队列和 Wake Signal 不能成为可靠性依据。
- 保持 WAL、`synchronous=FULL`、外键和 busy timeout。
- 当前部署模型是单实例、单持久卷；不要通过共享 SQLite 文件扩展多实例。
- 事务必须短小，禁止在事务中执行网络调用、等待锁或 sleep。
- 竞争更新必须检查 `RowsAffected()`。
- 通知状态和对应 Attempt 的完成必须在同一事务中更新。
- 不要提供任意 `UpdateStatus`；状态转换应通过语义化 Store 方法完成。
- 已发布 Schema 不应被静默重写。需要变更时增加显式 migration 和版本兼容测试。
- 查询必须使用参数绑定，不拼接用户输入。

## 调度与并发规则

- 顺序必须是：预留全局许可 -> 预留 Provider 许可 -> Claim -> 立即执行。
- Claim 失败时立即释放所有许可。
- Worker 必须使用 `defer` 释放许可。
- 不要先批量 Claim 再排入内存队列，这会让 Lease 在排队时消耗。
- Provider 公平性只在当前最高有效优先级层内轮转。
- Aging 只改变调度时的有效优先级，不能修改数据库中的原始优先级。
- Retry、恢复和 Replay 都必须受最大尝试次数及状态机约束。
- Provider 已返回后，本地完成写回可重试，但不得因此再次发送 HTTP。

## HTTP 与安全规则

- 调用方只能提交 Provider ID，不能提交目标 URL。
- 使用 `http.MaxBytesReader`、严格 JSON 字段检查和单文档校验。
- 认证 Token 使用常量时间比较。
- 生产配置必须启用认证、HTTPS，并禁止私网和环回目标。
- HTTP Client 必须设置连接超时、请求超时、响应读取上限。
- 禁止使用环境代理和自动重定向。
- DNS 解析后的实际 IP 必须在连接时再次检查，不能只做启动时字符串校验。
- 保留 Header 不能被 Provider 静态 Header 或幂等 Header 覆盖。
- Bearer Token 和 API Key 只从环境变量解析。
- 错误响应使用稳定错误码，不返回内部错误、堆栈或凭证。
- 普通调用方只能查询自己的任务；只有管理员可以 Replay。

## 投递与重试规则

默认分类：

| 结果 | 分类 | 是否重试 |
|---|---|---|
| 配置认可的 2xx | `NONE` | 否 |
| DNS/连接失败 | `DNS_ERROR` / `CONNECT_ERROR` | 是 |
| 请求发出后的传输失败 | `TRANSPORT_UNKNOWN` | 是 |
| 超时 | `TIMEOUT_UNKNOWN` | 是 |
| 3xx | `UPSTREAM_REDIRECT` | 否 |
| 408/425/429 | 对应稳定分类 | 是 |
| 5xx | `UPSTREAM_5XX` | 是 |
| 401/403 | `AUTH_REJECTED` | 否 |
| 其他 4xx | `UPSTREAM_4XX` | 否 |

重试使用有上限的指数退避和抖动。`Retry-After` 只影响可重试响应，并受最小、最大退避时间限制。

不要把原始网络错误或响应 Body 直接写入 `SafeMessage`。

## 测试约定

- 测试文件与实现放在同一个 `notifier` package，以便验证内部不变量。
- SQLite 测试使用 `t.TempDir()` 和真实数据库，不使用字符串模拟 SQL 行为。
- HTTP 测试使用 `httptest.Server`。
- 时间敏感逻辑通过 `Clock` 或显式时间参数测试，避免无必要的真实 sleep。
- 并发测试必须可在 `go test -race` 下通过。
- 新增错误分类、状态或指标标签时，补齐分类测试和低基数检查。
- 修复崩溃恢复问题时，至少覆盖旧 Worker 写回被拒绝。
- 不在单元测试中依赖公网、真实 Provider 或用户机器上的固定数据。

## 可观测性规则

- Prometheus 标签必须保持低基数。
- 禁止使用 `notification_id`、`caller_id`、URL、原始错误文本作为标签。
- 接口失败率和投递失败率通过 Counter 的时间窗口计算，不新增易失真的瞬时失败率 Gauge。
- 数据库派生 Gauge 在后台刷新，不能在每次 `/metrics` 请求时扫描数据库。
- 日志使用 `slog` 结构化字段，不记录 Token、Cookie、API Key、完整 Payload 或完整响应体。

## 配置与秘密

- YAML 使用 `KnownFields(true)`，未知字段必须报错。
- YAML 中只保存环境变量名，不保存生产密钥值。
- `configs/config.example.yaml` 和 `configs/config.docker.yaml` 是本地示例。
- `docker-compose.yml` 中的 Token 仅供本地演示，不能复制到生产环境。
- 新增配置时必须同时更新结构体、校验、示例配置、README 和测试。

## 变更纪律

- 优先沿用现有语义方法和模块边界，不引入通用任务框架或第二套状态源。
- 不进行与当前任务无关的重构。
- 不删除或覆盖不属于当前修改的工作树变更。
- 不提交数据库文件、WAL、日志、构建产物或真实凭证。
- 保持提交范围单一，提交信息使用现有风格，例如：

```text
feat: add ...
fix: prevent ...
test: cover ...
docs: explain ...
```

- 修改行为时同步更新相关文档；只修改文档时无需重复运行耗时的恢复和压测脚本。

## 完成标准

任务完成前确认：

1. 行为与 PRD、技术方案一致；
2. 关键不变量没有被削弱；
3. 新行为有自动化测试；
4. 聚焦测试和必要质量门通过；
5. 文档、配置和示例保持同步；
6. `git diff --check` 通过；
7. 工作树中没有意外生成物或敏感信息。
