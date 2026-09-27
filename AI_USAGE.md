# AI 使用说明

本项目在需求分析、架构调研、实施规划、代码生成和验证阶段使用了 AI 辅助。可靠性契约和最终技术方案均经过人工审查确认后再进入实现。

## 关键决策

- 根据人工确认，将事件优先级提升为 P0 能力。
- 最终方案采用 SQLite WAL、进程内有界调度器、Provider 级并发限制、Lease Epoch、Version CAS、稳定通知 ID 和 At-least-once 投递语义。
- 通知优先级由服务端配置决定。通过 Aging 提升长期等待任务的有效优先级，避免低优任务永久饥饿。
- 外部 HTTP 调用始终在数据库事务之外执行，避免慢请求长期占用 SQLite 写锁。
- 外部系统的副作用无法由本地事务回滚，因此不承诺 Exactly-once；供应商需要使用稳定的 `notification_id` 实现幂等。

## AI 参与范围

AI 参与了以下工作：

- 将原始需求整理为结构化 PRD；
- 分析可靠投递、崩溃恢复、幂等和优先级需求；
- 比较 SQLite、PostgreSQL、消息队列和通用任务框架；
- 编写技术方案和分阶段实施计划；
- 按测试驱动方式生成实现与测试；
- 编写 Mock Provider、故障恢复脚本和性能测试；
- 整理 README、配置示例、容器化文件和仓库协作说明；
- 执行测试、Race Test、静态检查、构建和端到端验证。

AI 不负责最终产品决策。以下内容由人工明确确认：

- 采用方案 A 展开技术设计；
- 事件优先级属于本次 MVP 的必要能力；
- 技术方案确认后才开始编码；
- README 和项目说明使用中文。

## 框架调研与取舍

设计阶段评估了 Asynq、River、Backlite 和 Machinery 等现有任务框架，但 MVP 未直接采用。

### Asynq

Asynq 提供 Redis 持久队列、自动重试、任务优先级和 Web UI。

未采用原因：

- 引入 Redis，扩大部署和运维边界；
- 业务任务状态、Replay 审计及本地 fencing 仍需单独实现；
- 双状态源会增加一致性推理成本。

适用时机：系统已经依赖 Redis，并需要多机 Worker 和成熟队列运维能力。

### River

River 提供基于 PostgreSQL 的任务、唯一性、重试、优先级和多实例能力。

未采用原因：

- PostgreSQL 是其主要运行目标；
- 当前 MVP 明确采用单实例 SQLite；
- 严格优先级仍需额外解决低优任务饥饿问题。

适用时机：系统迁移到 PostgreSQL，并需要多实例调度时。

### Backlite

Backlite 与本项目同样采用嵌入式 SQLite，并支持持久化任务、延迟执行和重试。

未采用原因：

- 公开能力不能直接覆盖本项目的 Aging；
- Provider 公平性和两级并发限制仍需定制；
- Epoch Fencing、Attempt 证据和 Replay 审计仍需自行实现。

适用时机：任务模型较通用，不需要本项目定制状态机时。

### Machinery

Machinery 支持多种 Broker、结果后端和工作流编排。

未采用原因：

- Broker 和工作流能力超过当前需求；
- 增加额外基础设施和状态协调；
- 不能替代通知任务自身的幂等、审计和 Provider 语义。

适用时机：系统确实需要分布式任务编排和已有 Broker 基础设施时。

## 最终方案为何采用自有轻量调度

本项目需要的核心不是通用 DAG 或 Cron，而是：

```text
按时间发现持久任务
-> 按有效优先级选择
-> 预留全局与 Provider 容量
-> 原子 Claim 并记录 Attempt
-> 在事务外调用 HTTP
-> 使用 Fencing Token 写回
```

这些逻辑与业务状态机高度相关。一个可测试的 SQLite 查询和显式事务比引入第二套任务状态更容易证明正确性。

该选择仅适用于当前单实例 MVP，不代表自研队列普遍优于成熟框架。当系统需要多实例、高可用或独立扩缩容时，应重新评估 PostgreSQL 队列或 Outbox + MQ。

## AI 建议的处理结果

### 已采纳

- 使用 SQLite WAL 作为单实例事实源；
- 使用 `caller_id + idempotency_key` 唯一约束；
- 对规范化后的有效请求内容计算版本化摘要；
- 使用 Lease Epoch 与 Version CAS 防止旧 Worker 写回；
- 每次真实 HTTP 调用前持久化 `STARTED` Attempt；
- 使用稳定通知 ID 支持外部幂等；
- 使用服务端优先级映射和 Aging；
- 使用全局与 Provider 两级并发许可；
- 使用严格 YAML、环境变量密钥和连接时 IP 校验；
- 使用 Prometheus 低基数指标；
- 使用 Mock Provider 和进程级故障脚本验证恢复能力。

### 未采纳

- **直接承诺 Exactly-once**：外部 HTTP 副作用无法与本地 SQLite 组成原子事务。
- **写数据库后直接发 MQ**：存在数据库与 MQ 双写不一致窗口。
- **首版引入完整 MQ**：需要额外实现 Outbox、Relay、发布确认和消费幂等。
- **使用内存队列作为事实源**：进程崩溃会丢失已受理任务。
- **在数据库事务内调用 Provider**：慢请求会长时间占用写锁。
- **调用方自行提交优先级**：容易被滥用，破坏关键事件调度。
- **严格优先队列**：持续 HIGH 流量可能使 LOW 永久饥饿。
- **通用模板或脚本引擎**：扩大执行安全和版本治理风险。
- **自动跟随重定向**：可能绕过目标地址限制并造成 SSRF。

## 实施方法

开发按照小步 Git 提交和 RED -> GREEN -> REFACTOR 执行：

1. 先编写能描述行为和边界的失败测试；
2. 运行聚焦测试，确认失败原因来自缺失能力；
3. 实现满足测试的最小生产代码；
4. 执行完整包测试和 Race Test；
5. 每个阶段独立提交；
6. 最后执行端到端、崩溃恢复和性能验收。

测试覆盖包括：

- 严格配置解析和密钥注入；
- 持久化后返回 `202`；
- 并发幂等创建和内容冲突；
- 规范化请求摘要和调用方隔离；
- Provider Header、鉴权和 Body 契约；
- 重定向阻断和 SSRF 防护；
- HTTP 状态、DNS、连接和超时错误分类；
- 指数退避、抖动和 `Retry-After`；
- 优先级、Aging 和防饥饿；
- Claim 与 Attempt 原子创建；
- 过期租约恢复和旧 Worker 写回拒绝；
- 自动重试、死信和 Replay；
- 低基数指标和优雅停机；
- 正常流程、强杀恢复和入口性能。

## 验证方式

最终质量门命令：

```bash
gofmt -w $(find cmd internal -type f -name '*.go')
env -u GOROOT go test ./...
env -u GOROOT go test -race ./...
env -u GOROOT go vet ./...
env -u GOROOT go build ./cmd/...
make demo
make recovery-demo
make benchmark
```

已完成的验证包括：

- 单元测试和 SQLite 集成测试通过；
- Race Test 通过；
- `go vet` 通过；
- 两个命令行程序构建通过；
- 正常投递 Demo 通过；
- `SIGKILL` 崩溃恢复 Demo 通过；
- 500 请求、并发 20 的本地基准通过；
- 最近一次完整验证结果约为 `3758 req/s`、P95 `13.63 ms`。

上述性能数据只代表本地验收环境，不是生产 SLO。

## AI 内容验证原则

- AI 生成的结论不能替代可执行测试；
- 每项可靠性声明都应对应单元测试、集成测试或故障演示；
- 所有数据库状态转换必须通过真实 SQLite 验证；
- 并发相关修改必须通过 Race Test；
- 安全相关修改必须检查敏感信息、目标地址和 Header 所有权；
- 无法执行的验证必须明确记录原因，不能描述为已经通过。

本项目的详细需求、设计和实施记录分别见：

- [产品需求文档](docs/PRD.md)
- [技术方案](docs/TECHNICAL_DESIGN.md)
- [实施计划](docs/IMPLEMENTATION_PLAN.md)
- [项目运行说明](README.md)
