# kubernetes-controller-runtime-go（本地版）

零外部依赖（仅 Go 标准库）的本地 Kubernetes 风格控制器运行时：用进程内对象
存储模拟集群状态，支持变更去重调谐、多实例租约选主、版本号乐观并发与
finalizer 删除清理。详细设计见 [`docs/design.md`](docs/design.md)。

## 能力一览

- **去重调谐**：同一对象在上一轮处理结束前不会被并发重复调谐；处理期间的
  多次变更合并为一次后续处理；失败按指数退避重试，成功或对象删除后不再产生
  新调谐。
- **多实例选主**：多个实例共享同一份本地状态，任一时刻最多一个实例写入；
  租约续期保活，实例干净停止时立即释放，意外停止后在
  `LeaseDuration + RetryPeriod` 内被接管，接管期无双写。
- **乐观并发**：对象写入基于 resourceVersion，冲突拒绝并依据最新状态重新判定，
  绝不覆盖。
- **finalizer**：清理未完成对象不被真正删除；清理失败保留对象并退避重试，
  可恢复、不误删、不卡死。

## 快速开始

```bash
go version            # 需要 Go 1.25+

go test ./...         # 全部测试
go test ./... -race -count=10   # 竞态检测 + 重复稳定性
go run ./cmd/demo     # 两实例选主/切换/清理失败恢复演示
```

## 目录

```
pkg/api             核心类型（Object/Key/Event/Result）
pkg/storage         内存对象存储：rv、OCC、finalizer、list/watch
pkg/workqueue       去重限速队列（串行 + 指数退避）
pkg/leaderelection  Lease 租约选主（acquire/renew/release/expire）
pkg/reconciler      Widget 示例资源、外部系统模拟、调谐状态机
pkg/controller      watch→队列→worker 编排与 leader 门控
pkg/runtime         组装单个运行时实例
cmd/demo            可运行演示
docs/design.md      调谐状态机 / 租约 / 冲突规则 / 验证方法
```
