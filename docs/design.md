# 本地 Kubernetes 风格控制器运行时 — 设计说明

一个零外部依赖（仅标准库）的、可在本地稳定运行的控制器运行时：用一个进程内
的「本地对象存储」模拟 Kubernetes API server，多个运行时实例共享同一份状态，
通过租约竞争唯一写入权；资源变更进入去重队列，由调谐器按声明式方式收敛。

## 1. 组件总览

```
                ┌──────────────────────── shared memoryStore ───────────────────────┐
                │  objects map + 全局 resourceVersion + watch 扇出 + Lease(kind=Lease) │
                └───────▲───────────────────▲───────────────────────────▲────────────┘
                        │ Get/List          │ Create/Update(带 rv OCC)  │ Lease Create/Update
   ┌────────────────────┴───────┐  ┌────────┴────────────────┐  ┌───────┴──────────────┐
   │ Instance A (可能是 leader)  │  │ Instance B (standby)     │  │ ExternalSystem 模拟   │
   │ watcher → 去重队列 → worker │  │ watcher(抑制入队)         │  │ (widget 外部资源)      │
   │        → WidgetReconciler   │  │ workers 不写              │  └──────────────────────┘
   │        → leader election    │  │ leader election          │
   └─────────────────────────────┘  └──────────────────────────┘
```

包结构：

| 包 | 职责 |
|---|---|
| `pkg/api` | `Object`/`ObjectMeta`/`Key`/`Event`/`Result` 等核心类型 |
| `pkg/storage` | 内存对象存储：版本号、乐观并发、finalizer、list/watch |
| `pkg/workqueue` | 去重 + 每 key 串行 + 指数退避限速队列 |
| `pkg/leaderelection` | 基于存储 Lease 的租约选主（acquire/renew/release） |
| `pkg/reconciler` | `Widget` 示例资源、外部系统模拟、finalizer 调谐逻辑 |
| `pkg/controller` | watch→队列→worker 编排，leader 门控，失败语义 |
| `pkg/runtime` | 把选主 + 控制器组装成一个可运行实例 |
| `cmd/demo` | 两实例演示：选主、切换、清理失败恢复 |

## 2. 调谐状态机

每个对象 key 是一个独立状态机，驱动者是「去重队列 + worker」。

```
                       watch 事件(ADDED/UPDATED/DELETED)
                                   │
                                   ▼
                          ┌─────────────────┐
              ┌──────────▶│      Add(key)    │◀───────────┐
              │           └─────────────────┘             │
              │                     │ Get                 │
              │                     ▼                     │
   处理中再次 Add            ┌─────────────────┐    Done 后发现 dirty
   只标记 dirty（去重）       │   in-flight 处理  │ ─────────▶ 恰好再排一次
              │           └─────────────────┘
              │                     │ Reconcile(ctx,key)
              │       ┌─────────────┼───────────────────────────┐
              │       ▼             ▼             ▼             ▼
              │   成功          对象已删除      OCC 冲突        失败/Requeue
              │   Forget        Forget       等 watch 最新版    AddRateLimited
              │   (无新调谐)    (无新调谐)    短延迟安全重排     (指数退避重试)
              └──────────────────────────────────────────────────┘
```

**去重与串行规则**（`pkg/workqueue`）：

- 同一 key 在队列中只保留一份；处理期间（in-flight）再次 `Add` 只置 `dirty`，
  绝不产生第二个并发处理；`Done` 时把处理期间的所有变更**合并成恰好一次**后续处理。
- `Get` 返回的 key 在 `Done` 前属于 in-flight，因此多 worker 下同一 key 最多
  只有一个 worker 在执行（集成测试用 `maxConcurrency == 1` 断言）。
- 成功或对象已删除后调用 `Forget`，在没有新的 watch 事件前不会再产生调谐。
- 失败按 key 维度指数退避：`baseDelay * 2^(attempt-1)`，封顶 `maxDelay`；
  `MaxAttempts>0` 时超预算不再新增排队（默认 0 = 无限重试，避免卡死）。

### 2.1 对象级状态机（Widget + finalizer）

`WidgetReconciler.Reconcile` 每次都**先按 key 重新 Get 最新对象**，再判定：

```
 Get(key)
   ├─ NotFound ───────────────▶ 返回 ObjectNotFound → Forget（已物理删除/不存在）
   ├─ DeletionTimestamp == nil（存活）
   │     1. ext.Ensure(name)           // 创建外部资源（幂等）
   │     2. 若缺少 finalizer：基于刚读到的 rv Update 附加 finalizer
   │        - 冲突(Conflict) → 不覆盖，等携带最新 rv 的 watch 事件重新判定
   │     3. 成功 → 结束
   └─ DeletionTimestamp != nil（删除中）
         1. ext.Cleanup(name)
              - 失败：保留对象与 finalizer，返回 Requeue + ErrCleanupFailed
                      → 队列退避重试（不死等、不误删）
         2. 清理成功：基于刚读到的 rv Update 移除 finalizer
              - 存储语义：带 deletionTimestamp 且 finalizer 清空的 Update
                会被存储物理删除并发 DELETED 事件
```

关键保证：

- **清理未完成绝不真正删除**：有 finalizer 的对象 `Delete` 只会设置
  `deletionTimestamp`；最后一个 finalizer 被移除的那一刻才物理删除。
- **清理失败可再次尝试且可区分**：返回可区分的失败原因
  `ExternalCleanupRetrying` 与哨兵错误 `ErrCleanupFailed`，对象原样保留，
  退避重试；外部故障解除后下一轮收敛。

## 3. 租约与单写入者规则（`pkg/leaderelection`）

租约本身就是存储里的一个普通对象（`kind=Lease`），复用同一套 OCC。

- **获取（acquire）**：Lease 不存在则 `Create`（多实例同时抢只有一个成功）；
  存在但持有者是别人且 `renewTime + leaseDuration > now` 则等待；已过期则
  基于当前 rv `Update` 抢占（冲突者放弃，下轮重试）。
- **续租（renew）**：leader 每 `retryPeriod` 用**最新 rv** 更新 `renewTime`。
  - 若发现持有者已不是自己（fencing）或 rv 已变 → 立刻判定失主，
    **先 cancel leader 工作上下文（停止一切写），再发出失主回调**。
  - 续租超过 `renewDeadline` 未成功 → 失主，不主动释放，等待租约按
    `leaseDuration` 自然过期（与 Kubernetes 行为一致）。
- **干净退出**：实例 ctx 取消时，若自己仍是持有者则把租约「立即置过期」
  （OCC 保证只有真正持有者能释放），standby 因而可在**一个 retryPeriod 内**
  接管，无需等满 leaseDuration。
- **接管窗口**：被动故障（进程死掉/续租失败）的接管上界约为
  `leaseDuration + retryPeriod`；测试用例分别覆盖了「干净退出立即接管」和
  「租约过期接管」。
- **无双写**：
  1. 同一时刻存储层保证 Lease 只有一个持有者；
  2. 非 leader 的 watcher 抑制事件入队、worker 在调谐前检查 `IsLeader()`，
     standby 绝不调用 Reconciler，因此不产生任何业务写；
  3. leader 在新实例上任时先 `Relist()` 全量补队（因为 standby 期间它抑制了
     watch 入队），随后继续消费增量 watch 事件；
  4. 即便出现极端网络/调度窗口，所有写都带 rv，旧 leader 的过期写会被 OCC
     拒绝，且它检测到被接管后先停写。

> 说明：本运行时为进程内模型，「无外部依赖」意味着不提供跨进程的真实
> fencing token 存储；双写防护由「单持有者 + 写入前 leader 检查 + rv OCC」
> 三层共同保证。

## 4. 冲突处理规则（乐观并发 OCC）

- 存储中每次 `Create/Update/Delete` 都让全局 `resourceVersion` 单调递增；
  对象携带创建/更新时分配的 rv。
- `Update(obj)` 仅当 `obj.resourceVersion == 存储当前 rv` 才生效，否则返回
  `*ConflictError{Wanted, Current}`，**绝不覆盖**。
- 调谐器拿到冲突后不做盲写重试，而是返回 `OptimisticConflictRelist`，依赖
  「获胜版本」对应的 watch UPDAED 事件携带最新 rv 重新入队、重新读、重新
  判定；另加一个极短延迟安全重排兜底事件顺序边界。
- Lease 的竞争同理：所有抢占/续租都带 rv，从机制上排除「两个持有者都写成功」。

## 5. list/watch 一致性

`Subscribe(kind)` 在**同一把写锁**内返回「当前快照 + 事件通道」，所有变更也
都在持锁状态下发布事件，因此：

- 不会收到比快照更旧的事件，也不会漏掉快照之后的新事件（无 list/watch 缝隙）；
- 通道带缓冲，慢消费者产生背压而不是丢事件。

## 6. 可解释性

- 每个关键判定都有结构化日志（`slog`）：watch 输入（type/key/rv/是否
  leader）、调谐输入（key/并发度）、调谐判定（reason/requeue/err）、选主
  （acquired rv / lost reason）。
- 失败原因是可程序化区分的字符串常量（`pkg/reconciler/reconcile.go`）与
  哨兵错误 `ErrCleanupFailed`，测试据此断言不同失败路径。

## 7. 本地验证方法

要求 Go 1.25+，无任何外部依赖（不连真实集群、不启 etcd）。

```bash
# 1. 全部单元 + 集成测试（默认）
go test ./...

# 2. 带竞态检测，重复多次验证稳定性
go test ./... -race -count=10

# 3. 只看某一层
go test ./pkg/workqueue/   -race -v   # 去重/串行/退避
go test ./pkg/storage/     -race -v   # rv/OCC/finalizer/watch
go test ./pkg/leaderelection/ -race -v # 单 leader/干净切换/过期接管
go test ./pkg/runtime/     -race -v   # 端到端：并发变更、切换、清理失败、冲突

# 4. 运行演示（两实例竞争 + 故障切换 + finalizer 清理失败恢复）
go run ./cmd/demo
```

测试场景与需求对应：

| 需求 | 测试 |
|---|---|
| 并发变更去重、同 key 不并发调谐 | `workqueue.TestNoConcurrentSameKey`、`runtime.TestConcurrentChangesConverge` |
| 失败退避重试、成功/删除后不再调谐 | `workqueue.TestExponentialBackoff/MaxAttempts`、`reconciler.TestDeleteCleanupFailingKeepsObjectAndRetries` |
| 多实例竞争、单写入、切换无双写 | `leaderelection.Test*`、`runtime.TestInstanceFailoverReconciles` |
| rv 乐观并发、冲突重读不覆盖 | `storage.TestCreateUpdateConflictDelete`、`runtime.TestVersionConflictDoesNotOverwrite` |
| finalizer 清理失败保留对象、可重试、不误删 | `reconciler` 两个删除用例、`runtime.TestCleanupFailureThenRecovery` |
| 可本地稳定重复运行 | 全部用例无随机抖动（退避无 jitter）、`-race -count=10` 通过 |
