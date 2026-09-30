# kubernetes-controller-runtime-go

一套**本地可运行**的 Kubernetes 风格控制器运行时：用磁盘上的 JSON 快照 + POSIX
文件锁模拟集群状态（etcd/apiserver 的最小子集），在其之上实现了选主、租约、
fencing、乐观并发控制、去重工作队列与 finalizer 状态机。零第三方依赖，
`go test ./...` 即可在本地稳定、重复地验证全部收敛与故障切换场景。

## 目录结构

```
pkg/
  store/     本地对象存储：flock 跨进程互斥、版本号、fencing、finalizer、租约
  lease/     选主：获取/续租/退位、TTL 接管、epoch fencing token
  queue/     去重工作队列：同 key 串行、处理中合并、指数退避重试
  client/    版本化客户端：所有写携带 fence，finalizer 读-改-写辅助
  reconcile/ 控制器：选主 + 变更监听 + worker 调谐循环；finalizer 状态机
cmd/demo/    端到端演示（两个实例 + 创建/更新/删除 + 清理）
```

## 1. 调谐状态机

变更进入运行时后走一条**单线程归属**的流水线（每个 key 同一时刻只有一个
调谐在执行）：

```
                 ┌──────────────────────────────────────────────┐
 watchLoop ─────▶│              去重工作队列 Queue                │
 (List 比对 RV)  │  dirty（待处理） ──▶ ready ──▶ processing      │
                 │    ▲ 处理中再次 Add 只置 dirty                  │
                 │    └──────────── Done 后再投递恰好一次 ◀───────│
                 └───────────────┬──────────────────────────────┘
                                 ▼
                        worker: Reconcile(key)
                                 │
            ┌────────────────────┼─────────────────────────────┐
            ▼                    ▼                              ▼
       Get NotFound         Reconcile 成功                  返回 error
   （对象已删除/未创建）   Done；RequeueAfter>0        （非 fence/ctx 错误）
       丢弃，不再入队      则定时再调谐一次            Retry(item)：指数退避后重投
```

关键规则：

- **自动去重**：队列维护 `dirty` / `processing` 两个集合。key 在处理期间收到的
  任意次变更只把它重新标记为 dirty，本轮 `Done` 后**恰好再投递一次**，因此
  「上一轮处理结束前不会并发重复调谐同一对象」。测试中 30 次并发更新被折叠为
  个位数的调谐（见 `TestNoConcurrentReconcileSameObject`）。
- **多 worker**：不同 key 可并行（测试中 4 worker 达到 4 并发），同一 key
  永远串行（per-key max in-flight = 1）。
- **失败退避重试**：调和返回普通错误 → `Retry` 按 `initial*2^n`（封顶 max）
  延迟重投；可配置最大次数作为死信。成功或对象已 `NotFound` 后不再产生新调谐。
- **被 fence 拒绝不重试**：写返回 `ErrFence`（领导权已丢）时直接结束本轮，
  交给新 leader，避免旧主做无意义重试。

### Finalizer / 删除状态机

`reconcile.FinalizerReconciler` 实现与 Kubernetes 一致的删除协议：

```
Get 对象
  ├─ NotFound ............................ 什么都不做（已彻底消失）
  ├─ 存活且无 finalizer .................. AddFinalizer → Ensure(期望状态)
  ├─ 存活且有 finalizer .................. Ensure(期望状态)
  └─ 已打删除标记(DeletionTimestamp!=nil)
        └─ 含本 finalizer：
              Cleanup() 成功 → 重新 Get 最新对象 → RemoveFinalizer → Delete 清除
              Cleanup() 失败 → 返回错误，保留对象+finalizer，退避后重试
```

- 对象带 finalizer 时 `Delete` **不会真正删除**，只设置 `DeletionTimestamp`
  （tombstone）并返回哨兵错误 `ErrHasFinalizers`；对象仍可 Get。
- 清理失败时对象与 finalizer 都保留，因此**既不会卡死**（退避不断重试）
  **也不会误删**（finalizer 不清除就不能物理删除）。
- 清理成功后先重新读取（tombstone 已推进 RV），再移除 finalizer 并发起最终
  删除，避免用陈旧 RV 写入。

## 2. 租约、选主与防双写（fencing）

租约保存在同一份共享状态里（`state.json` 的 `lease` 字段）：

```json
{ "holder": "instance-B", "epoch": 2, "renewedAt": …, "expiresAt": … }
```

- **获取**：租约不存在或已过期才能获取；获取时 `epoch` 全局单调 +1。
- **续租**：只有 `holder + epoch` 同时匹配才能续租；按 `RenewPeriod` 周期续，
  每次把 `expiresAt = now + TTL`。
- **接管**：续租失败立即取消 leader 上下文、本实例停止一切写；其他实例在当前
  租约超过 `expiresAt`（TTL）后获取到**更大的 epoch** 并接管。主动让位会立刻
  `ReleaseLease`；父 context 被取消（崩溃式停止）则**不释放**，强制走 TTL
  接管，测试测得接管时间 < TTL（400ms TTL 下约 350ms）。
- **互斥**：存储层用 `sync.Mutex`（进程内）+ `flock(LOCK_EX)`（跨进程）双重
  串行化，任何时刻物理上只有一个写者在临界区；flock 获取带超时，永不无限自旋。

**Fencing token 防双写**：leader 的每次写都携带 `(holder, epoch)`。存储在
每次 Create/Update/Delete 时校验 token 必须等于当前有效租约，否则返回
`ErrFence`。于是一个「续租失败但自认为还是主」的旧实例，其 epoch 已小于新主，
所有写都会被拒绝——从机制上保证接管期间**不可能双写**。

## 3. 乐观并发控制（OCC）与冲突处理

每个对象有单调递增的字符串版本号 `resourceVersion`（每次成功写 +1）：

- `Update` 仅当提交对象的 RV 与当前存活 RV **完全相等**才接受，否则返回
  `ErrConflict`，且**不会覆盖**最新状态。
- 正确处理范式是「读取最新 → 基于最新判定 → 写入」，冲突后**重新 Get 最新
  状态再判定**，而不是拿旧对象重试。`FinalizerReconciler` 在内部对
  `ErrConflict` 做有界次数的重读重试（`MaxConflictRetries`，默认 5）。
- `StatusUpdate` 先重新读取存活对象再只改 status，避免 status 写覆盖并发的
  spec 变更。
- 冲突、fence、finalizer 三类失败用**可区分的哨兵错误**表达：
  `ErrConflict` / `ErrFence` / `ErrHasFinalizers` / `ErrNotFound`，测试据此
  断言失败原因。

## 4. 关键输入与「判定依据」可解释性

控制器在 INFO/DEBUG/WARN 级别打印每一步的输入与决策依据，例如：

- `change detected key=… oldRV=… newRV=… deleting=…`（为什么入队）
- `reconcile start key=… rv=… finalizers=[…] deleting=… fenceEpoch=…`（输入快照）
- `finalizer added` / `cleanup attempt … deleting=…` / `cleanup failed,
  keeping object for retry err=…`（finalizer 决策与失败原因）
- `conflict, re-reading latest state key=… rv=… attempt=…`（冲突重读依据）
- `became leader … epoch=…` / `write fenced, new leader will reconcile`（领导权）

单测通过 `slog → t.Log` 桥接（`testLogger`），这些关键输入与判定都会出现在
`go test -v` 输出里。

## 5. 本地验证

```bash
# 编译
go build ./...

# 全量单测（去重、退避、OCC 冲突、选主切换、防双写、finalizer 清理失败…）
go test ./...

# 带竞态检测并重复多次（CI 推荐）
go test -race -count=5 ./...

# 查看每个用例的关键输入与判定日志
go test -race -v ./...

# 端到端演示：两个实例共享 ./.demo-state，创建→收敛→删除→清理→清除
go run ./cmd/demo
# 观察结束后对象为 ErrNotFound；.demo-state/state.json 是共享状态快照
```

### 测试与需求的对应关系

| 需求场景 | 测试 |
| --- | --- |
| 并发变更自动去重、同对象不并发调谐 | `queue`: `TestReaddedWhileProcessingDeliveredOnce`；`reconcile`: `TestNoConcurrentReconcileSameObject`、`TestMultipleWorkersParallelDistinctKeysSerialSameKey` |
| 失败退避重试、成功/删除后不再调谐 | `queue`: `TestRetryBackoffThenSuccess`、`TestMaxAttemptsDeadLetter`；`reconcile`: `TestFailedReconcileBacksOffAndRetries`、`TestConvergesSpecToStatus` |
| 多实例竞争、租约续租、有限时间接管、无双写 | `store`: `TestFenceRejectedAfterLeaseExpiry`、`TestLeaseRenewAndRelease`；`lease`: `TestElectorRenewsThenAnotherTakesOverAfterStop`；`reconcile`: `TestLeaderFailoverNoDoubleWrite` |
| 版本号 OCC、冲突重读不覆盖 | `store`: `TestCreateGetUpdate_OptimisticConcurrency`；`client`: `TestFinalizerHelpers`；`reconcile`: `TestConflictTriggersRereadNotOverwrite` |
| finalizer 清理失败保留对象、可重试、不误删 | `store`: `TestFinalizerDeleteIsTombstoneThenPurge`；`reconcile`: `TestFinalizerCleanupFailureKeepsObjectAndRetries` |
| 失败原因可区分 | 哨兵错误 `ErrConflict/ErrFence/ErrHasFinalizers/ErrNotFound` 在上述用例中分别断言 |
| 状态持久化 | `store`: `TestDurableAcrossReopen` |

## 设计取舍

- 用文件快照 + flock 而非真 etcd/apiserver，保证零依赖、单仓可跑；fencing 与
  OCC 在接口层与 K8s 语义对齐，便于将来替换成真实 apiserver 客户端。
- 监听采用周期性 `List` 比对 RV（resync），不做长连接 watch，简化本地实现，
  同时天然覆盖 create/update/delete/tombstone 全部变化。
- 时间相关参数（TTL、续租、resync、退避）全部可在 `reconcile.Config` 配置，
  测试使用小的时间常数以快速且确定性地复现故障切换与退避。
