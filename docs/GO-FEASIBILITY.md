# Go 可行性评估：Cordis 内核能否用 Go 复刻

> **结论先行**：**能**。Go 可以复刻 Rust 内核的**可观察契约**——作用域服务、严格/宽松双查询、
> inject 门、版本触发的重激活、逆序单次清理、六态生命周期、`LifecycleBusy`、级联卸载、双 lane
> 事件、waterfall、panic 遏制、quiescence——但**不能逐行等价**：
> Rust 的 `Drop` 确定性、`Weak` 存活语义、`thread_local` 执行帧、future drop 取消、`TypeId`/
> 类型系统这五处需要重新定义或替换；其中 **Go 1.27 的泛型方法**消除了 API 形态差异
> （`ctx.Get[T](key)` 直接可用），仅剩 `reflect.Type` 身份与 lane 合并两处语义差。
> `CORE-033/045/046/049` 等用例需要改写断言或标记 N/A。
>
> 本文逐项评估语言机制、CORE 可复现性、性能与风险，作为 [GO-DESIGN.md](GO-DESIGN.md) 的决策依据；
> Rust 侧背景见 [ARCHITECTURE-ANALYSIS.md](ARCHITECTURE-ANALYSIS.md)，逐用例见
> [05-core-cases](analysis/05-core-cases.md)。
>
> **快照**：2026-11-06 14:30，Go 工作区最终状态（Go 1.27.3）。当前已落地内核主体、
> `conformance/`（154 个测试，覆盖 51/51 CORE）+ 根包 56 个 smoke/edge/API 测试、
> `internal/archtest`、`examples/` 与 CI，`go test -race ./...` 全绿。
> 行号不用于 Go（会漂移），一律引用文件与符号；Rust 行号相对 `reference/zeokeel-cordis-rust/`。

---

## 目录

[0 结论](#0-结论) · [1 评估框架](#1-评估框架) · [2 机制映射](#2-rust--go-语言机制逐项映射) ·
[3 CORE 可复现性](#3-core-用例可复现性评估) · [4 性能](#4-性能评估) · [5 风险](#5-风险登记) ·
[6 最终结论](#6-最终结论)

---

## 0. 结论

| 维度 | 结论 | 置信度 | 依据 |
|---|---|---|---|
| 服务/作用域/配置/元数据/accessor | **可 1:1（语义）** | 高 | `(scope,name)` 键、最近 scope、无回退、accessor 优先均直接映射 |
| effect 时序/撤销/panic | **可 1:1（语义）** | 高 | 单有序列表、逆注册序、单次、首个 panic 重抛都可直接实现 |
| 六态生命周期/级联/子所有权 | **可 1:1（顺序）**，实现结构不同 | 高 | 状态机、withdraw→drain、ownership effect 直接映射；注入链两阶段迭代，ownership 递归 |
| inject 门/版本/epoch/refresh | **可 1:1（语义）** | 高 | 严格 provider Active、dep version、XT-02 回滚均已在 v0 实现 |
| 生命周期互斥/重入 | **部分重定义** | 中 | operation token 可做，但“同线程”改为“同 goroutine”，跨 goroutine 迁移不保证 |
| 异步/取消 | **重新定义边界** | 中 | future drop→`ctx` 取消；in-flight cleanup 丢失、无 parked 帧恢复是已知差异 |
| 事件/waterfall | **可 1:1（语义）** | 高 | scope 相等、once/prepend/global、continuation、fail-closed 均可实现 |
| 注册表/去重/静默 | **可 1:1（语义）** | 高 | 去重、快照 API、`is_idle`/`when_idle`、`Delete`（含隐式 dispose）均已对齐 |
| 性能（深链/大 fanout） | **可接受，但需优化** | 中 | cascade 用简化 depIndex；refresh 全量；ownership 递归→O(depth) 栈；Registry 线性删除 |
| 编译期保证（unsafe/panic=abort/Send+Sync） | **无等价物** | — | 只能靠 CI 护栏 + `-race` + 文档承诺 |

一句话：**Go 适合做这个内核，代价是放弃“确定性析构、线程局部执行帧、零成本泛型身份、编译期并发
约束”，改用显式生命周期、goroutine 作用域、`reflect.Type`、运行时锁与测试护栏。**

---

## 1. 评估框架

### 1.1 等价级别

| 级别 | 含义 | 例子 |
|---|---|---|
| **L0 1:1** | 可观察输入/输出/顺序/错误变体一致 | `(scope,name)` 唯一、逆序 drain、`DuplicateService` |
| **L1 适配** | 机制不同、契约一致 | `Arc`→GC 指针、`catch_unwind`→`recover`、`Mutex`→`sync.Mutex` |
| **L2 重定义** | 契约需要改写断言/API 形态 | `Drop` 兜底、future drop 取消、`thread_local` 帧、`TypeId` 泛型 |
| **L3 不可复现** | Rust 特有观测点，只能声明 N/A | `Arc::strong_count`、Miri 泄漏检查、`panic="abort"` 编译守卫 |

### 1.2 风险等级

- **高**：影响公共 API 形态或核心顺序语义，返工成本大；
- **中**：影响边界行为/性能，可通过实现或文档缓解；
- **低**：局部替换，行为基本一致。

评估方法：以 `spec/CORDIS-CORE.md:832-940` 的 51 个用例为清单，对照参考实现（§13 已给证据）与
Go v0 代码逐项判定；无法判定的标“待验证”，不臆测。

---

## 2. Rust → Go 语言机制逐项映射

### 2.1 所有权、`Arc`、`Weak` → GC + `weak.Pointer`

| 项 | 内容 |
|---|---|
| Rust 机制 | `Arc<ContextInner>`/`Arc<Mutex<FiberData>>` 共享；`Weak` 用于 root 自链接、`ServiceEntry.provider`、`ServiceGate.watchers`、disposer 捕获 weak Context、`children`（`src/context.rs:121-124,178-184`；`src/service.rs:76-100,196-249`；`src/fiber.rs:127-146,1470-1543`） |
| Go 等价 | 指针/接口共享 + GC；`weak.Pointer[T]`（Go 1.24+）用于 provider/watch/根引用；`Fiber` 句柄为 `*fiberData` |
| 语义差异 | ①Go 无引用计数，“最后一个句柄”不可观测；②`weak.Pointer.Value()` 返回 nil 的时机取决于 GC，不能当作“fiber 已卸载”；③Rust `Weak` 失效在 `provider_is_active` 中是 **fail-open=true**，Go 若用 GC 弱指针会得到 **false**，需显式判 `State()`（v0 `ServiceEntry.providerActive` 正是这样做的） |
| 风险 | **中高**：provider 存活/服务可见性可能因 GC 时机抖动 |
| 缓解 | 不用 weak 判“是否可用”；provider 存 weak 但可用性由 `fiber.stateSnapshot()` 决定（v0 已如此）；children 由 Registry 强持有 + 显式注销；文档禁止依赖 GC 回收时点 |
| v0 状态 | 已实现：`service.go` 的 `weak.Pointer[fiberData]` + `providerActive()`；`ServiceGate.watchers` 用 weak；`l2.go:120` `makeWeakFiber`；`Registry.root` 用 `weak.Pointer`（CORE-033），root fiber 自身的 Context↔Fiber 强环由 GC 回收 |

**结论**：Arc/Weak 的**共享**语义可直接迁移；**存活感知**语义必须改为显式状态，不能依赖 GC。

### 2.2 `Drop`/确定性清理 → 显式 `Dispose`/`DisposeAll`/`Abandon` + orphan

| 项 | 内容 |
|---|---|
| Rust 机制 | `Disposer::drop` 在最后一个句柄释放时执行 cleanup（`src/effect.rs:60-79`）；`AsyncDisposer::drop` 安全网（`src/async_effects.rs:598-629`）；`FiberData::drop` 逆序 sync cleanup，async 进 orphan（`src/fiber.rs:234-330`）；大量 RAII Guard（LifecycleGuard/RefreshGuard/RecoveryGuard/FinalizeGuard） |
| Go 等价 | 显式 `Dispose()`/`DisposeAsync(ctx)`/`DisposeAll()`；`Fiber.Abandon()` 对应“丢最后一个句柄”：sync cleanup 立即跑，async 有 `BlockingExecutor` 则阻塞跑、否则进有界 orphan 队列；RAII Guard 改为 `defer` |
| 语义差异 | ①**丢句柄不会触发清理**（GC 不保证、finalizer 不推荐）；②析构顺序不再由编译器/运行时保证，依赖调用方；③`Abandon` 只能在调用方主动调用时生效；④`Drop` 中“最后一个 handle”判断（`strong_count==1`）无法移植 |
| 风险 | **高**：资源泄漏/清理时机变化，是所有 v0 用户代码的第一契约变化 |
| 缓解 | ①把“谁负责 Dispose”写进所有 API 文档与示例；②`Fiber.Abandon()` 提供显式兜底；③orphan 队列容量 256 + 计数 API；④测试用 `runtime.NumGoroutine`/自定义资源计数做泄漏基线；⑤将来可用 `runtime.AddCleanup`（Go 1.24+）做**非确定性**最后兜底，但绝不作为主路径 |
| v0 状态 | 已实现：`effect.go` Disposer 单次/逆序；`async_effects.go` AsyncDisposer+orphan（`OrphanCapacity=256`、4 个 API）；`fiber.go` `Abandon()`；`context.go` `DisposeAll` |

**结论**：可撤销语义 1:1，**确定性清理不可 1:1**；契约从“drop 即清理”改为“Dispose 即清理，Abandon
是显式放弃”。

### 2.3 `catch_unwind` → `recover`（同 goroutine 限制）

| 项 | 内容 |
|---|---|
| Rust 机制 | 所有 embedder 回调用 `catch_unwind` 包裹；`contained()` 统一 log+fallback（`src/internal.rs:83-102`）；future poll 用 `catch_panic`（`src/exec.rs:163-187`）；waterfall fail-closed（`src/waterfall.rs:102-115`）；`panic="abort"` 编译期拒绝（`src/lib.rs:48-56`） |
| Go 等价 | `defer func(){ if r:=recover(); r!=nil {...} }()`；v0 的 `contains.go` 提供 `contained`/`containedVoid`/`callContained`/`panicText` |
| 语义差异 | ①`recover` **只对同 goroutine 的 panic 有效**；跨 goroutine 的用户回调（事件异步 lane、EffectStream 消费者、用户自己起的 goroutine）必须各自 recover，否则整个进程崩溃；②`runtime.Goexit`、fatal error（栈溢出、并发 map 读写、OOM）不可 recover；③panic 值任意类型，`nil` panic 需特殊处理；④Rust 可 `resume_unwind` 原 payload，Go 用 `panic(v)` |
| 风险 | **中**：内核派发路径都包了；用户 goroutine 是契约边界 |
| 缓解 | 每个用户回调入口 defer recover（v0 已覆盖 apply/cleanup/listener/hook/accessor/schema/exporter）；waterfall 记录后 **re-panic**（不能返回零值）；文档明确“跨 goroutine 的用户代码自行负责”；测试覆盖 panic 矩阵 |
| v0 状态 | 已实现：`contains.go`；`fiber.go` activateSync/Async 用 `callContained`；`effect.go`/`async_effects.go` 每 cleanup catch；`events.go` callSync/AsyncListener；`waterfall.go` re-panic；`internal.go` 各 hook contained |

**结论**：L1 适配；**同 goroutine 限制**是必须在文档和 API 设计中承认的边界。

### 2.4 `thread_local`（activation frame / op token / drain frame）→ goid 注册表 + `context.Context`

| 项 | 内容 |
|---|---|
| Rust 机制 | ①`ACTIVATION_FRAMES: RefCell<Vec<(fiber_id,epoch)>>` 给 effect 打“当前尝试 epoch”（`src/fiber.rs:1810-1857`）；②`THREAD_TOKEN`/`CURRENT_OP_TOKEN` 生命周期重入（`src/fiber.rs:3658-3676`）；③`DRAIN_FRAMES` self-join（`src/async_effects.rs:80-139`） |
| Go 等价 | ①epoch：v0 **不用 thread_local**，而是 fiber 上的 `activatingEpoch atomic.Uint64` + `pushEffect` 读取（单次激活内有效；嵌套/并发激活需注意，见风险）；②operation token：`op_token.go` 用 `currentGoroutineID()` + `sync.Map[uint64]*goroutineOps` 存 token 栈；③drain frame：把 frame 放进传给 cleanup 的 `context.Context`（`async_effects.go` 的 `withDrainFrame`/`drainingInContext`），cleanup 必须把收到的 ctx 传回 `Dispose` 才能 self-join 短路 |
| 语义差异 | ①Go 官方**不提供 goroutine id**，v0 用 `runtime.Stack` 解析，属于非支持 API，存在性能与兼容风险；②goroutine 可能被调度到不同线程，但 goroutine id 不变（这点优于 Rust 线程 token）；③async 操作若在**另一个 goroutine** 恢复，token 栈看不到，重入会被误判为 Busy；④`ctx` 不传则 self-join 失效（文档必须强调） |
| 风险 | **高**：goid 是“实现技巧”，不是语言契约；影响 CORE-045/046 的精确语义 |
| 缓解 | ①goid 只用于“是否同一在飞操作”的判定，不用于所有权转移（v0 注释已声明）；②所有生命周期入口走 `withLifecycle` 安装 token；③提供显式 `DrainFrame` 语义（ctx 传递）；④若 Lead 否决 goid：改为“所有入口新 token + 内部显式 op handle”，代价是 cleanup 内重入变为 LifecycleBusy（与 Rust 不同），需在 GO-DESIGN 标注；⑤测试覆盖“同 goroutine 重入/跨 goroutine Busy” |
| v0 状态 | 已实现：`runtime_goid.go`（`runtime.Stack` 解析）、`op_token.go`（token 栈）、`async_effects.go`（drain frame ctx）；**activation epoch 用 fiber 字段 + 显式参数近似**，没有 Rust 的嵌套 frame 栈 |

逐站点对照：

| 站点 | Rust 机制 | Go v0 机制 | 状态 / 风险 |
|---|---|---|---|
| activation frame | `ACTIVATION_FRAMES` + `EpochFrame`/`EpochFrameFuture` 每 poll 安装 | fiber 的 `activatingEpoch` 字段 + 显式 `activation` 标签；**无嵌套 frame 栈** | 近似；嵌套激活/并发 activation 可能打错 epoch（中高） |
| lifecycle thread token | `THREAD_TOKEN` 每线程惰性一个；`current_lifecycle_token()` | `runtime_goid.go` 解析 goroutine id + `op_token.go` token 栈 | 已实现；goid 非官方 API（高） |
| current op token | `CURRENT_OP_TOKEN` Cell，`WithOperationToken` 每次 poll 安装/还原 | `pushCurrentOpToken(token)` 在 `withLifecycle` 安装/还原 | 已实现；async 操作换 goroutine 后 token 不可见（高） |
| drain frame / self-join | `DRAIN_FRAMES` thread-local，key=DisposeState 地址 | `context.Context` 里的 `drainFrames`，`withDrainFrame` 每 cleanup 派生 | 已实现；cleanup 不把 ctx 传回则 self-join 失效（中） |

**结论**：三处 thread_local 各有替代，但 **goid 是唯一“非官方”依赖**，是 v0 的主要技术债。

### 2.5 `async`/future → goroutine + `context.Context`

| 项 | 内容 |
|---|---|
| Rust 机制 | `BoxFuture`/poll/Waker；`block_on` thread-park；`BlockingExecutor` 首次安装（`src/exec.rs:26-234`）；取消 = drop future（`CancelSafeUnload`/`park_cancelled_frame`/`AsyncActivationGuard`） |
| Go 等价 | async API 改为阻塞函数 `func(ctx context.Context) error`；`AsyncDisposer` 顺序执行 cleanup；`DisposeAsync(ctx)` 取消 = `ctx.Done()`；sync 路径直接调用（默认）或经 `BlockingExecutor`；orphan 队列兜底 |
| 语义差异 | ①没有“future 被 drop”这一隐式取消点：**已启动的 cleanup 函数**只能靠 ctx 协作退出，内核不能中断；②未启动 cleanup 可恢复（S-1，v0 已做）；fiber 卸载取消时 sync inline、async 进 deferred 并落目标态（A-9，v0 已做）；③Rust 的 parked cascade 帧 + recovery（F1/F4/F5）v0 **未实现**——Go 的取消卸载会逐帧完成 teardown，不需要“停摆帧恢复”，但代价是取消路径可能仍执行清理并阻塞；④`BlockingExecutor` 在 Go 中是可选 seam，**安装后**同步卸载经其执行（conformance CORE-034 可复现），默认 inline 是与 Rust 的差异 |
| 风险 | **中高**：取消边界/清理完成时机与 Rust 不同，测试需改写 |
| 缓解 | ①明确契约：取消时“未启动恢复、已启动等待或放行、目标状态必达”；②`DisposeAsync` 返回 `ctx.Err()`；③`DisposeBlocking(ctx)` 作为重试恢复入口；④`Abandon`+orphan 处理无显式 dispose；⑤不做假 `block_on`，禁止在事件循环 goroutine 上调用同步 Dispose（文档） |
| v0 状态 | 已实现：AsyncDisposer（claim/join/cancel restore/self-join）、`drainEffectsAsync`（A-9 落 Pending/Disposed）、orphan；`BlockingExecutor` 可选。未实现：parked frames/recovery、`when_idle` recovery 循环（因无停摆帧，见 §3.3） |

取消点的逐项映射：

| Rust 的“drop 点” | Rust 语义 | Go v0 等价 | 保留 / 丢失 |
|---|---|---|---|
| 激活 future 被 host drop | `AsyncActivationGuard` 回收本 epoch effect；必要时 Failed | `ActivateAsync` 返回 `ctx.Err()`；defer 中 `discardActivationEpoch` + `transitionIfEpoch(Loading,Pending)` | 改写：Go 回 **Pending**（Rust 可能 Failed），更利于重试 |
| `dispose_async()` future 被 drop | `CancelSafeUnload` 完成 teardown；未启动 async 进 deferred（A-9） | `DisposeAsync` 的 `drainEffectsAsync` 在 ctx 取消时 park 剩余 async、sync inline、`finishUnload(target)`、deferred 存 fiber | **保留**；in-flight cleanup 丢失与 Rust 一致 |
| `AsyncDisposer::dispose()` future 被 drop | `CancelSafeDrain` 把未启动 cleanup 放回 slot，下次重试 | `drain()` 每步检查 `ctx.Err()`，`restore(remaining)` + `state.cancel()` | **保留**（S-1） |
| 级联 future 被 drop | 非根帧 `park_cancelled_frame`，后续 recovery（F1/F4/F5） | 无 parked；每个 dependent 自行 `unload` 并落 Pending/Disposed | **丢失** parked/recovery；取消路径可能更长 |
| `select!`/超时取消插件工作 | `checkpoint()` 协作退出 | `ctx` + `Checkpoint()` | 一致（协作） |
| host 丢最后一个 Fiber 句柄 | `FiberData::drop` 安全网 | `Abandon()`（显式） | 改写：丢句柄不触发 |

**结论**：async 语义**必须重新定义**，但核心承诺（顺序、单次、join、未启动恢复、目标状态）可保。

### 2.6 `TypeId`/泛型 → `reflect.Type` + Go 1.27 泛型方法

| 项 | 内容 |
|---|---|
| Rust 机制 | `ServiceKey<T>` 携带 `PhantomData`，`TypeId` 校验写入（`src/service.rs:152-176`）；`PluginKey::Type(TypeId)`（`src/registry.rs:120-128`）；方法可带泛型参数 `ctx.get<T>(key)` |
| Go 等价 | `ServiceKey[T any]{name string}` + **Go 1.27 泛型方法**：`ctx.Provide[T]` / `ctx.Get[T]` / `ctx.Require[T]`（类型参数由实参推断），包装函数 `cordis.Provide/Get/Require` 保留；类型身份用 `reflect.TypeFor[T]()`，服务条目存 `reflect.TypeOf(value)`；`PluginKey` 为 `{kind, name, reflect.Type}`；`TypedPluginKey[T]()` + `ctx.PluginTyped[P]` |
| 语义差异 | ①**Go 1.27 已支持泛型方法**：`ctx.Get(key)` 与 Rust `ctx.get(KEY)` 形态一致；但**接口方法不能声明类型参数**（Go 1.27 规范），所以 `Plugin`/`AsyncPlugin`/`Service` 等接口仍保持非泛型，typed 身份通过泛型方法/自由函数桥接；②`reflect.TypeOf(value)` 是**动态类型**，接口类型 key 与实现类型值的关系需约定（v0 用值断言而非 key 类型校验）；③typed-nil 陷阱：`any((*T)(nil)) != nil`，但 `reflect.TypeOf(nil)==nil`；④v0 **合并了 sized/any 两条 lane**（Go 的 any 天然存接口值），因此 Rust 的 `ArcWrap` 身份规则、跨 lane 读写失败（CORE-032、fix13 typed-vs-any）**无法原样复现**；⑤`Set` 用 `reflect.TypeOf(value) == entry.typ`，比 Rust 的 `TypeId` 宽松（接口值可存实现类型） |
| 风险 | 中：接口方法泛型限制带来少量桥接 API；lane 合并丢失部分类型安全测试点 |
| 缓解 | ①统一“key 名字是身份、类型是读写校验”的文档；②保留 `ServiceTypeMismatch`/`DuplicateService` 错误变体；③为接口服务增加约定（`Provide[T]` 存实现值，`Get[I]` 断言）；④如未来需要严格 lane，可加 `ServiceKey[T]` 的 `lane` 字段 + 包装类型，但 v0 不做；⑤写 conformance 测试覆盖 typed-nil、接口读写、类型不匹配；⑥用 Go 1.27 泛型方法提供 `ctx.*` 方法形态并以 `api_test.go` 钉住 |
| v0 状态 | 已实现：`service.go` `ServiceKey[T]`/`ServiceType`；`context.go` 泛型方法 `Provide/Get/Require/ProvideChecked/ProvideGated/ProvideService/PluginTyped/PluginAsyncTyped/Meta` + 包级包装；`registry.go` `TypedPluginKey[T]`；`SetNamed` 类型校验；`api_test.go` 验证泛型方法推断。未实现：严格 lane 标记 |

API 形态对照：

| Rust API | Go v0 API | 备注 |
|---|---|---|
| `ctx.get(KEY)` | `ctx.Get(key)`（包装：`cordis.Get(ctx,key)`） | Go 1.27 泛型方法，类型参数推断 |
| `ctx.provide(KEY, Arc<T>)` | `ctx.Provide(key, value)`（包装：`cordis.Provide(ctx,key,value)`） | 值不是 `Arc`；共享用指针 |
| `ctx.provide_any(KEY, Arc<T>)` | 无独立 API，统一 `Provide` | lane 合并（见 §3.2 CORE-032） |
| `ctx.set(name, value)` | `ctx.SetNamed(name, value)` / `ctx.SetRevertibleNamed(name, value)` | 后者返回 inverse Disposer（CORE-041 已对齐） |
| `ServiceKey::<T>::new("x")` | `NewServiceKey[T]("x")` | 运行时 `reflect.TypeFor[T]()` |
| `PluginKey::Type(TypeId::of::<P>())` | `TypedPluginKey[P]()` | `reflect.Type` 作键 |
| `BoxFuture`/`ctx.effect_future` | `func(ctx) error` / `EffectFuture(cleanups,label)` | 无 future body，只有已求值 cleanup |
| `Disposer::drop` | `Disposer.Dispose()` / `Fiber.DisposeAll()` / `Fiber.Abandon()` | 显式生命周期 |

**结论**：类型安全**部分保留**（编译期 key 类型 + 运行时 reflect 校验）；`*_any` 双 lane 与
`TypeId` 精确身份属于**重新定义**。

### 2.7 `feature gate` → 独立包/始终开启

| 项 | 内容 |
|---|---|
| Rust 机制 | 唯一 feature `supervision`，默认关闭；`l2` 模块、re-export、fiber/context/registry 方法均 `#[cfg]`（`Cargo.toml:26-30`，`src/lib.rs:59,68-69`） |
| Go 等价 | v0 **始终开启** L2（`l2.go` 无 build tag）：`FiberQuota`、`IsolationCapabilities`、`StalledFibers` 始终可用；build tag 未使用 |
| 语义差异 | 默认构建 API 面更大；无编译期裁剪 |
| 风险 | 低 |
| 缓解 | 保持 L2 簿记便宜（每 fiber 几个原子）；若需裁剪，后续拆 `internal/l2` 或 build tag；CI 两套构建（GO-DESIGN §9） |
| v0 状态 | 已实现（始终开启） |

**结论**：feature gate 的**目的**（可选能力/编译裁剪）在 Go 里用“始终开启/独立包/build tag”均可，
v0 选择始终开启，代价可忽略。

### 2.8 `panic="unwind"` 编译期 guard → Go 无等价

| 项 | 内容 |
|---|---|
| Rust 机制 | `#[cfg(panic="abort")] compile_error!` 拒绝 abort 构建（`src/lib.rs:48-56`），因为遏制依赖 unwind |
| Go 等价 | 无。Go 的 `recover` 依赖 panic，但不区分 unwind/abort；**fatal error 不可 recover**（栈溢出、OOM、并发 map 读写、`runtime.throw`、`Goexit`） |
| 语义差异 | Rust 的编译期承诺变成 Go 的“运行期尽力而为”；无法阻止宿主构建出“遏制承诺不成立”的二进制（例如 CGO/unsafe） |
| 风险 | 低（对内核 API）/中（对隔离承诺的表述） |
| 缓解 | 文档明确“不可遏制清单”；CI 禁 `unsafe`/cgo（架构测试）；L3/L4 由宿主提供；不在文档中承诺沙箱 |
| v0 状态 | 文档已声明（`doc.go`）；`internal/archtest` 检查 import 边界（禁 unsafe/cgo）；Go 无 panic=abort 等价 guard |

**结论**：**无等价物**，只能通过文档 + 架构测试 + L3/L4 建议管理预期。

### 2.9 `Send + Sync` → 运行时锁 + race detector

| 项 | 内容 |
|---|---|
| Rust 机制 | `ServiceKey<T: Send+Sync>`、`ServiceValue: Send+Sync`、所有 hook `Send+Sync`；编译期禁止非线程安全共享 |
| Go 等价 | 无编译期约束；内核用 `sync.Mutex`/`atomic` 保护共享状态；回调契约要求并发安全，靠 `-race` 与文档 |
| 语义差异 | 用户可以把非线程安全值注册为服务并在多 goroutine 使用；内核无法阻止 |
| 风险 | 中 |
| 缓解 | 文档标注“服务值/回调可能被多 goroutine 访问”；`-race` 全量测试；关键路径快照 + 锁外回调 |
| v0 状态 | 已实现（运行时锁）；conformance + CI `-race` 已覆盖；缺系统压力/属性测试 |

### 2.10 Miri/UB → Go 内存安全 + data race 语义

| 项 | 内容 |
|---|---|
| Rust 机制 | `#![forbid(unsafe_code)]` + Miri 泄漏/UB 检查（CORE-051 `miri_leaks.rs`） |
| Go 等价 | Go 无 unsafe（内核不 import `unsafe`）；内存安全由运行时保证；**data race 是未定义行为**（race detector 检测，非编译期） |
| 语义差异 | 无 Miri 级泄漏检查；CORE-033 的“无强环”由 GC 替代，CORE-051 的 Miri 断言改为“无 goroutine 泄漏 + registry 空 + 幂等” |
| 风险 | 低（安全）/中（验证强度） |
| 缓解 | `-race`、goroutine 基线测试、`go vet`/staticcheck、架构测试禁 unsafe/cgo；CORE-033/051 改写断言（见 §3） |

### 2.11 次要机制

| Rust | Go v0 | 差异/风险 |
|---|---|---|
| `&'static str` 名字/label | `string` | 名字可动态；label 表可能增长（低） |
| `serde_json::Value` | `Value = any`（`map[string]any`/`[]any`） | 无 `Null` 与 missing 区分；数值类型需规范化（中） |
| `HashMap<(u64,String),Entry>` | `map[serviceStoreKey]*serviceEntry` + Mutex | 键可比较（低） |
| `AtomicU64/Bool` | `sync/atomic`（`atomic.Uint64` 等） | Go 顺序一致，简化（低） |
| `?Sized`/`ArcWrap` | 合并 lane，`any` 存接口/指针 | 丢失 lane 身份（中） |
| 闭包/Fn trait | 函数类型（`func(...)`） | 无捕获生命周期问题（低） |
| `tracing` | `log/slog` + `SlogExporter` | 无 span/字段宏，语义近似（低） |
| `OnceLock` BlockingExecutor | `sync.RWMutex` + 首次安装胜 | 等价（低） |
| `Drop` Guard | `defer` | defer 不跨 goroutine；panic 时仍执行（低） |

### 2.12 Go 语言特性利用清单（Go 1.27）

本移植按 Go 1.27 的官方语言/标准库能力重新审视，实际采用情况：

| Go 特性 | 版本 | 在本内核中的用法 | 状态 |
|---|---|---|---|
| **泛型方法** | 1.27 | typed 服务读写/插件身份/元数据是 `Context` 的泛型方法（`ctx.Provide/Get/Require/ProvideService/PluginTyped/Meta`），类型参数由实参推断；包级泛型函数保留为包装；`api_test.go` 钉住形态。接口方法仍不能带类型参数（规范限制），故 `Plugin`/`AsyncPlugin`/`Service` 接口保持非泛型 | 已采用 |
| `sync.WaitGroup.Go` | 1.25 | `events.parallel_async` 的并发 listener 分发 | 已采用 |
| `weak.Pointer` | 1.24 | 服务条目的 provider 弱句柄、registry 的 root fiber 弱句柄（断开 GC 环） | 已采用 |
| `log/slog` | 1.21 | 内核诊断通道（`slog.Default()`，宿主可替换 handler）；`SlogExporter` 把服务日志桥接到 slog；Go 1.26 的 `slog.NewMultiHandler` 让宿主可组合多 handler | 已采用 |
| `runtime.GC` + `weak` | — | CORE-033 的“dispose 后整图可回收”用 finalizer/GC 探针验证（与 Rust 结构无环是不同机制） | 已采用（测试） |
| `testing/synctest` | 1.25 稳定 | 可对 async/listener 并发做确定性气泡测试；当前用 channel 闸门 + `-race`，未强制引入 | 待评估 |
| `encoding/json/v2` + `jsontext` | 1.27 | `Value = any` 与 v1/v2 解码结果形状兼容；内核不绑定具体 JSON 实现，宿主可选用 v2（`jsontext.Value` 也可直接作为 `Value` 存入） | 可选，不绑定 |
| `runtime.AddCleanup` | 1.24 | 未用于 fiber 兜底清理：dispose 后 fiber↔context↔registry 形成环，Go 对“环中含终结器对象”的回收/终结顺序无保证；确定性清理仍是显式 `Dispose`/`Abandon` 契约 | 不采用（有原因） |
| `iter`/`slices`/`maps`/`min/max` | 1.21–1.23 | 未强依赖；内核保持朴素循环以便审计 | 未采用（可选） |

### 2.13 映射总表

| # | Rust 机制 | Go 方案 | 语义差异 | 风险 | 缓解 | v0 |
|---|---|---|---|---|---|---|
| 1 | 所有权/Arc/Weak | GC + `weak.Pointer` | 无引用计数/析构；weak 失效时机不定 | 中高 | provider 判 State；Registry 强持有；文档 | 已实现 |
| 2 | Drop/确定性清理 | Dispose/DisposeAll/Abandon+orphan | 丢句柄不清理；顺序靠调用方 | **高** | 显式契约；Abandon；orphan；泄漏基线 | 已实现 |
| 3 | catch_unwind | recover | 仅同 goroutine；fatal 不可恢复 | 中 | 每个回调入口 recover；waterfall re-panic | 已实现 |
| 4 | thread_local | goid 注册表 + ctx frame | goid 非官方；跨 goroutine 重入丢失 | **高** | token 栈；显式 ctx；文档/测试 | 已实现（epoch 近似） |
| 5 | async/future | goroutine + ctx | 取消语义/清理完成时机不同；无 parked 帧 | 中高 | 契约化；deferred/orphan；重试入口 | 部分实现 |
| 6 | TypeId/泛型 | reflect.Type + Go 1.27 泛型方法 | 接口方法不能带类型参数；lane 合并；typed-nil | 中 | key 名身份；类型校验；测试 | 已实现（lane 简化） |
| 7 | feature gate | 始终开启/独立包 | 无编译裁剪 | 低 | 保持轻量；可选 build tag | 始终开启 |
| 8 | panic=unwind guard | 无 | 无编译期承诺 | — | 文档+CI+L3/L4 | 无 |
| 9 | Send+Sync | 运行时锁 + -race | 无编译期约束 | 中 | 文档；-race；快照 | 已实现 |
| 10 | Miri/UB | Go 内存安全 + race 语义 | 无 Miri；data race 仍可能 | 低-中 | `-race`；goroutine 基线；archtest | 已实现（无 Miri） |

---

## 3. CORE 用例可复现性评估

> 断言细节见 [05-core-cases](analysis/05-core-cases.md)；Rust 实现见
> [ARCHITECTURE-ANALYSIS §13](ARCHITECTURE-ANALYSIS.md#13-core-001051-与实现映射)。

### 3.1 可 1:1 或近 1:1（保留全部可观察契约）

`CORE-001`–`021`（除 020 的日志细节）、`022`、`023`–`026`、`027`–`032`、`034`（027 body 简化、032
改用 Go 接口断言、034 安装 executor）、`035`–`041`、`042`、`043`、`047`（顺序一致；注入链两阶段
迭代、ownership 递归）、`048`、`050`、`051`（Miri 断言改写）；`033` 见 §3.2/§3.4 的 N/A。

这些用例的共同点：断言的是**返回值/状态/错误变体/顺序/计数**，不依赖析构时点、线程身份或 future
调度。v0 已覆盖的机制：服务 store、inject gate、effect 逆序、状态机、withdraw 顺序、waterfall、
internal hooks、config/logger/clock、replace、dispose_all、checkpoint。

### 3.2 必须重新定义或改写断言的用例

> 工作区当前已对齐 **022（Delete 隐式 dispose）、026（UID 清空 / AssertActive→InactiveEffect）、
> 034（BlockingExecutor 可安装并可断言）、037（sync Refresh 跳过 async）、041（`SetRevertibleNamed`）**，
> 并且 conformance 已覆盖；这些项不再属于重定义。

| CORE | 原语义 | Go 重定义 | 原因 |
|---|---|---|---|
| **027** | async effect body 失败→激活失败；stream 逆序 | v0 `EffectFuture` 只接收**已求值** cleanup，没有 future body；stream 已实现 | API 简化；body 失败需由调用方在 `EffectAsync` 内返回 error 表达 |
| **033** | root 图无强引用环（`Arc::downgrade` 后 upgrade 为 None） | 由 Go GC 保证可达性；conformance 用 finalizer/GC 探针断言“可回收” | 无引用计数；Rust 的强环语义不可直接观测 |
| **045** | 生命周期互斥基于 operation token；同线程重入允许，async 任务迁移线程后语义不变 | v0 用 **goroutine id** 识别重入；同一 goroutine 内 cleanup 自 dispose 可重入；跨 goroutine（含 async 操作迁移）一律 Busy | Go 无任务本地 token 传播；除非所有 async API 都显式传 operation handle |
| **046** | AsyncDisposer 恢复未启动项（S-1）；fiber 取消卸载完成 teardown（A-9）；**parked cascade 帧 + F1/F4/F5 恢复** | v0 实现 S-1/A-9/self-join/偶发 in-flight 丢失；**未实现 parked 帧与 recovery**（取消卸载逐帧完成，不停摆） | Go 的取消路径可以同步/异步完成每一帧，不需要“停摆帧”；但取消时可能长时间阻塞，且没有“恢复被取消级联”的独立入口 |
| **049** | `when_idle` recovery-aware，等待期间恢复 parked 帧；排除调用线程自己的注册（ThreadId） | v0 `IsIdle` 严格 + `WhenIdleContext` 排除**当前 goroutine** 的注册；无 recovery 循环（无 parked 帧） | 无 parked 帧 → 无需 recovery；ThreadId→goid；两线程互等注册窗口仍可死锁（与 Rust 相同未检测） |
| **020** | waterfall middleware panic 记录后重抛；sync terminal 的 panic 不在 catch_unwind 内但仍上抛 | v0 行为一致；sync terminal 同样未包日志 | 非语义差异，仅日志细节；测试不要断言“terminal panic 被记录” |
| **021** | 9 个 internal hook + raw `InternalBus` 非 fiber-owned | v0 已实现 9 个 + lifecycle；raw bus 所有权一致；conformance 覆盖 | 无契约差异；panic fallback 以实现为准 |

### 3.3 不可复现的观测点（必须在测试中声明 N/A 或改写）

| Rust 观测点 | 为什么不可复现 | Go 替代断言 |
|---|---|---|
| `Arc::strong_count`/`Weak::upgrade` 时点 | GC 无引用计数 | 结构无强环（人工审查）+ registry 强持有 + 显式注销 |
| `Disposer::drop` 自动执行 | 无确定性析构 | 显式 Dispose；`Abandon`；orphan 计数 |
| `FiberData::drop` 的 sync/async 分流 | 无析构 | `Abandon` 的同等分流（有 executor 则阻塞，否则 orphan） |
| Miri 泄漏检查 | 无 Miri | `-race` + `runtime.NumGoroutine` 基线 + registry 空 + 幂等 |
| `panic="abort"` 构建必须失败 | 无编译期 guard | 文档 + CI 架构测试（禁 unsafe/cgo） |
| 线程身份（`ThreadId`） | goroutine 无稳定 id 的官方 API | goid 解析（v0）或显式 operation token |
| `parallel_async` 的确定性轮转 poll | goroutine 真并发 | 只断言“sync 先于 async、Bail 被忽略、每个 listener 至多一次”，不断言 interleaving |
| `BlockingExecutor` 默认路径 | Rust 无 executor 时 thread-park block_on；Go 默认 inline | conformance CORE-034 安装计数 executor 后断言同步卸载走之 |
| `ORPHAN` 的 Drop 触发 | 无 Drop | 显式 `Abandon` 后计数；GC 不作为触发源 |
| `catch_unwind` 跨线程传播 | recover 不跨 goroutine | 每个 goroutine 边界自行 recover（测试覆盖进程不崩） |

### 3.4 逐例结论（51 项）

> 判定口径：**1:1** = 可逐条保留原断言（API 名可不同）；**部分** = 核心断言可保、边界需改写；
> **重定义** = 语义/API 需改；**N/A** = Rust 特有、Go 无对应观测点。

| CORE | 判定 | 说明 |
|---|---|---|
| 001 | 1:1 | 状态/清理/幂等可断言 |
| 002 | 1:1 | 树共享 store + 依赖者 Pending |
| 003 | 1:1 | 最近 scope、无回退 |
| 004 | 1:1 | 锁内查重 + 错误变体 |
| 005 | 1:1 | 严格 gate + refresh 驱动 |
| 006 | 1:1 | check + `Refresh()` |
| 007 | 1:1 | 单有序列表跨 lane 逆序 |
| 008 | 1:1 | Disposer/AsyncDisposer 单次 |
| 009 | 1:1 | sync Dispose 阻塞到 async cleanup 完成 |
| 010 | 1:1 | provide disposer + withdraw |
| 011 | 1:1 | 记录 first、drain 后重抛 |
| 012 | 1:1 | live 诊断树（空 children 用 `[]`） |
| 013 | 1:1 | checked/gated 两条路径 |
| 014 | 1:1 | 关闭 gate 不回退 |
| 015 | 1:1 | check 锁外可重入 |
| 016 | 1:1 | 依赖移除 → Active 卸载 |
| 017 | 1:1 | remove_owner 三路径 |
| 018 | 1:1 | once/prepend/global/nil-false bail |
| 019 | 1:1 | scope map 相等 |
| 020 | 1:1 | 顺序/veto/重复 next/terminal/fail-closed；sync terminal 日志细节不同 |
| 021 | 1:1 | 9 hooks + lifecycle；panic fallback 细节以最终代码为准 |
| 022 | 1:1 | `Delete` 删除 runtime 并逐个 dispose fiber；conformance 覆盖 |
| 023 | 1:1 | 阈值/ring/seq/intercept/clock 快照 |
| 024 | 1:1 | 五步 validate + 浅 merge；数值表示需规范化 |
| 025 | 1:1 | accessor 优先/专属错误/meta/labeled |
| 026 | 1:1 | Disposed 后 `UID()` 返回 false；`AssertActive` 返回 `ErrInactiveEffect`；conformance 覆盖 |
| 027 | 部分 | `EffectFuture` 只接收已求值 cleanup，无 future body；stream 已实现 |
| 028 | 1:1 | take effects + 逆序 drain + Failed |
| 029 | 1:1 | apply panic 隔离、fiber Failed |
| 030 | 1:1 | cleanup panic 不跳过、重抛首个 |
| 031 | 1:1 | listener panic → Continue |
| 032 | 1:1 | 接口服务通过泛型 `Provide/Get` + 断言工作；lane 身份规则合并（无 `ArcWrap`），conformance 覆盖接口用例 |
| 033 | N/A | GC 无强环观测；断言改为“结构无环 + 可回收性（不稳定）” |
| 034 | 1:1 | 安装 `BlockingExecutor` 后同步卸载经其执行并可断言计数；默认 inline 是与 Rust 的差异 |
| 035 | 1:1 | AsyncPlugin 激活/卸载 |
| 036 | 1:1 | async 失败回滚并 await cleanup |
| 037 | 1:1 | sync `refreshPass` 跳过 async plugin；`RefreshAsync` 才推进 async 激活/卸载；conformance 覆盖 |
| 038 | 1:1 | 失败同步激活 inline drain async |
| 039 | 1:1 | restart 后仍检查依赖 |
| 040 | 1:1 | raw/resolved 分离，不复合 |
| 041 | 1:1 | `SetRevertibleNamed` 返回 inverse Disposer；版本/分类错误/恢复均有 conformance 覆盖 |
| 042 | 1:1 | withdraw → dependents cleanup → provider cleanup |
| 043 | 1:1 | claim + join（不重复执行） |
| 044 | 1:1 | Disposed 终态 + lifecycle seq 广播 |
| 045 | 重定义 | goroutine id 重入；async 迁移 goroutine 后 Busy |
| 046 | 部分 | S-1/A-9/self-join 已实现；parked 帧与 F1/F4/F5 未实现 |
| 047 | 1:1 | 顺序/回滚/有界记账可保；注入依赖链两阶段迭代，ownership 子树仍递归（Go 栈增长） |
| 048 | 1:1 | 身份/raw config/失败回滚；`Ok≠Active` 需断言 State/ErrorDetail |
| 049 | 部分 | 无 parked recovery（N/A）；自身注册排除用 goid；严格 IsIdle/WhenIdle 契约保留 |
| 050 | 1:1 | Cancel + Checkpoint |
| 051 | 1:1 | 全尝试 + panic 优先 + 幂等；Miri 泄漏改为 goroutine/资源基线 |

**汇总**：46 项可直接 1:1（含 020/021 的细节差异、032 的 lane 合并、034 的默认 inline 差异）；
1 项重定义（045）；3 项部分（027/046/049）；1 项 N/A（033）。

---

## 4. 性能评估

### 4.1 依赖刷新：无反向索引 vs 有索引

Rust 的 `DepIndex` 是“候选缩小器”，快路径只重评 dirty 服务的依赖者；Go v0 有**简化版**
`depIndex`（inject 名→fiber ids），但只用于 cascade，refresh 仍是全量：

| 场景 | Rust | Go v0 | 影响 | 缓解 |
|---|---|---|---|---|
| 单服务变化触发的 refresh | 候选集 ≈ 依赖者数 | 每轮遍历全部 fiber；最多 64 轮 | 大 fanout/频繁 set 时 O(F×64) | P1 加 dirty 集合 + 候选过滤 |
| `ctx.refresh()` | 全量（规范要求） | 全量 | 一致 | — |
| cascade 依赖前沿 | 索引候选 + 全量兜底 | `beginCascade` 用 `dependentsOfNames` 逐层取候选，`visited` 去重 | 近似按受影响 dependent 收敛 | 已有；保留“候选需复验 dependenciesSatisfied” |
| ownership 子树 | 显式 DFS 栈 | ownership effect cleanup 递归 `child.Dispose()` | 深父子链 O(depth) 栈 | P1 显式帧栈 |
| 版本比较 | `dep_versions` 向量 | `depVersions []depVersion` 线性查找 | 依赖数 k 小，O(k²) 可忽略 | 依赖多时改 map |

结论：**cascade 已从 O(F²) 降到按 dependent 收敛；refresh 仍是全量**，适合中小树，路线图 P1 补
refresh 的 dirty/索引快路径（见 GO-DESIGN §8）。注意 Replace 改变 inject 后当前未重索引 depIndex，
热替换后 cascade 可能漏候选（已列入 GO-DESIGN §8 P0）。

### 4.2 深链级联：迭代 O(1) vs Go 栈增长

Rust 用显式 DFS 栈，调用栈 O(1)（`src/fiber.rs:2502-3127`，2 MiB 线程 depth 800 通过）。
Go v0 的**注入依赖级联**已改为两阶段 `beginCascade`（`depIndex` 候选 + `visited` 去重 + 逆序
drain frames），不再递归；**ownership 子树**仍通过 effect cleanup 递归 `child.Dispose()`。

- 注入链（1500 深依赖）不再有 O(depth) 递归；顺序与 Rust 的两阶段一致（dependent-before-provider）；
- 父子树（深嵌套 `ctx.plugin`）仍是 O(depth) 递归；Go goroutine 栈按需增长，depth 1500 通常可承受
  （远小于 1GB 上限），但“调用栈 O(1)”契约只对注入链成立；
- 断言：注入链顺序正确、不 panic；父子树可用 `MaxPluginDepth=256` 约束，极端 churn 需压测。

结论：**注入链已恢复近 O(1) 帧；父子树仍放弃 O(1) 栈**，若要严格 O(1)，需把 ownership 也改为显式帧栈
（路线图 P1）。

### 4.3 注册表存储：`FiberSlots` vs slice

| 操作 | Rust | Go v0 | 影响 |
|---|---|---|---|
| runtime 成员删除 | O(1)（tombstone + positions） | `PluginRuntime.remove` O(n) 线性扫描 | 大 fanout dispose O(n²) |
| 全局 fiber 删除 | FiberSlots O(1) | `Registry.removeFiber` O(n) | 大量 churn 时 |
| 快照 | 克隆 Vec | 克隆 slice | 一致 |
| 去重 | HashMap | HashMap | 一致 |

结论：中小规模无感；5000 fibers 共享 runtime 的 fanout 场景需补 tombstone（路线图 P1）。

### 4.4 事件与日志

- 事件：快照分配 `[]*listenerEntry`，与 Rust 的 Vec clone 同阶；`parallel_async` 每 async listener
  一个 goroutine（Rust 是单线程轮转 poll），并发度更高但 goroutine 创建/调度成本更大；
- 日志：v0 ring buffer 用 `copy(s.records, s.records[1:])`，每条记录 O(buffer) 拷贝；Rust 用
  `VecDeque` O(1) 摊还。buffer=1000 时每条日志 ~1000 次拷贝，热路径需改环形索引（路线图 P2）；
- 锁：v0 用粗粒度 Mutex；Rust 也是 Mutex，但 Go 的 `sync.Mutex` 在竞争下会自旋/排队，事件派发
  快照已释放锁，影响可控。

### 4.5 并发与内存

- goroutine 成本（~2KB 栈）远高于 Rust future（零分配可 inline）；大量 `parallel_async`/EffectStream
  会产生更多堆/栈；
- `weak.Pointer` 需要 GC 支持，频繁创建/失效有额外成本（v0 用于 provider/watch/registry.root）；
- `-race` 的原子/锁开销约 5–20 倍，仅测试启用。

### 4.6 性能结论

| 维度 | 结论 |
|---|---|
| 中小树（<100 fibers） | 无显著问题，v0 可直接用 |
| 大 fanout / 频繁 set | cascade 已用 depIndex；refresh 仍需 dirty/索引快路径（P1） |
| 深链（>1000） | Go 栈可承受，但内存/耗时线性增长；需压测 |
| 高吞吐日志（>10k/s） | ring buffer 拷贝是瓶颈，需环形索引（P2） |
| 并发事件 | goroutine 数需限流/池化（P2） |

---

## 5. 风险登记

| # | 风险 | 等级 | 触发条件 | 缓解 | 决策点 |
|---|---|---|---|---|---|
| R1 | goid 依赖 `runtime.Stack` | 高 | 高频生命周期入口 / Go 版本变化 | 只在入口用；token 栈；可替换为显式 op handle | Lead 决定是否接受 |
| R2 | 丢句柄不清理导致泄漏 | 高 | 用户忘记 Dispose | 文档/示例；Abandon；orphan；泄漏测试 | 契约必须写清 |
| R3 | 接口方法不能带类型参数（Go 1.27 规范） | 低 | 需要泛型接口/插件契约的扩展 | 泛型方法（Go 1.27）+ 可选元数据接口 + 自由函数桥接；`api_test.go` 钉住形态 | 已缓解 |
| R4 | 取消时 in-flight cleanup 丢失 | 中高 | `DisposeAsync` 取消 | 文档边界；deferred 重试；测试 | 接受（与 Rust A-9 类似） |
| R5 | 无 parked 帧恢复 | 中 | 级联取消 | 逐帧完成；不暴露“恢复”API；文档 | 接受/后续补 |
| R6 | 显式 refresh 仍全量扫描 | 中 | 大 fanout / 频繁 set | dependents 刷新已用 depIndex+dirty；显式 Refresh 全量（P1 可加候选过滤） | 缓解 |
| R7 | ownership 子树递归栈增长 | 中 | 深父子链 / churn | Go 栈 + MaxPluginDepth；P1 显式帧栈 | 接受 |
| R8 | recover 跨 goroutine 无效 | 中 | 用户自起 goroutine | 文档；每个入口 recover | 契约 |
| R9 | lane 合并丢失类型安全 | 中 | 接口服务/any 注册 | 文档；类型校验；测试 | 接受 |
| R10 | 残余语义差：046（无 parked）、049（goid 注册表而非线程 id）、045（goid 迁移）、032（lane 合并） | 中 | 迁移用户/并发取消/热替换 | 已在 GO-DESIGN §7 逐项文档化并标注 N/A 边界；conformance 覆盖可观察行为 | 接受 |

---

## 6. 最终结论

1. **能做**：Go 可以承载完整内核语义；v0 已实现 Context/Service/Fiber/Effect/Events/Registry/
   Config/Logger/Clock/L2 的主体，公开契约与 Rust 高度同构。
2. **不能 1:1 的只有 5 类**：确定性析构（Drop）、弱引用存活语义、线程局部执行帧（thread_local）、
   future drop 取消、TypeId/类型系统；它们分别被显式 Dispose/Abandon、显式状态、goid+ctx、
   ctx 取消、reflect.Type + Go 1.27 泛型方法替代（泛型方法使 typed API 与 Rust 形态一致）。
3. **CORE 51 例中**：46 例可直接按原断言实现（conformance 已覆盖 51/51）；`033` 标记 N/A（GC 探针）；
   `045` 重定义（goid），`027/046/049` 为部分实现（EffectFuture body/parked/recovery）。
4. **性能**：cascade 与 dependents 刷新已用 depIndex+dirty 快路径收敛；显式 refresh 全量在中小树
   可接受；大 fanout/深父子链/高吞吐日志还需显式 Refresh 候选过滤、ownership 显式栈、环形 buffer
   三项优化（路线图）。
5. **工程建议**：保留 `BlockingExecutor` seam、`Abandon`+orphan、9 个 internal hook、lifecycle
   广播、可注入 Clock 等 Rust-only 扩展；把“同步 Dispose 禁止在事件循环 goroutine 调用”写入契约。

→ 具体 API、锁层级、已实现/简化清单与测试映射见 [GO-DESIGN.md](GO-DESIGN.md)。
