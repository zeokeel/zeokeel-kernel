# Go 层设计：cordis-go v0

> **定位**：三件套第三篇，说明 Go 实现的包布局、公共 API、并发/锁层级、生命周期与异步语义、
> 与 Rust 的差异、已知简化、测试策略与 CORE→测试映射。可行性论证见
> [GO-FEASIBILITY.md](GO-FEASIBILITY.md)，Rust 架构见
> [ARCHITECTURE-ANALYSIS.md](ARCHITECTURE-ANALYSIS.md)，逐用例断言见
> [05-core-cases](analysis/05-core-cases.md)。
>
> **状态快照（2026-11-06 14:06，HEAD `401c55f` + 工作区）**：内核主体、`conformance/`（约 150 个测试，
> 覆盖 50/51 CORE，037 待补）、`internal/archtest`、`examples/`（4 个）、CI/Makefile 已落地；
> `context.go`/`fiber.go`/`internal.go` 等仍在并行收尾。本文以工作区当前代码为准，未合并的改动可能
> 带来细节差异；`conformance/` 的实际测试名以仓库为准。

---

## 目录

[0 现状](#0-现状已实现--简化--未实现) · [1 目标](#1-设计目标与约束) · [2 布局](#2-包与文件布局) ·
[3 API](#3-公共-api-清单与命名约定) · [4 并发](#4-并发模型与锁层级) · [5 生命周期](#5-生命周期实现) ·
[6 异步](#6-异步与取消) · [7 差异](#7-与-rust-的差异清单) · [8 路线图](#8-已知简化与路线图) ·
[9 测试](#9-测试策略) · [10 CORE 映射](#10-core-用例--go-测试映射) · [11 结论](#11-结论)

---

## 0. 现状：已实现 / 简化 / 未实现

### 0.1 已实现（v0）

- **Context**：`Root`、`Extend`/`Isolate`/`IsolateLabeled`/`Intercept`/`ExtendMeta`、`(scope,name)`
  服务表、最近 scope 查找、无祖先回退、`ResolveConfig`（base→root→leaf→head）、`FindSchema`。
- **Service**：泛型 `ServiceKey[T]` + `Provide/Get/Require`、`ProvideChecked/ProvideGated/
  ProvideWithSchema`、`ProvideService`（可选 Check/Schema 接口）、`GetNamed`/`SetNamed`/
  `SetRevertibleNamed`、`ServiceAvailable`、`Accessor`/`AccessorWithSetter`、`Meta/MetaAny`、
  `ServiceGate`。
- **Fiber**：六态状态机、`Dispose/DisposeAsync/Abandon`、`Restart/RestartAsync`、`Update/
  UpdateAsync`、`ReplacePlugin/ReplacePluginAsync`、`AwaitSettled(Context)`、`Cancel/Checkpoint`、
  `Heartbeat/IdleFor`、effect 诊断树、`Effects`；Disposed 后 `UID()` 返回 false、
  `AssertActive()` 返回 `ErrInactiveEffect`（CORE-026）。
- **Effect**：单有序列表（sync+async）、逆注册序 drain、单次、首个 panic 重抛、deferred、
  `AttachEffect`、`EffectFuture/EffectStream`（简化）、`AsyncDisposer`（claim/join/cancel restore/
  self-join）、orphan 队列（256）+ 4 个 API。
- **生命周期**：operation token（goid）重入、`LifecycleBusy`、refresh 驱动卸载占槽、两阶段
  withdraw→drain、依赖 dependent 两阶段级联（简化 depIndex 候选）、child ownership effect + sweep +
  detach、`MaxPluginDepth=256`。
- **Registry**：`PluginKey`（Named/Typed）、runtime 去重/回收、快照 map API、`IsIdle`/`WhenIdle`、
  注册窗口（goid 排除自身）、`Refresh/RefreshAsync`（64 轮 + dirty 合并）、`Replace/ReplaceAsync`、
  `DisposeAll`（panic 优先）、`Delete`（删除 runtime 并逐个 dispose fiber）、`SetQuota/Quota`。
- **Events/Waterfall**：sync listener（emit/bail/parallel/serial）、async listener（parallel/serial）、
  once/prepend/global、scope 相等过滤、owner 回收、sync/async waterfall（Next/Terminal/Remaining/
  Args、fail-closed）、9 个 internal hook + lifecycle 广播 + raw `InternalBus`。
- **Config/Logger/Clock/L2**：`ConfigSchema`/`JsonConfigSchema`（五步 validate + 浅 merge）、
  `LogLevel`（Error=0…Debug=3）、ring buffer、exporter、`SlogExporter`、facade clock 快照、
  `SetClock` 仅 root、`FiberQuota`（effects/listeners/child fibers 实测）、`IsolationCapabilities`、
  `StalledFibers`。
- **错误与工程化**：17 个 `ErrCode` + `CordisError`/sentinels/`errors.Is`；`conformance/`（3 包 约 150 测试，
  覆盖 50/51 CORE）、`internal/archtest`（import 边界 + 420 API cap + 自测）、`examples/`（4 个）、
  `.github/workflows/ci.yml`、`scripts/ci.sh`、`Makefile`、`README.md`。

### 0.2 简化/差异（有意为之）

| 项 | v0 行为 | 影响 |
|---|---|---|
| `Drop` 安全网 | 无；显式 `Dispose`/`DisposeAll`/`Abandon` | 丢句柄不清理 |
| `Weak` 存活 | provider 用 `weak.Pointer`，但可用性由 `State()` 判 | 不依赖 GC 时点 |
| 双 lane | 合并为 `any`，无 `ArcWrap`/lane 标记 | CORE-032 接口用例可过，但原 lane 身份规则丢失 |
| 反向依赖索引 | 简化版 `depIndex`（inject 名→fiber ids）仅用于 **cascade**；refresh 仍全量扫描 | cascade 按受影响 dependent 收敛；refresh 大 fanout 仍是 O(F×64) |
| 迭代级联 | 依赖 dependent 已两阶段 `beginCascade`+逆序 drain；ownership 子树仍靠 effect cleanup 递归 | 注入链近 O(1) 帧；父子链依赖 Go 栈增长 |
| `FiberSlots` | slice 线性删除 | 大 fanout 删除 O(n) |
| parked cascade | 未实现；取消卸载逐帧完成 | CORE-046 的 F1/F4/F5 N/A |
| sync `Refresh` 与 async | Pending async 不激活，但 Active async 会被 blocking 卸载 | CORE-037 与 Rust 的“跳过”不一致 |
| `EffectFuture` | 只接收已求值 cleanup | 无 async future body |
| supervision | 始终开启，无 build tag | 默认 API 面更大 |
| 日志 | `log/slog` + `SlogExporter` | 无 tracing span |
| Miri | 无 | CORE-051 断言改写 |

### 0.3 未实现 / 待补

`FiberSlots`/O(1) 删除、refresh 的 dirty/索引快路径（目前只 cascade 用索引）、parked cascade/recovery、
无自带 `block_on`（只需可选 `BlockingExecutor`）、supervision build tag、`Context.SetQuota` 转发；测试侧还缺 CORE-037、
property/redteam/api_surface/blocking 专项、benchmark；`EffectFuture` 的 body 版本。详见 §8 路线图。

---

## 1. 设计目标与约束

1. **可观察契约优先**：状态、顺序、错误变体、计数、清理次数与 Rust 对齐；不追求实现结构等价。
2. **标准库 only**：`go.mod` 无第三方依赖；`weak`（1.24+）、`log/slog`、`reflect`、`context` 可用。
3. **无 unsafe/cgo**：内核不 import `unsafe`、不做 `//go:linkname`；`internal/archtest` 已强制 import 边界 + 420 导出 API cap。
4. **API 亲和 Go**：用泛型自由函数（Go 无泛型方法）、`error` 哨兵 + `errors.Is`、`context.Context`
   作为取消/超时载体、显式 `Dispose` 作为生命周期终点。
5. **锁内不跑用户代码**：这是与 Rust 相同的硬约束；Go 的 Mutex 不可重入且无锁中毒，违反即自死锁。
6. **如实标注简化**：不在文档/注释中承诺未实现的能力（尤其 Drop 语义、反向索引、parked 恢复）。

---

## 2. 包与文件布局

v0 是**单包** `cordis`（模块 `github.com/zeokeel/cordis-go`，Go 1.27）。文件按职责划分：

| 文件 | 职责 | 关键类型/入口 |
|---|---|---|
| `doc.go` | 包文档：五个对象、两维可组合性、Go 设计说明 | — |
| `value.go` | `Value=any`、`Clock`/`SystemClock` | `Clock` |
| `error.go` | 错误模型 | `ErrCode`（17）、`CordisError`、sentinels、`ValidationError`、`CodeOf/IsCode` |
| `contains.go` | panic 遏制基础设施 | `contained`、`containedVoid`、`callContained`、`panicText`、`rethrow` |
| `effect.go` | 同步 effect | `Cleanup`、`Effect`、`Disposer`、`NewDisposer` |
| `async_effects.go` | 异步 effect/取消/孤儿 | `AsyncCleanup`、`AsyncEffect`、`AsyncDisposer`、orphan 4 API |
| `exec.go` | 同步↔异步桥 | `BlockingExecutor`、`SetBlockingExecutor`、`HasBlockingExecutor`、`YieldNow` |
| `runtime_goid.go` | goroutine id（唯一栈检查点） | `currentGoroutineID`、`runtimeGosched` |
| `op_token.go` | 生命周期 token 栈 | `nextOperationToken`、`pushCurrentOpToken`、`currentOpToken` |
| `service.go` | 服务身份/条目/gate/accessor/meta | `ServiceKey[T]`、`ServiceEntry`、`ServiceGate`、`Accessor`、`ContextMeta`、`Service` 接口 |
| `context.go` | Context 树、服务 API、effect/plugin/event 注册、config/logger/internal 包装 | `Context`、`Root`、泛型 `Provide/Get/Require` |
| `config.go` | schema/合并/校验 | `ConfigSchema`、`JsonConfigSchema`、`JsonType` |
| `logger.go` | 日志服务/门面/exporter | `LogLevel`、`LogRecord`、`LoggerService`、`Logger`、`SlogExporter` |
| `events.go` | 事件注册表与派发 | `Events`、`EventOptions`、`EventResult`、`ListenerFunc` |
| `waterfall.go` | continuation 中间件 | `WaterfallNext`、`AsyncWaterfallNext`、`WaterfallTerminal` |
| `internal.go` | 9 个 typed hook + lifecycle | `InternalBus`、`ConfigNext`/`UpdateNext`/`GetNext`/`SetNext`、`LifecycleEvent` |
| `fiber.go` | 状态机/激活/卸载/级联/effect 列表 | `FiberState`、`fiberData`、`Fiber`、`EffectMeta` |
| `registry.go` | runtime/fiber 表/refresh/replace/dispose_all/quiescence | `Registry`、`PluginRuntime`、`PluginKey`、`Plugin`/`AsyncPlugin` |
| `l2.go` | 配额/能力/watchdog | `FiberQuota`、`IsolationCapabilities`、`StalledFiber`、`StalledFibers` |
| `smoke_test.go` | 12 个冒烟测试（根包 `_test`） | 见 §9/§10 |
| `kernel_edge_test.go` | 并行成员补充的边缘测试（工作区，覆盖部分 CORE-006/021/024/039-049） | 见 §9.1 |
| `conformance/lifecycle/` | CORE-001/007-009/011-012/026-029/034-036/038-039/043-047/050-051 | 约 150 个测试之一部分 |
| `conformance/runtime/` | CORE-017-021/023-024/031/049（events/config/logger/clock/quiescence） | — |
| `conformance/services/` | CORE-002-006/010/013-016/022/025/032-033/040-042/048 | — |
| `internal/archtest/` | import 边界 + 420 导出 API cap + 扫描器自测 | `TestKernelBoundary` |
| `examples/{hello,services,events,async_plugin}/` | 四个可运行骨架 | `go run ./examples/...` |
| `.github/workflows/ci.yml` / `scripts/ci.sh` / `Makefile` | CI：gofmt/vet/build/test/`-race` | 见 §9.3 |
| `README.md` | 项目状态、快速开始、文档导航 | — |

模块保持单包可避免循环依赖，也便于 `go doc` 暴露完整 API；如果未来拆分，应保持根包 re-export 兼容。
`conformance`、`internal/archtest`、`examples` 是独立包/目录，不参与内核包的依赖图（archtest 会检查
内核不 import 它们）。

---

## 3. 公共 API 清单与命名约定

### 3.1 命名约定

| 约定 | 规则 | 例子 |
|---|---|---|
| 类型/常量 | 大驼峰；错误码 `CodeXxx` | `ServiceKey`、`CodeLifecycleBusy` |
| 泛型 key | `NewServiceKey[T](name)`；`key.Name()` | `configKey := NewServiceKey[*Config]("config")` |
| 注册/查询 | 包级泛型 `Provide/Get/Require`；无 `ctx.Get[T]` | `cordis.Get(root, configKey)` |
| 生命周期 | `Dispose`/`DisposeAsync(ctx)`/`Abandon`；Fiber 方法 | `fiber.DisposeAsync(ctx)` |
| 事件 | `On/Once/OnAsync/OnWaterfall*`；`Emit/Bail/ParallelAsync/SerialAsync/Waterfall*` | `ctx.On("demo", fn)` |
| 异步 | 接受 `context.Context` 的入口带 `Context` 后缀或 `Async` 后缀 | `WhenIdleContext`、`DisposeAsync` |
| 错误 | 哨兵 `ErrXxx` + 构造函数 `XxxError(...)` + `errors.Is` | `errors.Is(err, cordis.ErrLifecycleBusy)` |

### 3.2 服务 API

```go
type ServiceKey[T any] struct{ /* name */ }
func NewServiceKey[T any](name string) ServiceKey[T]      // + ServiceType[T]() reflect.Type
func Provide[T any](c Context, key ServiceKey[T], value T) (*Disposer, error)
func ProvideChecked[T any](...); ProvideGated[T any](...); ProvideWithSchema[T any](...)
func Get[T any](c Context, key ServiceKey[T]) (T, bool)
func Require[T any](c Context, key ServiceKey[T]) (T, error)
```

| Context 方法 | 语义 |
|---|---|
| `GetNamed(name) (ServiceValue,bool)` | 非严格读 lane（accessor→internal/get→store） |
| `SetNamed(name, value) error` | 受控写入：owner/type/Unloading 校验 + inverse effect + refresh |
| `SetRevertibleNamed(name, value) (*Disposer,error)` | 同 `SetNamed`，但返回可提前撤销的 inverse Disposer |
| `ServiceAvailable(name) bool` | 严格注入门（provider Active） |
| `FindSchema(name)` / `ResolveConfig(name,base,head)` | 最近 scope schema；root→leaf 合并 |
| `Accessor(name,get)` / `AccessorWithSetter(name,get,set)` | accessor 注册（fiber-owned） |
| `MetaAny(key)`（包级 `Meta[T]`） | 沿 parent 链读元数据 |

`ServiceValue=any`；服务值通常是指针（共享可变状态）或接口值。`ProvideService[S Service]` 接受
实现 `Name() string` 的对象，可选实现 `Check() AvailabilityCheck`、`ConfigSchema() ConfigSchema`。

### 3.3 Context API

| 分组 | 方法 |
|---|---|
| 构造/派生 | `Root()`、`Extend()`、`Isolate(name)`、`IsolateLabeled(name,label)`、`Intercept(name,config)`、`ExtendMeta(meta)` |
| 句柄 | `Fiber()`、`FiberID()`、`PluginName()`、`Registry()`、`Events()`、`Clock()`、`SetClock()` |
| effect | `Effect/EffectLabeled`、`EffectAsync/EffectAsyncLabeled`、`EffectFuture`、`EffectStream`、`AttachEffect` |
| 插件 | `Plugin/PluginAsync`、包级 `PluginTyped/PluginAsyncTyped`、`Inject(deps, callback)` |
| 事件 | `On/OnWith/OnWithOptions/Once/OnAsync/OnAsyncWith/OnWaterfall/OnWaterfallAsync`、`Emit/Bail`、`ParallelAsync(Context)`、`SerialAsync(Context)`、`Waterfall/WaterfallAsync` |
| 生命周期 | `Refresh/RefreshAsync`、`DisposeAll`、`WhenIdle/WhenIdleContext`、`Checkpoint` |
| 观测 | `OnLifecycle`、`OnInternal*`、`Logger/LoggerNamed/LoggerService/LogExporter` |

### 3.4 Fiber API

| 分组 | 方法 |
|---|---|
| 身份/状态 | `Name/ID/UID/Parent/State/IsActive/AssertActive/Context` |
| 配置/错误 | `Config/RawConfig/Error/ErrorDetail` |
| 诊断 | `Effects/AwaitSettled/AwaitSettledContext/Heartbeat/LastActivityMs/IdleFor` |
| 生命周期 | `Dispose/DisposeAsync/Abandon/Restart/RestartAsync/Update/UpdateWithNoSave/UpdateAsync/UpdateAsyncWithNoSave` |
| 替换 | `ReplacePlugin/ReplacePluginAsync` |
| 取消 | `Cancel/IsCancelled` |

语义提醒：`Restart/Update/ReplacePlugin` 返回 `Ok` **不代表 Active**；必须查 `State()/ErrorDetail()`。
`Dispose/DisposeAsync` 返回 `ErrLifecycleBusy`；`Dispose` 会重抛首个 cleanup panic（dispose_all 会收集
并 panic 优先）。

### 3.5 Registry API

`NewRegistry`、`Size/Get/Has/Delete/Keys/Values/Entries/ForEach/Fibers/FiberCount/FiberByID`、
`IsIdle/WhenIdle/WhenIdleContext`、`Refresh/RefreshAsync`、`Replace/ReplaceAsync`、`DisposeAll`、
`SetQuota/Quota`、`StalledFibers`、`NextScopeID/LabeledScopeID`、`BeginRegistration`（返回 guard，
`defer guard.End()`）。

**快照语义**：每个方法只锁一个结构，跨调用可短暂不一致；需要一致视图用 `IsIdle/WhenIdle`。
`Delete` 只删除空 runtime（与 Rust 不同）；`Keys` 排序保证确定性。

### 3.6 Events / Waterfall / Internal API

- `NewEvents()`、`Events.Internal()`、`ListenerCount/AsyncListenerCount/WaterfallListenerCount`；
  注册/派发通过 Context 包装（owner=fiber id）。
- `EventOptions{Prepend,Global,Once}`、`EventResult{Continue,Bail}`、`ListenerFunc`、
  `AsyncListenerFunc`。
- `WaterfallNext.Args/Remaining/Next/Terminal`；`AsyncWaterfallNext` 另有 `NextWith/TerminalWith`；
  `NullWaterfallTerminal/NullAsyncWaterfallTerminal`、`WaterfallListener/WaterfallTerminalOf`。
- `InternalBus.On*`（调用方自持 Disposer）与 `Context.OnInternal*`（fiber-owned）；`LifecycleEvent
  {FiberID,Name,From,To,Seq}`；`ConfigNext/UpdateNext/GetNext/SetNext`。

### 3.7 插件接口与可选元数据

```go
type Plugin interface {
    Name() string
    Activate(ctx Context, config Value) error
}
type AsyncPlugin interface {
    Name() string
    ActivateAsync(ctx context.Context, c Context, config Value) error
}
type PluginKeyer interface{ PluginKey() PluginKey }        // 可选：去重身份
type PluginInjector interface{ Inject() []string }          // 可选：依赖门
type PluginConfigSchema interface{ ConfigSchema() ConfigSchema } // 可选：schema
```

元数据回调（`Name/PluginKey/Inject/ConfigSchema`）全部用 `contained` 包裹；panic 降级为占位值，
不会从 `ctx.Plugin` 逃逸。`Inject` 返回的切片会被拷贝/去重（避免调用方复用切片造成串扰）。

### 3.8 错误模型

`ErrCode`（17 个，与 Rust 变体一一对应）：

`CodeInactiveEffect`、`CodeDuplicateService`、`CodeUnknownService`、`CodeInvalidPlugin`、
`CodePluginFailed`、`CodeDisposed`、`CodeValidation`、`CodeServiceWriteForbidden`、
`CodeAccessorReadOnly`、`CodeAsyncPluginNeedsAsync`、`CodeCancelled`、`CodeQuotaExceeded`、
`CodeLifecycleBusy`、`CodeServiceTypeMismatch`、`CodeRollback`、`CodePlugin`、
`CodeAccessorWriteRejected`（共 17 个）。

`CordisError` 携带 code + name/detail/kind/validation/cause；`Is` 按 code 匹配，因此
`errors.Is(err, ErrLifecycleBusy)` / `errors.Is(err, ErrDuplicateService)` 可匹配任意同类错误；
`CodeOf/IsCode` 用于分支；`PluginError(plugin,kind,message)` 是结构化插件错误，内核**原样传播**
（不 `%w` 包装），`ErrorDetail()` 读取。`ValidationError` 的 `Error()` 文本对齐 Rust
（`invalid config:\n  - issue`）。

### 3.9 日志/配置/时钟/L2

- `LogLevel`：`LogError=0 < LogWarn < LogInfo < LogDebug`；`ParseLogLevel` 支持 `"warning"`；
  `LogLevelFromU8` 对 >3 **clamp 到 Error**；`LoggerService` 有 ring buffer（默认 1000，0 禁用）、
  exporter 列表、per-exporter 阈值；`Logger` facade 快照 clock；`SlogExporter` 桥接 `log/slog`。
- `ConfigSchema{Validate(Value)(Value,error); Merge(base,overrides) Value}`；`JsonConfigSchema`
  builder（`Required/DefaultValue/DenyUnknown/Property`）；`JsonType` 五类。
- `Clock{NowUnixMs() int64; NowMonotonicMs() int64}`；`SetClock` 仅 root（`FiberID()==0`），
  写槽 + 更新活 fiber；facade 创建时快照。
- `FiberQuota{MaxEffects,MaxListeners,MaxChildFibers int}`（0=无限）；`DetectIsolationCapabilities()`；
  `Registry.StalledFibers(timeout)` 只观测。

---

## 4. 并发模型与锁层级

### 4.1 锁清单

| 锁 | 保护 | 规则 |
|---|---|---|
| `fiberData.mu` | state/plugin/config/effects/children/depVersions/parent/ctx | **持有时不调用用户代码**；快照后释放 |
| `fiberData.lifecycleMu` | lifecycleOwner/token | 只做 token 认领/释放；不跨用户代码 |
| `contextInner.accessorMu` | accessors map | getter/setter 在锁外调用 |
| `serviceStore.mu` | 服务表 | 只做 get/insert/delete；check/事件/refresh 在锁外 |
| `clockSlot.mu` | tree clock | 短暂持有 |
| `Events.mu` | listeners/waterfall 三表 | 取快照后释放；派发无锁 |
| `InternalBus.hookList.mu` | 每类 hook 列表 | snapshot 后锁外执行 |
| `Registry.mu` | runtimes/byID/root | 拓扑锁；不在持有时跑用户代码；`PluginRuntime.mu` 只允许在其内单向嵌套 |
| `Registry.fibersMu` | 全局 fiber 表 | 与 `Registry.mu` 不嵌套；快照后锁外 |
| `Registry.depMu` | depIndex/fiberDeps | **叶子锁**；`dependentsOfNames` 持 depMu 取 id 后释放，再取 `Registry.mu`（单向） |
| `Registry.regMu`/`idleMu`/`refreshMu`/`scopeMu`/`quotaMu` | 注册窗口/等待/refresh 熔断/scope/quota | 短临界区；`refreshMu` 是 TryLock |
| `PluginRuntime.mu` | runtime fibers | 单向：`Registry.mu → PluginRuntime.mu` |
| `LoggerService.exporters`/`buffer` | exporter 表/ring | exporter 锁外调用；buffer 短临界区 |
| `Disposer.mu`/`AsyncDisposer.mu`+`asyncDisposeState.mu` | cleanup 列表/join 状态 | drain 前 take 列表；不在锁内跑 cleanup |

**顺序**：`fiber.mu → effectNode.mu`（诊断快照）；`Registry.mu → PluginRuntime.mu`；
`serviceStore.mu` 不嵌套 `fiber.mu`（entry clone 后判 provider 状态）；`Events.mu`/`hookList.mu` 都是
叶子锁（先取快照再回调）。**禁止** `fiber.mu` ↔ `registry.mu` 反向嵌套；**禁止**持有任何锁调用
`Plugin/check/accessor/hook/cleanup/schema/exporter`。

### 4.2 纪律

1. 所有锁用 `defer Unlock()`（panic 安全）；Go 无锁中毒，panic 后未解锁会永久死锁。
2. 调用用户代码前先 clone/快照（entry、listener、hook、effect slice），释放锁再调用；
   `serviceStore.get` 返回 entry 快照，`isAvailable` 在锁外执行 check。
3. `transition` 在 fiber 锁内更新状态+取 seq/ctx 快照，锁外发 status/lifecycle/notifyIdle。
4. `refreshPass` 对每个 fiber 取 `(ctx,state)` 快照后调用 activate/unload，不在 registry 锁内；
   `refreshAll` 用 `refreshMu.TryLock`+`refreshDirty` 合并重入，最多 64 轮。
5. 事件派发取快照后无锁执行；`parallel_async` 的 goroutine 只持有不可变快照。
6. `AsyncDisposer` 的 claim 保证只有一个 drain owner；其他调用 join/等待（见 §6）。

### 4.3 goid operation token 机制

- `currentGoroutineID()` 用 `runtime.Stack` 解析（`runtime_goid.go`）；**唯一**读取 goroutine 身份的地方，
  仅用于“是否同一在飞操作”，不用于资源所有权。
- `pushCurrentOpToken(token)` 把 token 压入 `sync.Map[goid]*goroutineOps` 的栈；返回 restore。
- `fiberData.beginLifecycleOp`：若当前 goid 的栈顶 token == 该 fiber 的 lifecycleOwner，则 `reentrant=true`
  直接复用；否则 CAS 式认领；已被占用则 `ErrLifecycleBusy`。
- `withLifecycle` 安装 token + `beginOperation` + `defer` 释放；`unloadForRefresh` 也安装 token（v0.6
  语义：refresh 卸载占槽）。
- **限制**：async 操作若在另一个 goroutine 恢复，token 栈不可见 → Busy；这是 Go 侧与 Rust 的主要差异
  （CORE-045）。文档需建议：不要在 cleanup 之外依赖“同线程重入”。

### 4.4 竞态契约

- 内核数据结构由上述锁保护；用户提供的服务值/回调**可能被多 goroutine 访问**（事件异步 lane、刷新、
  生命周期入口），必须自行保证线程安全。
- 所有测试与示例应 `go test -race`；CI 默认执行 `go test -race ./... -timeout 10m`。
- `Events` 的 once 用 `atomic.Bool.CompareAndSwap`，并发派发至多执行一次。

---

## 5. 生命周期实现

### 5.1 状态机映射

v0 与 Rust 同为六态（`Pending/Loading/Active/Failed/Unloading/Disposed`），`Disposed` 终态、
`Failed` 稳定；root 为 `Active`（id=0）。`transition` 拒绝同态与从 Disposed 迁出；`statusSeq`
从 0 递增；`transitionIfEpoch` 用于 XT-02。迁移图与 Rust 一致，见
[ARCHITECTURE-ANALYSIS §4.2](ARCHITECTURE-ANALYSIS.md#42-六态状态机)。

### 5.2 激活（`createFiber` + `activateSync/Async`）

`ctx.Plugin*`：`BeginRegistration`（窗口，guarded）→ `createFiber`（父状态检查、深度守卫、
`quota.MaxChildFibers`、分配 id、构造 fiberData、registerFiber/registerRuntimeFiber、
registerChildOwnershipEffect、emitPlugin）→ `beginOperation` → 若 `dependenciesReady` 则 activate。

`activateSync`：取 plugin/rawConfig 快照 → beginOperation → beginActivationEpoch → transition(Loading)
→ internal/config waterfall → validateConfig（失败→Failed）→ `activatingEpoch=epoch` → 执行
`Activate` 并 recover → 清 `activatingEpoch` → 分支：panic/Err/成功。成功分支查 stale → invalidated →
dependenciesReady（不满足则 `rollbackActivationToPending`）→ recordDepVersions → transition(Active) →
Refresh。`activateAsync` 同构；ctx 取消时丢弃本 epoch 并回 Pending。

### 5.3 卸载与级联（`unload`/`beginCascade`）

1. `beginUnload`：锁内取 effects+deferred+ctx；已 Disposed 且有残留→retry 分支（只排干 tail）；
   否则 transition(Unloading) → withdrawOwnerServices → Refresh（依赖方先失活）。
2. `beginCascade(frontier)`：用 `depIndex` 从 withdrawn 服务名出发取候选（`dependentsOfNames`），对
   Active 且 !dependenciesSatisfied 的 fiber `beginUnload` 并收集 frame；新 frame 的 withdrawn 继续
   frontier，`visited` 去重；**不递归**。随后**逆序 drain frames**（dependent-before-provider），再 drain
   本 fiber。cleanup panic/error 被 contained。
3. drain：`drainEffectsBlocking`（sync；async 经 runBlocking/disposeBlockingContained）或
   `drainEffectsAsync`（取消时剩余 async 进 deferred，sync inline）。
4. `finishUnload`：removeOwner → transition(target)；若 Disposed：父 detachChildOwnership、断
   ctx/parent、unregisterRuntimeFiber/removeFiber；notifySettle/notifyIdle。

**顺序**：withdraw 在 drain 前（CORE-042）；dependents 在 provider cleanup 前；child 在 parent 前
（ownership effect 逆序 + child sweep 兜底）。**差异**：注入依赖级联是两阶段迭代；ownership 子树仍通过
effect cleanup 递归 `child.Dispose()`，深父子链依赖 Go 栈增长；非根帧不 claim lifecycle slot（与 Rust
相同残余窗口）。

### 5.4 refresh

`Refresh`：`refreshMu.TryLock`（失败置 dirty 返回）→ 最多 64 轮：`refreshDirty=false`，`refreshPass`
遍历快照；Active 且 !satisfied→`unloadForRefresh(Pending)`（claim lifecycle，busy 跳过）；Pending 且
ready→activate；有 changed 或 dirty 则继续。`RefreshAsync` 用 `refreshPassAsync`。v0 的 refreshPass
**不区分 async fiber**（Rust 的 sync refresh 跳过全部 async）；同步模式不会激活 Pending async，但
Active async 会被 blocking 卸载（路线图 P0）。

### 5.5 busy/reentrancy

- 同 goroutine cleanup 内 `fiber.Dispose()`：`beginLifecycleOp` 见栈顶 token==owner → reentrant，允许
  （已验证 `TestReentrantDisposeFromCleanup`）。
- 其他 goroutine 并发 dispose/restart/update：`ErrLifecycleBusy`，不排队；owner 完成后可重试。
- `Dispose/DisposeAsync` 先查 Disposed+无残留→Ok（幂等）。
- refresh 卸载也占 slot，busy 则本轮跳过；`Abandon` 在 busy 时只 log 跳过。

### 5.6 dispose / dispose_async / abandon

| 入口 | 行为 |
|---|---|
| `Dispose()` | withLifecycle+beginOperation+unload(Disposed,false)；首个 cleanup panic 在 drain 后重抛；幂等 |
| `DisposeAsync(ctx)` | 同上但 async；ctx 取消→剩余 async 进 deferred，fiber 仍落 Disposed，返回 ctx.Err() |
| `Abandon()` | 显式“丢句柄”：取 token→beginUnload（含 withdraw/refresh）→逆序 sync cleanup；async 有 executor 则 DisposeBlocking，否则 orphan；finishUnload(Disposed)；busy 时 log 跳过 |

### 5.7 restart / update / replace

- `Restart`：withLifecycle→beginRestart（Disposed→Err、Unloading→InactiveEffect、sync+async→
  AsyncPluginNeedsAsync）→unload(Pending)→config=rawConfig→依赖 ready 则 activate（激活管线重新
  resolve+validate）。
- `Update`：resolveUpdate（internal/config→validate→internal/update，terminal 至多一次，替换值先校验，
  veto→Ok 不重启）→需要时 unload(Pending)+activate；noSave 只透传 hook。
- `ReplacePlugin`：swapPluginMetadata（plugin/name/inject/schema/key+rebindFiber）→unload(Pending)→
  依赖 ready 则 activate；失败 restorePluginMetadata+reloadAfterRestore。
- `Registry.Replace`：resolveMetadata→找 old runtime（空/不存在→InvalidPlugin）→bindReplacement（删
  old key、merge/new target、逐 fiber swap+rebind）→逐 fiber Restart；任一失败→重启已 reload 的 fiber+
  undoBind。v0 的 undo 重建 runtime 对象（不保留原 runtime 身份）；**swap/restore 改变 inject 后
  未调用 `reindexFiberDeps`**，替换后 depIndex 可能保留旧边（已知缺口，见 §8）。

### 5.8 静默

- `IsIdle`：Registry.pendingOps>0→false；每个 live fiber state∉{Loading,Unloading} 且 pendingOps==0。
- 注册窗口：BeginRegistration 增 pendingOps 与 regOwners[goid]；End 幂等、按 goid 释放。
  `WhenIdleContext` 用 `isIdleExcluding(registrationCountFor(goid))` 排除当前 goroutine 的注册；外部
  waiter 完整等待（CORE-049 的 C4）。
- `WhenIdle` 等待 idleCh（close/recreate 广播）；ctx 取消返回 ctx.Err()。
- **未实现** recovery-aware 循环（无 parked 帧）；两线程互相等待注册窗口仍可死锁（与 Rust 相同）。

---

## 6. 异步与取消

### 6.1 AsyncDisposer

```text
DisposeAsync(ctx):
  loop: if claim(): return drain(ctx)            // 唯一 owner
        if drainingInContext(ctx,d): return nil  // self-join 短路
        wait state.waitChan() or ctx.Done(); if !cancelled: return nil
        // cancelled teardown restored cleanups → 重新 claim 重试
drain(ctx): take cleanups; 逆序逐项：
    if ctx.Err()!=nil: restore(remaining); state.cancel(); return ctx.Err()
    callContained(cleanup(withDrainFrame(ctx,d)))   // cleanup 内再 Dispose 可 self-join
    panic→记 first 继续；err→记 firstErr 继续
  state.finish(); if firstPanic!=nil: panic(first)
DisposeBlocking(ctx): self-join/settled 短路，否则 DisposeAsync(ctx)
```

**语义**：单次、逆序、顺序执行不并发、join 不重复、未启动恢复（S-1）、in-flight cleanup 不可恢复
（与 Rust A-9 边界一致）、self-join 需要把 cleanup 收到的 ctx 传回。`IsDisposed` = settled && 列表空 &&
!cancelled。`discard` 用于父摘除已 dispose 的 child ownership effect。

### 6.2 deferred / parked / orphan

- **deferred**：`drainEffectsAsync` 在 ctx 取消时把未启动 async 放回 `fiberData.deferredAsync`，下次
  `Dispose/DisposeAsync` 先排干（`beginUnload` 的 retry 分支）。
- **parked**：v0 **没有** Registry 级 parked 帧；每个 fiber 的取消卸载都自行落到目标态。因此没有
  `when_idle` recovery 循环，也没有 `recover_parked_unloads` 公开 API。这是与 Rust 的有意差异
  （CORE-046 的 F1/F4/F5 不适用），代价是取消路径仍可能执行清理/阻塞，且不能“稍后由另一个入口恢复”。
- **orphan**：仅 `Abandon` 在无 `BlockingExecutor` 时把 async effect 放进有界队列（cap 256）；
  `OrphanAsyncCleanupsPending/OrphanAsyncCleanupCount/OrphanAsyncCleanupFailureCount` 可观测，
  `DrainOrphanAsyncCleanups()` 在任意可阻塞 goroutine 调用（先整体 take 再逐个 dispose，panic/err 计数）。

### 6.3 BlockingExecutor

`BlockingExecutor{BlockOn(ctx, fn)}`；`SetBlockingExecutor` 首次安装胜（`sync.RWMutex`）。
`runBlocking` 未安装时直接 inline；`Dispose`/`disposeBlockingContained` 通过它跑 async cleanup。
Go 没有 reactor 线程 park 死锁的同一问题，因此默认 inline 是合理选择；保留 seam 供宿主统一调度/
观测。**契约**：同步 Dispose 不应在事件循环 goroutine 调用（文档标注）；否则宿主应安装 executor 或
改用 `DisposeAsync`。

### 6.4 取消语义边界

| 场景 | v0 行为 |
|---|---|
| 激活 ctx 取消 | 丢弃本 epoch effects/children，回 Pending；不置 Failed |
| `DisposeAsync` ctx 取消 | sync inline、未启动 async 进 deferred、fiber 落 Disposed；返回 ctx.Err() |
| `AsyncDisposer.DisposeAsync` ctx 取消 | 未启动 restore、state.cancel、下次重试；in-flight 丢失 |
| 级联取消 | 每个 dependent 各自落 Pending/Disposed，无 parked 帧 |
| `Abandon` | 无 ctx；sync 立即，async 有 executor 则阻塞否则 orphan |
| 插件长任务 | `fiber.Cancel()` + `ctx.Checkpoint()` 协作退出（内核不强杀） |

---

## 7. 与 Rust 的差异清单

| # | 主题 | Rust | Go v0 | 级别 |
|---|---|---|---|---|
| 1 | 资源释放 | `Drop` 最后句柄自动清理 | 显式 `Dispose/DisposeAll`；`Abandon` 兜底 | L2 |
| 2 | 丢 fiber 句柄 | `FiberData::drop` 安全网 | 无动作（GC 回收内存，不跑清理） | L2 |
| 3 | 弱引用存活 | `Weak::upgrade` → None 作为 fail-open | `weak.Pointer` + 显式 `State()`；不依赖 upgrade 时机 | L1/L2 |
| 4 | future drop 取消 | 隐式 drop 点 + CancelSafe 收尾 | `ctx.Done()`；未启动 restore、in-flight 丢失；无 parked | L2 |
| 5 | BlockingExecutor | 同步 dispose 默认 thread-park block_on；CORE-034 必走 executor | 默认 inline；安装后同步卸载走 executor（CORE-034 已覆盖）；无 block_on | L1 |
| 6 | supervision | `#[cfg(feature)]`，默认关闭 | 始终开启，无 build tag | L1 |
| 7 | 反向依赖索引 | `DepIndex` + dirty 快路径 | cascade 用简化 `depIndex`；refresh 仍全量 | L1（性能） |
| 8 | 迭代级联 | 显式 DFS 栈，O(1) 调用栈 | dependent 两阶段迭代；ownership 子树递归 | L1（结构） |
| 9 | FiberSlots | tombstone + positions，O(1) 删除 | slice 线性删除 | L1（性能） |
| 10 | 双 lane | sized/any + `ArcWrap` TypeId | `any` 合并；无 lane 标记 | L2 |
| 11 | TypeId/泛型 | 泛型方法 + TypeId | 泛型自由函数 + reflect.Type | L2 |
| 12 | 生命周期 token | thread/task-local + op token | goid token 栈；async 迁移 goroutine 丢失重入 | L2 |
| 13 | activation epoch | 嵌套 frame 栈 + 每 poll 安装 | fiber `activatingEpoch` 字段（近似） | L1/L2 |
| 14 | `when_idle` | recovery-aware；ThreadId 排除自身注册 | 无 recovery；goid 排除自身注册 | L2 |
| 15 | `Delete` | 删除 runtime 并 dispose fibers | 已对齐：删除 runtime 并逐个 dispose fiber | L0 |
| 16 | `UID/AssertActive` | Disposed 后 uid=None；InactiveEffect | 已对齐：UID 返回 false；AssertActive 返回 ErrInactiveEffect | L0 |
| 17 | `set_revertible` | 公开返回 Disposer | 已对齐：`SetRevertibleNamed` 返回 inverse Disposer | L0 |
| 18 | 日志 | `tracing` | `log/slog` + `SlogExporter` | L1 |
| 19 | panic=abort 守卫/Miri | 编译期 + Miri | 无；`-race`/架构测试替代 | L3 |
| 20 | 深度限制 | 256（fiber parent chain） | 相同（`MaxPluginDepth=256`） | L0 |
| 21 | 事件 async options | async lane 只有 prepend | `OnAsyncWith(prepend,global)` 也支持 global | L1 |
| 22 | `Entries` | `Vec<(key,runtime)>` | `map[PluginKey]*PluginRuntime` | L1 |
| 23 | sync Refresh 与 async | `refresh()` 跳过所有 async fiber | Pending async 不激活，但 Active async 会被 blocking 卸载 | L2 |

---

## 8. 已知简化与路线图

**已完成（本快照）**：CORE-022 `Delete` 隐式 dispose、CORE-026 UID/AssertActive、CORE-034
BlockingExecutor、CORE-041 `SetRevertibleNamed`；`conformance/`、`internal/archtest`、`examples/`、
CI/`-race` 矩阵均已落地。

| 优先级 | 项 | 现状 | 计划 |
|---|---|---|---|
| **P0** | sync `Refresh` 跳过 async fiber | 会 blocking 卸载 Active async（CORE-037） | `refreshPass` 对 `isAsync` 直接跳过（Rust 语义）；补 CORE-037 测试 |
| **P0** | Replace 后重索引 | `swapPluginMetadata`/`restorePluginMetadata` 改变 inject 但未 `reindexFiberDeps` | 在 swap/restore 后调用重索引；补替换后 cascade 的 conformance |
| **P0** | internal hook panic fallback | `internal.go` 正在并行收尾 | 以最终代码为准，补 conformance panic 矩阵（CORE-021/containment） |
| **P1** | 索引快路径 | cascade 用简化 `depIndex`；refresh 全量 O(F×64) | 增加 `service_dirty`/候选过滤，保留“索引不决定可用性”原则 |
| **P1** | `FiberSlots` | slice 删除 O(n) | slots/positions/live + compact |
| **P1** | 显式栈级联 | dependent 已两阶段迭代；ownership 递归 | 为 child ownership 也用显式帧栈，恢复 O(1) 契约 |
| **P1** | `EffectFuture` body | 只接收已求值 cleanup | `EffectFuture(func(ctx)(AsyncEffect,error), label)`，失败→activation rollback |
| **P2** | parked/recovery | 无 | 仅在确认“取消卸载必须可暂停”后再引入；否则保持逐帧完成 |
| **P2** | supervision 打包 | 始终开启 | 可选 `l2` 子包/build tag + 两套 CI |
| **P2** | parallel async 限流 | 每 listener 一 goroutine | 可选 worker pool/上限，避免 goroutine 爆炸 |
| **P2** | logger ring buffer | 每记录 O(buffer) 拷贝 | head/size 环形索引 |
| **P2** | 属性/redteam/blocking/benchmark | 未落地 | `property_test.go` 固定 seed、`redteam_test.go` 去重场景、`blocking_test.go`（进程级 executor）、benchmark |
| **P2** | GC 安全网 | 无 | 评估 `runtime.AddCleanup` 仅做 orphan 兜底（非确定性，不承诺） |

---

## 9. 测试策略

### 9.1 现状

- `conformance/`（约 150 测试，3 包，只依赖公共 API）：`TestCoreNNN_*` 覆盖 50/51 CORE；唯一缺口是
  CORE-037（sync Refresh 与 async）。
- `smoke_test.go`（12 个）覆盖主路径；`kernel_edge_test.go`（15 个）覆盖 LifecycleBusy/重入、replace、
  when_idle 窗口、AsyncDisposer 取消等边缘。
- `internal/archtest`（2 个）：内核 import 边界（标准库白名单、禁 IO/网络/业务词/自引用）+ 420 导出
  API cap + 扫描器自测；`examples/` 4 个骨架；CI 跑 gofmt/vet/build/test/`-race`。
- **缺口**：CORE-037；property/redteam/api_surface/blocking 专项；benchmark。

### 9.2 conformance 目录与 CORE 归属（实际）

```
conformance/lifecycle/   async_test(034/043/046)  effects_test(007-009/011-012/030)
                         fiber_test(001/026-029/035-036/038-039/044-045/047/050-051)
conformance/runtime/     config_logger_test(023/024)  events_test(017-021/031)
                         quiescence_test(049)
conformance/services/    context_test(003/025/040)  registry_test(022/048)
                         service_test(002/004-006/010/013-016/032-033/041/042)
                         helpers_test（watchdog/newRoot/trace）
```

每个测试包各有 `helpers_test.go`（`newRoot`、`waitFor`、`assertState`、`assertOrder`、watchdog）；
CORE-037 尚无测试（P0）。

### 9.3 纪律与工具

- **CI**：`scripts/ci.sh` 依次跑 `gofmt -l`、`go vet ./...`、`go build ./...`、
  `go test ./... -count=1`、`go test -race ./... -timeout 10m`；`.github/workflows/ci.yml` 调用同一脚本，
  `Makefile` 提供 `make check/build/test/race/cover/ci`。**`-race` 是默认门禁**，不是可选项。
- **watchdog**：并发/取消/级联/quiescence 用例用 `waitFor`/`time.AfterFunc`，避免死锁永久挂起。
- **确定性**：`parallel_async` 不断言 interleaving；只断言 sync-before-async、Bail 忽略、每 listener
  至多一次；用 channel 闸门控制交错，不用 `Sleep` 猜时序。
- **fake clock**：idle/heartbeat/logger 时间断言用注入 `Clock`；`SetClock` 后重建 logger facade。
- **JSON 比较**：统一 `map[string]any`/`[]any`，数值规范化为 `float64`（或 canonicalize），避免
  `json.Number`/`float64` 的 `DeepEqual` 陷阱。
- **泄漏基线**：`runtime.NumGoroutine` + registry 空 + 二次 dispose 幂等；CORE-033 用 GC/finalizer
  探针（允许不稳定，超时失败）。
- **panic**：断言 drain 后重抛**同一个** recover 值（不包装）；waterfall 断言 re-panic。
- **架构测试**：`internal/archtest` 已实现 import 黑名单、标准库判定、导出自引用、420 API cap 与
  bypass 自测；不依赖 `go/types` 的完整 AST 分析（可按需扩展）。

---

## 10. CORE 用例 → Go 测试映射

> 现状列：`conf` = `conformance/` 已覆盖；`部分` = 覆盖主路径但边界/语义有差异；`—` = 待补。
> 实际测试名为 `TestCoreNNN_<短名>`；文件路径省略 `conformance/` 前缀与 `_test.go` 后缀。
> CORE 断言细节以 [05](analysis/05-core-cases.md) 为准。

| CORE | 断言焦点 | conformance 位置 | 现状 |
|---|---|---|---|
| 001 | 激活/卸载/清理/幂等 | lifecycle/fiber | conf |
| 002 | 兄弟 provider 共享 | services/service | conf |
| 003 | isolate 遮蔽/无回退 | services/context | conf |
| 004 | 重复 provide 拒绝 | services/service | conf |
| 005 | inject 门/Pending | services/service | conf |
| 006 | check gate 翻转 | services/service | conf |
| 007 | 跨 lane 逆序 | lifecycle/effects | conf |
| 008 | disposer 单次 | lifecycle/effects | conf |
| 009 | sync 等 async cleanup | lifecycle/effects | conf |
| 010 | provider 卸载反注册 | services/service | conf |
| 011 | async cleanup panic | lifecycle/effects | conf |
| 012 | effect 树 live | lifecycle/effects | conf |
| 013 | checked 查询/依赖 | services/service | conf |
| 014 | 关闭 gate 不回退 | services/service | conf |
| 015 | check 重入 | services/service | conf |
| 016 | 依赖移除卸载 | services/service | conf |
| 017 | listener 随 fiber | runtime/events | conf |
| 018 | prepend/global/once/bail | runtime/events | conf |
| 019 | scope 相等过滤 | runtime/events | conf |
| 020 | waterfall 全语义 | runtime/events | conf |
| 021 | 9 internal hooks | runtime/events | conf |
| 022 | map API/dedupe/Delete | services/registry | conf |
| 023 | logger/clock | runtime/config_logger | conf |
| 024 | schema/update | runtime/config_logger | conf |
| 025 | accessor/meta/labeled | services/context | conf |
| 026 | uid/parent/settled | lifecycle/fiber | conf |
| 027 | async effect body/stream | lifecycle/fiber | 部分（EffectFuture 无 body） |
| 028 | 激活失败回滚 | lifecycle/fiber | conf |
| 029 | apply panic 隔离 | lifecycle/fiber | conf |
| 030 | cleanup panic | lifecycle/effects | conf |
| 031 | listener panic | runtime/events | conf |
| 032 | 接口服务（lane 合并） | services/service | conf |
| 033 | 无强环 | services/service（GC 探针） | 改写 |
| 034 | BlockingExecutor | lifecycle/async | conf |
| 035 | AsyncPlugin 生命周期 | lifecycle/fiber | conf |
| 036 | async 失败回滚 | lifecycle/fiber | conf |
| 037 | sync refresh 不碰 async | — | **待补（P0）** |
| 038 | inline async drain | lifecycle/fiber | conf |
| 039 | restart 重查 gate | lifecycle/fiber | conf |
| 040 | raw config 不复合 | services/context | conf |
| 041 | set 可逆/版本 | services/service | conf |
| 042 | withdraw 顺序 | services/service | conf |
| 043 | 并发 async join | lifecycle/async | conf |
| 044 | Disposed/lifecycle | lifecycle/fiber | conf |
| 045 | LifecycleBusy/重入 | lifecycle/fiber | conf |
| 046 | 取消恢复/self-join | lifecycle/async | 部分（parked N/A） |
| 047 | child ownership/级联 | lifecycle/fiber | conf |
| 048 | replace 身份/回滚 | services/registry | conf |
| 049 | quiescence | runtime/quiescence | 部分（无 recovery） |
| 050 | cancel/checkpoint | lifecycle/fiber | conf |
| 051 | dispose_all | lifecycle/fiber | conf |

> 说明：`conformance/` 实际共 约 150 个测试，覆盖 50/51 CORE；CORE-037 待 P0 修复后补测。
> CORE-033 的 Rust 断言是“无强环”，Go 用 GC/finalizer 探针改写；CORE-046/049 的 parked/recovery
> 部分在 Go 中 N/A（见 §6.2）。测试断言以仓库当前版本为准。
---

## 11. 结论

v0 用约 8.1k 行 Go + 7.5k 行 conformance 实现了 Rust 内核的全部**组合原语**与大多数可观察语义；
主要简化集中在**确定性析构、Weak 存活、thread-local 执行帧、future drop 取消、TypeId/泛型**五处，
以及**refresh 索引快路径/parked recovery/FiberSlots** 三项性能/边界设施；均有显式替代或 N/A 标注。
下一步优先补 **CORE-037**（sync Refresh 跳 async）与剩余专项测试（property/redteam/blocking/bench），
再按需引入 P1 索引/显式栈；保持“代码现状 ↔ 文档承诺 ↔ 测试断言”三者一致。

→ 语言机制与风险见 [GO-FEASIBILITY.md](GO-FEASIBILITY.md)；
→ Rust 语义与证据见 [ARCHITECTURE-ANALYSIS.md](ARCHITECTURE-ANALYSIS.md)。
