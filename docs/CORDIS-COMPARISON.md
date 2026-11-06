# 官方 Cordis（TypeScript 4.1.0） vs Rust 参考 vs 本 Go 端口：特性差异与「通用内核」评估

> 本文回答两个问题：
> 1. **除语言差异之外，本 Go 内核和 Cordis 官方方案在特性上到底差什么？**
> 2. **「一个内核接入所有插件系统、任何系统都能用它累加模块拼成完整系统」这个目标，当前内核能不能承担？还缺什么？**
>
> 对照基线：
> - **官方 TS**：`@deepseek-ai/cordis` **4.1.0**（npm 包源码 `src/{context,fiber,registry,service,events,reflect,logger,utils}.ts`，2026-11 最新；生态包 loader/include/group/hmr/timer/logger-console 独立发布）
> - **Rust 参考**：`zeokeel/zeokeel-cordis` **v0.6.0**（明确对照官方 **4.0.5** 语义，差异见其 `spec/CORDIS-CORE.md` §5 与 `docs/CORDIS-RUST.md` §1）
> - **本 Go 端口**：v0（对照 Rust v0.6.0 + Go 1.27 语言特性；本文写于最终实现状态）

---

## 0. TL;DR

1. **核心组合原语高度同构**：Context 树/作用域、服务依赖注入、fiber 生命周期、可逆 effect、事件总线、registry 这些「内核」概念三者一致；本 Go 端口在**可观察生命周期契约**上对齐 Rust（并因此对齐官方 4.0.5 的绝大部分语义）。
2. **特性差异分三类**：
   - **A. 语言驱动（有意保留）**：官方 TS 的 Proxy 属性服务、可调用服务、Service 类继承、`ctx.mixin`、装饰器 `@Inject`、函数/类/对象三种插件形态、`Promise` 贯穿的异步 API、ESM 动态 import —— 这些依赖 JS 运行时特性，Rust/Go 用显式类型 API 等价替代；Rust spec §5 已明文列为「deliberate differences」。
   - **B. 官方 4.1.0 向前演进、Rust/Go 未跟进（版本差，非语言差）**：`internal/commit`、`fiber.effectiveConfig/resolveConfig/rawConfig` 可写、volatile config、Standard Schema + `Plugin.Transform`、`inject` 对象形式（每服务 intercept 配置）、插件元数据 `provide/intercept`、provider 保留到依赖方卸载（`committed`/`Impl.dependents`）、生成器/异步可迭代 effect、`ctx.root/baseUrl`、reflect 服务与 mixin、日志 printf/格式化 exporter、错误栈合成、`DisposableList` O(1)。其中一部分是 TS-only，一部分是**可以移植但还没做**。
   - **C. Go 端口相对 Rust 参考的简化（本仓库已知项）**：无 `Drop` 兜底（显式 Dispose/Abandon + orphan）、无 parked-cascade 恢复、显式 Refresh 全量扫描、`EffectFuture` 只接收已求值 cleanup、sized/any 双 lane 合并、无 Miri/GC 自动清理、goid 代替 thread id、supervision 常开等（详见 `docs/GO-FEASIBILITY.md` §0.2 与 §2.12）。
3. **还有 D：本内核比官方 core 多出来的能力**：`Registry.Replace`（热替换）、结构化插件错误、lifecycle 广播（带 seq 快照）、可注入 Clock、L2 配额/心跳/停滞观测、`Abandon`/orphan 队列、panic 遏制、内核边界 CI 护栏。
4. **「通用内核」目标**：作为 **Go 宿主进程内的模块组合内核**，当前底子足够；但「接入所有插件系统」**不是单个内核能完成的**——官方 Cordis 的通用性同样来自 `core + loader/include/group/hmr + 宿主/隔离` 分层，而不是 `cordis` 包本身。Go 侧要补三层：**模块来源/协议（编译期注册表 + 进程外/WASM 桥）→ loader/插件树（配置驱动）→ 隔离与策略**。Go 运行时没有安全的动态 Go 模块加载/卸载，这是必须用 adapter 或进程外方案绕开的硬边界。

---

## 1. 三个实现的定位

| | 官方 TS `@deepseek-ai/cordis` 4.1.0 | Rust `zeokeel-cordis` 0.6.0 | 本 Go 端口 v0 |
|---|---|---|---|
| 形态 | 内核（`cordis`）+ 生态包 | 纯 Rust 内核 crate | 纯 Go 内核包 |
| 组合单位 | Context proxy + plugin（function/class/object） | Context + `Arc<dyn Plugin>`（Named/Type key） | Context + `Plugin`/`AsyncPlugin` 接口（Named/Type key） |
| 服务访问 | Proxy 属性（`ctx.foo`）+ `ctx.get/provide/set/accessor/mixin` | 显式 `ServiceKey<T>` | 显式 `ServiceKey[T]` + Go 1.27 泛型方法 |
| 异步模型 | 全 Promise/await | 同步 + `*_async`（future） | 同步 + `*_async`（`context.Context`） |
| 模块加载 | loader 生态（ESM 动态 import、EntryTree、HMR） | 无（宿主责任） | 无（宿主责任） |
| 动态插件 | ESM 运行时导入 | 明确不支持（无 ABI/动态加载） | Go 静态链接；需 adapter（进程外/WASM/编译期注册） |
| 隔离 | 进程内 + 宿主（插件权限由 loader/宿主） | L1/L2 进程内 + L3/L4 宿主 | 同 Rust，L2 原语已内置 |
| 已知扩展 | volatile config、reflect、callable service… | 结构化错误、lifecycle 广播、Clock、Replace… | 同 Rust + Go 1.27 泛型方法 API |

**版本关系**：官方 4.1.0 比 Rust 移植基线 4.0.5 新；因此「本 Go 端口 vs 官方 4.1.0」的差异 = Rust 对 4.0.5 的显式差异 + 4.0.5→4.1.0 的版本漂移 + Go 相对 Rust 的简化。这是下文的分类依据。

---

## 2. 特性对照矩阵

图例：✅ 对齐/支持；⚠️ 部分等价（形态不同或边界不同）；❌ 缺失；➕ 本端口额外提供。

### 2.1 Context / 作用域

| 能力 | 官方 4.1.0 | Rust 0.6.0 | Go v0 | 说明 |
|---|---|---|---|---|
| 根上下文 | `new Context()` | `Context::root()` | `cordis.Root()` | 形态等价 |
| 子作用域继承 | `ctx.extend(meta?)`（原型链 + 任意 own props） | `extend()` / `extend_meta(meta)` | `Extend()` / `ExtendMeta(meta)` | Go 无原型链；meta 用显式 API |
| 服务隔离 | `isolate(name, label?)`，label 为 symbol | `isolate`/`isolate_labeled`，label `&'static str` | `Isolate`/`IsolateLabeled`，label string | 等价；Go 用字符串标签（可由宿主保证稳定） |
| 配置拦截 | `intercept(name, config)`（类型化到 service Config） | `intercept(name, Value)` | `Intercept(name, Value)` | 等价（Go 无法编译期约束 config 类型） |
| 根引用 | `ctx.root` | 无 | ❌ 无 `Root()` 方法 | **小差距**：宿主/插件若要向上取根，需要额外 API |
| baseUrl | `ctx.baseUrl`（loader 设置） | 无 | ❌ | 属于 loader 生态，内核可不做 |
| 服务属性代理 | `ctx.foo`（Proxy + 模块增强） | ❌（显式 key） | ❌（显式 key） | TS-only；Rust spec §5 有意差异 |
| 计算属性 | `ctx.accessor(name, {get,set})` | `ctx.accessor/_with_setter` | `Accessor/AccessorWithSetter` | 等价 |
| mixin | `ctx.mixin(service, keys)` 把服务成员混入 ctx | ❌ | ❌ | TS Proxy-only；Go 可用显式转发方法替代，未实现 |
| 上下文身份判定 | `Context.is(value)`（跨 realm 品牌） | 无 | 无 | TS-only，Go 用类型系统天然满足 |

### 2.2 服务与依赖注入

| 能力 | 官方 4.1.0 | Rust 0.6.0 | Go v0 | 说明 |
|---|---|---|---|---|
| 服务身份 | `ctx.provide(name, value)`（name 字符串） | `ServiceKey<T>`（name） | `ServiceKey[T]`（name） | 等价；Go 泛型方法 `ctx.Get(key)` 推断 T |
| 服务读取 | `ctx.get(name, strict?)`（strict 默认 true）；Proxy 读属性 | `ctx.get/require`（非严格）；`service_available`（严格） | `Get/Require`（非严格）；`ServiceAvailable`（严格）；`GetNamed`（非严格） | **小差距**：无 `GetNamedStrict(name)` 单入口（严格读需走 `ServiceAvailable`） |
| 依赖声明 | `inject: string[]` **或对象** `{service: interceptConfig}`；Plugin.Base `intercept` | `inject -> &'static [&str]`（仅名字） | `PluginInjector.Inject() []string`（仅名字） | **实质差距**：官方支持「每依赖的 intercept 配置」；Rust/Go 未实现 |
| 可选依赖 | 官方无显式 optional（未声明即可 `ctx.get` 探测；loader `inject` 可映射） | ❌ | ❌ | 若做通用内核，建议加 optional 声明（用于 Pending 不阻塞） |
| 提供确认/检查 | `provide(name, value, check?)`；`Impl.check` | `provide_checked/provide_gated`；`AvailabilityCheck` | `ProvideChecked/ProvideGated`；`ServiceGate` | Go 语义等价，另有 gate 自动 refresh（Rust/Go 扩展） |
| 服务覆写 | `ctx.set(name, value)`（简单覆写） | `set`/`set_revertible`（可逆 + 版本 + TypeId 校验） | `SetNamed`/`SetRevertibleNamed`（可逆 + 版本 + 类型校验） | **Go/Rust 更强**（official 简单 set） |
| accessor 遮蔽 | ✅ | ✅ | ✅ | 等价 |
| 严格注入门 | provider 必须 active（`_checkImpl`） | provider Active 才满足 inject | 同 Rust | 语义对齐 |
| provider 生命周期 | **实现保留到依赖方卸载完成**（`fiber.committed`/`Impl.dependents`/`release`；父 fiber 卸载时 `while provisions.dependents.size` 等待） | withdraw-before-drain + 级联；不保留“旧实现对象” | withdraw-before-drain + 级联；不保留 committed impl | **实质差距**：官方 4.1 的新语义（保证依赖方卸载期间仍能访问到旧实现），Rust/Go 是 4.0.5 语义 |
| 类型安全/双 lane | TS 类型层 + 运行时无 lane | sized / `?Sized` 双 lane（unsafe 拒绝跨 lane） | 单一 `any` 存储 + 泛型断言（接口/指针天然） | Go 合并 lane（简化） |
| reflect 服务 | `ctx.reflect.get/provide/set/accessor/mixin/notify/trace/bind` + `Impl` 存储 | 无公共 reflect（内部 service_store） | 无公共 reflect（内部 serviceStore） | **差距**：官方可作为扩展点操作服务存储/追踪；Go 未暴露 |

### 2.3 插件模型与配置

| 能力 | 官方 4.1.0 | Rust 0.6.0 | Go v0 | 说明 |
|---|---|---|---|---|
| 插件形态 | function / class / `{apply}` object | `Arc<dyn Plugin>`（含 AsyncPlugin） | `Plugin`/`AsyncPlugin` 接口 | Go 可加 `PluginFunc`/`PluginObject` 适配器，未加 |
| 元数据 | `name?`, `Config?`(StandardSchema), `inject?`, `provide?`, `intercept?` | name/key/inject/config_schema | Name/可选 Key/Inject/ConfigSchema | **差距**：`provide`/`intercept` 声明、`name?` 可省（loader 用）等未实现 |
| 配置 schema | `Plugin.Config` = StandardSchemaV1 | `ConfigSchema`（validate+merge） | `ConfigSchema`（validate+merge） | Go/Rust 可自定义 validator；官方标准是 StandardSchema（可适配） |
| 配置变换 | `Plugin.Transform`（`{schema:true, Config:(raw)=>resolved}`） | ❌ | ❌ | **差距**：官方支持「用户 config → 运行时 config」变换（如补默认/解析 URL） |
| 配置解析 | `fiber.resolveConfig(candidate)`（只解析不发布） | 无公共（`internal/config` + schema 在 activation 内） | ❌ 无公共 resolve（内部 activation 内做） | **差距**：宿主/loader 需要预校验/预览时无 API |
| 有效配置 | `fiber.effectiveConfig`（activation 传入对象；volatile update 保留引用） | `fiber.config()`（resolved 值） | `Fiber.Config()`（resolved 值） | 概念近似；官方区分 raw/effective/resolved |
| raw 配置读写 | `fiber.rawConfig` get/set（set 不触发更新） | `raw_config()` 只读 | `RawConfig()` 只读 | **小差距**：无 raw 写入 API |
| 更新语义 | `fiber.update(config,noSave?)`：`internal/config` → schema → `internal/commit`（noSave 跳过）→ `internal/update`（保留实例或 await 重启） | update：config→schema→update 终端；不保留实例（总是 unload+activate；veto 可阻止） | 同 Rust | **差距**：官方 4.1 的 `internal/commit` + 实例保留/volatile 更新未移植 |
| volatile config | schema 可返回 `Volatile<T>`；loader 只提交 volatile 值、发 `loader/volatile-update`，不重启 | ❌ | ❌ | **差距**（4.1 新特性；依赖 Schemastery/loader） |
| 插件标识 | callback 身份（函数/类对象） | `PluginKey::Named/Type` | `PluginKey` Named/Typed | 语义近似（dedupe 单位不同） |
| 插件返回值 | `Fiber & PromiseLike<Fiber>`（可 await 加载完成/失败） | `Result<Fiber>`（同步立即返回，状态异步推进） | `(*Fiber, error)` 同 Rust | Go 有 `Fiber.AwaitSettled` 可替代 await |
| 子插件所有权 | 核心无显式父子所有权（loader/group 负责树） | `ctx.plugin` 子 fiber 是父 effect，级联卸载（CORE-047） | 同 Rust | **Go/Rust 扩展**；官方用 loader 树管理 |
| 加载深度上限 | 无（loader 树深度由宿主控制） | `MAX_PLUGIN_DEPTH=256` | 同 Rust | Go/Rust 额外防护 |
| 热替换 | 核心无 `replace`（HMR 包实现） | `Registry::replace` / `Fiber::replace_plugin`（CORE-048） | 同 Rust | **Go/Rust 扩展** |

### 2.4 Effect（可逆副作用）

| 能力 | 官方 4.1.0 | Rust 0.6.0 | Go v0 | 说明 |
|---|---|---|---|---|
| effect 形态 | `Disposable` / `Iterable<Disposable>` / `Promise<Disposable>` / `AsyncIterable<Disposable>`（生成器逐个产出 disposer） | `Effect = Vec<Cleanup>`；`AsyncEffect = Vec<AsyncCleanup>` | `Effect []Cleanup`；`AsyncEffect []AsyncCleanup`；`EffectStream`（channel）近似 | **差距**：Go 无生成器/异步迭代器；`EffectStream` 是 channel 版近似，语义不完全等价 |
| 顺序/单次 | 逆序、单次、async disposer 可 await | 同 | 同 | 对齐 |
| panic 隔离 | JS 无 panic（异常走 Promise/reject）；unload 错误处理在宿主 | `catch_unwind`：cleanup panic 不跳过、drain 后重抛首个（CORE-030） | `recover`：同语义 | **Go/Rust 扩展**（JS 无对应） |
| 取消/取消安全 | unload 返回 Promise，宿主可 race/放弃；无“取消恢复未启动 cleanup”契约 | cancel-safe disposal（S-1/A-9）、deferred、self-join、orphan | 同 Rust（context 取消映射 future drop） | **Go/Rust 扩展** |
| 诊断树 | `getEffects()` + `EffectMeta` | `get_effects()` + `EffectMeta` | `Fiber.Effects()` + `EffectMeta` | 对齐 |
| O(1) 删除 | `DisposableList`（Map+Weak 键） | `Vec` + 墓碑/摊销 | slice + disposed 剪枝 | 差距：Go/Rust 大 churn 下 O(n)（Rust 有摊销，Go 只 prune） |

### 2.5 事件与 waterfall

| 能力 | 官方 4.1.0 | Rust 0.6.0 | Go v0 | 说明 |
|---|---|---|---|---|
| 类型化事件 | 模块增强 `interface Events`：编译期参数/返回值类型 | `serde_json::Value` 载荷，无类型表 | `[]Value` 载荷，无类型表 | **语言差**：Go 可用泛型事件辅助包近似，未做 |
| 派发模式 | `emit` / `parallel` / `serial` / `bail` / `waterfall`（Promise-aware） | sync lane：`emit/bail`；async lane：`parallel_async/serial_async`；waterfall 两条 | 同 Rust | 对应关系见 Rust spec §5（两 lane 是 Rust 有意设计） |
| 监听器选项 | `{prepend, global}` + `once` 独立 API | `prepend/global/once` + `once()` | `EventOptions{Prepend,Global,Once}` + `Once()` | 等价（Go 把 once 放进 options，属便利差异） |
| scope 过滤 | 上下文 filter 原型链（`symbols.filter`） | isolate map 相等 | isolate map 相等 | 语义近似；TS 支持继承过滤，Go/Rust 只做映射相等（Rust spec §5 有意差异） |
| 异步监听器 | 同一 lane 中 Promise listener 自然可 await | 独立 async lane；sync 派发不 poll async | 同 Rust | 有意差异 |
| listener 拦截 hook | `internal/listener`（bail 语义，可替换 listener） | `internal/listener`（ListenerHook/Override） | 同 Rust | 对齐 |
| dispatch 观测 | `internal/dispatch`（含 `thisArg`） | `internal/dispatch`（无 thisArg） | 同 Rust | 小差异 |
| 监听器所有权 | fiber-owned，卸载自动移除 | 同 | 同 | 对齐 |
| waterfall panic 策略 | JS 异常自然传播 | fail-closed 重抛 | fail-closed 重抛 | Go/Rust 显式化 |

### 2.6 生命周期 / Fiber

| 能力 | 官方 4.1.0 | Rust 0.6.0 | Go v0 | 说明 |
|---|---|---|---|---|
| 状态机 | PENDING/LOADING/ACTIVE/FAILED/DISPOSED/UNLOADING（数值顺序不同） | Pending/Loading/Active/Failed/Unloading/Disposed | 同 Rust | 语义对齐 |
| 生命周期互斥 | `inertia`（进行中的 load/unload Promise）+ 内部 epoch/revision | lifecycle slot（operation token）+ `LifecycleBusy` | 同 Rust（goid token） | 形态不同，语义等价 |
| 失败恢复 | Failed 需显式 `update()` 恢复（README 明言） | restart/update/replace 可恢复；Failed 稳定态 | 同 Rust | **语义差**：官方 4.1 失败后仅 update 恢复 |
| 状态观测 | `internal/status`（fiber, oldState）；无通用广播 | `internal/status` + `on_lifecycle` 快照广播（seq） | 同 Rust | Go/Rust 扩展 |
| await 完成 | fiber 可 await（PromiseLike）；`fiber.await()` | `await_settled()` | `AwaitSettled()` | 等价意图 |
| dispose/restart/update | 均返回 Promise/awaitable | 同步 + `*_async` | 同 Rust | 形态差异 |
| 取消 | 无内建协作取消（Promise race/宿主） | `cancel()` + `checkpoint()`（CORE-050） | 同 | Go/Rust 扩展 |
| 实例保留/committed | ✅（见 provider 生命周期） | ❌ | ❌ | 差距 |
| 诊断 | `uid/state/getEffects/effectiveConfig` | uid/state/effects/config/error_detail | ID/State/Effects/Config/ErrorDetail | 基本对齐，少数 API 缺 |

### 2.7 Registry / 内核边界 / 生态

| 能力 | 官方 4.1.0 | Rust 0.6.0 | Go v0 | 说明 |
|---|---|---|---|---|
| registry 服务 | `ctx.registry`（RegistryService，方法混入 ctx） | `Context::registry() -> Arc<Registry>` | `ctx.Registry() -> *Registry` | 形态差异；map API 对齐 |
| map 查询 | `size/get/has/delete/keys/values/entries/forEach/counter/resolve` | size/get/has/delete/keys/values/entries/for_each/fibers | 同 Rust + FiberByID/Quota | 对齐（官方 iterator 惰性，Go 快照） |
| 热替换 | ❌ 核心（由 HMR 包） | ✅ Registry::replace | ✅ | 额外能力 |
| 静默/quiescence | 无独立 API（测试/宿主自行 await） | `is_idle/when_idle`（CORE-049） | 同 | Go/Rust 扩展 |
| 内核边界护栏 | ❌（生态/工程约定） | ✅ tests/kernel_boundary.rs | ✅ internal/archtest（go/parser） | Go/Rust 扩展 |
| loader / 插件树 | ✅ 生态包：EntryTree、create/update/remove/await/locate、include YAML/JSON、group、hmr | ❌（宿主责任） | ⚠️ 编译期 `module` 包覆盖子集：typed 注册表 + JSON 装配文件 + diff 热重载 + Update/Reload/Recreate + locate；无动态 ESM/HMR、无 include/group 完整语义 | 非 core 功能；Go 侧按静态链接重新实现（见 [COMPILE-TIME-REGISTRY.md](COMPILE-TIME-REGISTRY.md)） |
| timer | ✅ @…/plugin-timer（timeout/interval/throttle/debounce，disposal-aware） | ❌ | ❌ | 宿主/工具包责任 |
| console logger exporter | ✅ 生态包 | `TracingExporter`（宿主 tracing） | `SlogExporter`（宿主 slog） | 对齐思想 |
| HMR | ✅ 生态包（chokidar/picomatch） | ❌ | ❌ | 宿主责任 |
| 模块动态加载 | ✅ ESM 动态 import（Node 宿主） | ❌ 无 ABI/动态加载 | ❌ Go 静态链接 | 语言/运行时边界 |

### 2.8 日志 / 错误 / 工具

| 能力 | 官方 4.1.0 | Rust 0.6.0 | Go v0 | 说明 |
|---|---|---|---|---|
| 日志级别 | ERROR=0…DEBUG=3 | Error=0…Debug=3 | 同 | 对齐（旧版差异已消失） |
| 日志消息 | printf 风格 args + formatters + inspect；exporter 可分 level/formatter | 结构化 message+fields；`LogExporter`；无格式串 | 结构化；`SlogExporter`；无 printf/format | **差距**：官方格式化/占位符/%s inspect 未移植 |
| 日志缓冲 | `bufferSize` + `Message[]` | ring buffer 1000 + 单调 seq | 同 Rust | 对齐 |
| 可调用 logger | `ctx.logger('name')`（callable service） | `logger_named(name)` | `LoggerNamed(name)` | TS callable 语言差 |
| 错误模型 | `CordisError(INACTIVE_EFFECT)` + `ValidationError`（standard-schema issues） | 17 变体 `CordisError` + 结构化 `Plugin{plugin,kind,message}` | 同 Rust（`ErrCode`） | **Go/Rust 更丰富** |
| 异步错误栈 | `composeError`/`buildOuterStack` 拼接 async 调用栈 | ❌ | ❌ | 差距（TS 诊断体验） |
| O(1) 容器 | `DisposableList` | `Vec` + 墓碑/摊销 | slice | 差距（性能） |
| 可观测扩展 | reflect.trace/bind（Proxy 追踪） | tracing/slog 诊断 | slog 诊断 | 生态差异 |

---

## 3. 差异归因：哪些该追、哪些不该追

### 3.1 A 类：语言驱动，**不应追求逐行等价**（保留文档差异即可）

- Proxy 服务属性、可调用服务（`ctx.logger()`）、Service 类继承/symbol 协议、`ctx.mixin`、装饰器 `@Inject`、函数/类/对象三种插件形态、`Context.is` 跨 realm 品牌、`extend(meta)` 原型链、Symbol 隔离 label；
- Promise 贯穿的 API（`await ctx.plugin()`、`fiber.dispose()` 返回 Promise）、`Promise`/`AsyncIterable` effect、typed `Events` 模块增强、StandardSchema 类型魔法；
- ESM 动态 import（loader 的基础）。

Rust 参考已在 `spec/CORDIS-CORE.md` §5 与 `docs/CORDIS-RUST.md` §1 把这些列为有意差异；Go 端口沿用同一结论。若下游需要「官方写法」的体验，可在 host/adapter 层做语法糖（如函数式插件适配器、日志格式化包装），但不必污染内核语义。

### 3.2 B 类：官方 4.1.0 演进，**可移植且值得评估**（版本差）

按对「通用模块内核」的价值排序：

| # | 特性 | 价值 | 移植难度 | 建议 |
|---|---|---|---|---|
| B1 | `inject` 对象形式（每依赖 intercept 配置）+ 插件 `intercept` 声明 | 高（loader/组合场景的配置分层核心） | 中（Go: `map[string]Value` + 依赖解析合并） | **P0 候选** |
| B2 | `internal/commit` + `rawConfig` 可写 + `effectiveConfig`/`resolveConfig` | 高（配置持久化/预览/loader 热更） | 中（fiber 配置管线已有 80%） | **P0 候选** |
| B3 | 插件元数据 `provide`/`intercept`/`Config Transform`/函数对象适配器 | 高（loader 需要提前知道插件提供什么） | 低-中 | **P0 候选** |
| B4 | provider 保留到依赖方卸载完成（`committed`/`Impl.dependents`/`release`） | 中高（热替换/卸载期服务稳定） | 高（生命周期核心重写风险） | P1（需单独设计+攻击面测试） |
| B5 | 生成器 / 异步可迭代 effect | 中（组合式资源管理；Go 可用 `iter.Seq`/channel 近似） | 中 | P1 |
| B6 | `ctx.root`/`baseUrl` | 低-中 | 低 | P1 |
| B7 | reflect 服务/`mixin`/trace | 中（扩展与可观测） | 中（Go 无 Proxy，mixin 只能显式转发） | P2 |
| B8 | 日志格式化/printf/exporter formatter | 中（开发体验） | 低 | P1（或宿主 slog 包装） |
| B9 | DisposableList O(1)、错误栈合成、volatile config | 低-中 | 低/中/高 | P2 |
| B10 | `GetNamedStrict` 等小 API 对齐 | 低 | 低 | P2 |

### 3.3 C 类：Go 端口相对 Rust 的简化（已在 `GO-FEASIBILITY.md` §0.2 列出，无需重复展开）

显式 Dispose/Abandon（无 Drop）、parked recovery 缺失、显式 Refresh 全量、EffectFuture 简化、lane 合并、无 Miri、goid 替代 thread id、supervision 常开、logger 无 tracing 等。这些是**语言映射代价**，均已有文档与测试边界。

### 3.4 D 类：本端口额外能力（官方 core 没有）

`Registry.Replace` 热替换、结构化插件错误、lifecycle 广播、可注入 Clock、L2 配额/心跳/停滞观测、`Abandon`/orphan、panic 遏制、内核边界 CI 护栏。做「通用内核」时这些反而是差异化资产（尤其 Replace + lifecycle 广播 + L2 观测）。

---

## 4. 「通用内核：接入所有插件系统」可行性分析

### 4.1 先拆解目标

「一个内核可以接入所有插件系统，任何系统都能用它累加模块拼成完整功能系统」实际上包含 4 个独立能力：

1. **组合原语**：作用域、依赖注入、生命周期、副作用撤销、事件 —— 内核核心；
2. **模块来源**：插件从哪里来、如何加载/卸载/升级（同进程静态模块？外进程？WASM？JS 生态？）；
3. **模块目录/配置**：谁声明「系统由哪些模块按什么顺序/层级/配置组成」——插件树/loader；
4. **隔离与治理**：不可信插件、权限、资源配额、故障边界、审计。

### 4.2 官方 Cordis 也不是「单内核通吃」

官方 README 明确：`cordis` core 只提供 context/registry/fiber/events/services/logger；`loader`、`include`、`group`、`hmr`、`timer`、`logger-console` 都是**独立包**。其通用性 = 协议（plugin/fiber/service/inject）+ loader 插件树 + Node ESM 动态导入 + 宿主隔离。跨语言/不可信插件同样需要宿主进程/WASM。

### 4.3 Go 侧可行架构（建议分层）

```
L4 可观测/工具     logger exporters · metrics/tracing · lifecycle broadcast · stalled/heartbeat · debug UI
L3 隔离与治理      process/WASM sandbox · quotas(L2 已有) · permissions/capabilities · watchdog policy
L2 模块目录/配置   Go loader: EntryTree(id/name/config/group/disabled/inject) · include JSON/YAML
                  · group · await/locate · update/reload · compile-time factory registry
L1 模块来源/协议   ① 编译期 Go 模块注册表（静态链接，推荐默认）
                  ② 进程外插件桥（JSON-RPC/MCP/gRPC；fiber effect 管理子进程生命周期/重启/健康）
                  ③ WASM 桥（wazero；沙箱/跨语言）
                  ④ 可选：Node/TS 子进程桥（复用 Cordis 生态）、C ABI/cgo（受控）
L0 本内核          Context/scope · Service DI · Fiber 生命周期 · Effect 撤销 · Events · Registry
```

关键判断：

- **L0 已具备**：本仓库的 51/51 CORE 测试 + 生命周期/服务/事件/registry 语义足以做「Go 宿主内模块组合」的地基。
- **L1 是「接入所有插件系统」的真正门槛**：Go 运行时不支持安全地动态加载/卸载任意 Go 代码（`plugin` 包限制多、无法卸载、ABI 风险；cgo 也有 ABI/平台约束）。**必须**用「编译期注册表 + 配置驱动」或「进程外/WASM 适配器」；每个外部插件系统写一个 adapter plugin（作为 fiber 管理子进程/沙箱的生命周期与 effect），内核本身仍保持语言无关的组合语义。这不是本内核的缺陷，是所有静态语言内核的共同边界。
- **L2 是「模块拼成完整系统」的核心**：需要官方 loader 等价物：`EntryTree`（id/name/config/group/disabled/inject override）、include（JSON/YAML 拆分）、group（嵌套子模块树）、`await()/locate()`、update/reload/remove、模块名→工厂解析、配置持久化。这部分在 Go 里可实现得比 JS 更确定（编译期类型/工厂 + 配置校验），建议作为下一步最高优先级。
- **L3**：内核已有 L2 原语（quota/heartbeat/stalled/panic containment），但策略（杀/隔离/告警）归宿主；进程/WASM 隔离层需要独立包。
- **L4**：lifecycle broadcast/effect 树/registry/logger 已具备可观测底子，metrics/tracing/exporters 可按需加。

### 4.4 具体差距清单（按优先级）

**P0（做「通用内核」必需，建议先做）**

1. **Go loader 包**（新包，不动内核语义）：编译期 `ModuleRegistry`（name→factory + manifest）+ 配置驱动 `EntryTree`（id/name/config/group/disabled/inject）+ `include`（JSON/YAML）+ `await/locate/update/remove` + 与 `ctx.plugin/isolate/intercept` 的映射。→ 对应官方 loader/include/group。
2. **进程外插件桥包**：JSON-RPC/MCP 协议 + 子进程管理（启动/健康检查/重启/退避/日志桥），把外部系统（Python/Node/Rust/Java CLI）包装为 Cordis plugin/fiber；effect 负责终止/清理。→ 让「接入所有插件系统」成立。
3. **WASM 桥包**（可选但推荐）：wazero + WIT/guest 接口，把 WASM 模块作为沙箱插件；内核侧提供 host functions（service/event/log）映射。
4. **内核 P0 特性补齐**（若追求与官方 4.1 语义对齐）：inject 对象形式 + intercept 配置合并；插件元数据 `provide/intercept/Config Transform`；`internal/commit` + `rawConfig` 可写 + `effectiveConfig`/`resolveConfig` 公共 API。

**P1**

5. **模块清单/manifest 规范**：name/version/config schema/inject/provide/intercept/隔离标签/权限/健康检查；给 loader 与 adapter 共用。
6. **定时器包**（对应官方 timer）：disposal-aware setTimeout/Interval/throttle/debounce（基于 effect/context）。
7. **console/slog exporter + printf 风格日志适配**（宿主层可先做）。
8. **provider 保留/committed 语义**（需单独设计，做热替换/长卸载场景时再上）。
9. **生成器/异步迭代 effect**（Go 1.23 `iter.Seq`/channel 包装）。
10. `ctx.Root()`、`GetNamedStrict`、`Context.baseUrl` 等小 API。

**P2**

11. reflect/mixin/trace（Go 无 Proxy，只能在显式 API 上做等价物）、错误栈合成、DisposableList O(1)、volatile config（取决于是否接 schemastery 式 schema 体系）、HMR 宿主包（fsnotify）、metrics/tracing 包。

### 4.5 不建议做的事

- 追求与 TS 相同的 **Proxy 属性访问 / 可调用服务 / 类继承 / 装饰器 / typed Events 模块增强**——语言不可为；
- 在内核里内置 **动态 Go 模块加载/热卸载**——Go 运行时不可靠支持，应该走进程外/WASM；
- 把 loader/HMR/timer 塞进内核——官方都放在独立包，内核保持小而稳定才可能被「任何系统」复用；
- 承诺「零成本接入任意插件系统」——每个外部系统都需要 adapter（协议/生命周期/配置/权限映射），这是工程成本，不是抽象泄漏。

---

## 5. 结论与建议

1. **和官方比，特性上有区别，但差异是可解释、可分类的**：核心组合语义对齐；语言驱动的差异（A）应保留；官方 4.1 演进（B）里有一半值得移植；Go 相对 Rust 的简化（C）已文档化；本端口另有增强（D）。
2. **「通用内核」在 Go 生态是可行的，但形态是「内核 + loader + adapter + 隔离层」**，而不是一个包通吃。官方 Cordis 也是同一形态（core + 独立生态包）。
3. **建议的下一步排序**（若目标是「任何系统都能用它拼装」）：
   - ① `loader`（配置驱动的插件树 + 编译期模块注册表 + include/group）——最快把「模块累加成系统」变成可用能力；
   - ② `adapter`（进程外 JSON-RPC/MCP 桥 + WASM 桥）——真正实现「接入外部插件系统」；
   - ③ 内核按 P0 列表补齐官方 4.1 的配置/依赖元数据语义；
   - ④ 视需要补 timer/console-exporter/observability 包；
   - ⑤ 最后再考虑 provider retention、volatile config、reflect/mixin 等高成本/低边际收益项。
4. 若目标只是「Go 宿主内的模块化产品内核」，当前实现已经够用：51/51 CORE、212 个测试、`-race` + 架构护栏、静态单二进制、泛型方法 API。

---

## 6. 证据与参考

- 官方 4.1.0 源码（npm `@deepseek-ai/cordis@4.1.0` tarball）：`src/context.ts`（Proxy/作用域）、`src/registry.ts`（Plugin 三种形态、Inject 对象形式、Runtime）、`src/fiber.ts`（状态机、effect 形态、update/commit/committed、raw/effective config）、`src/reflect.ts`（Impl/dependents/release、get/provide/set/accessor/mixin/notify/trace）、`src/events.ts`（EventOptions、10 个 internal hook：plugin/status/service/config/commit/update/get/set/listener/dispatch）、`src/service.ts`（Service 类/symbol 协议）、`src/logger.ts`（LoggerLevel ERROR=0…DEBUG=3、Message/Formatter/Exporter）、`src/utils.ts`（DisposableList/composeError/traceable）。
- 官方生态包（npm）：`@deepseek-ai/cordis-plugin-loader@1.1.0`（EntryTree/create/update/remove/await/locate/volatile commit）、`-include`（YAML/JSON）、`-group`、`-hmr`、`-timer`；README 明确 core 与生态分层。
- Rust 基线差异：`reference/zeokeel-cordis-rust/spec/CORDIS-CORE.md` §5、`docs/CORDIS-RUST.md` §1/§2。
- 本 Go 端口现状：`docs/GO-FEASIBILITY.md`（机制映射/简化清单）、`docs/GO-DESIGN.md`（API/差异/测试策略）、`conformance/`（51/51 CORE）。
