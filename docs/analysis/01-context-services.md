# 01 · Context 与 Service 层架构分析（Go 重实现依据）

> 上游：GitHub [`zeokeel/zeokeel-cordis`](https://github.com/zeokeel/zeokeel-cordis) v0.6.0（Rust）。
> 本文行号除特别注明外均指上游仓库的相对路径 `src/*.rs`；规范引用 `spec/CORDIS-CORE.md`、
> `docs/GUIDE.md`、`docs/DESIGN.md`（不变量 I1–I12）。
> 范围：`context.rs`（2359 行）、`service.rs`（375）、`reflect.rs`（218）、`config.rs`（423），
> 以及它们依赖的 `registry.rs`/`fiber.rs`/`internal.rs` 的最小接口。

---

## 0. 结论速览

- `Context` 是 **`Arc<ContextInner>` 的轻量句柄**（`context.rs:121-124`）；`clone` 共享同一个节点，
  `extend/isolate/isolate_labeled/intercept/extend_meta` 派生新节点，节点不可变（除 `accessors` 与 `clock` 槽）。
- 服务存储是 **整棵树共享的一张表**：`Arc<Mutex<HashMap<(scope_id, name), ServiceEntry>>>`
  （`context.rs:46-49`），默认 scope 为 `0`；作用域通过 `isolate: HashMap<name, scope_id>` 映射（`context.rs:41`）。
- 服务身份与名字分离：存储键只有 `(scope, name)`；类型安全靠 **读取时 downcast + 写入时 TypeId 校验**（`service.rs:76-100`、`context.rs:2123-2148`）。
- **两条 lane**：sized lane 直接存 `Arc<T> as Arc<dyn Any>`；unsized/trait-object lane 存 `Arc<ArcWrap<T>>`，
  由 `wrap_service/unwrap_service` 收放（`service.rs:34-60`）；两条 lane 的 `type_id` 不同，跨 lane 读写失败。
- **读 lane 非严格、注入门严格**：`get/get_named/get_any` 只看 `AvailabilityCheck`+withdrawn；
  `service_available`/`inject`/`dependencies_ready` 额外要求提供者 fiber `Active`（`context.rs:666-706`，`fiber.rs:1081-1088`）。
- `set`/`set_any` 是**可逆、带版本号的受控写入**：只能改自己 fiber 提供的服务、必须同 TypeId，
  写入前用 `internal/set` waterfall，成功后版本号自增使依赖方重算（`context.rs:1993-2084`）。
- `resolve_config` 合并链固定为 `base → 祖先 intercept(root→leaf) → head`，有 schema 走 `ConfigSchema::merge`，
  否则浅合并 JSON object（`context.rs:1847-1877`）。
- 所有**用户回调都刻意放在 service_store 锁外**（可用性检查、accessor、internal hooks、schema merge）；
  重入由 `internal/get` 全局 latch 与 refresh 的 `refresh_depth` CAS latch 兜底（`internal.rs:52-55, 703-726`，`registry.rs:2094-2104`）。
- Rust 专有机制集中在 Arc/Weak、`TypeId`、`&'static str`、`catch_unwind`、`thread_local` 激活帧、
  `Disposer` 的 `Drop` 安全网；Go 无 Drop、无 thread_local（注：本轮实现使用 Go 1.27，泛型方法已可用，早稿的“Go 无泛型方法”结论已过时），这两点是移植的主要风险（§6、§7）。

---

## 1. Context 内部表示

### 1.1 结构定义

`Context` 只有 `inner: Arc<ContextInner>` 一个字段（`context.rs:121-124`）：

```rust
#[derive(Clone)]
pub struct Context { pub(crate) inner: Arc<ContextInner> }
```

`ContextInner` 字段清单（`context.rs:37-57`）：

| 字段 | 类型 | 共享范围 | 语义 |
|---|---|---|---|
| `parent` | `Option<Context>` | 每个派生节点 | 强引用父节点；根为 `None`。用于 scope/accessor/meta/intercept 的向上查找 |
| `fiber` | `Fiber`（`Arc<Mutex<FiberData>>`，`fiber.rs:148`） | 派生时继承同一 fiber | 拥有者 fiber；`fiber_id()` 即 `FiberData.id` |
| `isolate` | `HashMap<&'static str, u64>` | 派生时 clone | 服务名→scope id 的映射（§1.5） |
| `intercepts` | `HashMap<&'static str, Value>` | 派生时清空（`intercept` 除外） | 本节点上的 config 拦截层（§1.6） |
| `events` | `Arc<Events>` | 树共享 | 事件总线；internal hooks 也在其中（`internal.rs`） |
| `registry` | `Arc<Registry>` | 树共享 | fiber/plugin runtime/scope 计数器/依赖索引 |
| `logger` | `Arc<LoggerService>` | 树共享 | 内置日志服务实例（`LOGGER` 服务值就是它） |
| `service_store` | `Arc<Mutex<HashMap<(u64, String), ServiceEntry>>>` | **树共享一张表** | `(scope_id, name)`→entry（`context.rs:46-49`） |
| `accessors` | `Mutex<HashMap<&'static str, Accessor>>` | 每节点独立 | 计算属性；不共享（每个派生节点新建） |
| `meta` | `ContextMeta` | 每节点独立 | 诊断元数据；`extend_meta` 设置后不可变（`reflect.rs:70-112`） |
| `clock` | `Arc<Mutex<Arc<dyn Clock>>>` | 树共享一个槽 | 可注入时钟；每棵树一个句柄槽（`context.rs:54-56`） |

### 1.2 `root()` 构造流程（`context.rs:128-193`）

1. 新建 `Events`、`Registry`、`LoggerService(Info)`（129-131）。
2. 构造根 fiber：`id=0`、`state=Active`、`plugin=RootPlugin`、`parent/ctx=None`、`registry=Some(..)`、空 effect 列表、`SystemClock`（132-162）。
3. 组装 `ContextInner`：`parent=None`、空 `isolate/intercepts`、新 `service_store/accessors/meta/clock`（164-176）。
4. 给根 fiber 写 **weak 自引用** `parent/ctx = FiberLink::Weak(downgrade(inner))`，避免 Context↔FiberData 强环泄漏（178-184；测试 `context.rs:2344-2358`）。
5. `registry.set_root_fiber(&fiber)`：根 fiber 不在 `fibers` 表里，但它的生命周期要计入 quiescence（187；`registry.rs:921-925`）。
6. 通过 `insert_service("logger", LoggerService, None, None)` 装入内置 LOGGER 服务，owner=0（190-191；`logger.rs:425-426`）。
   该调用同时会在根 fiber 上注册一个 `ctx.provide` 内部 effect 并触发一次 `refresh_dependents()`。

### 1.3 clone 语义

- `#[derive(Clone)]` 只克隆 `Arc`：**同一个 Context 节点、同一个 fiber、同一张 service_store、同一份 isolate/intercept 映射**。
- 因此 `ctx.clone()` 与 `&ctx` 语义等价；GUIDE 明确 “`b = root.clone()` 是同一个作用域的句柄”（`docs/GUIDE.md:107-119`）。
- `Context` 为 `Send + Sync`（文档承诺；字段全部由 `Arc<Mutex<..>>`/不可变 map 组成）。
- **Go 提示**：直接用指针 `*Context` 传参即可获得 clone 语义；不要深拷贝字段，派生用显式 `Extend/Isolate/...`。

### 1.4 派生（不可变子节点）

四个派生函数都在 `ContextInner` 上做 copy-on-write，共同规则（`context.rs:200-216, 231-234, 241-275, 277-294, 314-333`）：

| 派生 | `parent` | `isolate` | `intercepts` | `accessors` | `meta` | 共享 events/registry/logger/store/clock/fiber |
|---|---|---|---|---|---|---|
| `extend()` / `child()` | `Some(self)` | 继承（clone 父 map） | 空 | 新空表 | 空 | 是 |
| `isolate(name)` | `Some(self)` | 父 map clone **+ `name`→新 scope id** | 空 | 新空表 | 空 | 是 |
| `isolate_labeled(name,label)` | `Some(self)` | 父 map clone **+ `name`→label 对应 id** | 空 | 新空表 | 空 | 是 |
| `intercept(name,config)` | `Some(self)` | 继承 | `{name: config}` 单条 | 新空表 | 空 | 是 |
| `child_with_meta(meta)` | `Some(self)` | 继承 | 空 | 新空表 | 给定 meta | 是 |

- `child(parent, fiber)` 额外用于创建 plugin fiber 的上下文（`context.rs:200-216`）；普通派生不创建 fiber（`fiber` 字段原样继承）。
- 派生节点不被登记进 fiber 的 effect 列表，生命周期仅由 `Arc` 句柄与 `parent` 强链决定；根节点用 weak 自链接打破循环。
- 语义上 **派生不 fork 数据**：子节点只是在“查找起点”上不同；服务可见性、事件 dispatch scope 都由 `isolate` 映射决定。
- **Go 提示**：节点里共享的是指针/引用（Events、Registry、store、clock slot、fiber），不要复制 map；
  `accessors`/`meta`/`intercepts` 按节点独有。

### 1.5 scope id 分配与查找

- `isolate(name)`：`isolate.insert(name, registry.next_scope_id())`（`context.rs:241-245`）。
  `next_scope_id` 从 `Registry.scope_counter: Mutex<u64>` 自增（`registry.rs:404, 1034-1042`）；**每次调用都新建 scope**。
- `isolate_labeled(name,label)`：`registry.labeled_scope_id(label)`（`context.rs:252-256`）；
  `label_scopes: Mutex<HashMap<&'static str,u64>>` 首次使用分配、后续复用（`registry.rs:413, 1022-1032`），
  与 `isolate()` 共用同一计数器，命名空间不冲突。
- `service_scope(name)`：从当前节点向根**逐级查找 `isolate.get(name)`，返回最近匹配**，无匹配返回 `None`
  （`context.rs:296-306`）；调用侧统一 `unwrap_or(0)` → 默认 scope 0。
- 存储键 = `(scope_id, name.to_string())`；所以：同一 `(scope,name)` 全树唯一；
  `isolate` 后子树的同名服务落在新 scope，互不覆盖；未 isolate 的所有 fiber 共享 scope 0（`context.rs:46-49, 496-507`）。
- **无祖先回退**：最近 scope 若定义了该名字但不可用，读取返回 `None`，不会退回 scope 0 的祖先 provider
  （`context.rs:672-680` 注释 + `spec/CORDIS-CORE.md:72-74`，CORE-014）。
- 事件总线的 scope 过滤使用同一份 `isolate: HashMap<..>`（`context.rs:1785-1834` 传 `Some(&self.inner.isolate)`；
  类型别名 `events.rs:77`）。规则是“映射相等”而非“子树包含”（`docs/GUIDE.md:517-529`）。
- **Go 提示**：`type ScopeID uint64`；store key 用 `struct{ Scope ScopeID; Name string }`（可比较）；
  isolate 用 `map[string]ScopeID`，派生时复制 map；label 表放 Registry 级 `map[string]ScopeID` + Mutex。
  scope id 只需在单棵树内唯一（每 root 一个 Registry）。

### 1.6 intercept 链

- 每次 `intercept(name, config)` 产生一个**只含一条** `intercepts` 映射的新子节点（`context.rs:314-333`）。
- 层叠顺序由 `resolve_config` 收集 `self → parent → ... → root` 后 **reverse** 得到 root→leaf（§3.1）。
- `intercept` 只影响走 `Context::resolve_config` 的配置（内置 logger、`Service::resolve_config`），
  **不影响 plugin 配置**（plugin 配置走 `internal/config` waterfall，见 `spec/CORDIS-CORE.md:618-625`、`docs/GUIDE.md:500-529`）。
- 同一节点对同名多次 intercept 不可能（单条目 map），需要多层就多次派生。

### 1.7 meta / extend_meta

- `ContextMeta { entries: HashMap<&'static str, Arc<dyn Any + Send + Sync>> }`（`reflect.rs:70-73`）。
- 构建 API：`new/insert/insert_any/get/contains/len/is_empty`（`reflect.rs:75-112`）；`insert(_any)` 需要 `&mut self`。
- `ctx.extend_meta(meta)` → `child_with_meta`（`reflect.rs:193-199`；`context.rs:277-294`）；**父节点不被修改**。
- 读取 `meta_any(key)` / `meta::<T>(key)` 沿 `parent` 链 child→root 找最近命中（`reflect.rs:201-217`）；entry 不参与服务解析。
- **Go 提示**：`ContextMeta map[string]any`；派生时整个 map 交给新节点（值本身不可变即可）；
  读取沿 parent 链；不需要 `Arc`，用 `any` 存指针由 GC 管。

### 1.8 accessor（计算属性）

- 类型（`reflect.rs:28-39`）：
  - `AccessorGet = Arc<dyn Fn(&Context) -> ServiceValue + Send + Sync>`（形参是**发起解析的 context**，不是声明者）；
  - `AccessorSet = Arc<dyn Fn(&Context, ServiceValue) -> bool + Send + Sync>`（返回 `false` 拒绝写入）；
  - `Accessor { get, set: Option<AccessorSet> }`，`new`（只读）、`with_setter`。
- 注册：`Context::accessor` / `accessor_with_setter`（`reflect.rs:122-147`）→ `register_accessor`（149-177）：
  1) 锁**本节点** `accessors`；同名冲突返回 `InvalidPlugin("property ... already declared")`（只查当前节点，允许子节点遮蔽祖先）；
  2) 插入后构造 disposer：用 **weak inner** 捕获，避免 Context↔fiber effect 强环（166-174）；
  3) `register_effect_labeled(disposer, "ctx.accessor")` 挂到当前 fiber。
- 查找：`find_accessor(name)` 沿 parent 链 child→root，返回最近 accessor 的 clone（`reflect.rs:179-191`）。
- 读取：`Accessor::read` 经 `internal::contained` 包裹，getter **panic 时返回 `None`**，与“属性不存在”同义，不返回占位值
  （`reflect.rs:55-65`，CORE-041）。
- 写入：`Context::set`/`set_any` 发现 accessor 时走 setter 分支（§2.8）；无 setter → `AccessorReadOnly`；
  setter 返回 false 或 panic → `AccessorWriteRejected`。
- **accessor 优先于服务**且**完全遮蔽**：`get` 一旦命中 accessor，即使 getter 返回 None 也不会回退去读 service
  （`context.rs:565-576`）。accessor 永远不满足 `inject`（严格查询只读 store）。
- **Go 提示**：`Accessor struct { Get func(*Context) any; Set func(*Context, any) bool }`；
  `sync.Mutex` 保护 map；注册/注销均需检查是否已存在；调用 getter 时必须已释放锁。

### 1.9 clock

- 树级可注入时钟槽：`ContextInner.clock: Arc<Mutex<Arc<dyn Clock>>>`（`context.rs:54-56`）。
- `clock()`：取锁 clone 句柄，锁中毒时回退 `SystemClock`（`context.rs:345-351`）。
- `set_clock(clock)`：只允许 `fiber_id()==0` 的 context（否则 `InvalidPlugin`）；写共享槽后，
  遍历 `registry.fibers()` + 根 fiber 更新每个 `FiberData.clock` 快照（`context.rs:380-402`）。
- 契约：`Clock::now_unix_ms`/`now_monotonic_ms`，默认 `SystemClock`（`clock.rs:36-67`）；应 `root()` 后立即安装、避免运行时切换；
  logger facade 在创建时 snapshot 时钟（`logger.rs:170-184`、`context.rs:1908-1926`）。
- **Go 提示**：`type Clock interface { NowUnixMS() uint64; NowMonotonicMS() uint64 }`；
  树级槽用 `sync.Mutex`/`atomic.Pointer[Clock]`；子 context 共享同一槽指针。

### 1.10 其余共享句柄

`fiber()`（218-220）、`registry()`（222-224）、`events()`（226-228）、`fiber_id()`（335-338）都只是 clone `Arc`/读 fiber；
派生节点之间这些句柄必须指向同一实例，Go 实现要避免“每节点 new 一个”。

---

## 2. Service 子系统

### 2.1 `ServiceKey<T>` 与身份

```rust
pub struct ServiceKey<T: ?Sized + Send + Sync + 'static> {
    name: &'static str,
    _marker: PhantomData<fn(&T)>,
}
impl<T: ?Sized + Send + Sync + 'static> ServiceKey<T> {
    pub const fn new(name: &'static str) -> Self;
    pub const fn name(&self) -> &'static str;
}
// Clone/Copy（service.rs:152-176）
```

- **存储身份只有 `name`**（+ 调用方 `isolate` 出的 scope）；`T` 只参与读取时 downcast 与写入时类型校验。
- `T: ?Sized` 允许 `ServiceKey<dyn Trait>`；这类 key 只能配 `provide_any/get_any/require_any/set_any`，
  因为 `Arc<dyn Trait>` 不能 unsize 成 `Arc<dyn Any>`（`service.rs:29-39`；CORE-032）。
- 同一名字用 `ServiceKey<A>` 与 `ServiceKey<B>` 是“同一个服务槽、两个类型期望”：读错类型得到 `None`，写错类型得到 `ServiceTypeMismatch`；
  注册重复只看 `(scope,name)` → `DuplicateService`。
- `ServiceKey` 按值传参（Copy）。名字是 `&'static str`，进程级稳定。
- **Go 提示**（按 Go 1.27 更新）：Go 1.27 起支持**泛型方法**，typed API 可以做成
  `ctx.Get[T](key)` / `ctx.Provide[T](key, v)`（类型参数由实参推断）。建议：
  - `type ServiceKey[T any] struct { name string }` + `func NewServiceKey[T any](name string) ServiceKey[T]`；
  - **方法形式为主**：`func (c Context) Provide[T any](key ServiceKey[T], v T) (Disposer, error)`、
    `Get/Require/ProvideChecked/ProvideGated/ProvideService/PluginTyped/Meta` 同理；包级泛型函数作包装；
  - 接口方法仍不能声明类型参数（Go 1.27 规范）→ `Plugin`/`AsyncPlugin` 接口保持非泛型，typed 插件身份用
    `ctx.PluginTyped[P](p, cfg)` 泛型方法；
  - untyped lane 用普通方法（`GetNamed(name) (any, bool)`、`SetNamed(name string, v any) error`）；
  - `T` 为接口时用 `reflect.TypeFor[T]()` 得到接口类型作为 type token（Go 反射可直接比较接口类型）。

### 2.2 ServiceValue 与 typed/untyped 双 lane

- `ServiceValue = Arc<dyn Any + Send + Sync>`（`service.rs:15`），即类型擦除后的可 clone 句柄。
- `service_type_id(value)`：`(&**value as &dyn Any).type_id()`（`service.rs:51-60`）。
  - sized lane：`ServiceValue` 里是 `T` → `TypeId::of::<T>()`；
  - untyped lane：`ServiceValue` 里是 `ArcWrap<T>` → `TypeId::of::<ArcWrap<T>>()`。
  这是 `ctx.set` 比较的身份（FIX-13），也解释了两 lane 不能混用。
- untyped lane 收放（`service.rs:29-49`）：

```rust
pub(crate) struct ArcWrap<T: ?Sized + Send + Sync + 'static>(pub(crate) Arc<T>);
fn wrap_service<T: ?Sized + Send + Sync + 'static>(value: Arc<T>) -> ServiceValue;      // Arc::new(ArcWrap(value))
fn unwrap_service<T: ?Sized + Send + Sync + 'static>(v: &ServiceValue) -> Option<Arc<T>>; // downcast ArcWrap<T>, clone 内层
```

- 双 lane API 对照（`context.rs`）：

| 能力 | sized/typed lane | unsized lane |
|---|---|---|
| 注册 | `provide`(406-412)、`provide_checked`(420-431)、`provide_gated`(435-443) | `provide_any`(589-595) |
| 读 | `get`(565-576)、`require`(578-581) | `get_any`(598-609)、`require_any`(612-618) |
| 写 | `set`(1993-2013, 按名字)、`set_revertible`(按名字) | `set_any`(627-655) |
| 服务对象注册（带 schema/check） | `provide_service`（`service.rs:313-322`、`context.rs:324-329`） | 无（Service trait 只走 sized lane；`provide_service_value` 是内部入口，`context.rs:457-465`） |

- untyped lane 的 typed 读取必须用 `get_any`；`get` 对 `ArcWrap` downcast 失败返回 `None`，反向亦然。
- **Go 提示**：Go 的 `any` 本身可存任意值（含接口值），不存在 unsize 限制，但仍**建议保留 lane 标记**以复刻 TypeId 身份规则：
  `ServiceEntry{ Value any; Lane TypedLane|AnyLane; Type reflect.Type }`；typed lane 存 `*T`、
  any lane 存接口值或包装结构；`Set` 校验 `Lane + Type` 完全一致。注意 Go 的 typed-nil 陷阱：
  `any((*T)(nil)) != nil`，注册/判空要显式检查反射值。

### 2.3 存储、`ScopeId` 与 `ServiceEntry`

- 表：`Arc<Mutex<HashMap<(u64, String), ServiceEntry>>>`，树共享（`context.rs:46-49`）。
- 键：`(scope_id, name)`；scope_id 的解析见 §1.5（默认 0）。`ServiceEntry` 自身不记录 scope。
- `ServiceEntry` 字段（`service.rs:75-100`，`#[derive(Clone)]`）：

| 字段 | 类型 | 含义 |
|---|---|---|
| `value` | `ServiceValue` | 擦除后的服务值 |
| `check` | `Option<AvailabilityCheck>` | 可用性谓词（Cordis `Service.check`） |
| `schema` | `Option<Arc<dyn ConfigSchema>>` | 配置 schema；只有 `provide_service` 传入 |
| `owner` | `u64` | 提供者 fiber id；`ctx.set` 的所有权判据（根/内置服务为 0） |
| `type_id` | `TypeId` | 具体存储类型身份（sized: `T`；untyped: `ArcWrap<T>`） |
| `provider` | `Option<Weak<Mutex<FiberData>>>` | 提供者 fiber 弱引用；注入门用它判 `Active` |
| `version` | `u64` | 条目版本；依赖方按版本重算（Cordis epoch） |
| `withdrawn` | `bool` | 提供者卸载时先置 true，让依赖方先失活再移除绑定 |

- `ServiceEntry::clone` 只 clone `Arc`/`Value`，`lookup*` 因此可以“锁内取 clone、锁外判定”。
- **Go 提示**：`type ServiceEntry struct { Value any; Check func() bool; Schema ConfigSchema; Owner uint64; Type reflect.Type; Lane Lane; Provider *Fiber; Version uint64; Withdrawn bool }`；
  Provider 不必是 weak：Go GC 可回收，但**必须显式判 `fiber.State()==Active`**，不能依赖指针可空/GC 来模拟 Rust 的 fail-open。

### 2.4 可用性判定（AvailabilityCheck / ServiceGate）

```rust
pub type AvailabilityCheck = Arc<dyn Fn() -> bool + Send + Sync>;  // service.rs:62-73
fn is_available(&self) -> bool {                                   // service.rs:103-111
    if self.withdrawn { return false; }
    match &self.check { None => true,
        Some(check) => catch_unwind(AssertUnwindSafe(|| check())).unwrap_or(false) }
}
fn is_available_for_injection(&self) -> bool { self.is_available() && self.provider_is_active() } // 120-122
```

- `provider_is_active`（`service.rs:124-143`）：`provider=None` 或已 upgrade 失败 → **true**（历史/手工 entry 兼容）；
  upgrade 成功则 lock `FiberData` 判 `state == Active`；**锁中毒 → true（fail-open）**，避免一次 contained panic 造成永久服务不可用（S-3）。
- `ServiceGate`（`service.rs:196-249`）：
  - 字段 `available: Arc<AtomicBool>`、`watchers: Arc<Mutex<Vec<Weak<ContextInner>>>>`；
  - `new(bool)`、`is_available()`、`check()` 返回谓词闭包；
  - `set(bool)`：`swap` 无变化返回 false；有变化 → `notify()` 对每个存活 watcher 执行 `Context{inner}.refresh()`
    （**全量** refresh，不是 fast path），返回 true；
  - `watch(ctx)` 由 `provide_gated` 调用（`context.rs:435-443`），压入 `Arc::downgrade`；`notify` 顺手 retain 清理已死 weak。
  - 注意：watch 不按 ctx 去重；同一 ctx 多次 `provide_gated` 会重复注册，flip 时重复 refresh（可容忍的幂等浪费）。
- `provide_checked`/`provide_gated` 本身不刷新：`provide_checked` 的 backing state 变化后需调用方 `ctx.refresh()`
  （`context.rs:414-431`，CORE-013）；`provide_gated` 通过 gate 自动刷新。
- **Go 提示**：`Check func() bool` + `defer recover()` 包装成 `safeCheck`；Gate 的 watchers 用显式注册表（`[]*Context`，允许去重）；flip 时逐 ctx 调 `Refresh()`（与 Rust 全量语义一致）。

### 2.5 注册（provide 家族）插入流程

`provide*` → `provide_entry` / `provide_service_value` → `insert_service`（`context.rs:445-541`）。`insert_service` 步骤：

1. 构造 `ServiceEntry`：`type_id = service_type_id(&service)`；`provider = weak(fiber.data)`；
   `owner = fiber_id()`；`version = next_version()`；`withdrawn=false`（474-491）。
2. 解析 scope：`service_scope(name).unwrap_or(0)`，键 `(scope, name.to_string())`（496-497）。
3. 锁 `service_store`：已存在 → `Err(DuplicateService(name))`；否则 `insert`（498-508）。**锁只覆盖 contains+insert**。
4. `registry.note_service_changed(name)`：把名字放进反向依赖索引的 dirty 集合（511；`registry.rs:1594-1604`）。
5. `events.internal().emit_service(ctx, name, Some(&value))`：`internal/service` 观察者（contained，不能 veto）（512-515）。
6. 构造 disposer：清理闭包内**锁 store 移除 key** → `note_service_changed` → 若 Context 还能 upgrade 则
   `emit_service(ctx,name,None)` + `ctx.refresh_dependents()`；捕获的是 `Arc<store>/Arc<events>/Arc<registry>` + **weak inner**（516-537）。
7. `register_effect_labeled(disposer, "ctx.provide")`：注册为**内核内部 effect**（不占用户 `max_effects` 配额），归属当前 fiber（538）。
8. `refresh_dependents()`：注册后立即尝试激活依赖方（539 → `registry.refresh_all()`）。

- 关键点：注册的原子性来自“锁内查重+插入”，而事件、effect、refresh 都在锁外；
  正常路径不返回用户可见的半成品。**当前 Rust 实现有一个保守边界**：若第 7 步 `register_effect_labeled`
  因 fiber 已 `Unloading/Disposed` 失败（`push_effect` 抛 `InactiveEffect`，`context.rs:994-1013`），
  store 中的 entry **没有回滚**（`?` 直接返回）。Go 实现应把“注册 effect 失败 → 回滚 store”补上（见 §5.3）。
- `Disposer` 移除语义：没有 `Context::remove`；撤销 = 调用返回的 `Disposer::dispose()`（或 fiber 卸载时 effect 逆序执行）。
  disposer 只做 `HashMap::remove`，不校验当前 entry 是否还是自己的（若已被 `set` 替换，remove 仍然会删）。

### 2.6 读 lane（非严格）：`get` / `require` / `get_named` / `get_any`

`get`（`context.rs:565-576`）：

1. `find_accessor(name)`：命中则读 accessor（panic→None）并 downcast `T`；**命中即返回，不查服务、不跑 internal/get**。
2. 否则 `events.internal().waterfall_get(self, name, terminal = lookup(name).value)`（572-574）：
   `internal/get` 是 waterfall 链，hook 可观察/覆盖返回值；链的终点是**非严格** `lookup`。
3. 对结果 `downcast::<T>()`；类型不符 → None（hook 返回错误类型也安全降级为 None）。

`get_named`（1976-1982）与 `get_any`（598-609）同构：accessor 优先 → `internal/get` → 非严格 lookup；
`get_any` 用 `unwrap_service` 还原 `Arc<T>`，`get_named` 直接返回 `ServiceValue`。
`require`/`require_any` 把 None 映射为 `UnknownService(name)`（578-581, 612-618）。

`lookup` 的非严格语义（`context.rs:682-694`；`spec/CORDIS-CORE.md:122-134`）：

- `lookup_entry(name)`：锁 store，取 `(service_scope(name) or 0, name)` 的 **clone**（672-680）；
- `lookup`：`lookup_entry(...).filter(|e| e.is_available())` —— 只受 `withdrawn`/`check` 约束，
  **不看 provider fiber 状态**，所以 provider `Loading` 时也能读到（TS property-read 语义）；
- 读 lane 永远不把“hook 虚拟服务/accessor”当成可注入服务。

### 2.7 严格查询（injection gate）

- `lookup_for_injection(name)`：`lookup_entry(...).filter(|e| e.is_available_for_injection())`（`context.rs:696-706`）；
- `service_available(name)` 就是它的 `is_some()`（666-668）；
- `inject` 门：`dependencies_ready(ctx, deps)` 要求每个名字 `lookup_for_injection` 命中（`fiber.rs:1081-1088`，
  CORE-006/016，FIX-12）；
- 依赖激活后记录的版本也用非严格 `lookup` 抓取（`fiber.rs:1091-1099`），
  但“是否仍满足”用严格查询 + 版本相等（`dependencies_satisfied`，`fiber.rs:1101-1118`）；
- 提供者卸载顺序：fiber 先 `transition(Unloading)` → `withdraw_owner_services(owner)`（把该 owner 所有 entry 置 `withdrawn=true`）
  → 再 drain 自身 effect（provider-withdrawal order，`fiber.rs:2362-2394`；CORE-042）。
  这样依赖方在绑定仍存在时先失活，不会观察到“值还在但提供者已死”的窗口。
- **Go 提示**：必须有两条独立查询函数（`lookup` / `lookupForInjection`），绝不能合并；
  `inject` 判定与 `get` 判定要共用 entry 结构但不同谓词；withdraw 标志位与 store 移除要两个阶段完成。

### 2.8 写入：`set` / `set_any` / `set_revertible`

`set(name, value)`（`context.rs:1993-2013`）：

1. 若存在 accessor：有 setter → `catch_unwind(setter(ctx,value))`，false/panic → `AccessorWriteRejected`；
   无 setter → `AccessorReadOnly`。**accessor 分支不经过 internal/set**。
2. 否则 `set_revertible(name, value).map(|_| ())`（丢弃返回的 disposer 句柄，但 inverse effect 已挂在 fiber 上）。

`set_any(key, value)`（627-655）：accessor 分支先用 `wrap_service(value)` 包装再调 setter；
非 accessor 分支同样走 `set_revertible(wrapped)`，因此与 `set` 共用所有权/类型/版本规则。

`set_revertible(name, value)`（2024-2084），顺序不可换：

1. `check_set_target(name)`：scope 内无 entry → `UnknownService`；`owner != self.fiber_id()` → `ServiceWriteForbidden`（2107-2121）。
2. `check_set_type(name, &value)`：`service_type_id(value)` 必须等于 entry.type_id，否则 `ServiceTypeMismatch(name)`
   （2123-2148；DESIGN I5 / FIX-13）。
3. fiber 状态是 `Unloading/Disposed` → `InactiveEffect`（**先拒绝再写**，避免“无 inverse 的写入”，2027-2032）。
4. 锁 store 取 `previous = entry.clone()`（2033-2040）。
5. `internal/set` waterfall：`waterfall_set(ctx,name,&value, terminal = write_service(name,&value).is_ok())`（2041-2047）；
   hook 不调 `next` → 返回 `Ok(Disposer::new(vec![]))`，store 不变（2048-2051）。
6. 构造 inverse disposer：恢复 `previous`（无则 remove）；若恢复时 owner fiber 正在 `Unloading/Disposed`，
   把恢复的 entry 标 `withdrawn=true`（防止复活正在释放的绑定）；然后 `note_service_changed` + `refresh_dependents`（2052-2080）。
7. `register_effect_labeled(inverse, "ctx.set")`，再 `refresh_dependents()`（2081-2083）。

`write_service`（2161-2193）：锁内再次校验 TypeId（防御纵深）→ `entry.value = new`、`entry.version = next_version()`；
锁外仅在成功时 `note_service_changed`。失败（entry 消失/类型不符）返回错误，但被第 5 步的 `.is_ok()` 折叠成“veto”
（=> `set_revertible` 返回 `Ok(空 Disposer)` 而 store 未变）——这是 Rust 现状的一个易误读点。

- **`set` 的代价**：每次成功写入都追加一个 inverse effect 到提供者 fiber（XT-07），线性增长；
  高频更新应原地改服务对象；`set_revertible` 返回的 disposer 可让调用方主动折叠/提前撤销 inverse。
- **Go 提示**：把 1/2/3 步在锁外做“预检”，第 4/5/7 步需注意并发窗口；推荐在锁内做
  “校验 + 写 + 记录上一版本”，锁外做 hooks/refresh；若 hook veto 或 effect 注册失败要回滚 store（Rust 在 effect 失败窗口未回滚）；
  且 `set` 语义上要能返回 inverse（Go 用显式 `SetRevertible(...) (Disposer, error)`）。

### 2.9 撤销 / 移除 / withdraw

- 唯一公开移除路径：`provide*` 返回的 `Disposer::dispose()`（`context.rs:526-537`）或 fiber 卸载时 effect 逆序执行；
  **没有 `Context::remove` 方法**。
- `withdraw_owner_services(owner)`：遍历共享 store，把 `entry.owner==owner` 的全部 entry 标 `withdrawn=true`，
  返回被撤销的服务名列表（2094-2105，遍历所有 scope）。调用点在 fiber unload 起点（`fiber.rs:2362-2394`），
  之后每个被撤销名字 `note_service_changed`，级联用 S3 候选集。
- `set_revertible` 的 inverse 恢复时若 owner 在卸载中则恢复 entry 仍 `withdrawn`（2057-2072）。

### 2.10 provider epoch / version 与反向依赖索引

- 全局单调版本：`static SERVICE_VERSION: AtomicU64`，初值 1，`next_version()=fetch_add(1)`（`service.rs:17-27`）。
  entry 在注册与每次 `write_service` 时取新版本。
- 依赖记录：activation 成功后 `record_dep_versions(data, ctx, inject)` 以**非严格** lookup 记录
  `Vec<(&'static str, u64)>`（`fiber.rs:1091-1099`）；
- 依赖满足：`dependencies_satisfied` 要求严格可用 **且** 记录版本等于当前 entry 版本（`fiber.rs:1101-1118`）；
  → provider 被 `set` 或替换（同名重新注册）都会让依赖方卸载回 `Pending` 并重新激活（Cordis epoch）。
- 反向依赖索引（derived state，`registry.rs`）：
  - 数据来自每个 fiber 的 `inject: &'static [&'static str]`；`note_service_changed(name)` 把名字记入
    `service_dirty`（`registry.rs:1594-1604`），是**超集**（跨 isolate scope 同名合并），只作候选过滤；
  - `refresh_candidates()` 把 dirty 名字映射为“注入了这些名字的 fiber id 集合”（`registry.rs:1640-1651`）；
  - `refresh_all()` 用候选集（快路径）；用户 `ctx.refresh()` → `refresh_all_full()` 全量扫描（`registry.rs:2110-2121`）；
  - 索引损坏（锁中毒/窗口）时 `take_dirty_services` 返回 None → 回退全量扫描（`registry.rs:1606-1619`）；
  - **索引永远不决定可用性**：每个候选仍走 `dependencies_ready`/`dependencies_satisfied`。
- 刷新固定点：`refresh_all_impl` 最多 64 轮，每轮按注册顺序快照推进；正序链一轮稳定、逆序链约一轮一层；
  重入通过 `refresh_depth` CAS + `refresh_dirty` 合并（`registry.rs:2094-2104, 2123-2205`）；
  `RefreshGuard` 的 `Drop` 在 panic 路径也复位（`registry.rs:677-685`）。
- async 版本：`refresh_async()` → `refresh_all_async_full()`；sync refresh 不 await async 生命周期（`context.rs:782-793`）。

### 2.11 `Service` trait 与 `provide_service`

```rust
pub trait Service: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn check(&self) -> Option<AvailabilityCheck> { None }
    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> { None }
    fn resolve_config(&self, ctx: &Context, base: Option<Value>, head: Option<Value>) -> Value {
        ctx.resolve_config(self.name(), base, head)
    }
}
pub fn provide_service<S: Service>(ctx: &Context, service: Arc<S>) -> Result<Disposer>;
impl Context { pub fn provide_service<S: Service>(&self, service: Arc<S>) -> Result<Disposer>; }
```

（`service.rs:251-329`）

- 这是“服务对象自带 name/check/schema”的入口：`name` 作为注册名，`check` 转成 `AvailabilityCheck`，
  `config_schema` 存进 entry（这是 `schema` 字段唯一的公开写入点），然后 `ctx.provide_service_value(name, value, check, schema)`
  （sized lane，`ServiceValue = Arc<S>`；entry.type_id = `TypeId::of::<S>()`）。
- 返回 disposer 同上；注册后依赖方会被 refresh。
- **Go 提示**：`type Service interface { Name() string; Check() func() bool; ConfigSchema() ConfigSchema; ResolveConfig(ctx *Context, base, head any) any }`；
  `ProvideService(ctx *Context, svc Service) (Disposer, error)`；Go 接口自带类型，可作 type token（`reflect.TypeOf(svc)` 得具体类型或接口类型，需明确约定）。

---

## 3. `resolve_config` / `find_schema` / ConfigSchema

### 3.1 分层合并算法（`context.rs:1841-1877`）

1. `intercepts: Vec<Value>`，`cursor = Some(self.clone())`；沿 parent 链 child→root，若 `cursor.inner.intercepts` 含 `name`
   则 push 该 config（每个节点至多一条）。
2. `intercepts.reverse()` → 得到 **root→leaf** 顺序（祖先低优先级）。
3. `head` 存在则 push 到最后（最高优先级；`set_revertible`/service 调用方传入）。
4. `find_schema(name)` 命中：`contained_merge(schema, base.unwrap_or(Value::Null), &intercepts)`（1860-1868）；
   `ConfigSchema::merge(base, overrides)` 内由 schema 决定合并策略；`contained_merge` 用 `catch_unwind` 包裹，
   schema panic → **退化为 base**（`config.rs:133-150`，CORE-042）。
5. 无 schema：`result = {}`；有 base 则 `merge_into(&mut result, base)`；再逐个 `merge_into(result, layer)`（1869-1876）。
   `merge_into` 仅当 target/source 都是 JSON object 时逐 key 覆盖；**target 恒为 object，非 object 的 base/layer 被静默丢弃**（1958-1964）。
6. 返回值恒为 `Value`（通常是 object；即使没有任何层也是 `{}`，不是 `Null`）。

### 3.2 `find_schema`（`context.rs:1879-1889`）

- `scope = service_scope(name).unwrap_or(0)`；锁 store 取 `(scope,name)` entry，返回 `entry.schema.clone()`。
- 只从**最近 scope** 找；不看 availability/withdrawn（裸 entry），所以不可用的 provider 的 schema 仍会被用于配置合并。
- `schema` 只在 `provide_service`/`provide_service_value` 路径写入；`provide`/`provide_any` 恒为 None。

### 3.3 调用点

- `Context::logger()`/`logger_named()`：`resolve_config("logger", None, None)`，再取 `name`/`level`
  （`context.rs:1903-1926`）；`config_level` 把数值 >3 夹到 `Error`，字符串走 `LogLevel::parse`（1942-1956）。
- `Service::resolve_config` 默认实现委托 `ctx.resolve_config(self.name(), base, head)`（`service.rs:297-303`）。
- 用户可直接 `ctx.resolve_config(...)` / `ctx.find_schema(...)`。
- **边界**：plugin 配置不走这条链（`spec/CORDIS-CORE.md:618-625`；`docs/GUIDE.md:517-529`）；它走 `ctx.plugin` 传入 config → `internal/config` waterfall → `ConfigSchema::validate`。

### 3.4 `ConfigSchema` 与 `JsonConfigSchema` 要点

- trait（`config.rs:93-112`）：`validate(&self, value) -> Result<Value, Vec<String>>`、`merge(&self, base, overrides) -> Value`；
  schema 由 embedder 实现，允许 panic，调用点必须 containment。
- `JsonConfigSchema`（`config.rs:152-377`）：
  - 校验：null→空对象；注入 defaults（typed nullable 默认不注入，可选字段保持 absent）；required 检查；类型检查；`deny_unknown` 拒绝未声明键（253-326）。
  - `merge`：base + overrides 逐层浅覆盖，再补 defaults；typed-null default 不 materialize（FIX-17）（352-376）。
  - `contained_validate` 用于 plugin config 校验（`config.rs:114-131`），不在本层主线。

---

## 4. 锁与临界区纪律

### 4.1 本层涉及的锁

| 锁 | 位置 | 保护对象 | 关键规则 |
|---|---|---|---|
| `service_store`（`Arc<Mutex<HashMap>>`） | 全树共享（`context.rs:49`） | 服务表 | 只做 map 读/写/查重/remove；**绝不跨用户回调持有** |
| `accessors`（per-node `Mutex`） | 每节点（`context.rs:51`） | 计算属性表 | 注册/查找/注销时短暂持有；getter 在锁外调用 |
| `clock`（per-tree `Mutex<Arc<dyn Clock>>`） | 树共享（`context.rs:56`） | 时钟句柄槽 | `clock()`/`set_clock` 短暂持有 |
| `FiberRef = Arc<Mutex<FiberData>>` | `fiber.rs:148` | fiber 状态/effect 列表/dep_versions/children | 内核各路径频繁使用；用户代码在锁外调用（例如 `Fiber::name` 先 clone plugin 再调用） |
| `Registry` 多个锁 | `registry.rs:402-...` | `counter/scope_counter/fibers/runtimes/label_scopes/idle_waiters/dep_index/service_dirty` | `dep_index` 是**叶子锁**：只做 map 操作，持有期间不得再取 fibers/runtimes/fiber.data（`registry.rs:452-455` 注释） |
| internal `Hooks<T>` Mutex | `internal.rs:459-494` | 各 hook 列表 | 注册/快照短暂持有；**snapshot 后锁外调用 hook**（`internal.rs:487-493`） |
| LoggerService 内部锁 | `logger.rs` | exporters/ring buffer | 不在本层主线 |

### 4.2 刻意放在锁外的用户代码（Go 必须照抄的纪律）

- **可用性谓词**：`lookup_entry` 在锁内 clone entry，`is_available()` 的 `check()` 在锁外执行（`context.rs:688-694` 注释；
  CORE-015）。这样 check 可以重入读服务/打日志，否则持有 store 锁调用用户代码会自死锁。
- **accessor getter/setter**：`find_accessor` 返回 clone 后，在锁外调用 getter/setter（`reflect.rs:61-65`；`context.rs:1996-1999`）。
- **internal hooks**：`emit_service`、`waterfall_get`、`waterfall_set`、lifecycle 广播都在快照后锁外调用（`internal.rs:620-664, 703-763`）。
- **schema merge/validate**：`contained_merge`/`contained_validate` 在 store 锁外（`find_schema` 先 clone schema 出锁）。
- **effect 注册的用户 setup**：`run_labeled_effect` 先压/弹 `effect_stack`（fiber 锁内短临界区），setup body 在锁外执行（`context.rs:1066-1096`）。
- **provider 状态判定**：entry clone 出 store 后，`provider_is_active` 再单独锁 provider fiber（`service.rs:124-143`），
  避免 store 锁 → fiber 锁的嵌套。

### 4.3 重入处理

- **`internal/get` 全局重入 latch**：`InternalBus.get_depth: AtomicUsize`；`waterfall_get` 在 latch 已置位时
  **直接走 terminal**（跳过所有 get hooks）（`internal.rs:703-726`）。这是“全局”而非 per-name 的：
  hook 内 `ctx.get(other)` 也不会再次触发 hooks。文档明确这是故意代价（`internal.rs:52-55`）。
- **refresh 重入**：`refresh_depth` CAS 0→1，失败则 `refresh_dirty=true` 并返回（外层循环接手）；
  `RefreshGuard::drop` 复位（`registry.rs:2094-2104, 677-685`）。`refresh_all_impl` 用 `refresh_dirty.swap` 决定是否再来一轮。
- **可用性检查重入**：无 latch，允许（CORE-015）；但 check 内部再 `ctx.get(name)` 不会递归到同一 check 的无限循环，
  除非 check 自己无限递归（内核不防栈溢出）。
- **`ctx.set` 重入**：`internal/set` hook 内再调 `ctx.set` 没有 latch；会嵌套写入/嵌套 inverse effect（Go 实现需保持同序或加显式保护。
  与 get 不同，set 不能简单“跳过 hooks”，需保留可观测性）。
- **accessor getter 递归**：getter 内再 `ctx.get(同 name)` 会再次命中同一 accessor → 无终止保护（Rust 会栈溢出 abort）。
  Go 移植建议加 per-goroutine/per-name 递归守卫或文档禁止（见 §7 风险）。
- `ServiceGate::notify` 对每个 watcher 调 `refresh()`（全量），可能嵌套在 provide/set 的 refresh 之中；
  由 refresh latch 合并，不会指数爆炸。

### 4.4 其他临界区事实

- `push_effect` 在 fiber 锁内检查 `state == Disposed/Unloading` 后才 push（`context.rs:1005-1050`），
  并在锁内 retain 掉已 disposed 的 effect（配额只数 live effect）。
- `insert_service` 第 3 步的锁范围仅 `contains_key` + `insert`；`note_service_changed`、`emit_service`、
  effect 注册、refresh 全在锁外（§2.5）。
- `write_service` 的锁内做 TypeId 复检 + 赋值 + 版本自增；`note_service_changed` 在锁外（`context.rs:2161-2193`）。
- `withdraw_owner_services` 一次锁内遍历全表并置位（`context.rs:2094-2105`），锁外才发事件/级联。
- `Registry::refresh_all_impl` 每轮对 fibers 取快照（`fibers.lock().snapshot()`），逐 fiber 时短暂锁 `FiberData`，
  **激活/卸载等用户代码在锁外**（`registry.rs:2163-2199`）。

---

## 5. 不变量与 panic 遏制

### 5.1 与本层直接相关的不变量（`docs/DESIGN.md:159-172`）

| 编号 | 内容 | 本层落点 |
|---|---|---|
| I3 | panic 不跨越内核边界：apply/cleanup/listener/hook/schema/exporter 逐个隔离 | §5.2 全部 containment 点；crate 有 `panic=unwind` 编译期契约（`lib.rs:49-56`） |
| I4 | 依赖变化驱动激活/停用与重算 | §2.10 版本号 + 索引 + `refresh_dependents`/`refresh` |
| I5 | 服务身份由 `(scope,name)` 唯一确定；类型错误必须报错而不是 UB | 存储键 `(scope,name)`；`set` TypeId 校验 → `ServiceTypeMismatch`（`context.rs:2123-2148`） |
| I6 | `ctx.set` 只允许改自己提供的服务；撤销按注册逆序 | `check_set_target` → `ServiceWriteForbidden`；inverse effect 挂 fiber，逆序执行（`context.rs:2024-2084`） |
| I7 | 生命周期互斥；Disposed 是终态 | `set` 在 `Unloading/Disposed` 拒绝；`push_effect` 拒绝（`context.rs:2027-2032, 1011-1013`） |

CORE 条目映射：CORE-004（DuplicateService）、CORE-006/013/014/015/016（可用性/严格门/无回退/锁外检查）、
CORE-025（accessor/meta）、CORE-032（unsized lane）、CORE-041（revertible set）、CORE-042（provider withdrawal order）。

### 5.2 panic 遏制点（本层）

| 位置 | 机制 | 失败降级 |
|---|---|---|
| `ServiceEntry::is_available` | `catch_unwind(check)` | panic → `false`（服务视为不可用，`service.rs:103-111`） |
| `Accessor::read` | `internal::contained` | panic → `None`（按属性不存在处理，`reflect.rs:61-65`） |
| accessor setter（`set`/`set_any`） | `catch_unwind` | panic → `false` → `AccessorWriteRejected`（`context.rs:1996-2007, 637-650`） |
| `ConfigSchema::merge` | `contained_merge` | panic → `base`（`config.rs:133-150`） |
| `ConfigSchema::validate` | `contained_validate` | panic → 校验错误（plugin 配置路径，`config.rs:114-131`） |
| internal/service 观察者 | `contained` per hook | panic 被记录，其余 hook 继续；不可 veto（`internal.rs:635-641`） |
| internal/get waterfall | `contained` + 链继续 | panic → `next.next()`（`internal.rs:703-726`） |
| internal/set waterfall | `contained` + 链继续 | panic → `next.next()`，不会否决（`internal.rs:729-748`） |
| internal/config、internal/update | `contained` + 链继续 | update panic 不 veto（文档明确，`internal.rs:343-379`） |
| internal/listener | `contained` + first-wins | panic 视为无替换（`internal.rs:751-763`） |
| lifecycle 广播 | `contained` per subscriber | 其余订阅者继续（`internal.rs:623-633`） |
| `Disposer::dispose` | 每个 cleanup `catch_unwind`，记录首个 panic 后继续，最后 re-raise | 不跳过后续 cleanup（`effect.rs:36-58`）；`Drop` 路径吞掉 panic 只记日志（`effect.rs:60-86`） |
| plugin 元数据（`name/inject/schema` 等） | `*_contained` helper（registry/fiber） | 退化为占位运行时（不在本层展开） |

政策：**panics never cross the kernel boundary**；fallback 语义见上表（get/set/config/update “continue with incoming value”，accessor “absent”，check “false”，schema “base”）。Rust 用 `panic = "unwind"` 才成立，`panic=abort` 直接编译失败（`lib.rs:49-56`）。

### 5.3 Rust 现状中的边界/欠账（Go 实现要修）

1. `insert_service` 在 store 插入之后才 `register_effect_labeled`；后者可能因 fiber `Unloading/Disposed` 返回
   `InactiveEffect`，但 store entry **未回滚**（`context.rs:507-539`）。Go 应改为“先能注册 effect，再提交 entry”或失败回滚。
2. `register_accessor` 同样先写 accessors 再注册 effect（`reflect.rs:149-177`），effect 注册失败时不回滚 accessor。
3. `set_revertible` 在写入（waterfall terminal）之后才注册 inverse effect；若注册失败，store 已被改、inverse 丢失（`context.rs:2041-2083`）。
4. `write_service` 的错误被 `.is_ok()` 折叠成 “hook veto”（`context.rs:2045-2051`）：并发下 entry 被移除时 `set` 返回 `Ok` 但未写入，
   Go 实现最好区分“veto”与“terminal 错误”。
5. `provide` 的 disposer `remove` 不校验 owner/版本：若同一 `(scope,name)` 已被 `set` 替换，disposer 移除的是当前 entry（`context.rs:526-530`）。
6. `ServiceGate::watch` 不去重（`service.rs:230-235`），重复 provide_gated 会重复 refresh。
7. accessor getter / `internal/set` hook 的重入没有终止保护（§4.3）。

---

## 6. Rust 专有机制与 Go 移植含义

| Rust 机制 | 使用位置 | Go 替代建议 | 风险 |
|---|---|---|---|
| `Arc` / `Arc::clone` 共享句柄 | Context/Store/Events/Registry/Schema/accessor/check | 直接用指针 + GC；共享字段放 `*treeCore`；不要复制 | 低 |
| `Weak` 打破环 + 判存活 | 根 fiber `FiberLink::Weak`（`fiber.rs:127-146`）、`ServiceEntry.provider`、`ServiceGate.watchers`、disposer 捕获 weak Context（`context.rs:525`、`reflect.rs:167`） | Go GC 能收环；但“存活感知/不复活”要显式化：provider 存 `*Fiber` 且判 State；watcher/disposer 存带 `closed/live` 标志的引用或 `runtime.AddCleanup` | **中高**：Rust 的 weak→None 被当作 fail-open（`provider_is_active` 返回 true），Go 没有 weak 语义，需明确“已释放 = 不可见还是 fail-open” |
| `TypeId` | `ServiceKey<T>` 类型槽、`ServiceEntry.type_id`、`ctx.set` 校验（`service.rs:58-60`） | `reflect.Type`（可比较/可 hash）；typed lane 存 `*T`，any lane 包一层以区分 lane；接口类型用 `reflect.TypeOf((*I)(nil)).Elem()` | **中**：Go 1.27 支持泛型方法，typed API 可写成 `ctx.Get[T](key)`；接口方法仍不能带类型参数；typed-nil、指针/值类型差异、跨包同名类型 |
| `thread_local!` 激活帧 | `fiber.rs:1810-1857`：`ACTIVATION_FRAMES: RefCell<Vec<(fiber_id,epoch)>>`，给 effect 打“当前尝试 epoch”（FIX-5）；`async_effects.rs:80` 同类 | Go 无 goroutine-local；把 epoch 作为显式参数穿过 activation/effect 注册 API（或放在 `context.Context` value 里随调用链传递）；不要用 goroutine id hack | **高**：影响 effect 归属/回滚精确性，是 fiber 层接口设计的关键输入 |
| `catch_unwind` + `AssertUnwindSafe` | 本层全部 containment 点（§5.2） | `defer func(){ if r:=recover(); ... }()` 包每个用户回调；**只对同 goroutine 有效**；不要跨 goroutine 依赖 recover | 中：Go panic 跨 goroutine 直接进程崩溃；契约上用户回调必须同步调用 |
| `Disposer` + `Drop` 安全网 | `effect.rs:22-86`：single-shot cleanup，`Drop` 时若强引用唯一则执行 | `Disposer{ once sync.Once; cleanups []func() }` + `Dispose()`；GC 安全网用 `runtime.AddCleanup`（Go 1.24+）或显式要求 Dispose；**不能依赖 GC 做顺序清理** | **高**：Rust `Drop` 可保证“最后一个句柄释放即清理”，Go 没有确定性析构；需把“谁负责 Dispose”写进 API 契约 |
| `Mutex` 中毒语义 | `provider_is_active` 锁中毒 fail-open（`service.rs:132-141`）；许多地方 `map_err -> Disposed` | Go `sync.Mutex` 不会中毒；去掉 poison 分支，统一“锁失败/状态未知”的降级（Rust 的 fail-open 要成为显式策略） | 中 |
| `&'static str` 名字/标签 | ServiceKey name、scope label、effect label | `string`；键可任意生命周期；标签仅诊断用 | 低 |
| `serde_json::Value` | config/intercept/base/head | `any`（`map[string]any`）或 `json.RawMessage`；合并仍是浅层 object 覆盖；注意 nil 语义 | 中：Go nil map/typed-nil 与 JSON null 的区分 |
| `HashMap<(u64,String),..>` | service store | `map[ServiceKeyStruct]ServiceEntry` + `sync.Mutex`（或 `RWMutex`/COW 快照） | 低 |
| `AtomicU64` 全局版本 | `SERVICE_VERSION`（`service.rs:23-27`） | `atomic.Uint64`；每棵树独立或全局均可（版本只比相等） | 低 |
| `unsized`（`?Sized`/`dyn Trait`/`ArcWrap`） | any lane（`service.rs:29-60`） | Go `any` 存接口值天然支持；用 lane 标记 + 包装类型复刻 TypeId 身份规则 | 中 |
| 泛型 `impl Trait`/closure 参数 | provide_checked 的 `F: Fn() -> bool`、hooks | Go 函数类型（`func() bool` 等）；不要为 hook 造泛型接口 | 低 |
| async/await（`when_idle`、`refresh_async`、async plugin） | `context.rs:759-793`、fiber async 路径 | Go 无 async 颜色：`WhenIdle(ctx context.Context) error` 阻塞/等待；`Refresh` 同步；“async 插件”后期用 goroutine + 完成通道设计 | 中：需重新设计生命周期等待原语（WaitGroup/Cond/chan） |
| `Send + Sync` 约束 | ServiceKey/ServiceValue/所有 hook | Go 并发安全由实现保证；文档标注哪些回调必须并发安全（check/hook 可能被多 goroutine 调用） | 中 |

---

## 7. Go 设计建议与顶层风险

### 7.1 建议的内部形状（字段级，不含代码）

- `Context`：`struct { core *treeCore; parent *Context; fiber *Fiber; isolate map[string]ScopeID; intercepts map[string]any; accessors *sync.Mutex + map; meta ContextMeta }`。
- `treeCore`：`struct { events *Events; registry *Registry; logger *LoggerService; store *sync.Mutex + map[ServiceKey]ServiceEntry; clock *atomic.Pointer[Clock] }`；
  派生 context 共享 `core` 指针；`ServiceEntry` 见 §2.3；`ServiceKey` 可比较（`ScopeID uint64`+`Name string`）；Effect 归 `Fiber.effects []Effect` 逆序执行。

### 7.2 Top 风险（按严重度）

1. **typed API 形态**：Go 1.27 起可用泛型方法 → `ServiceKey[T]` + `ctx.Get[T](key)`/`ctx.Provide[T]`（类型参数推断），包级自由函数作包装。接口方法不能带类型参数，故 Plugin 等接口保持非泛型。
2. **确定性清理**：Rust 用 `Drop`/effect 逆序保证卸载即清理；Go 只有显式 Dispose + GC。必须把 dispose 责任、幂等、逆序、失败策略写进契约。
3. **panic 遏制**：Go recover 仅同 goroutine；且 Rust 的 fallback 语义各 hook 不同（get/set/config/update “继续”，check “false”，accessor “absent”）。必须逐个复刻并写 conformance 测试。
4. **锁外用户代码**：Go `sync.Mutex` 不可重入且无中毒兜底；一旦在 store 锁内调用 check/accessor/hook 就会自死锁。Rust 的“clone entry 出锁再判定”模式必须照抄。
5. **双 lane + TypeId 身份**：Go 的 typed-nil、接口值、指针 vs 值会让“同一服务被不同类型覆盖”更难守；保留 Lane 标记与 `reflect.Type` 双检。
6. **thread_local 激活帧**：影响 effect 的 epoch 归属与回滚精确性；Go 必须显式传参（建议 `Activation` 句柄或 `context.Context`）。
7. **重入 latch**：internal/get 全局 latch、refresh_depth CAS 必须复刻；accessor/set 的递归风险建议加 per-goroutine 守卫。
8. **provider 存活语义**：Rust 的 Weak 失效 = fail-open（可用）；Go 若存强指针会延长 provider 生命周期，存弱引用需自定义。需产品级决定。
9. **Rust 现状欠账**（§5.3）：Go 实现建议直接修正（失败回滚、veto 与错误的区分），但要在文档标注与上游的行为差异。
10. **async 生命周期**：`when_idle`/`refresh_async`/async plugin 在 Go 需要新的等待原语；建议 v1 先只实现同步路径 + 显式等待接口。

### 7.3 conformance 测试映射（建议）

- CORE-004/005/014/015/025/032/041/042 直接对应用例（`spec/CORDIS-CORE.md:832-940`）：
  DuplicateService、scope 遮蔽、check 锁外可重入、accessor 优先/写拒绝、trait-object lane、set 可逆+版本重算、withdraw 顺序。
- 双 lane 交叉测试：typed 注册 vs `get_any` 读、any 注册 vs `set` 写，期望均失败/NotFound。
- 重入测试：check 内 `ctx.get`、internal/get hook 内 `ctx.get`（应绕过 hooks）、gate flip 期间的 refresh。
- 锁纪律测试：`-race` 下 check/accessor/hook 内再次读服务；dispose 与 read 并发。

---

## 8. 附录 A：public 方法/类型签名清单

> 仅列 public（`pub(crate)` 内部入口注明）。行号在括号中。

### A.1 `context.rs` · 构造与派生

```rust
pub fn root() -> Self;                                      // 128
pub fn fiber(&self) -> Fiber;  pub fn registry(&self) -> Arc<Registry>;  pub fn events(&self) -> Arc<Events>; // 218/222/226
pub fn extend(&self) -> Self;                               // 231
pub fn isolate(&self, name: &'static str) -> Self;          // 241
pub fn isolate_labeled(&self, name: &'static str, label: &'static str) -> Self; // 252
pub fn intercept(&self, name: &'static str, config: Value) -> Self; // 314
pub fn fiber_id(&self) -> u64;                              // 336
pub fn clock(&self) -> Arc<dyn crate::clock::Clock>;        // 345
pub fn set_clock(&self, clock: Arc<dyn Clock>) -> Result<()>; // 380
// 内部：from_weak(196,pub(crate))、child(200,priv)、child_with_isolate(258,priv)、
//       child_with_meta(277,pub(crate))、service_scope(297,pub(crate))
```

### A.2 服务注册/读取/写

```rust
pub fn provide<T: Any + Send + Sync>(&self, key: ServiceKey<T>, value: Arc<T>) -> Result<Disposer>; // 406
pub fn provide_checked<T, F>(&self, key: ServiceKey<T>, value: Arc<T>, check: F) -> Result<Disposer>
    where T: Any + Send + Sync, F: Fn() -> bool + Send + Sync + 'static;                            // 420
pub fn provide_gated<T: Any + Send + Sync>(&self, key: ServiceKey<T>, value: Arc<T>, gate: &ServiceGate) -> Result<Disposer>; // 435
pub fn get<T: Any + Send + Sync>(&self, key: ServiceKey<T>) -> Option<Arc<T>>;                     // 565
pub fn require<T: Any + Send + Sync>(&self, key: ServiceKey<T>) -> Result<Arc<T>>;                // 578
pub fn provide_any<T: ?Sized + Send + Sync + 'static>(&self, key: ServiceKey<T>, value: Arc<T>) -> Result<Disposer>; // 589
pub fn get_any<T: ?Sized + Send + Sync + 'static>(&self, key: ServiceKey<T>) -> Option<Arc<T>>;    // 598
pub fn require_any<T: ?Sized + Send + Sync + 'static>(&self, key: ServiceKey<T>) -> Result<Arc<T>>; // 612
pub fn set_any<T: ?Sized + Send + Sync + 'static>(&self, key: ServiceKey<T>, value: Arc<T>) -> Result<()>; // 627
pub fn service_available(&self, name: &str) -> bool;                                              // 666
pub fn get_named(&self, name: &str) -> Option<ServiceValue>;                                      // 1976
pub fn set(&self, name: &'static str, value: ServiceValue) -> Result<()>;                         // 1993
pub fn set_revertible(&self, name: &'static str, value: ServiceValue) -> Result<Disposer>;        // 2024
pub fn resolve_config(&self, name: &str, base: Option<Value>, head: Option<Value>) -> Value;      // 1847
pub fn find_schema(&self, name: &str) -> Option<Arc<dyn ConfigSchema>>;                           // 1881
// 内部：provide_entry(445,priv)、provide_service_value(457,pub(crate))、insert_service(467,priv)、
//       accessor_value(548,priv)、lookup_entry(672,priv)、lookup(688,pub(crate))、
//       lookup_for_injection(703,pub(crate))、withdraw_owner_services(2094,pub(crate))、
//       check_set_target(2107)/check_set_type(2130)/write_service(2161)
```

### A.3 effect / plugin / 生命周期 / 事件

```rust
pub fn checkpoint(&self) -> Result<()>;                                                   // 713
pub fn dispose_all(&self) -> Result<()>;  pub async fn when_idle(&self);                   // 738/759
pub fn refresh(&self);  pub async fn refresh_async(&self);                                 // 775/790
pub fn attach_effect(&self, disposer: Disposer, label: &'static str) -> Result<()>;        // 798
pub fn effect<F: FnOnce() -> Effect>(&self, setup: F) -> Result<Disposer>;                  // 1132
pub fn effect_labeled<F: FnOnce() -> Effect>(&self, label: &'static str, setup: F) -> Result<Disposer>; // 1141
pub fn effect_async<F: FnOnce() -> AsyncEffect>(&self, setup: F) -> Result<AsyncDisposer>;  // 1170
pub fn effect_async_labeled<F>(&self, label: &'static str, setup: F) -> Result<AsyncDisposer>; // 1178
pub fn effect_future<F, Fut>(&self, body: F, label: &'static str) -> Result<AsyncDisposer>; // 1648
pub async fn effect_future_async<F, Fut>(&self, body: F, label: &'static str) -> Result<AsyncDisposer>; // 1664
pub fn effect_stream<S: Stream<Item = AsyncCleanup> + Send + 'static>(&self, stream: S, label: &'static str) -> Result<AsyncDisposer>; // 1687
pub fn plugin(&self, plugin: Arc<dyn Plugin>, config: Value) -> Result<Fiber>;              // 1187
pub fn plugin_typed<P: Plugin>(&self, plugin: Arc<P>, config: Value) -> Result<Fiber>;      // 1200
pub async fn plugin_async(&self, plugin: Arc<dyn AsyncPlugin>, config: Value) -> Result<Fiber>; // 1402
pub async fn plugin_async_typed<P: AsyncPlugin>(&self, plugin: Arc<P>, config: Value) -> Result<Fiber>; // 1423
pub fn inject<F>(&self, deps: &'static [&'static str], callback: F) -> Result<Fiber>
    where F: Fn(Context) -> Result<()> + Send + Sync + 'static;                             // 1454
#[cfg(feature="supervision")] pub fn heartbeat(&self); pub fn set_quota(&self, quota: FiberQuota);
pub fn quota(&self) -> FiberQuota; pub fn stalled_fibers(&self, timeout: Duration) -> Vec<StalledFiber>; // 910/916/922/929
pub fn on<F>(&self, name: &'static str, listener: F) -> Result<Disposer>;                   // 1480
pub fn on_with<F>(&self, name, prepend: bool, global: bool, listener: F) -> Result<Disposer>; // 1487
pub fn on_with_options<F>(&self, name, options: EventOptions, listener: F) -> Result<Disposer>; // 1510
pub fn once<F>(&self, name, listener: F) -> Result<Disposer>;                                // 1539
pub fn on_async<F, Fut>(&self, name, listener: F) -> Result<Disposer>;                       // 1584
pub fn on_async_with<F, Fut>(&self, name, prepend: bool, listener: F) -> Result<Disposer>;   // 1594
pub fn on_waterfall<F>(&self, name, listener: F) -> Result<Disposer>;                        // 1745
pub fn on_waterfall_async<F>(&self, name, listener: F) -> Result<Disposer>;                  // 1767
pub fn emit(&self, name: &str, args: &[Value]); pub fn bail(&self, name: &str, args: &[Value]) -> Option<Value>; // 1785/1791
pub async fn parallel_async(&self, name: &str, args: &[Value]);                              // 1798
pub async fn serial_async(&self, name: &str, args: &[Value]) -> Option<Value>;               // 1805
pub fn waterfall(&self, name: &str, args: &[Value], terminal: Option<WaterfallTerminal>) -> Value; // 1812
pub async fn waterfall_async(&self, name: &str, args: &[Value], terminal: Option<AsyncWaterfallTerminal>) -> Value; // 1824
// 内部：register_effect_labeled(935,pub(crate))、register_user_effect_labeled(944,pub(crate))、refresh_dependents(1837,pub(crate))
```

### A.4 logger / internal hooks

```rust
pub fn plugin_name(&self) -> String;  pub fn logger(&self) -> Logger;  pub fn logger_named(&self, name: &str) -> Logger; // 1899/1908/1921
pub fn logger_service(&self) -> Arc<LoggerService>;  pub fn log_exporter(&self, exporter: Arc<dyn LogExporter>) -> Result<Disposer>; // 1929/1935
pub fn on_internal_plugin(&self, hook: PluginHook) -> Result<Disposer>;                     // 2202
pub fn on_lifecycle(&self, listener: LifecycleListener) -> Result<Disposer>;               // 2252（不随 fiber 卸载）
pub fn on_internal_status(&self, hook: StatusHook) -> Result<Disposer>;                     // 2284
pub fn on_internal_service(&self, hook: ServiceHook) -> Result<Disposer>;                   // 2291
pub fn on_internal_config(&self, hook: ConfigHook) -> Result<Disposer>;                     // 2298
pub fn on_internal_update(&self, hook: UpdateHook) -> Result<Disposer>;                     // 2305
pub fn on_internal_get(&self, hook: GetHook) -> Result<Disposer>;                           // 2312
pub fn on_internal_set(&self, hook: SetHook) -> Result<Disposer>;                           // 2319
pub fn on_internal_listener(&self, hook: ListenerHook) -> Result<Disposer>;                 // 2326
pub fn on_internal_dispatch(&self, hook: DispatchHook) -> Result<Disposer>;                 // 2333
```

### A.5 `reflect.rs`（`impl Context` 与类型）

```rust
pub type AccessorGet = Arc<dyn Fn(&Context) -> ServiceValue + Send + Sync>;                 // 28
pub type AccessorSet = Arc<dyn Fn(&Context, ServiceValue) -> bool + Send + Sync>;           // 30
pub struct Accessor { pub get: AccessorGet, pub set: Option<AccessorSet> }                  // 33-39
impl Accessor { pub fn new(get) -> Self; pub fn with_setter(get, set) -> Self; }            // 43-53
pub struct ContextMeta;                                                                     // 70-73
impl ContextMeta { pub fn new() -> Self; pub fn insert/insert_any(&mut self, key, value);    // 77/82/87
    pub fn get<T: Any + Send + Sync>(&self, key: &'static str) -> Option<Arc<T>>;           // 92
    pub fn contains(&self, key: &'static str) -> bool; pub fn len(&self) -> usize; pub fn is_empty(&self) -> bool; } // 99-111
impl Context {
    pub fn accessor<Get>(&self, name: &'static str, get: Get) -> Result<Disposer>;          // 127
    pub fn accessor_with_setter<Get, Set>(&self, name, get, set) -> Result<Disposer>;       // 136
    pub fn extend_meta(&self, meta: ContextMeta) -> Context;                                // 197
    pub fn meta<T: Any + Send + Sync>(&self, key: &'static str) -> Option<Arc<T>>;          // 202
    pub fn meta_any(&self, key: &'static str) -> Option<Arc<dyn Any + Send + Sync>>;        // 208
}
// 内部：Accessor::read(61,pub(crate))、register_accessor(149,priv)、find_accessor(180,pub(crate))
```

### A.6 `service.rs` public

```rust
pub type ServiceValue = Arc<dyn Any + Send + Sync>;                                         // 15
pub type AvailabilityCheck = Arc<dyn Fn() -> bool + Send + Sync>;                           // 73
pub struct ServiceKey<T: ?Sized + Send + Sync + 'static>;                                   // 152
impl<T: ?Sized + Send + Sync + 'static> ServiceKey<T> { pub const fn new(name) -> Self; pub const fn name(&self) -> &'static str; } // 158/165
pub struct ServiceGate;   // new(204)/is_available(210)/set(216)/check(225)                 // 196-249
// trait Service: name()/check() -> Option<AvailabilityCheck>/config_schema() -> Option<Arc<dyn ConfigSchema>>/
//                resolve_config(ctx, base, head) -> Value（默认实现委托 ctx.resolve_config） // 283-304
pub fn provide_service<S: Service>(ctx: &Context, service: Arc<S>) -> Result<Disposer>;     // 313
impl Context { pub fn provide_service<S: Service>(&self, service: Arc<S>) -> Result<Disposer>; } // 326
// 内部：ArcWrap/wrap_service/unwrap_service/service_type_id(34-60)、next_version(25)、
//       ServiceEntry 字段(76-100)、is_available(103)、is_available_for_injection(120)、
//       provider_is_active(124)、ServiceGate::watch(230,pub(crate))
```

### A.7 `config.rs` / `clock.rs`（本层直接使用的）

```rust
pub trait ConfigSchema: Send + Sync {                                                       // config.rs:97
    fn validate(&self, value: Value) -> std::result::Result<Value, Vec<String>>;            // 103
    fn merge(&self, base: Value, overrides: &[Value]) -> Value;                             // 111
}
pub struct JsonConfigSchema; // new/required/default_value/deny_unknown/property/defaults/required_keys + impl ConfigSchema // 152-377
pub trait Clock: Send + Sync { fn now_unix_ms(&self) -> u64; fn now_monotonic_ms(&self) -> u64; } // clock.rs:36-48
pub struct SystemClock;                                                                      // clock.rs:53-67
```

---

## 9. 附录 B：错误变体清单（`error.rs:7-88`）

| 变体 | 本层触发点 | 语义 |
|---|---|---|
| `InactiveEffect` | `push_effect` 在 fiber `Disposed/Unloading`（`context.rs:1011-1013`）；`set_revertible` 在卸载中（2027-2032） | effect/写入被拒绝 |
| `DuplicateService(String)` | `insert_service` 查重（`context.rs:504-506`） | 同 `(scope,name)` 已存在 |
| `UnknownService(String)` | `context.rs:578-581, 612-618`；`check_set_target/check_set_type/write_service` | 服务不存在/当前不可读 |
| `ServiceWriteForbidden(String)` | `check_set_target`：owner != 当前 fiber（`context.rs:2116-2118`） | 改别人的服务 |
| `AccessorReadOnly(String)` | `set`/`set_any` 命中无 setter 的 accessor（2009, 650） | accessor 只读 |
| `AccessorWriteRejected(String)` | accessor setter 返回 false 或 panic（2006, 647） | 计算属性拒绝写入 |
| `ServiceTypeMismatch(String)` | `check_set_type` / `write_service`（2144, 2178） | 写入类型与注册 TypeId 不符（DESIGN I5） |
| `Disposed` | 各锁 `map_err`（如 `context.rs:387, 502, 2038`）；`push_effect` 锁失败 | fiber/内核已释放 |
| `InvalidPlugin(String)` | accessor 同名重复（`reflect.rs:156-159`）；`set_clock` 非根 context（`context.rs:381-385`） | 插件/用法错误 |
| `Validation(ValidationError)` | plugin config schema 校验（`contained_validate` 相关） | 配置校验失败 |
| `QuotaExceeded(String)` | `supervision`：listener/effect/child_fibers 配额（`context.rs:821, 1033, 1278`） | 资源配额超限 |
| `LifecycleBusy` | 生命周期互斥（fiber 层主线） | 生命周期操作重叠 |
| `PluginFailed(String)` / `Plugin{plugin,kind,message}` / `Rollback(String)` | 激活失败/回滚（fiber/registry 主线） | 插件失败 |
| `Cancelled` | `Context::checkpoint`（`context.rs:713-730`） | 协作取消 |
| `AsyncPluginNeedsAsync(String)` | sync API 用于 async 插件（fiber 主线） | 需 async 入口 |

完整枚举（17 项）见 `error.rs:9-88`；本层高频是 `DuplicateService/UnknownService/ServiceWriteForbidden/AccessorReadOnly/AccessorWriteRejected/ServiceTypeMismatch/InactiveEffect/Disposed/InvalidPlugin`。

---

## 10. 与 Go 实现对齐的检查清单（交付给 Lead/实现者）

1. 先定 typed lane API 形态（`ServiceKey[T]` + Go 1.27 泛型方法，包级自由函数作包装），再写 `Context` 骨架。
2. `treeCore` 共享（events/registry/logger/store/clock）+ 每节点 `isolate/intercepts/accessors/meta` 复制（§1.4）。
3. `ServiceEntry` 双 lane + `Type`/反射校验 + owner/version/withdrawn/provider 状态；三组查询谓词分开实现：
   `lookup`（非严格）、`lookupForInjection`（严格）、accessor（只读 lane）。
4. `Provide` 失败回滚、`SetRevertible` 的 inverse effect、`WithdrawOwnerServices` 顺序、`Refresh` 固定点（64 轮 + latch）。
5. containment 包装（safeCheck/safeAccessor/safeHook/safeMerge）；锁外调用用户代码；`resolve_config` 的
   root→leaf 顺序、head 最高、schema 优先、浅合并 fallback；`find_schema` 只查最近 scope。
6. 公共 API 与错误码按附录 A/B 生成；为 CORE-004/005/006/013/014/015/025/032/041/042 写 conformance 测试。
