# 编译期模块注册表：设计审查与原型验证

> 本文回答：**如果只做「编译期注册表」这一条路线（不引入进程外/WASM/动态加载），
> 这个内核的设计思想核心上有没有问题？**
>
> 结论先行：
> 1. **没有致命问题**。Cordis 的 context/fiber/effect/service/event 组合模型可以承载
>    「编译期模块组合」；内核几乎不需要改动，缺的模块系统语义（清单、预检、可选依赖、
>    失败策略、稳定实例 ID、装配顺序）可以在内核**之上的 `module` 包**里补齐。
> 2. **但必须先接受一个前提**：编译期注册表是 **link-time composition（链接期组合）**，
>    不是 runtime plugin host。模块集合在宿主构建时确定；「接入一个新插件系统」= 把它的
>    Go adapter 包加进宿主并重新构建，而不是运行时装上一个新插件。若目标里包含
>    「不重新编译宿主就能接入任意第三方插件系统」，则这条机制与目标**自相矛盾**，
>    需要重新定义目标。
> 3. 在这个前提下，原型（`module/` 包 + 测试）已经验证：类型化清单、预检（缺 provider/
>    类型不匹配/重复 provider/依赖环/坏配置）、可选依赖、provider-first 装配顺序、
>    required/optional 失败策略、回滚、稳定 entry ID、reload、locate、运行时清单校验、
>    多宿主隔离都能在**不改内核**的情况下成立。

---

## 1. 机制定义：编译期注册表到底是什么

```
构建期（link time）                         运行期（runtime）
┌─────────────────────────────┐             ┌────────────────────────────────┐
│ host main                   │             │  Root Context（内核）          │
│  ├─ import module packages  │             │    ├─ entry "db"   → fiber     │
│  ├─ registry.Register(...)  │────────────▶│    ├─ entry "http" → fiber     │
│  └─ module.Load(cfg)        │   config    │    └─ entry tree / effects     │
└─────────────────────────────┘   (JSON)    └────────────────────────────────┘
```

- **编译期固定的是「代码/模块集合」**：每个模块是一个 Go 包，导出一个
  `module.Descriptor[T]`（ID、版本、提供/依赖的服务契约、typed config、Factory），
  宿主在 `main`（或生成的 assembly 文件）里显式注册。
- **运行期可变的是「实例/配置/树形」**：entry 树决定加载哪些模块实例、用什么配置、
  如何嵌套、哪些 disabled/optional；可以 reload 实例（dispose + 重建），但**不能换代码**。
- **和官方 Cordis 的关系**：官方 core 也不含 loader；loader 是独立包。我们等价于
  「cordis core + 一个编译期版 loader（EntryTree 的 Go 子集）」，不包含 ESM 动态 import。

### 1.1 能做到 / 不能做到（必须写进产品预期）

| 能力 | 编译期注册表 |
| --- | --- |
| 任意 Go 模块（被编译进宿主的）按配置组合成系统 | ✅ |
| 类型化配置、服务契约、依赖门控、生命周期/回滚 | ✅（内核 + module 包） |
| 多产品/OEM 共用同一内核、各自组装不同模块集 | ✅（各自 main + build tags） |
| 运行期新增一个**未编译进二进制**的模块 | ❌ 必须重新构建 |
| 加载非 Go 插件（Python/Node/Rust/Java…） | ❌（除非编译期写 adapter 包，由它管理外部进程；那已超出「纯编译期」定义） |
| 不可信插件沙箱/权限隔离 | ❌（同二进制内所有模块同等可信；靠代码审查/供应链治理） |
| 模块代码级热更新（换实现不重启进程） | ❌（只能实例级 dispose/recreate） |
| 跨版本稳定 ABI | ❌（也不需要：全部静态链接） |
| 单二进制、无运行时依赖、启动确定、可交叉编译 | ✅ 这是该路线最大的收益 |

> 如果「接入所有插件系统」的真实含义是上表 ❌ 的任一行，那么问题不在内核设计，
> 而在机制选型：编译期注册表天然做不到。若含义是 ✅ 各行（所有系统都是 Go 程序，
> 各自把需要的模块编进自己的二进制），则设计成立。

---

## 2. 为什么这个内核适合做编译期组合的底座

| 内核机制 | 对模块系统的价值 |
| --- | --- |
| **反应式 inject 门控**（依赖就绪自动激活、失效自动卸载） | 装配顺序与依赖解析解耦：entry 可以任意顺序创建，provider 后到也能激活；模块作者只需声明依赖 |
| **fiber 事务化激活**（apply 失败回滚所有注册） | 单个模块启动失败可精确回滚，不污染系统 |
| **effect 逆序撤销** | 模块卸载/重载 = 撤销它的一切，天然适合「模块实例」语义 |
| **child fiber 所有权** | entry 树 = fiber 树；父模块卸载级联卸载子树；loader 不需要自己记录清理 |
| **effect 上的服务/listener 所有权** | 模块提供的服务、事件监听随模块实例销毁，避免「幽灵注册」 |
| **panic 遏制 + 结构化错误** | 一个模块崩溃不会拖垮宿主；loader 能把 Failed 实例报告出来 |
| **typed ServiceKey + Go 1.27 泛型方法** | 服务契约在编译期可检查（包级 contract），运行期还有类型校验 |
| **Registry/IsIdle/WhenIdle** | 装配完成后等待静默、确认启动结果；支持宿主 shutdown |
| **无 ABI/无 unsafe/纯静态** | 运维、交叉编译、供应链审计简单；没有动态链接器/沙箱攻击面 |

**结论**：内核作为「进程内模块组合运行时」的语义是完备的；它本身不是模块系统，
但正好是模块系统需要的那种小内核。

---

## 3. 核心设计审查：12 个必须明确的设计点

> 标注：**P0** = 不解决模块系统就不可用；**P1** = 规模化/生产化需要；**P2** = 可延后。
> 「原型」列表示 `module/` 包是否已处理。

### I1. 身份与命名空间（P0）

**问题**：Cordis 有三层名字容易混淆：模块 ID、实例 ID（entry）、服务名。
- 内核 `PluginKey` 以插件 Name/Type 去重；若用模块显示名做 key，两个不同包的同名模块会
  被合并成一个 runtime，配置/实例互相污染。
- 服务名是全局字符串（同一 scope）；第三方模块套件很容易都叫 `"db"`/`"cache"`/`"logger"`。
- entry 树需要**稳定实例 ID** 用于 reload/locate/持久化配置；内核 Fiber ID 在重启后变化。

**后果**：命名冲突、错误去重、reload 找不到实例、配置漂移。
**对策（已实现）**：模块 ID 用包路径式全局唯一；entry ID 由配置显式给出且全树唯一；
Adapter 的 kernel plugin key 用 `module:<ID>`；服务契约推荐放进共享 contract 包并用
`ServiceKey[T]` 作为唯一权威；预检检测同一 scope 内重复 provider。
**仍建议**：约定服务名带模块前缀（`app/db`、`acme/cache`）；或后续给 loader 加
group/include + isolate 策略做命名空间隔离。

### I2. 依赖模型：只有「全 required 的服务名」不够（P0）

**问题**：内核 `inject` 只支持必选服务名列表；没有 optional、没有模块级依赖、没有版本/能力
声明。直接后果：
- 配置里少了一个 provider → 依赖方**永久 Pending**，不报错、不 Failed，用户以为"加载了"；
- 可选模块（metrics/tracing）缺失 → 要么写进 inject 把核心模块拖成 Pending，要么完全不声明、
  运行期 if nil 满天飞；
- 同名服务类型不一致 → `Get` 静默返回 nil；
- 服务依赖环 → 全部 Pending，无任何诊断。

**对策（已实现）**：模块清单声明 `Provides []Provider` / `Requires []Requirement`
（`Optional` 标志）；预检做 provider 唯一性、required 缺失、类型 assignability、required
依赖环检测，一次性收集所有 issue；optional 缺失只 warning；Adapter 的 inject 只包含
required，optional 由模块体 `ctx.Get` 探测。
**仍建议**：清单加 `Version`/能力（capability）字段与兼容性规则（P1）；对"可选依赖在
provider 后加载"的场景用 loader 的 provider-first 调度（已实现）或声明 optional 即接受
"加载时快照"语义。

### I3. 系统级启动事务与失败策略（P0）

**问题**：内核的事务粒度是**单个 fiber**；没有"整个系统要么全部就绪、要么回滚"的概念，
也没有"required 模块失败 → 启动失败"的概念。模块系统必须有明确策略，否则产品启动会出现
"部分模块活着、部分静默 Pending"的半可用状态。
**后果**：启动不确定性、健康检查无法表达、排障困难。
**对策（已实现）**：`module.Load` 先全量预检（不创建任何 fiber）→ 按 provider-first 调度
创建 → `WhenIdle` + 逐实例 `AwaitSettled` → 校验状态：
- required 实例 Failed/Pending → 整体回滚 + `LoadError`（列出所有问题）；
- optional 实例 Failed/Pending → 保留实例 + warning，不阻塞系统；
- 预检错误 → 不创建 fiber。
**仍建议**：增加可配置的启动策略（`FailFast`/`BestEffort`/`RequireActive`）与健康/就绪 API；
若需要"部分启动 + 后台重试"，在 Loader 层做，不建议改内核。

### I4. 进程级全局状态 vs 多宿主/多实例（P1）

**问题**：内核有几个进程级单例：`SetBlockingExecutor`（首次安装胜）、orphan 队列、
`slog.Default` 诊断通道。单宿主没问题；但「任何系统都用这个内核」意味着可能同进程多宿主
（测试、多租户、插件市场宿主）——这些单例会互相影响。
**对策**：模块注册表本身已经是**显式值**（不是包级 init 全局），每个宿主一份；
Loader/Registry 也都是 per-host。**仍建议**：把 BlockingExecutor/orphan 队列下沉为
`Root` 选项（内核 P1 改造），或至少提供 `Reset…` 测试钩子。

### I5. 配置模型：内核 `Value=any` vs 编译期 typed config（P0）

**问题**：内核配置是 JSON-like `any` + 代码定义的 `ConfigSchema`；编译期模块系统天然希望
`struct` 类型化配置（字段名/类型/默认值/校验都在编译期）。
**对策（已实现）**：`module.Descriptor[T]` 用泛型把 config 类型带到注册期；预检时
`encoding/json` round-trip 解码成 T 并调用 typed `Validate`，错误在创建 fiber 之前报出；
内核交互仍是 `Value`，不要求内核改。
**仍建议**：为 YAML/JSON 之外的自定义格式提供 `WithDecode`（已有）；把 schema 生成
（JSON Schema/编辑器提示）作为 P2 工具。

### I6. 装配顺序与可选依赖确定性（P0）

**问题**：内核是反应式的，required 依赖与加载顺序无关；但 **optional 依赖**如果 provider
在 consumer 之后加载，consumer 在激活时探测不到（内核不会为 optional 重新激活它）。
**后果**：可选模块行为依赖配置顺序，难以解释。
**对策（已实现）**：`Plan` 用确定性拓扑调度（parent→child、provider→consumer，包含 optional
边），把 provider 排在 consumer 前；optional 边成环时回退配置顺序并警告（required 环已被
预检拒绝）。
**仍建议**：若未来支持运行期动态提供 optional 服务，需要模块显式声明"可重算"或内核增加
可选的依赖重算钩子（不建议把 optional 塞进内核 inject）。

### I7. 可观测性：为什么 Pending / 谁提供了什么（P1）

**问题**：内核能看 fiber 状态/效果树，但没有"这个 fiber 缺哪些注入服务"的 API；模块系统
排障必须能回答："哪个 entry 卡住了、缺哪个模块、哪个模块提供了重复服务"。
**对策（已实现）**：Loader 在预检报告里给出缺失/冲突/环路径；`Loader.MissingServices(id)`
从 manifest + 运行时可达性计算 Pending 原因；运行时校验模块**真的**提供了清单声明的服务
（防清单撒谎）；`Warnings()` 汇总 optional 问题。
**仍建议**：内核增加只读诊断 API（`Fiber.PendingOn() []string`、`Registry.Providers(name)`），
让不带 loader 的宿主也能排障（P1）。

### I8. Context 不可变 / 无 reparent（P1）

**问题**：内核 Context 派生后不可变；kernel 没有"把 fiber 移到另一个 parent"或"修改已有
entry 的 isolate/intercept"的 API。任何需要重新注册 Context 结构的变更，本质上只能重建 fiber。
**后果（若简单处理）**：reload 会丢失模块实例状态；entry 移动/组变更需要重建子树。
**对策（已实现，见 §4.2）**：把 reload 拆成三条路径——
① 纯配置变更走 `Update`（不重建、不触发依赖、状态零丢失，推荐默认）；
② 需要重新注册的变更走 `Reload`（重建 fiber，但通过 Snapshot/Restore + 依赖闭包保持状态，
   不满足条件时在动树之前报错，绝不静默丢状态）；
③ `Recreate` 是显式状态丢失的重建，仅供调试/明确可丢状态的场景。
**仍建议**：若未来需要 isolate/intercept 的原地热 patch：内核加"context factory + reparent"
（复杂）或 Loader 做"双实例切换 + 原子绑定"（先建新、后停旧；当前 isolate scope 下可通过
隔离上下文实现，但依赖闭包/服务可见性规则需要单独设计）。

### I9. 服务之外的扩展点（P1）

**问题**：Cordis 的扩展机制是「服务 + 事件」。真实系统里大量模块贡献是**集合项**：
命令、工具、路由、菜单、迁移脚本……官方靠 host/插件自己实现 registry 服务。
**后果**：每个套件重复实现"有序贡献 + 所有权 + 优先级 + 冲突检测"。
**对策（未实现）**：在 `module` 包（或独立小包）提供泛型 `ExtensionPoint[T]`/`Contribution[T]`：
贡献项带 fiber 所有权、顺序/优先级、冲突检测，底层仍是 `ctx.Provide` + `ctx.Effect`。
**建议**：这是让「模块累加成完整系统」真正可用的关键小工具，建议 P0 之后立刻做。

### I10. 编译期路线的固有约束（P0 文档化）

- 不能运行时接入未编译模块（重新构建是契约的一部分）；
- 模块全部可信（同进程、同二进制）；供应链风险靠 go.sum/vendor/审计；
- 无模块级沙箱/配额策略（内核 L2 只有每 fiber 资源计数，不是安全边界）；
- 模块代码热更新不可能；配置热更新只能重启实例（当前 update 总是重启；没有官方 4.1
  的 effectiveConfig/commit/volatile）；
- 二进制体积/启动链接成本随模块集增长 → 用 build tags/按产品拆 `main` 包；
- 模块版本 = 构建版本；跨版本持久化配置要处理 unknown/removed module（预检已报 unknown，
  建议 loader 增加 migrate/ignore 策略）。

### I11. 契约演进与版本（P1）

**问题**：服务名 + Go 类型是唯一契约；模块升级可能改变服务语义/配置结构/事件载荷。
**对策（当前）**：编译期 Go 类型检查 + 运行时 assignability 校验 + `Descriptor.Version` 诊断；
配置解码失败会在预检报错。
**仍建议**：contract 包按 major 版本分目录（`contracts/v1`）；清单声明 `Provides`/`Requires`
的版本范围；loader 提供"兼容性检查/migration hook"。

### I12. 测试与装配纪律（P0）

**问题**：模块系统最常见的腐化是"注册表靠 init() 隐式增长、测试互相污染、装配错误要到
运行时才炸"。
**对策（已实现）**：注册表是显式值（`NewRegistry` + `Register`），重复注册返回 error 而非
panic；每个测试可建独立 root/registry/loader；预检可单测（纯函数式 plan）；原型测试覆盖
12 个场景（缺 provider/重复/环/类型/坏配置/optional/失败回滚/清单撒谎/reload/响应式卸载/
多宿主/builtin）。
**建议**：真实产品增加一个 `TestAssembly`：对每个产品的 entry 配置跑一次 `Validate`，
CI 中保证任何装配漏配/环/版本冲突在合并前失败。

---

## 4. 原型：`module` 包（已实现并通过测试）

### 4.1 API 摘要

```go
// 编译期注册（Go 1.27 泛型方法：T 从描述符推断）
registry := module.NewRegistry()
registry.Register(
    module.Define("app/db", func(ctx cordis.Context, cfg DBConfig) error {
        _, err := ctx.Provide(DBKey, openDB(cfg))
        return err
    }).
        AddProvides(module.Provides(DBKey)).
        AddRequires(module.Requires(cordis.LoggerKey)).
        WithValidate(func(cfg DBConfig) error { /* ... */ return nil }))

// 运行期装配（配置驱动）
loader, err := module.Load(root, registry, []module.Entry{
    {ID: "database", Module: "app/db", Config: map[string]cordis.Value{"dsn": "..."}},
    {ID: "app", Module: "app/core", Children: []module.Entry{
        {ID: "http", Module: "app/http"},
    }},
})
defer loader.Close()
loader.MissingServices("http")              // Pending 原因
loader.Update("database", newConfig)        // 原地更新配置：不重启 fiber、不触发依赖重算
newInst, _ := loader.Reload("database")     // 状态保持的受控重建（Snapshot/Restore + 依赖闭包）
loader.Recreate("database")                 // 显式接受状态丢失的重建（可选路径）

// 文件驱动：
loader, _ = module.LoadFile(root, registry, "assembly.json") // 读文件→解析→预检→Load
report, _ := loader.ApplyFile("assembly.json")               // 配置变更→diff→应用
watcher, _ := loader.WatchFile("assembly.json", module.WatchOptions{
    Interval: 200 * time.Millisecond,
    OnChange: func(report *module.ReloadReport, err error) { /* ... */ },
})
defer watcher.Stop()
```

关键语义：

- `Load` 先 `Validate`（零 fiber 创建），再按 provider-first 调度创建，最后静默/校验；
- required 失败 → 全树回滚 + `*LoadError`；optional 失败 → 保留 + warning；
- `Close` 逆序 dispose（child fiber 级联）；`Reload` 保留 entry ID、重建子树；
- 内置契约：kernel root 的 `logger` 自动声明为 builtin provider，可 `AddBuiltin` 扩展。

### 4.2 状态保持的配置更新与 reload（针对「不接受状态丢失」的设计）

用户约束：**不接受 “reload = 实例级重启（状态丢失）”**。因此 loader 区分三种操作：

| 操作 | fiber | 模块状态 | 依赖方 | 适用 |
| --- | --- | --- | --- | --- |
| `Update(entryID, cfg)` | **不重建** | 保持 | 不重算（kernel 服务版本不变） | 纯配置变更（推荐默认） |
| `Reload(entryID)` | 重建（entry ID 不变） | **Snapshot/Restore 保持** | 自动纳入依赖闭包并保持状态 | 需要重新注册资源/effect 的变更 |
| `Recreate(entryID)` | 重建 | 丢失（显式选择） | 依赖方因版本变化重算 | 明确可丢状态/调试用 |

**Update（原地更新）**

```
decode+Validate(candidate) → module.Update(ctx, oldCfg, newCfg) → 更新 entry/raw config
```

- 不调用 kernel 的 `Fiber.Update`，因此不触发 service version bump、不重算 dependents；
- 模块必须在 `Update` 中就地应用配置；**对外服务句柄必须稳定**（mutate 已有对象/接口，
  不要替换 service 值），否则依赖方持有的旧句柄会看到旧状态（见风险 R2）；
- 无 `Update` hook → 返回 `ErrUpdateUnsupported`，**绝不静默重启**；
- 验证失败/模块拒绝 → 旧配置、旧状态保持不变（模块需自行保证 Update 的原子性）。

**Reload（状态保持重建）**

1. 计算 **reload closure**：entry 子树 + 所有（传递地）required-inject 了该子树所提供服务的
   loader-managed entry（否则 kernel 的 provider 版本变化会把它们重新激活、丢状态）；
2. 校验：闭包全部实例必须 Active，且每个模块都实现 Snapshot/Restore；否则**在动任何东西之前**
   返回 `ErrReloadNotStateful`（旧树保持不变）；
3. 快照：按 parent-first 加载顺序调用 `Snapshot`（此时旧实例与依赖都还活着）；任一失败 → 中止，
   旧树不变；
4. 逆序 dispose 闭包根（child 级联），按 provider-first 调度重建闭包；
5. 按 parent-first 顺序 `Restore`；随后 WhenIdle + 状态/清单校验；
6. 任一步失败 → 拆除新实例并返回错误（旧实例已释放，文档标注该失败路径状态丢失）。

**边界与已知限制**

- closure 只覆盖 loader 管理的 entry；模块内部用 `ctx.Plugin` 动态创建的子 fiber 不在闭包内，
  它们会随父卸载但不会被快照恢复（模块应避免把关键状态放在动态子 fiber，或自行持久化）。
- Snapshot/Restore 是内存态、同一进程；不做序列化/跨进程迁移。
- 代码级热更新仍不可能（编译期路线的定义），Reload 只是“实例重建 + 状态迁移”；
- `Instance` 句柄会随 Reload 更新（entry ID 稳定，fiber/uid 变化）；宿主应以 entry ID 为键；
- 服务句柄稳定性是 Update 正确性的前提：推荐模块只 `Provide` 一次 long-lived 对象，
  配置变更后 mutate 其内部字段，而不是重新 provide 新对象。

### 4.3 配置文件与热重载（配置如何传给模块、变更如何送达）

**先澄清层级**：内核根包（`cordis`）本身没有「内核配置文件」——内核只有 context/fiber/service/event;
logger 之类的内核服务配置通过 `ctx.Intercept("logger", ...)` / 根模块提供。**装配配置文件属于
可选的 `module` 宿主层**：它描述「加载哪些模块实例、各自的配置、树形结构」，并且可以把每个
entry 的 `config` 以 **typed config** 交给对应模块。`module` 包作为宿主层允许使用标准库
`os`/`encoding/json`（内核根包仍保持无 IO、可被任何宿主复用）。

**文件格式（严格 JSON，unknown field 报错；也接受裸数组）**

```json
{
  "version": 1,
  "entries": [
    {"id": "database", "module": "app/db", "config": {"dsn": "postgres://..."}},
    {"id": "app", "module": "app/core", "children": [
      {"id": "http", "module": "app/http", "config": {"addr": ":8080"}}
    ]}
  ]
}
```

- `ParseConfig([]byte)` / `LoadConfigFile(path)` / `LoadFile(ctx, registry, path)`；
- 配置在**预检阶段**解码为模块的 `T`（`Descriptor[T]` + `decode/Validate`），启动后模块拿到的
  是类型化配置；配置错误在创建 fiber 之前报错；
- 配置变更时 `Loader.Update(id, raw)`/文件热重载会把 **old/new 两个 typed config** 交给模块的
  `Update(ctx, old, new)`；`Reload` 重建时 Factory 收到新 typed config，`Snapshot/Restore` 负责状态迁移；
- YAML/TOML 等格式：`module` 包不引入第三方解析器；宿主自己解析后构造 `[]module.Entry` 调用
  `Load/ApplyEntries` 即可（或后续加可插拔 `Parser` hook）。

**热重载（`ApplyFile` / `WatchFile`）**

`WatchFile` 用标准库轮询（默认 500ms，可配），内容变化 → `ParseConfig` → `ApplyEntries` →
`OnChange(report, err)`；宿主若有 fsnotify 等事件源，可直接调用 `ApplyFile`，不必用轮询。

`ApplyEntries` 的 diff 语义：

| 变更 | 行为 |
| --- | --- |
| 配置变化 + 模块有 `Update` | **原地更新**：fiber 不变、依赖方不重算、状态保持（默认路径） |
| 配置变化 + 模块无 `Update` | 默认 **拒绝**（`ApplyError`，不静默重启）；可用 `WithUpdateFallback(FallbackReload/FallbackRecreate)` 显式选择状态保持重建或状态丢失重建 |
| 新增 entry | provider-first 调度创建；无历史状态 |
| 删除 entry | 停止并释放（状态随删除消失，这是删除语义） |
| disabled 翻转 | 等价于删除/新增 |
| 仅 `optional` 等元数据变化 | 只更新元数据，不重建 |
| entry ID 改 module / 改 parent | **拒绝**（entry ID 不可变；用删除+新 ID 新增，避免状态语义歧义） |
| 配置解析/预检/策略错误 | **不修改运行中的树**（先全量预检+策略检查，再动手） |
| 运行时失败（重建/启动/校验） | 返回 `ApplyError`，报告已完成的部分变更；文档标注此路径可能部分应用 |

`ReloadReport.Changes` 用 `ChangeUpdated/ChangeReloaded/ChangeRecreated/ChangeAdded/ChangeRemoved/
ChangeMetadata` 精确报告每个 entry 发生了什么，宿主可以记日志/审计/告警。

**边界**：

- 热重载的是**配置与实例结构**，不是代码；编译期模块集的代码级热更新仍不可能；
- 热重载后的 fiber/uid 会变化（entry ID 稳定）；宿主以 entry ID 为键，不缓存 `*Instance` 语义；
- 对正在执行 `Update/Reload` 的条目，`ApplyEntries` 用 Loader 的 RWMutex 串行化，不会并发撕裂；
- 轮询 watcher 有间隔延迟（默认 500ms），对毫秒级配置一致性有要求的宿主用事件源主动调用；
- 若配置文件被外部破坏（非法 JSON/未知模块），Watcher 只回调错误并保留运行中的树，不会把系统打挂。

### 4.4 测试覆盖（`go test -race ./module/`，连续多轮通过）

| 场景 | 结果 |
| --- | --- |
| typed config 解码 + 嵌套 entry 树 + locate | ✅ |
| 依赖后到（先 consumer 后 provider）自动激活 | ✅ |
| 缺 provider / 未知模块 / 重复实例 ID / disabled provider | ✅ 预检报错 |
| 重复 provider / 类型不匹配 / required 环 / 坏配置 | ✅ 预检报错 |
| optional 缺失/present；optional 实例失败保留 + warning | ✅ |
| required 失败整体回滚（0 fiber 残留） | ✅ |
| 清单声明了服务但实际没提供 | ✅ 运行时校验报错 |
| reload 保留 entry ID、新 fiber、旧实例 Disposed | ✅ |
| provider 运行时被停 → 依赖方自动 Pending + MissingServices | ✅ |
| 多宿主/多 registry 隔离 | ✅ |
| 未声明的 builtin → Load 失败 | ✅ |
| 原地配置更新：fiber 不变、服务句柄不变、模块状态与依赖激活次数不变 | ✅ |
| 无 Update hook → `ErrUpdateUnsupported`（不静默重启） | ✅ |
| 非法配置更新 → 旧配置/旧状态不变 | ✅ |
| Reload 自动包含依赖闭包，provider+dependent 状态经 Snapshot/Restore 保持 | ✅ |
| 依赖方缺 Snapshot/Restore → `ErrReloadNotStateful` 且旧树不变 | ✅ |
| Snapshot 失败 → 中止且旧树不变 | ✅ |
| 配置文件解析（对象/裸数组/严格字段/版本）→ LoadFile 把 typed config 交给模块 | ✅ |
| ApplyFile 配置变更：有 Update → 原地更新（fiber/服务/状态不变） | ✅ |
| 无 Update + 默认策略 → ApplyError 且旧树不变 | ✅ |
| FallbackRecreate/FallbackReload：显式重建 / 状态保持重建 | ✅ |
| 配置增删 entry（disabled 翻转同）→ 报告 added/removed，运行树同步 | ✅ |
| 非法配置/预检失败 → 运行树不变 | ✅ |
| WatchFile 轮询检测文件变更并触发 Update | ✅ |
| 注册表重复注册/非法描述符 → error | ✅ |

> 原型刻意**不改内核**：所有模块系统语义都在 `module` 包完成。这验证了内核对上层扩展的
> 友好度，也把「内核该不该变大」的决策留到后面。

### 4.5 与官方 loader 的对应关系

| 官方 `cordis-plugin-loader`（生态包，非 core） | 本仓库 `module` 包 |
| --- | --- |
| `EntryTree`（id/name/config/group/disabled） | `Entry` 树（ID/Module/Config/Children/Disabled/Optional） |
| 模块按名 import（ESM 运行时） | 编译期 `Registry.Register`（显式 assembly；无动态加载） |
| `create/update/remove/await/locate` | `Load/ApplyEntries/Reload/Recreate/Close/Instances/Locate` |
| **配置文件 + include**：`plugin-loader` 从配置树加载，`plugin-include` 支持 YAML/JSON 拆分 | **JSON 装配文件已实现**：`ParseConfig/LoadFile/ApplyFile`（严格 JSON、typed config、diff 热重载）；YAML/TOML 由宿主解析后传 `[]module.Entry`（或未来 Parser hook）；include 拆分未实现 |
| group（嵌套子模块组） | `Entry.Children` 树（基础嵌套；官方 group 的加载/卸载/配置语义未完全对齐） |
| HMR（ESM 模块热替换） | **不适用**（编译期路线无代码热更新）；配置/实例热重载已实现（`ApplyFile/WatchFile`） |
| `await/locate` 完成与定位 | `WhenIdle` 静默校验、`Loader.Locate` |
| `inject` 覆盖 / `loader/patch-context` | 暂未实现（entry 级 isolate/intercept 是 P1） |
| volatile config / `internal/commit` | 未实现（依赖内核 4.1 配置语义）；当前用 `Update`（通常不重启）或 `Reload`（状态迁移重建）替代 |
| 配置变更 `update` 的实例保留 | 官方通过 `internal/update` waterfall + 4.1 `internal/commit`/effectiveConfig 保留实例；本仓库用模块 `Descriptor.Update` 实现同等“原地更新”目标 |

> **结论：没有重复造 core。** 官方把「加载器/配置树/HMR」放在 `cordis-plugin-loader` /
> `plugin-include` / `plugin-group` / `plugin-hmr` 等独立生态包里，`cordis` core 只做
> context/fiber/service/events/logger/reflect；Rust 参考也明确把 loader 归为宿主责任。
> `module` 包在 Go/静态链接路线下承担的是**官方 loader 生态的对应层**，并额外提供官方没有的
> 编译期类型化清单/预检、required/optional 失败策略与回滚、状态保持 Update/Reload 契约。
> 真正需要避免的“做重”只有两种：把 loader 功能塞进 core（本仓库没有），或同时维护两套
> 配置更新机制（将来若把官方 4.1 的 `internal/commit` 语义下沉到内核，应让 module 的
> `Update` 委托内核 commit，而不是各做一套）。

---

## 5. 结论：设计思想核心的判断

### 5.1 没有致命问题的地方

- **组合模型与语言无关**：context 树、服务 DI、effect 撤销、child ownership 都不依赖
  动态加载；编译期注册表只是把「模块来源」从 ESM import 换成 Go 静态链接。
- **顺序无关性**：Cordis 的反应式 inject 门控恰好让「配置顺序 / 加载顺序」不再重要，
  这是它相比 DI 容器式 loader 的核心优势，编译期场景同样成立。
- **故障隔离粒度合理**：fiber 级事务 + 树形所有权 + panic 遏制，足以支撑模块级回滚/重启；
  系统级事务可在 loader 层组合出来（原型已证）。
- **内核足够小**：没有 loader/IO/ABI 污染，任何宿主都能复用它；module 包是可选层。

### 5.2 真正的「设计思想风险」（按严重度）

1. **目标—机制错配风险（最高）**：若「接入所有插件系统」被理解为运行时热插拔任意系统，
   编译期注册表永远做不到。必须先与需求方对齐这句话的语义。本文的成立前提是
   **「所有模块都是 Go 包，随宿主编译，运行期只做实例/配置/树组合」**。
2. **DI 语义不足风险**：只靠服务名 + 全 required，会产出静默 Pending 系统。必须采用
   「声明式清单 + 预检 + optional + 失败策略」，否则规模一大就不可维护。原型已证明可在
   内核外解决，但**团队必须把清单当契约维护**（清单撒谎由运行时校验兜底）。
3. **信任边界风险**：同二进制模块全可信；一旦要接不可信第三方，必须回到进程/WASM 隔离，
   此时编译期注册表只能作为控制面，不能作为隔离边界。
4. **状态保持的实现风险（新增）**：`Update` 要求模块**稳定对外服务句柄**（原地 mutate，
   不重新 provide），否则依赖方持有旧句柄；`Reload` 要求模块实现 Snapshot/Restore 且覆盖
   依赖闭包，闭包外的动态子 fiber 不在保护范围。也就是说“不丢状态”是**模块契约责任**，
   loader 只能做到“不支持的模块直接报错，绝不静默丢失”。需要在模块模板/评审清单中强制。
5. **演化风险**：模块配置/服务契约随版本演进；没有 contract 包/版本规则 + 配置迁移，
   跨版本组合会失控。编译期类型检查只能覆盖代码内契约，覆盖不了持久化配置。
6. **配置热重载的运行时风险（新增）**：文件监听延迟/半写文件/外部编辑冲突、运行时失败导致的部分应用、
   watcher 与宿主其它事件源的并发——`ApplyEntries` 已做预检原子性与锁串行化，但"文件内容"本身是
   不可信输入，宿主应对 `ApplyError` 做告警/回滚策略；配置审计与版本化建议由宿主保存。
7. **工程复杂度风险**：每个外部插件系统仍要写 Go adapter（即使纯编译期，也要适配其
   API/生命周期/错误模型，包括 Update/Snapshot 契约）；这不是内核能消除的成本。

### 5.3 建议的下一步（在这个路线内）

| 优先级 | 事项 | 说明 |
| --- | --- | --- |
| P0 | 把 `module` 包从原型硬化 | 已实现 Update/Reload/Recreate、状态保持、JSON 配置文件、ApplyFile diff 热重载、WatchFile 轮询；下一步：错误类型/API 稳定、文档、示例、CI，YAML 可插拔解析、entry 级 isolate/intercept、配置持久化/审计钩子 |
| P0 | **ExtensionPoint[T]/Contribution[T] 工具** | 让模块贡献命令/工具/路由等集合项，统一所有权/顺序/冲突 |
| P0 | 产品装配测试模板 | 每个产品的 entry 配置在 CI 里跑 `Validate`；防止漏配/环/未知模块 |
| P1 | build tags / 多产品 assembly 模板 | 按产品拆 main + 模块集，控制二进制体积与攻击面 |
| P1 | 契约包规范（contracts/v1）+ 版本兼容规则 | 服务 key、事件名、配置结构、版本范围 |
| P1 | 内核只读诊断 API | `Fiber.PendingOn`、`Registry.Providers`，减少 loader 重算 |
| P2 | `internal/commit`/effectiveConfig/volatile | 只有需要"配置热更不重启"时再做 |
| P2 | HMR/文件监听 | 宿主层 fsnotify；Go 只能实例级重启，不能换代码 |
| P2 | 进程外/WASM adapter | 仅当确实需要非 Go/不可信插件时（另一条路线，不是本路线的必需项） |

**最终判断**：作为一个「Go 生态内、多产品共享、静态链接、可配置组合」的内核，**设计思想
核心是成立的**；真正需要在设计阶段定死的不是内核，而是模块系统的三件事：
**清单契约（provides/requires/config）**、**预检与失败策略**、**实例身份与装配纪律**。
这三件在 `module` 原型里已经跑通。剩下的风险是目标语义（编译期 vs 运行时热插拔）与
信任边界（同进程可信 vs 沙箱），必须在产品层面拍板。
