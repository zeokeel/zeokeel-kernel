# 04 · 运行时层：config / logger / exec / clock / error / L2 supervision / async effects / 内核边界与移植笔记

> 分析对象：`reference/zeokeel-cordis-rust`（zeokeel/zeokeel-cordis **v0.6.0**，Rust 2021，MSRV 1.85）。
> 所有 `file:line` 相对该目录；语义以 `spec/CORDIS-CORE.md` 为准。目标读者：Go 重实现团队。
> 覆盖 `src/{config,logger,exec,clock,error,lib,l2,async_effects}.rs`、`docs/{CORDIS-RUST,GUIDE,KERNEL-BOUNDARY,KERNEL-ISOLATION}.md`、`examples/*.rs`、CHANGELOG 0.4.2–0.6.0。
> 关联任务：task-4；本文只做只读分析、不含 Go 实现代码，唯一允许写的文件即本文。

## 0. 结论速览（TL;DR）

1. **config**：schema 可选；`validate` 固定五步（作者错误 → 注入 defaults → required → 类型/未知键 → 聚合 issues）；`merge` 浅合并且 defaults 不覆盖显式值；`resolve_config` 顺序 `base → root intercept → child intercept → head`；intercept 只影响 logger/`Service::resolve_config`，**插件配置不走该链**（走 `internal/config` waterfall + schema）。
2. **logger**：`LogLevel` 数值序 `Error=0 < Warn=1 < Info=2 < Debug=3`（与 TS enum 数值不同），非法数字 **clamp 到 Error**；ring buffer 默认 1000（0 禁用）；seq 在写 buffer 前原子分配（并发下 buffer 内可能乱序）；exporter 锁外调用、每次调用独立 contained；facade **创建时快照 clock**；root 默认不装 exporter（内核 tracing 诊断是独立通道）。
3. **exec**：内核自带依赖零的 thread-park `block_on`（只推进自我唤醒的 future）；`BlockingExecutor` seam **首次安装生效**，供同步卸载边驱动 async cleanup（CORE-034）；默认 `block_on` 无法驱动 reactor-backed future，故 `FiberData::drop` 无 executor 时**跳过** async cleanup 交给 orphan 队列（避免死锁）。
4. **error**：17 个变体、`#[non_exhaustive]`；结构化 `Plugin{plugin,kind,message}` + `CordisError::plugin()` 在激活/`await_settled`/restart/update/replace 中**原样传播**，`Fiber::error_detail()` 读取；**`Ok` 不等于 `Active`**（reload 失败时返回 Ok 但 fiber 落 Failed）。
5. **lib**：crate 级 `#![forbid(unsafe_code)]`；`panic="abort"` 用 `compile_error!` 编译期拒绝（panic 遏制依赖 unwind）；唯一 feature `supervision` 默认关闭；运行时依赖白名单仅 `serde_json`/`tracing`/`futures-util`。
6. **clock**：`Clock{now_unix_ms, now_monotonic_ms}`；`SystemClock` 用进程级共享单调原点；`set_clock` **仅 root fiber 上的 context 可调用**且会同步更新活 fiber；facade 快照，只影响新观测；unix 只用于日志时间戳，monotonic 用于 idle/heartbeat。
7. **l2**：`FiberQuota{max_effects, max_listeners, max_child_fibers}` 按**活资源**计数（Q11：max_effects 只计用户 effect）；`heartbeat`/`last_activity_ms`/`idle_for`；`Registry::stalled_fibers` 只观测不强杀，且**看不到**注册窗口（用 `when_idle()` 等）。
8. **async_effects**：async cleanup 工厂 + 确定性逆序 + join + cancel-safe restore + 自 join 短路；Drop 有安全网；Drop 跳过的 async cleanup 进入**有界 orphan 队列（cap 256）**，三个计数器可观测，`drain_orphan_async_cleanups()` 幂等且须在非 reactor 阻塞线程调用。
9. **移植主线**：`serde_json::Value→any`、`tracing→log/slog`（`LogExporter→SlogExporter`）、`exec→goroutine+context`、`feature→独立包/build tag`、`ring buffer→slice/container`；panic 遏制用 `recover`（fatal error 不可恢复）；内核边界/CI 护栏用 `go list` import 白名单 + `go/parser` AST 扫描 + 分词业务词 + 公共 API cap。

---

## 1. `config.rs`：配置 schema、默认值、分层合并

### 1.1 ValidationError（`src/config.rs:21-56`）

- `pub struct ValidationError { pub issues: Vec<String> }`（`Debug/Clone/PartialEq/Eq/Default`）。
- `message()`（`config.rs:33-42`）：首行 `invalid config:`，每条 issue 一行 `\n  - <issue>`；`Display` 即 `message()`（`config.rs:50-54`），实现 `std::error::Error`（`config.rs:56`）。
- 对齐 Cordis `ValidationError`；`issues` 公开，测试按包含关系断言（`tests/config.rs:76-88`）。

### 1.2 JsonType（`config.rs:58-91`）

仅 5 变体 `String/Number/Bool/Object/Array`，**无 `Null`**（`null` 语义由“缺省/可选字段”表达）。`name()`（`config.rs:70-78`）给出 `"string"/"number"/"boolean"/"object"/"array"`；`matches(&Value)`（`config.rs:81-90`）按 serde_json kind 做 `matches!`，`Null` 不匹配任何 kind。

### 1.3 `ConfigSchema` trait 与 panic 遏制（`config.rs:93-150`）

```rust
pub trait ConfigSchema: Send + Sync {
    fn validate(&self, value: Value) -> Result<Value, Vec<String>>; // 返回归一化后的值
    fn merge(&self, base: Value, overrides: &[Value]) -> Value;     // 层合并
}
```

- 等价 Cordis `Plugin.Config` + 服务 `Config.merge` hook；schema 可选，无 schema 保留历史“浅合并 + 原值”行为（`config.rs:10-12`）。
- `contained_validate`（`config.rs:120-131`）：panic → `Err(vec!["config schema panicked during validation"])`；`contained_merge`（`config.rs:137-150`）：panic → 退化到最低优先级 `base`（clone fallback），`resolve_config` 永不 unwind。
- 唯一调用点 `fiber::validate_config`（`src/fiber.rs:1216-1227`）；`internal::contained` 实现见 `src/internal.rs:83-102`（`catch_unwind` + `tracing::error!` target `zeokeel_cordis::containment` + fallback）。

### 1.4 `JsonConfigSchema` 构造器与字段（`config.rs:165-251`）

私有字段：`required: Vec<String>`、`defaults: Map<String,Value>`、`additional_properties: bool`（默认 `true`）、`properties: BTreeMap<String,JsonType>`（`config.rs:166-182`）。

| 方法 | 行号 | 语义 |
|---|---|---|
| `new()` | `config.rs:186-188` | 允许未知键、无 required/default |
| `required(name)` | `config.rs:194-197` | 追加 required，在 defaults 注入**之后**检查 |
| `default_value(name, value)` | `config.rs:214-229` | 存 default；非 `null` 值且属性未声明 kind 时 `entry().or_insert(kind)` 顺带声明；`null` 不声明类型 |
| `deny_unknown()` | `config.rs:232-235` | `additional_properties = false` |
| `property(name, kind)` | `config.rs:238-241` | 显式声明 kind（`insert`，覆盖推断，后写生效） |
| `defaults()` / `required_keys()` | `config.rs:244-251` | 只读访问 |

**default 声明类型规则（易错）**：① 非 null default 自带 kind；② `null` default 永不声明 kind（`JsonType` 无 nullable）；③ `null` default 是否注入取决于是否已有声明 kind——**未声明类型** → 缺键注入 `null`（键算已声明、不做类型检查）；**已声明类型**（`property` 或更早的非 null default）→ 缺键保持 absent（可选字段），提供值时按声明 kind 校验，`required` 可强制显式提供；④ 显式 `property` 与 default 冲突是 schema 作者错误，`validate` 报告且不注入，`merge` 不做作者校验、仍物化 defaults（`docs/CORDIS-RUST.md:397-403`）。

### 1.5 `validate` 精确算法（`config.rs:253-350`）

输入分派（`config.rs:340-350`）：`Null` → 空对象；`Object` → 正常；其他 → `Err(["expected an object, got <kind>"])`。`validate_object` 顺序：

1. **作者错误**（`config.rs:264-277`）：遍历非 null defaults，若已声明 kind 不匹配 → issue ``default value for `<name>` must be {expected}, got {actual}``。
2. **注入 defaults**（`config.rs:278-299`）：缺键时—声明 kind 且 default 为 `null` → 跳过（可选字段）；default 与声明 kind 冲突 → 跳过（避免二次 issue）；否则插入（含无类型 null default）。
3. **required**（`config.rs:300-304`）：注入后仍缺 → ``missing required field `<name>` ``。
4. **类型 + 未知键**（`config.rs:305-319`）：声明 kind 不匹配 → ``field `<name>` must be {expected}, got {actual}``；`!additional_properties` 且键不在 `properties` 也**不在 `defaults`** → ``unknown field `<name>` ``（仅 `required` 不声明字段，会被算 unknown）。
5. 无 issue → `Ok(Object(normalized))`；否则 `Err(issues)`（一次返回全部，不短路）。`json_kind`（`config.rs:329-338`）渲染 `null/boolean/number/string/array/object`。

### 1.6 `merge`（`config.rs:352-377`）

层顺序 `base`（最低）+ `overrides` 依序（最高）；`Null`/非对象层跳过；对象层**浅合并**（同键后者覆盖）。合并后 `entry(name).or_insert(default)`：defaults **永不覆盖显式提供的值**；`null` default 且键已声明 kind → **不物化**（FIX-17，与 validate 可选字段一致）。merge 不做类型校验/作者错误/未知键拒绝。

### 1.7 `resolve_config` 与 intercept（`src/context.rs:1841-1889`）

1. 沿 `self → parent` 收集该 `name` 的 intercept，`reverse()` 得 root→leaf（`context.rs:1848-1856`）；有 `head` 则最后 push（最高优先级，`context.rs:1857-1859`）。
2. `find_schema(name)`（`context.rs:1881-1889`）：按 `service_scope(name)`（隔离 label→scope id）查 `service_store[(scope,name)].schema`，子 scope 遮蔽祖先。
3. 有 schema → `contained_merge(schema, base.unwrap_or(Null), &intercepts)`（`context.rs:1860-1868`）；无 schema → 从 `{}` 开始浅合并 `base`，再按 root→leaf→head 合并（`context.rs:1869-1876`，`merge_into` 见 `context.rs:1958-1964`）。
4. **顺序结论**：`base → root intercept → child intercept → head`（spec `CORDIS-CORE.md:620-622`）。

**intercept 边界**：`ctx.intercept(name, config)`（`context.rs:308-333`）只影响走 `resolve_config` 的配置（内置 `logger` 与实现 `Service::resolve_config` 的服务）；**插件配置不走这条链**（来自 `ctx.plugin` config + `internal/config` waterfall + schema），`root.intercept("http", …)` 不覆盖 `http` 插件配置（GUIDE:519-522）。例（`tests/config.rs:150-171`）：root 与 child 分别 intercept `svc`，`leaf.resolve_config("svc", Some({"z":0}), Some({"c":3}))` = `{"z":0,"a":1,"b":2,"c":3}`。

### 1.8 与 fiber 激活/更新

- raw config 每次激活**重新解析一次**（CORE-040）：`Fiber::raw_config()`/`config()`（`src/fiber.rs:502-522`），restart/update 不叠加派生值。
- `validate_config`（`fiber.rs:1216-1227`）：无 schema → 原值放行；有 schema → `contained_validate`，失败映射 `ValidationError::new(issues)`。
- `Fiber::update` 顺序（`fiber.rs:913-949`）：`internal/config` → schema 校验 → `internal/update`；失败 → `Validation` 且 fiber 不变；hook 替换配置先校验；hook 不调 `next` = veto → `Ok(())`、不重启、raw config 不变；`no_save` 只是持久化提示，成功 update 仍提交 raw config。
- 初始配置非法：fiber 返回但 `Failed`，`apply` 不执行（`tests/config.rs:101-125`）。

### 1.9 Go 移植含义（config）

- `serde_json::Value` → `any`（或边界处 `json.RawMessage`）；统一在 `any` 上校验。
- `ConfigSchema` → `interface { Validate(any) (any, []string); Merge(base any, overrides []any) any }`；`ValidationError.Error()` 文本逐字对齐。
- `JsonConfigSchema` → builder（Required/DefaultValue/Property/DenyUnknown）；`JsonType` 自定 enum（不要用 `reflect.Kind`）。
- 逐条保留：非 null default 声明类型、null default 不声明、typed null default 可选不注入、作者错误文本、required 在 defaults 后、unknown 基于 `properties ∪ defaults`、merge 浅合并且 defaults 不覆盖显式值、`null` 输入视作 `{}`。
- `resolve_config` 沿 parent 收集+反转+head 最后；有 schema 走 `Merge`，否则浅合并；`find_schema` 按 isolate scope 查。embedder 的 `Validate/Merge` 用 `defer recover()` 遏制。

---

## 2. `logger.rs`：LogLevel / LogRecord / ring buffer / LoggerService / Logger / exporter / tracing 桥

### 2.1 LogLevel（`src/logger.rs:30-81`）

| 变体 | 数值 | 发出条件 |
|---|---|---|
| `Error` | 0 | 总是 |
| `Warn` | 1 | threshold ≥ 1 |
| `Info` | 2 | threshold ≥ 2（root 默认 threshold，`context.rs:133`） |
| `Debug` | 3 | threshold = 3 |

- `#[repr(u8)]`；`PartialOrd/Ord` 派生，**数值序=严重度序** `Error=0 < Warn=1 < Info=2 < Debug=3`；记录在 `level <= threshold` 时发出。
- **与官方 TS 差异**：TS 是 `INFO=1, WARN=2`；Rust 刻意用严重度数值序（`logger.rs:16-19`、`CORDIS-RUST.md:25-29`）。Go 必须选一种并写进契约/测试。
- `parse`（`logger.rs:42-50`）大小写不敏感，`"warn"`/`"warning"` 都是 Warn；`as_str`（`logger.rs:53-60`）；`from_u8`（`logger.rs:62-75`）0..=3 映射、**>3 一律 clamp 到 Error（fail-closed）**，绝不放大到 Debug（XT-09/S2，`tests/logger.rs:222-260` 断言 4/255→Error）；`as_u8`（`logger.rs:77-80`）。

### 2.2 LogRecord（`logger.rs:83-98`）

| 字段 | 类型 | 语义 |
|---|---|---|
| `seq` | `u64` | 进程内单调序号，**从 1 开始**（`fetch_add(1)+1`） |
| `ts_unix_ms` | `u64` | Unix 毫秒，来自记录时的 `Clock::now_unix_ms()` |
| `name` | `String` | logger 名（fiber/插件名或 intercept 覆盖） |
| `level` | `LogLevel` | 严重度 |
| `message` | `String` | 已渲染消息 |
| `fields` | `Map<String,Value>` | 结构化字段；非对象输入包装为 `{"value": ...}`（`logger.rs:428-438`） |

`Debug/Clone/PartialEq`；`records()` 返回克隆快照。

### 2.3 `LogExporter` 与 TracingExporter（`logger.rs:100-111, 398-423`）

- trait：`fn export(&self, record: &LogRecord)`（**在 service 锁外调用**，exporter 可自己 log/注册 exporter 而不死锁）；`fn level(&self) -> Option<LogLevel> { None }`（每 exporter 阈值，None=用 service 阈值）。
- 每次 exporter 调用独立 contained：`level()` panic → 用本次 threshold；`export()` panic → 记录后继续剩余 exporter 与 buffer 写入（`logger.rs:262-280`；CORDIS-RUST panic 表 `docs/CORDIS-RUST.md:523`）。
- `TracingExporter`（`logger.rs:402-423`）：转发到 `tracing`，`target: "cordis"`，`name`/`fields` 作为结构化字段；内核自身 tracing 诊断是**独立通道**。root 默认**不装 exporter**（GUIDE:679-686、spec `CORDIS-CORE.md:600-614`）。
- `LOGGER: ServiceKey<LoggerService> = ServiceKey::new("logger")`（`logger.rs:426`），root 在 `Context::root()` 注册（`context.rs:187-191`），可 `ctx.get(LOGGER)` 注入（`tests/logger.rs:159-173`）。

### 2.4 LoggerService 与 ring buffer（`logger.rs:113-143`）

| 字段 | 类型 | 说明 |
|---|---|---|
| `level` | `AtomicU8` | service 默认阈值（Relaxed） |
| `exporters` | `Mutex<Vec<(u64, Arc<dyn LogExporter>)>>` | id 单调（exporter_seq），disposer 按 id 移除 |
| `buffer` | `Mutex<VecDeque<LogRecord>>` | 有界 ring buffer |
| `buffer_size` | `usize` | `DEFAULT_BUFFER_SIZE = 1000`（`logger.rs:113-114`，对齐 Cordis bufferSize）；`0` 禁用 |
| `seq` / `exporter_seq` | `AtomicU64` | 记录序号（从 1）/ exporter id 分配器 |

`with_buffer_size` 用 `VecDeque::with_capacity(size.min(64))`（`logger.rs:138`，容量提示）；`new(level) = with_buffer_size(level, 1000)`（`logger.rs:128-130`）。

### 2.5 LoggerService 方法语义

| 方法 | 行号 | 语义 |
|---|---|---|
| `level()` | `146-148` | `from_u8(load)`，坏值也 clamp Error |
| `set_level` | `151-153` | 原子替换默认阈值 |
| `is_enabled(level)` | `156-158` | `level.as_u8() <= self.level().as_u8()` |
| `logger(name)` | `164-166` | SystemClock、无 override |
| `logger_with(name, Option<LogLevel>)` | `185-191` | 可 override（ctx.intercept 路径） |
| `logger_with_clock(name, level, clock)` | `170-182` | `pub(crate)`，Context 传树 clock |
| `exporter(exporter)` | `194-205` | 分配 id、入表、返回 `Disposer`（按 id retain 移除）；幂等 |
| `exporter_count()` | `208-213` | 诊断 |
| `log(level,name,msg,fields)` | `220-229` | **永远 SystemClock** + service 阈值 |
| `log_with_clock(...)` | `232-289` | 完整记录路径（见下） |
| `records()` | `292-297` | 克隆快照，oldest first |
| `clear_buffer()` | `300-304` | 清 buffer，不动 seq |

`log_with_clock` 顺序（`logger.rs:232-289`）：① `level > threshold` → return；② 构造记录 `seq=fetch_add+1`、`ts=clock.now_unix_ms()`、`fields_as_map`；
③ 锁 exporters，**clone 出 `Arc` 列表后立即释放锁**（`logger.rs:252-261`，锁中毒用空列表）；
④ 每 exporter `contained(level())` 失败→用本次 threshold，`level <= exporter阈值` 时 `contained(export(&record))`——单 exporter 失败不影响其余与 buffer；⑤ `buffer_size>0` 时锁 buffer，`len==size` 先 `pop_front` 再 `push_back`；
⑥ **并发语义**：seq 在 push 前原子分配，故多线程下 buffer 内 seq 可能乱序（`logger.rs:528-537`），保证“唯一 seq、不丢失/不重复、buffer 有界”。

### 2.6 Logger facade（`logger.rs:307-396`）

字段 `service: Arc<LoggerService>`、`name: String`、`level: Option<LogLevel>`、`clock: Arc<dyn Clock>`；`Clone`。

| 方法 | 行号 | 语义 |
|---|---|---|
| `name()/level()/service()` | `333-345` | 只读 |
| `child(name)` | `347-355` | `"{parent}.{name}"`，继承 level override 与 clock |
| `log(level,msg,fields)` | `357-362` | `threshold = self.level.unwrap_or(service.level())`，用 facade clock 调 `log_with_clock` |
| `clock()` | `364-370` | 创建时快照（诊断/测试） |
| `error/warn/info/debug` | `372-386` | 便捷方法，无字段传 `Value::Null` |

**时钟快照语义**：facade 创建时捕获 `Clock`（`logger.rs:310-318`、`context.rs:370-378`）；`Context::set_clock` 只影响之后创建的 facade，已持有者继续用旧 clock。裸 `LoggerService::{logger,logger_with,log}` 永远 SystemClock。插件通常在 activation 期间取 logger（宿主装好 clock 后），常规无感；确定性测试需在 `set_clock` 后重新 `ctx.logger()`。

### 2.7 Context 的 logger 入口（`src/context.rs:1892-1956`）

| API | 行号 | 语义 |
|---|---|---|
| `ctx.logger()` | `1903-1918` | `resolve_config("logger", None, None)`；name 取配置 `"name"` 或 `plugin_name()`；level 取 `config_level`；clock 用 `self.clock()`（树 clock） |
| `ctx.logger_named(name)` | `1920-1926` | 同上 level，但名字显式给定 |
| `ctx.logger_service()` | `1928-1931` | 共享 `Arc<LoggerService>` |
| `ctx.log_exporter(exporter)` | `1933-1939` | 注册 exporter 并挂成当前 fiber 的 effect（卸载自动移除） |
| `config_level(config)` | `1942-1956` | 只接受 `as_u64()` 数字或字符串；数字 **>3 clamp Error**；字符串走 `parse`；缺失/其他 → None（回落 service 阈值） |

`ctx.intercept("logger", {"name": ..., "level": <0..3|string>})` 对应官方 `LoggerService.Intercept`（`tests/logger.rs:142-156, 222-248`）。

### 2.8 Go 移植含义（logger）

- `LogLevel` → 自定义 `type Level uint8`（Error=0…Debug=3）；**slog 数值方向相反**（Debug=-4,Info=0,Warn=4,Error=8），映射放在 exporter 内；`ParseLevel` 支持 `"warning"` 别名。
- `LogRecord` → 普通 struct + `fields map[string]any`；`LoggerService` → `sync.Mutex`（exporters/buffer）+ `atomic.Uint64`（seq/exporterSeq）+ `atomic.Uint8`（level）；ring buffer 用预分配 slice + head/size 或 `container/list`。
- `Logger` facade → 持有 `*LoggerService`、name、`*Level`（nil=service 阈值）、`Clock` 快照；不要直接用 `*slog.Logger` 当 facade（会丢 level override 与 clock 快照语义）。
- `LogExporter` → `interface { Export(*LogRecord); Level() (Level,bool) }`；锁外调用、每 exporter recover、service 侧过滤；内置 `SlogExporter` 替代 `TracingExporter`。
- `seq` 用 `atomic.AddUint64` 在写 buffer 前分配；service 内建 ring buffer 保留（诊断/测试），内核自身诊断走独立 `slog` logger。

---

## 3. `exec.rs`：极简 executor / BlockingExecutor / sync dispose 桥

### 3.1 公开面（`exec.rs:1-234`）

- `BoxFuture<'a,T> = Pin<Box<dyn Future<Output=T> + Send + 'a>>`（`exec.rs:26-27`）；`boxed(future)` 装箱（`exec.rs:42-47`）；`PanicPayload`（`exec.rs:29-30`，`pub(crate)`）。
- `yield_now()`（`exec.rs:100-123`）：首次 poll `Pending` + `wake_by_ref`，第二次 `Ready(())`；用于观察并发交错。
- `join_all(futures)`（`exec.rs:125-161`，`pub(crate)`）：每轮按输入顺序各 poll 一次未完成 future，保序返回；一个 pending 不阻塞其他（支撑 `Events::parallel_async`）。
- `catch_panic(future)`（`exec.rs:163-187`，`pub(crate)`）：`catch_unwind` 包住每次 poll，返回 `Result<T,PanicPayload>`；`Pin<Box<dyn Future>>` 恒 `Unpin`，无需 unsafe（crate `forbid(unsafe_code)`）。

### 3.2 `block_on`：thread-park 极简 executor（`exec.rs:49-98`）

```text
Parker { thread: Thread, notified: AtomicBool }
loop { poll(future) => Ready(v) | Pending => park() }
park(): while !notified.swap(false, AcqRel) { thread::park(); }   // 消费与 Pending 竞争的唤醒，不丢通知
wake_by_ref(): notified.store(true, Release); thread.unpark()
```

- 目的：内核**不依赖 async runtime**；该 executor 只服务内核自己的同步边界（`Fiber::dispose()`、无 `dispose_async` 的 `AsyncDisposer::drop`）。
- **局限**：只能在 future **自我唤醒**时推进；Tokio timer/socket 等 reactor-backed future 需要外部 runtime 已在驱动（`exec.rs:49-56`）。
- `std::pin::pin!` 把 future 固定栈上，无 unsafe（`exec.rs:64-65`）。

### 3.3 BlockingExecutor seam 与 CORE-034（`exec.rs:189-234`）

```rust
pub trait BlockingExecutor: Send + Sync { fn block_on(&self, future: BoxFuture<'static, ()>); }
static BLOCKING_EXECUTOR: OnceLock<Arc<dyn BlockingExecutor>>;            // exec.rs:207
pub fn set_blocking_executor(...) -> bool;                                // exec.rs:209-215
pub(crate) fn has_blocking_executor() -> bool;                            // exec.rs:217-225
pub(crate) fn block_on_unit(future: BoxFuture<'static, ()>);              // exec.rs:227-234
```

- **第一个安装者胜**（`OnceLock::set` 失败返回 `false`），库 crate 不能悄悄替换宿主执行器；未安装时 `block_on_unit` 回落默认 `block_on`。
- 使用点：`Fiber::dispose/restart/update`、`AsyncDisposer::dispose_blocking` 等同步卸载边，用于驱动 defer 的 async cleanup；`effect_future`/`effect_stream` 注册直接用裸 `block_on`、**忽略**已安装 executor（`docs/CORDIS-RUST.md:61-68`）。
- **CORE-034**（spec `CORDIS-CORE.md:680-684`、`tests/executor.rs:38-52`）：同步卸载边必须走 embedder 的 `BlockingExecutor`；async host 应安装“驱动自身 runtime 或在 async 线程上大声失败”的执行器。这是多线程 server host 里让同步 dispose 安全的受支持方式。
- **默认 thread-park block_on 在有 reactor 的宿主为何危险**：单线程 runtime（如 tokio `current_thread`）上 `dispose()` park 唯一 worker，本应由它驱动的 timer/io 永不就绪 → **死锁**；多线程 runtime 也会阻塞一个 worker，reactor-backed cleanup 同样可能永不推进。故 `FiberData::drop`（`src/fiber.rs:234-330`）用 `has_blocking_executor()` 判断：无 executor 或正在 panicking 时**跳过** async cleanup（进 orphan 队列，见 §8），只在有 executor 时于 Drop 内 `dispose_blocking`（XT-01，`CORDIS-RUST.md:535-559`）。

### 3.4 Go 移植含义（exec）

- Go 无 future/poll；async 边界用 `func(ctx context.Context) error`；`block_on` ≈ 直接调用（或 channel 汇聚）。Go 不存在 reactor 饥饿的同一模型，但**在事件循环 goroutine/持锁时阻塞**是同构风险。
- `BlockingExecutor` → `type BlockingExecutor interface { BlockOn(func()) }`（或 `func(fn func())`）；`SetBlockingExecutor` 首次生效（`sync.Once`/CAS）。默认直接执行；宿主可换成“调度到自身事件循环”或“非事件循环线程才允许，否则报错”。
- `catch_unwind`/`catch_panic` → 所有 embedder 回调 `defer func(){ recover() }()`；**注意** Go `recover` 只覆盖普通 panic，`runtime.throw`/fatal（栈溢出、并发 map 读写、OOM）不可恢复 → 写进不可遏制边界。
- `yield_now` → 测试用 `runtime.Gosched()`；`join_all` → `sync.WaitGroup` + 保序结果 slice。
- 同步 dispose API 保留但文档标注“勿在回调/事件循环 goroutine 调用”；server host 首选 `DisposeAsync`。

---

## 4. `error.rs`：CordisError 全变体、Display、结构化 Plugin 错误

### 4.1 变体清单（`error.rs:7-88`，`#[non_exhaustive]`，`Debug/Clone/PartialEq/Eq`）

| 变体 | 字段 | 产生场景 |
|---|---|---|
| `InactiveEffect` | — | 非 active/disposed context 注册 effect（Cordis `INACTIVE_EFFECT`） |
| `DuplicateService` | `String` | 同 scope 重复 provide |
| `UnknownService` | `String` | `require` 的服务不存在 |
| `InvalidPlugin` | `String` | 插件非法：加载深度超限、set_clock 非 root、替换目标非法 |
| `PluginFailed` | `String` | 激活失败（apply 返回 Err）；replace/reload panic 遏制后的消息含 `reload panicked` |
| `Disposed` | — | 使用已销毁句柄 |
| `Validation` | `ValidationError` | schema 校验失败（update / 初始激活置 Failed） |
| `ServiceWriteForbidden` | `String` | `ctx.set` 修改别人提供的服务 |
| `AccessorReadOnly` | `String` | 对无 setter 的 computed accessor 调 set |
| `AsyncPluginNeedsAsync` | `String` | 对 `AsyncPlugin` 用同步生命周期 API |
| `Cancelled` | — | `Context::checkpoint` 观察到协作取消 |
| `QuotaExceeded` | `String` | L2 配额超限（`"effects"/"listeners"/"child_fibers"`） |
| `LifecycleBusy` | — | 同 fiber 生命周期操作互斥（跨线程并发、refresh 卸载占用、async 操作挂起期间 sync 调用） |
| `ServiceTypeMismatch` | `String` | `set/set_any` 替换服务类型不同（消息可含两个类型名） |
| `Rollback` | `String` | `Registry::replace` 回滚未完全成功（含原始错误+回滚错误） |
| `Plugin { plugin, kind, message }` | 三个 `String` | 插件返回的**结构化**错误，内核原样传播 |
| `AccessorWriteRejected` | `String` | 可写 accessor setter 返回 `false`（区别于所有权/只读） |

> 0.6.0 起 `LifecycleBusy` 是**操作级 token**：refresh 触发的卸载也占槽；同线程 sync 调用在 async lifecycle 操作挂起期间同样 Busy（不属于该 async token，不误判 reentrant）（`CORDIS-RUST.md:467-499`）。

### 4.2 Display 精确文本（`error.rs:90-133`，逐字对齐）

| 变体 | Display |
|---|---|
| `InactiveEffect` | `cannot create effect on inactive context` |
| `DuplicateService(n)` | `service already provided: {n}` |
| `UnknownService(n)` | `service not available: {n}` |
| `InvalidPlugin(m)` | `invalid plugin: {m}` |
| `PluginFailed(m)` | `plugin failed: {m}` |
| `Disposed` | `fiber already disposed` |
| `Validation(e)` | `{e}`（即 `invalid config:\n  - issue…`） |
| `ServiceWriteForbidden(n)` | `service provided by another fiber: {n} (ctx.set)` |
| `AccessorReadOnly(n)` | `computed accessor is read-only: {n} (ctx.set)` |
| `Cancelled` | `operation cancelled` |
| `QuotaExceeded(k)` | `resource quota exceeded: {k}` |
| `LifecycleBusy` | `another lifecycle operation is already in progress` |
| `ServiceTypeMismatch(n)` | `service type mismatch: {n}` |
| `Rollback(m)` | `replace rollback failed: {m}` |
| `AccessorWriteRejected(n)` | `computed accessor rejected the write: {n}` |
| `Plugin{p,k,m}` | `plugin error: {p} ({k}): {m}` |
| `AsyncPluginNeedsAsync(n)` | ``plugin `{n}` implements AsyncPlugin: use plugin_async/restart_async/update_async`` |

### 4.3 结构化 Plugin 错误与衔接

- `CordisError::plugin(plugin, kind, message)`（`error.rs:135-153`）：`plugin` 显示名、`kind` 插件自定义分类（`"config"/"network"/"auth"`，内核不解释）、`message`。
- 传播：从 `apply/apply_async` 返回后，在**激活、`Fiber::await_settled`、restart/update、`Registry::replace`** 中原样保留，**不压平成 `PluginFailed`**（`CORDIS-RUST.md:52-65, 444-448`）。
- `Fiber::error_detail() -> Option<CordisError>`（`src/fiber.rs:759-768`）保留结构化错误；`Fiber::error() -> Option<String>` 是 Display 文本。
- **`Ok` 不等于 `Active`**：restart/update/replace_plugin 的 post-reload 失败返回 `Ok(())` 而 fiber 落 `Failed`，错误在 `error_detail()`；validation/veto/lifecycle 错误才 `Err`（`CORDIS-RUST.md:460-465`、GUIDE:545-549）。
- `impl std::error::Error`（`error.rs:155`）；`pub type Result<T> = Result<T, CordisError>`（`error.rs:157`）。

### 4.4 Go 移植含义（error）

- `#[non_exhaustive]` → Go 用“接口 + 未导出 marker 方法”或 `type Error struct { Code ErrorCode; … }`，避免 switch 穷尽承诺：`type CordisError interface { error; IsCordisError() }` + 每变体 struct，配 `errors.As/Is`。
- 等值变体 → 包级 sentinel `var ErrDisposed = errors.New(...)` 等；带字段变体 → struct（`ValidationError{Issues []string}`、`PluginError{Plugin,Kind,Message string}`、`QuotaError{Kind string}`、`RollbackError`）。
- 字符串（`"effects"/"listeners"/"child_fibers"`）与 `Error()` 文本保留，便于日志/指标稳定；内核层**不要 `%w` 包装 `PluginError`**（会破坏原样传播的可匹配性），需上下文时保留 `Unwrap()` 并用 `errors.As` 测试。

---

## 5. `lib.rs`：crate 级约束、feature、公共 re-export

### 5.1 crate 级约束（`lib.rs:46-55`）

- `#![forbid(unsafe_code)]`（`lib.rs:46`）：内核自身零 `unsafe`。
- **panic=unwind 编译期 guard**（`lib.rs:48-55`）：`#[cfg(panic="abort")] compile_error!("zeokeel-cordis requires panic = \"unwind\" …")`——panic 遏制靠 `catch_unwind`，abort 下核心承诺失效，宁可编译失败。CI 反向验证 `RUSTFLAGS="-C panic=abort" cargo check --lib` 必须失败（`.github/workflows/ci.yml:121-127`）。
- `Cargo.toml` 最严格 profile：dev/release `panic="unwind"`、release `overflow-checks=true`、`codegen-units=1`、`lto="thin"`；`[lints.rust] unsafe_code="forbid"`；clippy `await_holding_lock=deny`、`mutex_integer=deny`（`Cargo.toml:36-58`）。

### 5.2 模块与 feature（`lib.rs:57-73`）

`pub mod async_effects, clock, config, context, effect, error, events, exec, fiber, internal, logger, reflect, registry, service, waterfall`；`#[cfg(feature="supervision")] pub mod l2;`。`[features] default=[]`、唯一 feature `supervision=[]`（`Cargo.toml:12-16`）——默认构建不含 L2 API。运行时依赖白名单 `serde_json`、`tracing`（features=["log"]）、`futures-util`（`Cargo.toml:60-64`），dev-deps 可用 tokio 等（`Cargo.toml:66-68`）。

### 5.3 公共 re-export 全清单（`lib.rs:75-106`，Go API 对照）

| 模块 | crate 根导出 |
|---|---|
| async_effects | `cleanup_async, drain_orphan_async_cleanups, orphan_async_cleanup_count, orphan_async_cleanup_failure_count, orphan_async_cleanups_pending, AsyncCleanup, AsyncDisposer, AsyncEffect` |
| clock / config | `Clock, SystemClock` / `ConfigSchema, JsonConfigSchema, JsonType, ValidationError` |
| context / effect | `Context` / `cleanup, Cleanup, Disposer, Effect` |
| error / events | `CordisError, Result` / `EventOptions, EventResult, Events` |
| exec / fiber | `block_on, boxed, set_blocking_executor, yield_now, BlockingExecutor, BoxFuture` / `EffectMeta, Fiber, FiberState, Settled` |
| internal | `ConfigHook, ConfigNext, DispatchHook, DispatchInfo, DispatchMode, GetHook, GetNext, InternalBus, LifecycleEvent, LifecycleListener, ListenerHook, ListenerOverride, ListenerRequest, PluginHook, ServiceHook, SetHook, SetNext, StatusHook, UpdateHook, UpdateNext` |
| l2（feature） | `FiberQuota, IsolationCapabilities, StalledFiber` |
| logger / reflect | `LogExporter, LogLevel, LogRecord, Logger, LoggerService, TracingExporter` / `Accessor, AccessorGet, AccessorSet, ContextMeta` |
| registry / service | `AsyncPlugin, IdleWait, Plugin, PluginKey, PluginRuntime, Registry` / `provide_service, AvailabilityCheck, Service, ServiceGate, ServiceKey, ServiceValue` |
| waterfall | `async_waterfall_listener, async_waterfall_terminal, null_async_waterfall_terminal, null_waterfall_terminal, waterfall_listener, waterfall_terminal, AsyncWaterfallListenerFn, AsyncWaterfallNext, AsyncWaterfallTerminal, WaterfallListenerFn, WaterfallNext, WaterfallTerminal` |

### 5.4 Go 移植含义（lib/feature）

- 根包 `cordis` 作为公共门面（对应 re-export），私有实现放 `internal/`；或子包 + 根包 re-export（type alias/var 转发）。
- `supervision` → 推荐**独立包**（避免 build tag 组合爆炸）：`supervision.NewQuota(...)` 等；若用 build tag `//go:build supervision`，CI 必须同时跑 `go test ./...` 与 `go test -tags supervision ./...`。
- `#![forbid(unsafe_code)]` → 架构测试禁 `unsafe`/`runtime/cgo`/`//go:linkname`（Go 无 crate 级属性）。
- `panic=abort` guard **无 Go 等价物**：Go 的 recover 只覆盖普通 panic，fatal error 不可恢复——写进“不可遏制边界”，不能假装有编译期守卫。
- 公共 API cap（Rust 420）→ Go 用 `go/types` 统计导出标识符或 `go doc -all` 行数，作为 CI 绊线。

---

## 6. `clock.rs`：可注入时钟

### 6.1 Clock trait（`clock.rs:24-43`）

```rust
pub trait Clock: Send + Sync {
    fn now_unix_ms(&self) -> u64;      // 墙钟：仅用于 LogRecord.ts_unix_ms，可冻结/漂移
    fn now_monotonic_ms(&self) -> u64; // 单调：idle/duration，不得回退，进程存活期间持续前进
}
```

实现必须廉价、非阻塞（热路径：每条日志；also contained 上下文）。内核自身**从不因策略读墙钟**，只*记录*时间与比较单调值（`clock.rs:1-7`）。

### 6.2 SystemClock（`clock.rs:45-67`）

零尺寸 `SystemClock`（`Debug/Default/Clone/Copy/PartialEq/Eq`）。`now_unix_ms`：`SystemTime::now().duration_since(UNIX_EPOCH).as_millis() as u64`，失败（1970 前）→ `0`。`now_monotonic_ms`：进程级 `OnceLock<Instant>` 原点（`clock.rs:52-55`），`elapsed().as_millis()`；**所有实例共享原点**，互换实例不重置 duration。

### 6.3 树 clock：`Context::{clock,set_clock}`（`src/context.rs:340-402`）

- 每棵 context 树一个 `Mutex<Arc<dyn Clock>>` 槽，root 派生 scope 共享。
- `set_clock`：① `fiber_id()!=0` → `Err(InvalidPlugin("clock must be set on the root context"))`（`context.rs:380-385`，防止插件静默改全局计时）；② 写槽（锁中毒→`Disposed`）；③ 遍历 registry 活 fiber + root，把 `FiberData.clock` 更新为新 clock（`context.rs:390-400`），保证 activity/idle 同一单调源。
- 契约：root 创建后、加载插件前**一次性安装**；运行期换 clock 允许但不鼓励（单调源跳变会让 idle 跳变，内核不隐藏）；只影响**新**观测，已记录时间戳不重写。
- 使用面：**unix** → `LogRecord.ts_unix_ms`（`logger.rs:246`）；**monotonic** → `FiberData.last_activity_ms`（`fiber.rs:440-479, 3856-3886`）、fiber 创建 `started_ms`（`context.rs:1284` 区域）、`idle_for` 差值。

### 6.4 Go 移植含义（clock）

- `Clock` → `interface { NowUnixMs() uint64; NowMonotonicMs() uint64 }`；`SystemClock` → `time.Now().UnixMilli()` + 包级 `processOrigin = time.Now()` 的 `time.Since(processOrigin).Milliseconds()`。
- 树级注入 `Context.SetClock(c Clock) error` 仅 root fiber 允许，否则 `ErrInvalidPlugin`；写后同步更新活 fiber 的 clock 引用（或 fiber 每次读树 clock，二选一并保持一致）。
- idle/duration 用显式比较避免 uint 下溢（Rust 用 `saturating_sub`）；测试用 fake clock 实现两方法。

---

## 7. `l2.rs`：supervision（能力自省、配额、心跳、watchdog）

### 7.1 IsolationCapabilities（`l2.rs:18-67`）

| 字段 | 类型 | 语义 |
|---|---|---|
| `panic_strategy` | `&'static str` | `"unwind"`/`"abort"`（本 crate 只能 unwind 编译） |
| `panic_containment` | `bool` | `cfg!(panic="unwind")` |
| `unsafe_forbidden` | `bool` | 恒 true（crate 级 forbid） |
| `process_isolation` / `wasm_isolation` | `bool` | 恒 false（L3/L4 是 embedder 层，内核不提供） |

`detect()`（`l2.rs:38-54`）运行时自省；`summary()`（`l2.rs:56-67`）输出 `isolation: panic=unwind containment=true unsafe_forbidden=true process=false wasm=false`。设计态度：**诚实**——只承诺 in-process unwinding panic 遏制，不假装是沙箱。

### 7.2 FiberQuota（`l2.rs:69-118`）

| 字段 | 类型 | 语义 |
|---|---|---|
| `max_effects` | `Option<usize>` | 每 fiber **用户** effect 上限（Q11：internal effect 不计） |
| `max_listeners` | `Option<usize>` | 每 fiber 活 listener 上限 |
| `max_child_fibers` | `Option<usize>` | 每 fiber 活子 fiber 上限 |

`Default` 全 None=无限制；`unlimited()`；builder `max_effects(n)/max_listeners(n)/max_child_fibers(n)`（`l2.rs:91-118`，`const fn`）。配额属 **registry**（整棵树）：`Registry::set_quota/quota/quota_limits`（`src/registry.rs:798-821`），`Context::set_quota/quota` 转发（`src/context.rs:914-924`）。

### 7.3 配额执行点与 live-resource 计数（v0.4.3 S1）

| 资源 | 执行点 | 规则 |
|---|---|---|
| effects | `Context::push_effect`（`context.rs:994-1044`） | push 前 `retain(effect_is_live)` 清死条目；`QuotaExceeded("effects")`；**只统计 `!internal`** 用户 effect（Q11，`context.rs:1021-1034`） |
| listeners | `Context::reserve_listener_slot`（`context.rs:811-830`） | `listener_count >= max` → `QuotaExceeded("listeners")`；`ListenerSlot` 一次性 release；`once` 自动移除用 `OnceListenerRelease` 兜底（`context.rs:832-850`）；`adopt_listener` 合并“释放槽+反注册”（`context.rs:852-877`） |
| child fibers | `Context::plugin*` 创建子 fiber 前（`context.rs:1250-1279`） | weak 句柄先 prune Disposed 子再计数；`QuotaExceeded("child_fibers")` |

超限走正常激活失败/回滚，不留半注册资源（GUIDE:894）；释放槽立即归还；internal effect（root logger provide、child ownership、内部 hook）永不占用户配额，故 `max_effects(1)` 仍能加载插件（`CORDIS-RUST.md:293-302`、spec `CORDIS-CORE.md:536-540`）。

### 7.4 心跳 / last_activity / idle_for（`src/fiber.rs:200-220, 433-479`）

`FiberData.last_activity_ms: AtomicU64`；`FiberData.clock` 创建时快照树 clock。`heartbeat()`（`fiber.rs:440-445`）存 `clock.now_monotonic_ms()`；`last_activity_ms() -> u64`（`fiber.rs:454-460`）；`idle_for() -> Duration` 为 `now_monotonic_ms().saturating_sub(last_activity_ms)`（`fiber.rs:468-479`）。生命周期操作也刷新活动时间：`begin_operation`/`end_operation` 各 store 一次（`fiber.rs:3849-3871`）——只有“有 in-flight 操作但长时间无心跳”才判 stalled。

### 7.5 Registry::stalled_fibers（`src/registry.rs:823-861`）

```text
fibers = registry snapshot + root fiber
for fiber: in_flight = pending_ops>0 || state in {Loading, Unloading}
           if in_flight && fiber.idle_for() >= timeout: push StalledFiber{fiber, idle_for}
```

`StalledFiber`（`l2.rs:120-149`）：字段 `fiber: Fiber`、`idle_for: Duration`；方法 `id()/name()`。**只观测、不强杀**（宿主决定 log/restart/`cancel()`/L3-L4）。**注册窗口不可见**：stalled 只看 fiber 级 `pending_ops` 与 `Loading/Unloading`，`internal/plugin` create hook 期间 fiber 仍 Pending 且无 fiber 级操作 → 要等注册窗口用 `when_idle()`（`CORDIS-RUST.md:263-269`）。`when_idle()` 覆盖注册→激活窗口（FIX-11b），`is_idle()` 严格、`when_idle()` 只排除调用线程自身注册（R3-A3/C4；`registry.rs:871-917, 1004`；GUIDE:614-619）。

### 7.6 feature 开关

`supervision` 是唯一 feature、默认关闭；L2 API 全部 `#[cfg(feature="supervision")]`（l2 模块、lib re-export、fiber/context/registry 方法）。无 feature 时 listener 记账退化为零尺寸 `ListenerSlot`（`context.rs:114-118, 883-906`），内核照常运行；测试两条构建：默认 466 tests vs `--features supervision` 481 tests（CHANGELOG 0.6.0）。

### 7.7 Go 移植含义（L2）

- `supervision` → 独立包优先，或 build tag；CI 跑两套。`FiberQuota` → struct + 可选上限（`*int` 或 `Limit{Value int; Set bool}`；**不要用 0 当“无限制”**）。
- 计数放 fiber 结构体（`atomic.Int64`），注册/释放路径增减；listener 用一次性 release（`sync.Once`/CAS），覆盖 disposer/once 自动移除/卸载三路径，防漏槽与双扣。
- `IsolationCapabilities`：`PanicContainment=true`（recover 存在）、`ProcessIsolation=false`、`WASMIsolation=false`，但注明 Go fatal error 不在 recover 覆盖范围。
- heartbeat/lastActivity 用 `atomic.Int64` 毫秒；`StalledFibers(timeout)` 返回快照 slice，**不提供 kill**；协作取消用 `context.Context`/`Cancel()`。

---

## 8. `async_effects.rs`：AsyncEffect / AsyncCleanup / AsyncDisposer / orphan queue

### 8.1 公开类型与构造器（`async_effects.rs:36-50`）

```rust
pub type AsyncCleanup = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;
pub type AsyncEffect  = Vec<AsyncCleanup>;
pub fn cleanup_async<F, Fut>(callback: F) -> AsyncCleanup;  // 把 async 闭包包成工厂
```

async cleanup 是**工厂**：调用时才创建 future，resolve 即资源释放完成。语义承诺（`async_effects.rs:19-23`）：清理逐个、逆注册序、调用方可观察完成（`dispose`/`dispose_async`/`dispose_blocking`），**不 spawn、不 fire-and-forget**；与 sync effect 共享同一条有序列表（`fiber.rs:90-105`），卸载顺序跨 lane 统一。

### 8.2 `AsyncDisposer` 结构（`async_effects.rs:52-235, 436-596`）

| 字段 | 类型 | 作用 |
|---|---|---|
| `inner` | `Arc<Mutex<Option<Vec<AsyncCleanup>>>>` | 剩余 cleanup 槽；None=已取走/已 dispose |
| `state` | `Arc<DisposeState>` | 跨句柄共享的 join 状态 |

`DisposeState`（`async_effects.rs:152-235`）：`done/cancelled/claimed: AtomicBool`、`waiters: Mutex<Vec<Waker>>`；`is_settled = done && !claimed`；
`claim()` CAS `claimed false→true` 成功后清 done/cancelled（新一轮尝试）；`release()` 清 claimed+唤醒 waiter；`finish_drain()` done=true/cancelled=false/release；
`cancel()` cancelled=true/done=true/release（取消的 teardown 不算完成，restored cleanup 待重试）；`key()` 用 `self as *const Self as usize`。
`drained_by_current_thread()` 查 thread-local `DRAIN_FRAMES: RefCell<Vec<usize>>`，`DrainScopeFuture` 在**每次 poll** cleanup future 时进入 drain frame（`async_effects.rs:80-139`）——判据是“当前调用栈是否在 drain 这个效果”，**不是线程 id**（多 task 共享 worker 线程时同线程无关 joiner 仍须等待，R2-1）。

### 8.3 `dispose()` 算法（`async_effects.rs:444-517`）

```text
loop:
  if !claim():
      if drained_by_current_thread(): return          // 自 join 短路（S-2/R2-1，等待自己必然死锁）
      wait_done().await                               // join 在飞 teardown
      if !is_cancelled(): return; continue            // 被取消的 teardown 恢复了 cleanup → 重试
  cleanups = take() or { finish_drain(); return }
  for cleanup in cleanups.rev():                      // 逆注册序，逐个 await
      outcome = catch_unwind(cleanup)                 // 工厂本身 panic
                .map(|fut| catch_panic(boxed(DrainScopeFuture{key, inner: fut})).await)
      if Err && drain.panic.is_none(): drain.panic = Some(payload)   // 记首个，继续
  armed = false; finish_drain()
  if let Some(payload) = drain.panic: resume_unwind(payload)         // 收尾完成后重抛首个
```

- `CancelSafeDrain::drop`（`async_effects.rs:237-266`）：`armed` 且尚有未启动 cleanup → 放回 `inner`（仅当槽为 None），`tracing::error!("async disposer cancelled mid-teardown; unstarted cleanups were restored")`，然后 `state.cancel()`。**Drop 中绝不执行 cleanup**。
- `wait_done`（`async_effects.rs:536-551`）：poll 注册 waker，settled 时 Ready；并发 dispose join 而非提前返回。
- `dispose_blocking`（`async_effects.rs:553-587`）：自 join 检查；`settled && !cancelled` → no-op；否则 `block_on_unit(boxed(async { self.clone().dispose().await }))`（走 embedder executor）。
- `is_disposed()`（`async_effects.rs:589-591`）；`discard()`（`async_effects.rs:519-534`，`pub(crate)`）：取走槽 + `finish_drain` 不执行 cleanup（R3-A4 父 fiber 摘除 child ownership effect）。
- `Drop for AsyncDisposer`（`async_effects.rs:598-629`）：① cancelled → 记录 error，restored cleanup 丢弃待显式 dispose；② `strong_count(&inner)!=1 || is_disposed()` → 静默返回；③ `thread::panicking()` → 取走槽、记录 `"async effect dropped during unwinding; its cleanups were not run"`；④ 否则 `catch_unwind(dispose_blocking())`（Drop 不得 panic 外泄）。

### 8.4 orphan queue（XT-01，`async_effects.rs:268-434`）

**问题**：`FiberData::drop` 在无 `BlockingExecutor` 或 unwinding 时不能跑 async cleanup（阻塞/死锁），0.6.0 前直接丢弃；现在改为**有界进程级队列**。

| 常量/静态 | 行号 | 值/语义 |
|---|---|---|
| `ORPHAN_CAPACITY` | `272-276` | `256`（超出 leak+计数，保证内存有界） |
| `ORPHANS` | `278-281` | `Mutex<Vec<AsyncDisposer>>` |
| `ORPHAN_COUNT` | `283-285` | `AtomicU64`：累计被迫泄漏（队列满/锁中毒） |
| `ORPHAN_FAILED` | `287-289` | `AtomicU64`：drain 时 cleanup panic 次数（可观测） |
| `ORPHAN_PENDING` | `291-294` | `AtomicUsize`：队列长度 gauge（不加锁） |

- `orphan_async_disposer(d)`（`async_effects.rs:296-324`，从 Drop 调用，**永不阻塞/unwind**）：`len<CAPACITY` → push + `PENDING++`；满或锁中毒 → `std::mem::forget(d)` + `COUNT++`（**不在这里 drop disposer**，否则可能触发阻塞的 Drop 网）。
- `orphan_async_cleanup_count()`/`orphan_async_cleanup_failure_count()`/`orphan_async_cleanups_pending()`（`async_effects.rs:326-357`，crate 根 re-export `lib.rs:75-79`）。
- `drain_orphan_async_cleanups()`（`async_effects.rs:359-424`）：① 锁队列**整体 take**（锁中毒用 `into_inner` 恢复），同时 `PENDING.fetch_sub(taken.len())`——先取走再执行，并发 hand-off/第二次 drain 不会重复执行；② 每个 disposer `catch_unwind(dispose_blocking())`，Err → `FAILED++` + `tracing::error!("orphan async cleanup panicked during drain: ...")`（不中断后续）；③ 返回处理数（含失败）；队列空返回 0（幂等）。
- 线程要求：**必须从非 reactor 的阻塞线程调用**；无 executor 时 `dispose_blocking` 回落 thread-park `block_on`，reactor-bound cleanup 可能失败（计入 failure count）→ drain 前应 `set_blocking_executor`。
- 附带：`ctx.plugin` 的**子 fiber ownership effect 是 async effect**，父 fiber Drop 未显式 dispose 时子 fiber 释放延迟到 drain（`fiber.rs:250-262` 注释、`CORDIS-RUST.md:539-559`）。

### 8.5 Go 移植含义（async effects / orphan）

- `AsyncCleanup` → `type Cleanup func(ctx context.Context) error`（返回 error 便于观察失败）；`AsyncEffect` → `[]Cleanup`；`CleanupAsync` 构造器。
- `AsyncDisposer` → struct：`mu sync.Mutex` + `remaining []Cleanup` + 状态（done/cancelled/claimed + `doneCh`/`sync.Cond` 做 join）；方法 `Dispose(ctx) error`、`DisposeBlocking()`、`IsDisposed()`、`Discard()`。
- Go 无 future 取消：用 `ctx.Err()` 检查 + defer 恢复未启动 cleanup（对应 CancelSafeDrain）；**在飞 cleanup 无法恢复**，与 Rust A-9 边界一致，写入文档。
- 自 join：Go 不能取 goroutine id → 把 drain 上下文作为显式参数/闭包传递，避免依赖 goroutine 身份。
- orphan queue → 包级 `chan AsyncDisposer`（buffer 256）或 `mutex+slice`；满时丢弃但计数（**不能阻塞 Drop**）；`DrainOrphans()` 先整体取走再逐个 `recover` 执行，返回处理数；`OrphanPending/Count/FailureCount` 三个 gauge/counter。Go 无确定性析构，文档强调宿主必须显式 `Dispose/DisposeAsync`。

---

## 9. 移植决策与用法骨架

### 9.1 关键决策表（`docs/CORDIS-RUST.md:9-50, 52-500` 摘要）

| 主题 | Rust 决策 | Go 方向 |
|---|---|---|
| 服务 | 类型化 `ServiceKey<T>` 值传递，无 property proxy/callable service/mixin | `ServiceKey[T]`（Go 泛型）+ `Provide/Get`，不模拟动态属性 |
| 内部钩子 | typed methods，名称/顺序对齐 JS；`internal/plugin` 仅 create 广播 | 接口 + 函数类型注册，9 个 typed hook 通道 |
| Logger 级别 | 严重度数值序 Error=0…Debug=3；非法数字 clamp Error | 自定义 Level，别直接暴露 slog 反向数值 |
| 监听器 | 两条 lane：sync（`on/once`）与 async（`on_async`）由对应 dispatch 调用，绝不混跑 | `On` / `OnAsync(ctx,…)` 两通道，同步派发不调 async |
| inject | 名称列表，严格 provider-Active 门 | `Inject() []string`，同严格门 |
| 事件作用域 | isolate-map **相等**过滤（非子树）；`global` 例外 | 比较 scope map 或显式 global |
| 动态加载 | 无 ABI/cdylib/module URL/config loader | 保持静态链接，插件=编译进来的 Go 值/接口 |
| Rust-only 扩展 | 结构化 Plugin 错误、生命周期广播、可注入 Clock、BlockingExecutor、L2 | 全部保留为可选机制 |

### 9.1b CHANGELOG 0.4.2–0.6.0 兼容性关键条目（`CHANGELOG.md` 对应版本段）

| 版本 | 关键兼容性/行为变化（与本文模块相关） |
|---|---|
| 0.4.2 | async waterfall panic 由“吞成 Null”改为 **re-raise**（与 sync 一致）；`ctx.set/set_any` 类型不匹配返回 `ServiceTypeMismatch`；`ctx.inject` 按插件类型分键（`registry.size()` 计数变化）；注入门要求 provider `Active`；`dispose_all` panic 优先；panic accessor 读作 absent；`MAX_PLUGIN_DEPTH` 改按 fiber 深度；`update` 顺序 `internal/config→schema→internal/update` 且 veto=不重启；`dispose_blocking` 真正 join + 重试被取消 teardown（S-1）+ 自 join 短路（S-2）；取消 fiber unload 变为“完成 teardown”（A-9）；replace/reload panic 转 `Err(PluginFailed)`；`is_idle/when_idle` 覆盖注册→激活窗口；`null` default 不声明类型、按声明类型决定注入；`restart` 补发 `Active→Unloading` status |
| 0.4.3 | 非法 logger 数字 level 从 fail-open（Debug）改为 **clamp 到 Error**（S2）；supervision 配额改按**活资源**计数（S1）；Plugin 元数据 panic contained 并确定性降级（C2）；深依赖链 unload 改两阶段迭代级联、限制移除（R3-A1）；`when_idle()` 不再在自身注册帧自锁（R3-A3/C4）；新增 `AccessorWriteRejected`；`Cargo.lock` 入库/`--locked`；已知限制：跨线程环形注册依赖、C2 降级 key 不可单独定位、fanout 性能回归（0.5.0 消除） |
| 0.5.0 | **破坏性 API 收敛**：`fiber::FiberRef/FiberData`、`registry::PluginKind`、`IdleOrRecoverableWait`、`Registry::counter` 转 `pub(crate)`；`Fiber::last_activity()->Instant` 移除，改 `last_activity_ms()->u64`（`supervision`）；Q11 `max_effects` 只计用户 effect；logger 时间源可注入（facade 快照）；`Ok`≠`Active` 语义澄清；新增 `Clock`/`LifecycleEvent`/结构化 `Plugin` 错误/`IdleWait` re-export；反向依赖索引消除 fanout 回归（纯内部） |
| 0.6.0 | **Breaking**：`LifecycleEvent` 加 `#[non_exhaustive]`、构造改 `LifecycleEvent::new(fiber_id,name,from,to,seq)`（迁移三步：literal→new、穷尽 match 加 `..`、只读者不变）；lifecycle 互斥改**操作级 token**（async 任务迁移后行为一致，async 挂起期间同线程 sync 调用也 Busy）；refresh 触发的卸载占用槽、外部 `restart/dispose/dispose_all` 得 `LifecycleBusy`、`await_settled` 等待该卸载；XT-02 activation epoch 精确回滚；**XT-01 orphan 队列**（Drop 跳过的 async cleanup 可回收/可观测，容量 256）；Drop 的 async cleanup 策略明确；`on_lifecycle` 与 raw `internal/status` 的“快照 vs 非快照”边界；`seq` 投递可能交错，按 `(fiber_id, seq)` 排序 |

### 9.2 panic 策略/遏制矩阵（`CORDIS-RUST.md:501-571`、`KERNEL-ISOLATION.md:95-108`）

| 边界 | 行为 |
|---|---|
| sync `apply` 返回 Err 或 panic | 激活失败，effect/service/listener 回滚，fiber `Failed`；panic 不穿出 `ctx.plugin` |
| async activation Err/panic | 同上回滚，await async cleanup |
| sync/async cleanup panic | 其余 cleanup 与 effect 继续；drain 完成后重抛首个 panic；激活失败回滚中只记录 |
| refresh/retire 触发的依赖方 unload panic | 记录并遏制；provider 仍到目标态（不卡 `Unloading`）；直接 `dispose` 仍重抛 |
| 父 fiber sweep 子 fiber | 逐 child `catch_unwind`，继续 sweep，父落目标态后重抛首个 payload |
| replace/reload panic | 遏制为 `Err(PluginFailed("<op>: reload panicked: …"))` + 回滚；回滚失败才 `Rollback` |
| Plugin 元数据回调 panic | 遏制+确定性降级：`name`→`"<panicked plugin>"`、`key`→`Named("<panicked plugin>")`、`inject`→`[]`、`config_schema`→`None` |
| `dispose_all` 同时 Err+panic | panic 优先：error 记日志，首个 payload 重抛（所有 fiber 都尝试过） |
| sync listener panic | 遏制，视为 `EventResult::Continue`，dispatch 继续 |
| async listener / internal hooks | 遏制并记录；hook 各自 fallback（默认配置/跳过 listener/continue） |
| Logger exporter level/export | 每 exporter 单独遏制；其余 exporter 与 buffer 写入继续 |
| reflect accessor read | 遏制；lookup 视为 absent（None） |
| `ConfigSchema::validate`/`merge` | 遏制；validate→validation failure，merge→base fallback |
| `AvailabilityCheck` | 遏制，视为 `false`（服务不可用） |
| **waterfall 中间件 panic（sync+async）** | **唯一 fail-closed：记录后中止整条链并重抛**，绝不放行/不吞成 Null |

**不遏制（进程内不可恢复）**：`panic=abort` 构建、栈溢出、OOM/allocator abort、`process::abort/exit`、`unsafe` UB、死锁、无限循环 → 需要 L3 进程隔离或 L4 WASM（§11）。

### 9.3 GUIDE 典型用法骨架（Go 设计必须能表达同样用法）

**配置 schema 插件**（GUIDE:119-156）

```rust
impl Plugin for HttpPlugin {
    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        Some(Arc::new(JsonConfigSchema::new()
            .required("port").property("port", JsonType::Number)
            .default_value("host", json!("127.0.0.1"))
            .default_value("proxy", Value::Null)   // 未声明类型→缺省注入 null
            .property("tls", JsonType::Object)
            .default_value("tls", Value::Null)))   // 已声明类型→可选、缺省 absent
    }
    fn apply(&self, _ctx: Context, config: Value) -> Result<()> { Ok(()) }
}
```

**服务 + inject 自动激活**（GUIDE:176-255；examples/services.rs）

```rust
static CLOCK: ServiceKey<Clock> = ServiceKey::new("clock");
ctx.provide(CLOCK, Arc::new(Clock { started: Instant::now() }))?;
fn inject(&self) -> &'static [&'static str] { &["clock"] }
let clock = ctx.require(CLOCK)?;   // 激活时依赖必已就绪；provider 撤销 → 回 Pending
```

**effect（sync/async/future/stream）**（GUIDE:303-342；examples/hello.rs）

```rust
ctx.effect(|| vec![cleanup(|| println!("closed"))])?;                       // sync
ctx.effect_async(|| vec![cleanup_async(|| async { /* await 关闭 */ })])?;   // async
ctx.effect_future(|| async { Ok(vec![cleanup_async(|| async {})]) }, "worker")?;
ctx.effect_future_async(|| async { Ok(vec![]) }, "worker").await?;
ctx.effect_stream(stream, "consumer")?;   // 逆注册序、sync/async 共享一条序、Disposer 单次幂等
```

**事件 / waterfall**（GUIDE:346-410；examples/events.rs）

```rust
ctx.on("demo", |args| EventResult::Continue)?;
ctx.once("demo", |_| EventResult::Continue)?;
ctx.on("demo", |_| EventResult::Bail(json!("vetoed")))?;
let verdict = ctx.bail("demo", &[json!(3)]);   // 第一个 Bail 决定结果
ctx.on_waterfall("request", |_a, next| json!({ "wrapped": next.next() }))?;
let out = ctx.waterfall("request", &[json!("payload")],
                        Some(waterfall_terminal(|args| json!({ "base": args[0] }))));
```

**异步插件 + executor**（GUIDE:419-475；examples/async_plugin.rs）

```rust
impl AsyncPlugin for Ticker {
    fn apply_async<'a>(&'a self, ctx: Context, _cfg: Value) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { ctx.effect_async(|| vec![cleanup_async(|| async {})])?; Ok(()) })
    }
}
let fiber = block_on(root.plugin_async(Arc::new(Ticker), Value::Null))?;
block_on(fiber.dispose_async())?;
set_blocking_executor(Arc::new(MyExecutor));   // 首次安装生效，宿主 runtime 桥
```

**生命周期 / 关闭 / 静默**（GUIDE:529-619）

```rust
fiber.update(json!({ "port": 9001 }))?;   // internal/config → schema → internal/update
root.registry().replace(PluginKey::Named("old"), Arc::new(NewPlugin))?;
root.dispose_all()?;                       // 逐个 dispose；panic 优先重抛
root.when_idle().await;                    // 静默检测（同步代码配合 block_on）
```

**logger / clock / L2 / 生命周期广播 / 结构化错误**（GUIDE:662-692, 861-903, 907-951）

```rust
let logger = ctx.logger(); logger.warn("slow", json!({ "ms": 120 }));
let _sub = ctx.log_exporter(Arc::new(TracingExporter))?;
root.set_clock(Arc::new(FakeClock))?;                      // 仅 root；之后重建 logger facade
root.set_quota(FiberQuota::unlimited().max_effects(256).max_listeners(64).max_child_fibers(32));
ctx.heartbeat();
for s in root.stalled_fibers(Duration::from_secs(5)) { s.fiber.cancel(); }
let sub = root.on_lifecycle(Arc::new(|e: &LifecycleEvent| { /* from/to/seq */ }))?;
let err = CordisError::plugin("my-plugin", "network", "upstream timeout");
```

### 9.4 examples 四个骨架

| 示例 | 核心骨架（行号） |
|---|---|
| `examples/hello.rs` | `Context::root()` → `root.plugin(Arc::new(Greeter), json!({"name":"Cordis"}))` → `apply` 内 `ctx.effect(|| vec![cleanup(...)])` → `fiber.dispose()` → `root.dispose_all()`（hello.rs:10-37） |
| `examples/services.rs` | `static CLOCK: ServiceKey<Clock>`；provide 于 `ClockPlugin.apply`；`UptimePlugin.inject()=&["clock"]` + `ctx.require(CLOCK)`；provider.dispose 后 consumer 回 `FiberState::Pending`，重新 provide 自动重激活（services.rs:11-70） |
| `examples/events.rs` | `ctx.on/once` + `Bail` listener + `ctx.on_waterfall`；`root.emit/bail/waterfall(..., Some(waterfall_terminal(...)))`（events.rs:10-62） |
| `examples/async_plugin.rs` | `impl AsyncPlugin` + `apply_async` 返回 `Box::pin(async move{…})`，内部 `ctx.effect_async(|| vec![cleanup_async(|| async{…})])`；`block_on(plugin_async)` / `block_on(dispose_async)`（async_plugin.rs:10-38） |

### 9.5 Rust → Go 设计映射表

| Rust 机制 | Go 映射建议 |
|---|---|
| `serde_json::Value` | `any`（JSON 解码值）或 `json.RawMessage`；schema/event payload 统一 `any` |
| `ConfigSchema`/`JsonConfigSchema` | 接口 + builder；`JsonType` 自定 enum |
| `tracing`（内核诊断） | `log/slog`（结构化 attrs）；内核一个 logger，exporter 走 handler |
| `LogExporter`/`TracingExporter` | `Exporter` 接口 / `SlogExporter` |
| `LogLevel` 数值序 | 自定义 `Level`（Error=0…Debug=3）+ `ParseLevel`/`String` |
| ring buffer | 预分配 slice 环形（head/len）或 `container/list` + mutex |
| `block_on`/future | 直接调用 `func()`；async 边界 `func(ctx context.Context) error` |
| `BlockingExecutor` | `interface{ BlockOn(func()) }`；`SetBlockingExecutor` 首次生效 |
| `catch_unwind`/`catch_panic` | `defer recover()`；只覆盖普通 panic |
| `Clock`/`SystemClock` | `Clock interface` + `time.Now` / 进程单调原点 |
| `feature supervision` | 独立包或 build tag；两套 CI |
| `#[non_exhaustive]` | 接口/构造函数 + 未导出字段；或 struct + `New` |
| `dispose_async` vs `dispose` | `Dispose(ctx)` / `DisposeBlocking()`，文档标注使用线程 |
| orphan queue | 有界 slice/chan + atomic 计数 + `DrainOrphans()` |

---

## 10. 内核边界（KERNEL-BOUNDARY）与 Go CI 护栏思路

### 10.1 三层模型与依赖方向（`KERNEL-BOUNDARY.md:28-46, 94-127`）

```text
业务/宿主 crate ──► 插件（impl Plugin / AsyncPlugin）──► zeokeel-cordis（内核）
       │                                                      │
       └──────────────► serde_json / tracing / futures-util ◄─┘
```

- 依赖**单向**：插件层/宿主可依赖内核；内核**绝不**依赖它们；插件/业务/传输/存储类型不得出现在内核公共 API。
- 运行时依赖白名单：`serde_json`、`tracing`、`futures-util`（三项且必须存在）；dev-deps 可放宽但不得成为运行时依赖。
- 禁止依赖类别：异步运行时（tokio/async-std/smol/futures-executor）、HTTP/网络（reqwest/hyper/axum/actix-web）、数据库/ORM（sqlx/diesel/rusqlite）、缓存/中间件（redis/mongodb）、序列化格式（serde_yaml/toml）、系统/环境（libc/nix/winapi/windows-sys/tempfile/walkdir）、时间/随机/ID（chrono/time/uuid/rand）、CLI（clap）（`KERNEL-BOUNDARY.md:101-107`）。
- 源码禁用 API：`std::fs/net/process/env/os/path/io`、`TcpStream/TcpListener/UdpSocket/UnixStream`、`File::open/OpenOptions/Command::new`、`thread::spawn/thread::Builder`、`tokio::/async_std::/smol::/reqwest::/hyper::/sqlx::`、`env!/option_env!/include!/include_str!/include_bytes!`（`KERNEL-BOUNDARY.md:108-113`）。`std::thread` 只保留 `park/current/Thread/panicking`（同步等待边界 + panic 路径）。
- 职责边界摘要（`KERNEL-BOUNDARY.md:47-66`）：config 只做 schema 机制（**不含领域字段**）；exec 只做 block_on/BlockingExecutor seam；logger 只做设施（**不选后端、不定义指标**）；l2 只做机制（**不决定阈值/处置策略**）；internal 只做 typed hook 通道（**不内置鉴权/限流**）。内核**提供机制，不提供策略**。

### 10.2 扩展点（KERNEL-BOUNDARY:130-149 摘要）

| 需求 | 扩展点 |
|---|---|
| 业务组件/资源生命周期 | `Plugin`/`AsyncPlugin` + effect（`apply`/`apply_async`、`ctx.effect*`、`provide*`） |
| 依赖其他插件服务 | `inject` + `ServiceKey`（严格 provider-Active 门） |
| 业务事件/解耦广播 | `Events`（on/once/emit/bail/parallel/serial/waterfall） |
| 横切拦截/审计/限流/配置来源 | 9 个 `internal/*` typed hook（plugin/status/service/config/update/get/set/listener/dispatch） |
| 策略包装/改写 | 同步/异步 waterfall（panic fail-closed） |
| 配额/watchdog/能力自省 | L2 `supervision`（默认关闭） |
| 可注入时间源 / 生命周期观测 | `Clock`/`SystemClock`（仅 root 设置）/ `on_lifecycle`+`LifecycleEvent`（Disposer 唯一所有者，drop=退订） |
| 插件错误分类 / 日志指标导出 | `CordisError::Plugin`+`error_detail()` / `LogExporter`（默认不装 exporter） |
| 同步 API 用于异步宿主 / 配置来源 | `BlockingExecutor`（宿主 runtime 桥）/ 宿主解析后交给 `ctx.plugin`、`resolve_config` |
| 加载器/权限/沙箱 | 宿主（内核只提供替换/重载原语） |

### 10.3 CI 护栏（Rust 现状，`KERNEL-BOUNDARY.md:153-226`）

`tests/kernel_boundary.rs` 共 **11 个测试 = 4 个真实断言 + 7 个防绕过自测**；CI 步骤 `cargo test --locked --test kernel_boundary`（`.github/workflows/ci.yml:74-75, 234-235`）。

**4 个真实断言**：① `runtime_dependencies_stay_generic_and_minimal`——解析 `[dependencies]` 平铺、`[dependencies.X]` 表、`[target.'cfg(...)'.dependencies(.X)]`（含平铺 section），把 `package="..."`/单引号重命名解析成真实包名；
运行时依赖（含 build-dependencies）⊆ 白名单；禁 build script/`src/bin`；`[lib] path` 只能 `src/lib.rs`；`[features]` 只允许 `default`/`supervision` 且不得 `dep:` 拉依赖。
② `sources_do_not_reference_io_net_process_env`——递归扫 `src/**`；注释用词法器剥离（字符串保留）；`use` 分组展开 + 全空白归一化；按 §10.1 列表匹配。
③ `sources_do_not_contain_business_vocabulary`——代码+字符串按标识符分词（snake_case 与 CamelCase 拆词小写）匹配 HARD 列表（payment/invoice/tenant/customer/checkout/warehouse/database/redis/kafka/rabbitmq/http/server/client）；
**公开文档**（`///`、`//!`、块文档、`#[doc=]`）用 `FORBIDDEN_DOC_WORDS`（同 HARD 但去掉 http/server/client，因为内核文档要描述 embedder 边界）；文件名 stem；普通注释与尾部 `#[cfg(test)]` 模块在词扫描前拆出。
④ `kernel_keeps_forbid_unsafe_and_a_bounded_public_surface`——注释剥离后 `src/lib.rs` 真实存在 `#![forbid(unsafe_code)]`；公共项计数（含 `pub use`/`pub mod`）≤ `PUBLIC_ITEM_CAP=420`（绊线）。

**7 个自测**（证明护栏能抓绕过）：表语法依赖+`package=` 重命名；平铺 target section+单引号重命名；每源文件最多一个尾部 `#[cfg(test)]`；`use` 分组展开+空格路径；snake/Camel 分词；doc 注释/`#[doc]` 收集；注释词法器保留字符串、拒绝伪造 `#![forbid(unsafe_code)]`。**口径**：普通注释不扫、公开文档要扫、字符串要扫；dev-deps/examples 不查；cap 是绊线不是硬上限；改护栏必须保留自测。CI 还反向验证 `panic=abort` 构建必须失败（`ci.yml:121-127`）。

### 10.4 Go 版护栏设计建议

| 检查 | Go 实现 |
|---|---|
| 运行时依赖白名单 | `go list -deps -json ./...` 取 import 图；只允许标准库子集（`fmt/errors/sync/sync/atomic/time/sort/strings/strconv/math/container/*/encoding/json/context`）；显式拒绝 `net`、`net/http`、`os/exec`、`database/sql`、`plugin`、`unsafe`、`runtime/cgo`、`syscall`；`go.mod` 第三方依赖零/白名单 |
| 禁用 API | `go/parser.ParseDir` + `go/ast` 遍历 `*ast.SelectorExpr`/`*ast.CallExpr`（`os.Open`、`net.Dial`、`exec.Command` 等）；禁 `//go:linkname`、`//go:embed`（或白名单） |
| 业务词 | `go/ast` 取 `*ast.Ident` 与字符串常量，按 Go 命名分词（`PaymentGateway`→payment/gateway）；`go/doc` 取导出符号 doc comment 单独用 `FORBIDDEN_DOC_WORDS` 扫；`_test.go` 与 `examples/` 排除 |
| 分层依赖方向 | `go list` 检查内核包不 import 插件/`cmd`/`examples`；或 depguard/自写 arch test |
| `forbid unsafe` | 扫 `import "unsafe"`、`//go:linkname`、cgo（`import "C"`）；`go vet` 不覆盖这条 |
| 公共 API cap | `go/types` 遍历 package scope 导出对象（var/func/type/const）计 cap；或 `go doc -all` 行数近似 |
| feature 等价 | build tag 文件归独立目录/包；CI 两套 `go test ./...` 与 `go test -tags supervision ./...` |
| 质量门禁 | `gofmt -l`、`go vet ./...`、`staticcheck ./...`、`go test -race ./...`、`go build -trimpath`、`govulncheck`/SBOM |
| 反向验证 | 护栏自测（表语法/重命名/分组 import/分词/注释伪造），对应 Rust 的 7 个 `guard_*` |

> Go 的 `go/parser` 用 `parser.ParseComments` 保留注释；剥离注释看 `ast.Expr`，字符串字面量在 `*ast.BasicLit{KIND: token.STRING}` 中仍参与业务词扫描（对齐 Rust 口径）。

---

## 11. KERNEL-ISOLATION：L1–L4 与不可遏制故障

### 11.1 结论（`KERNEL-ISOLATION.md:13-21`）

内核提供 **unwinding panic 的故障遏制（fault containment）**，不是沙箱。`panic="unwind"` 且插件不主动 abort/不越界时，一个插件的 panic 只让它自己的 fiber 回滚/失败，其它插件与内核继续；waterfall 中间件是唯一 fail-closed 例外。`panic="abort"`、栈溢出、OOM、`process::abort()/exit()`、插件 `unsafe` UB、死循环/死锁、线程/fd 泄漏在进程内**无法隔离**。

### 11.2 分层隔离（`KERNEL-ISOLATION.md:201-211`）

| 层 | 机制 | 买到什么 | 成本 |
|---|---|---|---|
| **L1** 进程内遏制（本 crate 已提供） | unwind 边界 + 回滚 + Drop 规则 + 锁内不跑用户代码 | 可捕获 panic 不扩散；故障归因到 fiber | 0 |
| **L2** 线程级监督 | 每插件/组一个监督者：心跳、deadline、协作取消（checkpoint）、资源计量 | 定位、降级、优雅重启；**不能杀线程** | 低-中 |
| **L3** 进程隔离（推荐给不可信插件） | 每插件组子进程+IPC；生命周期/配置/declarative effects 走 RPC；SIGKILL+rlimit/cgroup/seccomp；崩溃报告/自动重启 | abort/SEGV/UB/OOM/死循环全部可恢复 | 高 |
| **L4** WASM 沙箱 | wasmtime/wasmer + fuel/时间/内存上限、能力导入白名单 | 第三方代码的安全边界；无 unsafe | 高 |

判断标准：插件来自**可信仓库、同源代码** → L1+L2 足够；第三方/可被攻破 → 必须 L3 或 L4。

### 11.3 不可遏制故障清单（`KERNEL-ISOLATION.md:110-126, 213-257`）

| 故障 | 可遏制？ | 说明 |
|---|---|---|
| `panic="abort"` 构建下任何 panic | 否 | `catch_unwind` 失效；本仓库编译期 guard 拒绝该构建 |
| 栈溢出（插件自身递归/深 Drop） | 否 | 依赖链卸载已迭代化（不是溢出来源），插件自身递归仍会爆栈 |
| OOM / allocator abort | 否 | 分配错误默认 abort，不能 unwind |
| `process::abort()/exit()` | 否 | 直接杀进程/假成功 |
| 插件 `unsafe` UB / SIGSEGV | 否 | 同地址空间无法隔离 |
| 死循环/死锁/无限阻塞 | 否 | 没有安全方式终止线程；靠协作 checkpoint + watchdog 观测 |
| 线程/fd/内存无上限泄漏 | 否 | 内核不跟踪线程/fd；L2 只度量 effects/listeners/child fibers |
| 取消撞在飞的 async cleanup future | 否 | Rust 无 cancel-safe await；只保证未启动 cleanup 恰好执行一次、不留永久 Unloading（A-9/F1–F5/S7） |
| 跨线程环形注册等待 | 否 | 内核不检测环依赖；宿主必须避免同步注册环 |

### 11.4 编译期/CI 强制（`KERNEL-ISOLATION.md:213-257` 摘要）

| 威胁 | 处理 |
|---|---|
| `panic="abort"` | `#[cfg(panic="abort")] compile_error!`；CI 反证 `RUSTFLAGS="-C panic=abort" cargo check --lib` 必须失败 |
| 内核自身 unsafe | crate 级 `#![forbid(unsafe_code)]`；依赖用 cargo-audit/geiger 审计 |
| 整数溢出静默回绕 | release `overflow-checks=true` → 溢出 panic（可被遏制观测） |
| 持锁跨 await | clippy `await_holding_lock=deny`、`mutex_integer=deny`；CI `-D warnings` |
| 插件加载深度 | `MAX_PLUGIN_DEPTH = 256`（按 fiber 父链计数；root 级 plugin depth=1） |
| 深依赖链卸载栈溢出 | v0.4.3 两阶段迭代级联，栈 O(1)，2 MiB 线程 depth 800 debug+release 均通过 |
| OOM/死锁/外部 kill/UB | 无编译期方案 → L3/L4 |

> Go 对应物：`-race`、`go vet`/staticcheck、`recover` 遏制、`context` 协作取消、`maxDepth` 常量；但 Go **没有**“panic=abort 编译期守卫”等价物，且栈溢出/`runtime.throw`/OOM 不可 recover——必须写进 Go 内核“不可遏制边界”，并建议插件加载器做进程/WASM 隔离。

---

## 12. Go 移植检查清单（本文件范围内）

1. **config**：builder + validate 五步顺序 + merge 浅合并 + defaults 不覆盖显式值 + null default 双语义 + `resolve_config` 的 base→root→child→head 顺序；`ValidationError.Error()` 逐字对齐。
2. **logger**：Level 数值序 Error=0…Debug=3；非法数字 clamp Error；ring buffer 1000（0 禁用）；seq 从 1、写 buffer 前原子分配；exporter 锁外调用+每 exporter recover+per-exporter level；facade 快照 clock；默认无 exporter；`SlogExporter` 通用桥。
3. **exec**：同步/异步双入口；`BlockingExecutor` 首次安装生效；同步 dispose 禁止在回调/事件循环 goroutine；embedder 回调一律 `recover`。
4. **error**：全变体 + Display + `PluginError{Plugin,Kind,Message}` 原样传播 + `ErrorDetail()`；`Ok` 不等于 `Active` 写入注释与测试。
5. **clock**：双方法接口；仅 root 可 SetClock；facade 快照；unix 只用于日志、monotonic 用于 idle；fake clock 覆盖 L2 与日志测试。
6. **supervision**：独立包/build tag；活资源计数；effect 只计用户 effect（Q11）；listener 单次释放；child 按 live 计数；stalled 只观测不强杀；注册窗口不可见 → 用 when_idle 等。
7. **async effects**：cleanup 工厂 + 逆序 + join + cancel-safe restore + 自 join 短路 + Drop 安全网 + 有界 orphan 队列（cap 256）+ 三个计数器 + DrainOrphans（非 reactor 线程）。
8. **内核边界**：Go 架构测试（`go list` import 白名单 + `go/parser` AST 扫描 + 分词业务词 + 公共 API cap + forbid unsafe）；examples/测试豁免；护栏自测保留。
9. **隔离**：L1=recover 遏制（明确 fatal error 不可 recover）；L2=心跳/配额/协作取消；L3/L4 由宿主提供，内核不依赖进程/WASM 运行时。

**主要风险/待 Lead 决策**：① Logger 数值序采用 Rust 序（Error=0…Debug=3）还是 slog 序——建议保持 Rust 序（对齐规范/测试），slog 映射放 exporter；② `Clock` 用 `uint64` 毫秒还是 `time.Time`——建议 uint64 毫秒 + 便捷转换；③ `AsyncCleanup` 签名 `func(ctx) error` 还是 `func()`——建议带 ctx（可取消/超时），错误语义与 orphan failure counter 对齐；④ `supervision` 用独立包还是 build tag——建议独立包（避免默认/带 tag 构建 API 漂移）。
