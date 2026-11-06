# 03 — Registry 插件运行时 / 事件总线 / Waterfall 架构与语义

> 分析对象：`reference/zeokeel-cordis-rust`（zeokeel/zeokeel-cordis v0.6.0，纯 Rust 内核）。
> 必读证据：`src/registry.rs`（3036 行）、`src/events.rs`（673）、`src/waterfall.rs`（229）、`spec/CORDIS-CORE.md` §2.5/§2.6/§4、`docs/CORDIS-RUST.md` §3/§4。
> 约定：`path:line` 相对参考仓库根；叙述中文，标识符保持英文。本文只做只读分析，不含 Go 实现代码；Go 移植含义见 §8。
> 关联任务：task-3。唯一允许写文件：本文。

## 0. 结论速览（TL;DR）

1. **Registry 是「runtime 表 + 全局 fiber 表 + 派生依赖索引 + 空转/恢复协调器」四层合体**：`runtimes: HashMap<PluginKey, Arc<PluginRuntime>>` 管 dedupe，`FiberSlots` 是全局 id→fiber 存储，`DepIndex` 是纯派生反向索引，其余原子量/锁负责 quiescence、parked-cascade 恢复和 refresh 固定点（`src/registry.rs:401-482`）。
2. **PluginKey 决定 dedupe 粒度**：`Named(name)`（默认，同插件名共享 runtime）或 `Type(TypeId)`（`plugin_typed`/`inject`）。同 key 重复注册返回同一个 runtime 的新 fiber；最后一个 fiber dispose 时 runtime 被移除（CORE-022）。
3. **fiber 生命周期是「Pending→Loading→Active→Failed/Unloading→Disposed」状态机**，`id` 由 registry 单调分配（root=0），`uid = Some(id) unless Disposed`（`src/fiber.rs:116-125,353-363`）。
4. **每个 fiber 同一时刻只允许一个 lifecycle 操作**（operation token，跨 await 迁移稳定），并发第二调用得到 `LifecycleBusy`（CORE-045）。refresh 驱动的 unload 也占位（XT-02）。
5. **快照语义**：`size/get/has/keys/values/entries/for_each/fibers/fiber_count` 各自只锁一个结构，跨调用可短暂不一致；需要一致视图用 `is_idle()/when_idle()`（CORE-049）。
6. **`replace` 是「rebind 全部 fibers → 逐个 reload → 失败回滚」**：rollback 先恢复 runtime 表和 fiber 绑定，再把已 reload 到新实现的 fiber 用旧实现 reload 回去；rollback 自身失败才返回 `CordisError::Rollback`（`src/registry.rs:1101-1228,1334-1403`）。
7. **`Ok` 不代表 `Active`**：swap 后的 reload/activation 失败被收敛在 fiber 级（fiber 变 `Failed`，runtime 仍绑定新实现、旧实现不恢复，XT-05）；只有 `LifecycleBusy`/校验错误触发回滚。
8. **Events 是三条独立的有序注册表**（sync listener / sync waterfall / async waterfall），各一把 Mutex；listener 记录 `owner(fiber id)`、注册时 isolate map 快照、`prepend/global/once`。
9. **isolate 过滤 = 映射相等**：`global` 直接放行；否则 dispatch scope 必须与 listener 的 scope 快照逐项相等（CORE-019）。直接调 `Events::*`（scope=None）不过滤。
10. **sync 派发只跑 sync listener**（emit 忽略返回值；bail 遇到第一个非 null/false 的 `Bail` 停止）；**async 派发只由 `parallel_async/serial_async` 驱动**，`parallel_async` 先 inline 跑完所有 sync listener，再把 async future 收集起来并发 poll；`serial_async` 严格按注册顺序交错执行、遇 bail 停止。
11. **Waterfall 是显式 continuation 中间件，不是 bail lane**：不调用 `next` 即否决剩余链与 terminal（CORE-020）；`next.remaining()/terminal()` 可探测/跳过；`next` 可多次调用重跑剩余链。
12. **panic 策略**：event listener / internal hook / 插件元数据 panic 一律遏制并记录；**waterfall 是唯一 fail-closed 例外**——panic 中止链并把 panic 重抛给派发者（sync 与 async 一致，v0.4.2+）。
13. **parked cascade 恢复（F1/F4/F5）是 Registry 的隐藏职责**：cancelled async cascade 把非 root 帧 park 成 `Unloading`，由 `when_idle/refresh_async/dispose_all/root dispose` 单 recoverer、有界、可重入、幂等恢复；sync `refresh()` 故意不恢复（避免 reactor 线程死锁）。
14. **反向依赖索引 S1–S4 是性能设施而非语义来源**：所有可用性判定仍走 `lookup_for_injection`；索引不可信（poison/window/epoch 变化）时立即回退全量扫描（C2/C3 fail-safe）。

---

## 1. 组件总览与所有权

```
Context(tree) ─┬─ ContextInner.events:  Arc<Events>            （每个 context tree 一个）
               ├─ ContextInner.registry: Arc<Registry>          （每个 tree 一个，root 持有）
               ├─ ContextInner.service_store: Arc<Mutex<HashMap<(scope_id,name),ServiceEntry>>>
               ├─ ContextInner.isolate: HashMap<name, scope_id>  （创建子 context 时的 scope 快照）
               └─ ContextInner.fiber: Fiber
                        └─ FiberData { id, state, plugin, inject, effects, deferred_async,
                                       runtime_key, registry, children, pending_ops, ... }

Events ─┬─ listeners:            Mutex<HashMap<String, Vec<Listener>>>
        ├─ waterfall_listeners:  Mutex<HashMap<String, Vec<WaterfallEntry>>>
        ├─ async_waterfall_listeners: Mutex<HashMap<String, Vec<AsyncWaterfallEntry>>>
        └─ internal: Arc<InternalBus>（typed internal/* hooks）
```

- root context 创建 events/registry 并 `set_root_fiber`（`src/context.rs:128-193`；`registry.rs:921-925`）；root fiber **不在** `Registry::fibers` 中（`registry.rs:414-416`），但参与 quiescence。
- 子 context 通过 `child` 复制 events/registry/service_store/clock 句柄（`context.rs:200-216,258-275`），因此 **每个树共享唯一总线与注册表**；`isolate` 只改自己的 `isolate` map 并新建 scope id（`context.rs:241-256`）。
- fiber 创建路径：`ctx.plugin` → `plugin_with_key`（`context.rs:1373-1396`）→ `create_fiber_handle`（`context.rs:1207-1354`，`register_fiber` = 全局 fiber 表+依赖索引，`register_runtime_fiber` = runtime 成员）→ `internal/plugin` hook（`context.rs:1352`）→ 满足 inject 时 `activate_fiber`（`fiber.rs:2091`）。
- fiber 销毁路径：`dispose/finish_unload` → `transition(Disposed)` → `unregister_runtime_fiber`（runtime 成员 + 全局 fiber 表 + 反向索引） + `events.remove_owner(fiber_id)` + 断 `ctx/parent` 链（`fiber.rs:2408-2440,2418-2432`）。
- 数据流一句话：**注册与拓扑变更全部经过 Registry（有锁）→ 生命周期状态变更全部经过 Fiber（每 fiber 一把状态锁 + 原子 pending_ops/lifecycle token）→ 事件与 waterfall 只做无锁快照（Mutex 仅用于取/改 Vec）。**

---

## 2. Registry 数据结构

### 2.1 `Registry` 字段与锁分组（`src/registry.rs:401-482`）

| 字段 | 类型 | 锁/原子组 | 作用 |
|---|---|---|---|
| `counter` | `Mutex<u64>` | id 分配组 | fiber id 分配器（`next_id`，`registry.rs:1010-1018`） |
| `scope_counter` | `Mutex<u64>` | id 分配组 | isolate scope id 分配器（`next_scope_id`，`:1034-1042`） |
| `label_scopes` | `Mutex<HashMap<&'static str,u64>>` | id 分配组 | 命名 isolate label→scope id（`labeled_scope_id`，`:1022-1032`） |
| `fibers` | `Mutex<FiberSlots>` | 拓扑组（全局） | 全局 live fiber 表/id 索引（S2） |
| `runtimes` | `Mutex<HashMap<PluginKey, Arc<PluginRuntime>>>` | 拓扑组（根） | plugin runtime 表（dedupe 单位） |
| `root_fiber` | `Mutex<Option<Weak<Mutex<FiberData>>>>` | 拓扑组（root） | root fiber 弱句柄，仅用于 quiescence/`fibers()`/dispose_all |
| `idle_waiters` | `Mutex<Vec<(u64, Waker)>>` | quiescence 组 | `when_idle` waker 表，id 去重、Drop 注销 |
| `pending_registrations` | `AtomicUsize` | quiescence 组 | 「已创建 fiber 但 activation 未开始」窗口计数（FIX-11b） |
| `registration_owners` | `Mutex<HashMap<ThreadId, usize>>` | quiescence 组 | C4：每线程自己的 in-flight 注册数，供 `when_idle` 排除自身 |
| `refresh_depth` / `refresh_dirty` | `AtomicUsize` / `AtomicBool` | refresh 组 | refresh 固定点重入熔断 + dirty 标记 |
| `parked_unloads` | `Mutex<Vec<(Weak<Mutex<FiberData>>, FiberState)>>` | 恢复组 | F1：cancelled cascade 遗留的 `Unloading` 帧及原始目标状态 |
| `recovering_parked` | `AtomicBool` | 恢复组 | F1：单 recoverer 闩锁（RAII 释放） |
| `active_unloads` | `AtomicUsize` | 恢复组 | 当前正在跑的 unload/cascade body 数；>0 时禁止恢复 |
| `dep_index` | `Mutex<DepIndex>` | 索引组 | S1 派生反向索引（叶子锁） |
| `dep_index_windows` | `AtomicUsize` | 索引组 | S1/C2：replace/reindex 事务窗口数，>0 时禁用候选过滤 |
| `dep_index_broken` | `AtomicBool` | 索引组 | 索引永久失效 fail-safe（poison/元数据不可读） |
| `dep_index_epoch` | `AtomicU64` | 索引组 | 窗口代数；cascade 用快照 epoch 检测中途失效 |
| `service_dirty` | `Mutex<HashSet<String>>` | 索引组 | S4：自上次 refresh 起可用性/值变化过的服务名 |
| `quota` | `Mutex<FiberQuota>` | supervision 组 | 每 fiber 资源上限（`#[cfg(feature="supervision")]`） |
| `debug_cross_check` | `AtomicBool` | debug 组 | S3：debug 构建下索引候选集 vs 全量扫描交叉断言 |

**锁分组意图**：拓扑组（`runtimes → runtime.fibers`、`fibers`）保护注册关系；quiescence/恢复组全是原子量或短临界区，避免与拓扑组嵌套；索引组是**叶子锁**，任何路径都不得在持有 `fibers/runtimes/fiber.data` 时长时间持有它（`registry.rs:454-458`）。

### 2.2 PluginKey 语义与 dedupe

- 定义：`PluginKey::Type(TypeId)`（具体类型，`plugin_typed`/`plugin_async_typed`/`inject` 用）与 `PluginKey::Named(&'static str)`（`Plugin::name()` 默认）；`Plugin::key()` 默认返回 Named（`registry.rs:120-128`，`AsyncPlugin` 同 `:167-194`）。
- Typed 判定：`ctx.plugin_typed<P>` 用 `TypeId::of::<P>()` 作为 key（`context.rs:1200-1204`）；`ctx.inject` 用 `TypeId::of::<Injected<F>>()`，因此**每次 inject 调用是独立 runtime**（FIX-20，`context.rs:1446-1477`；`Fiber::name()` 仍是 `"injected"`，但 `replace(Named("injected"))` 不再命中）。
- C2 元数据遏制：`name/key/inject/config_schema` 是 embedder 代码，panic 时分别降级为 `"<panicked plugin>"` / `Named("<panicked plugin>")` / `&[]` / `None`，记录 error 且绝不 unwind 出 `ctx.plugin`（`registry.rs:205-338`，常量 `PANICKED_PLUGIN_NAME` `:223`）。注意：所有 key-panic 的插件**共享同一个 runtime**，`keys/replace/delete` 无法单独寻址（`registry.rs:250-258`）。
- dedupe 实现：`register_runtime_fiber` 用 `runtimes.entry(key).or_insert_with(PluginRuntime::new)`，每个 fiber 都 push 进该 runtime 的 `FiberSlots`（`registry.rs:1780-1805`）。`PluginRuntime` 本身不计数引用，**最后成员被移除时由 `unregister_runtime_fiber` 删表**（`:1823-1890`），另有 `prune_empty_runtimes` 兜底（`:1920-1938`）。
- 测试锚点：`tests/registry.rs:44-99`（同名/同类型共享 runtime、逐个 dispose 后 runtime 消失）、`tests/registry.rs:101-160`（map API / delete 语义）。

### 2.3 PluginRuntime 与 FiberSlots（S2）

- `PluginRuntime { name: String, key: PluginKey, fibers: Mutex<FiberSlots> }`（`registry.rs:70-107`）；诊断 API：`fibers() -> Vec<Fiber>`（快照）、`fiber_count()`、`is_empty()`。
- `FiberSlots`（`registry.rs:484-558`）是「tombstone 槽位 + id→槽位索引」：
  - `slots: Vec<Option<Fiber>>` 保留插入顺序（诊断友好）、删除 O(1)（置 None）、`positions: HashMap<u64, usize>`、`live: usize`。
  - `push`（`:499-515`）：若 id 已存在则**覆盖同槽**（C6：一个 fiber id 最多一条记录）；当 `slots.len() > live*2+64` 时 `compact()`（均摊 O(1)）。
  - `remove`（`:517-528`）/ `get`（`:530-533`）/ `snapshot`（`:535-537`）/ `compact`（`:547-557`）。
  - 动机（`:484-490`）：旧 `Mutex<Vec<Fiber>> + retain` 在 5000 fibers 共享一个 runtime 的 fanout dispose 下是 O(n²)（实测 ~2.2s）。
- **PluginRuntime 生命周期（创建/复用/回收）**：
  - 创建：`register_runtime_fiber` 在 `runtimes.entry(key).or_insert_with(PluginRuntime::new)` 内惰性创建（`:1791-1794`）；`rebind_runtime` 对 merge 目标也会创建（`:1349-1356`）；`PluginRuntime::new` 是 crate 私有（`:82-88`），外部无法构造 runtime。
  - 复用：同 key 的每次 `ctx.plugin*` 都复用同一 runtime，只新增 fiber 成员；`replace` 的目标 key 若已有 runtime 则合并（`rebind_runtime` 的 merge 分支）。
  - 回收：`unregister_runtime_fiber` 在「本次移除且有成员」且变空时删表（`:1843-1879`）；`undo_rebind`/`prune_empty_runtimes` 负责回滚/兜底清理（`:1287-1338,1920-1938`）；`delete` 直接先删表项再 dispose fibers（`:1076-1086`）。
- **注意**：`PluginRuntime.fibers` 与 `Registry.fibers` 是两套存储（runtime 成员 ⊆ 全局表；root fiber 只在全局表外单独保存），二者通过 `register_runtime_fiber/unregister_runtime_fiber/heal_disposed_registration` 维持一致。

### 2.4 fiber id / uid 分配器、label scope counter

- **fiber id**：`Registry.counter` 全局单调，`next_id()` 从 1 开始；root fiber 硬编码 `id=0`（`context.rs:132-134`），创建子 fiber 时 `let id = registry.next_id()`（`context.rs:1281`）。`Fiber::id()` 始终返回数字（`fiber.rs:353-355`）；`Fiber::uid() -> Option<u64>` 在 `Disposed` 时返回 `None`（`fiber.rs:357-363`）。`Registry::counter()` 自 0.5 起为 `pub(crate)`，没有公开访问器（`registry.rs:792-796`）。
- **scope id**：`scope_counter` 单调分配（`:1034-1042`）；`isolate(name)` 每次新建一个 id（`context.rs:241-245`），`isolate_labeled(name,label)` 通过 `label_scopes` 复用同名 label 的 id（`context.rs:252-256`）；**两种命名空间共用同一 counter**，避免 label 与 fresh scope 撞号（注释 `registry.rs:410-413`）。`label_scopes` 只增不减（Rust 侧 label 是 `&'static str`，天然有界；Go 侧若允许动态 label 需自行评估增长）。
- 潜在锁嵌套：`labeled_scope_id` 持 `label_scopes` 锁后调用 `next_scope_id`（再取 `scope_counter`），顺序固定为 label→scope_counter（`registry.rs:1022-1042`）。

### 2.5 Registry ↔ Fiber 交界（Go 侧必须照搬的字段）

`FiberData` 关键字段（`fiber.rs:152-220`）：

| 字段 | 语义 | 行号 |
|---|---|---|
| `id: u64` | 全局 fiber id（root=0） | `:153` |
| `state: FiberState` | 状态机当前值 | `:154`，枚举 `:116-125` |
| `plugin: PluginKind` | `Sync(Arc<dyn Plugin>)` / `Async(Arc<dyn AsyncPlugin>)` | `:156`，枚举 `registry.rs:196-203` |
| `inject: &'static [&'static str]` | 依赖服务名（'static 零拷贝，索引直接存同一 slice） | `:157` |
| `config/raw_config: Value` | 生效配置 / 原始配置（每次激活重新 resolve，CORE-040） | `:158-162` |
| `runtime_key: Option<PluginKey>` | 所属 runtime（root 初始 None） | `:179` |
| `effects/deferred_async` | 同步效果（LIFO 清理）/ 无法在同步帧等待的 async 清理 | `:172-177` |
| `dep_versions` | 激活时记录的各注入服务版本；版本变化即需重激活 | `:184` |
| `children: Mutex<Vec<Weak<FiberData>>>` | 由 `ctx.plugin` 创建的子树（weak） | `:189` |
| `pending_ops: AtomicUsize` | 进行中的 fiber 级生命周期操作数（`await_settled`/quiescence 依据） | `:216` |
| `lifecycle_owner/lifecycle_depth` | operation token 与同 token 重入深度 | `:211-214` |
| `settle_waiters: Mutex<Vec<(u64,Waker)>>` | `await_settled` 等待者 | `:219` |
| `activation_epoch` | 防旧激活覆盖新状态 | `:206` |
| `status_seq` | 生命周期事件单调序号（XT-03） | `:210` |
| `dispose_requested/cancel_requested` | 协作取消/请求 finalize | `:192-194` |

- 状态迁移统一走 `transition`/`transition_if_epoch`（`fiber.rs:1123-1179`），在 `FiberData` 锁内提交 state + 取 `ctx/seq` 快照，锁外发 `internal/status`、lifecycle 广播、`registry.notify_idle()`（`fiber.rs:1183-1208`）。Disposed 是终态：迟到的 transition 被忽略（`:1129-1134`）。
- `begin_operation/end_operation`（`fiber.rs:3856-3894`）维护 `pending_ops` 并在归零时唤醒 `settle_waiters` + `registry.notify_idle()`。

### 2.6 反向依赖索引（S1–S4）：结构与维护时机

**结构**（`registry.rs:560-571`）：

```
DepIndex {
  inject_by_fiber:      HashMap<u64, &'static [&'static str]>,   // fiber → 声明的 inject（同一 slice，零拷贝）
  dependents_by_service: HashMap<&'static str, HashSet<u64>>,    // 服务名 → 注入它的 fiber 集合（跨 isolate 的超集）
  runtime_by_fiber:     HashMap<u64, PluginKey>,                 // fiber → runtime key（S2 O(1) 反注册）
}
```

设计约束（`registry.rs:454-458`）：**纯派生状态，永不作为可用性权威**；所有激活/退场决定仍由 `lookup_for_injection` + `dependencies_ready/satisfied` 做；索引只用来缩小候选集（superset filter）。同名的跨 isolate 服务会并集候选，但每个候选仍按自己的 scope 判可用性（测试 `registry.rs:2925-2958`）。

**维护时机总表**：

| 时机 | 动作 | 入口/行号 |
|---|---|---|
| fiber 入注册表（含 Pending 从未激活者） | `index_fiber_deps(id, inject)`：原子替换旧边 | `register_fiber` `registry.rs:1756-1776` |
| fiber 最终销毁 | `unindex_fiber(id)`：删 inject 边 + runtime 绑定 | `unregister_runtime_fiber` `:1887-1889`、`unindex_fiber` `:1573-1589` |
| fiber 绑 runtime / 迁移 runtime | `index_fiber_runtime(id, key)` | `register_runtime_fiber` `:1799-1800`、`rebind_single_fiber` `:1278` |
| 单 fiber 原地换实现 | 先开 `dep_index_begin_window`（使候选过滤失效）→ 改 `inject/plugin` → `index_fiber_deps` → 关窗口 | `Fiber::swap_plugin` `fiber.rs:529-560` |
| 单 fiber replace 前置 | 先开窗口 → 改声明 → reindex → 关窗口 | `Fiber::begin_replace` `fiber.rs:691-726` |
| replace 回滚 | `swap_plugin(旧)` 再次走窗口+reindex | `Fiber::restore_replace` `fiber.rs:728-757` |
| **服务出现** | `note_service_changed(name)` | `service.insert_service` `context.rs:511` |
| **服务移除** | `note_service_changed(name)` + `refresh_dependents()` | 服务 disposer `context.rs:526-539` |
| `ctx.set` 值变更/回滚 | `note_service_changed` + refresh | `context.rs:2078-2082,2161-2193` |
| fiber unload 撤服务 | 对每个 withdrawn 服务 `note_service_changed` | `fiber.rs:2389-2404` |
| fiber 变 Active / Pending / Failed | 不直接改索引，只 `ctx.refresh_dependents()` | `fiber.rs:2130,2174,2241,2306` |

**窗口 / 代数 / 失效 fail-safe**：

- `dep_index_begin_window()`（`:1520-1524`）同时 `windows += 1`、`epoch += 1`，RAII `DepIndexWindow::drop` 减窗口（`:575-585`）。
- 判定：`dep_index_ready() = !broken && windows==0 && dep_index.try_lock().is_ok()`（`:1531-1537`）。
- `mark_dep_index_broken()`（`:1541-1543`）：索引锁 poison 或 fiber 元数据读不到时永久置位，之后所有候选过滤都退化为全量扫描（C2 fail-safe）。
- **顺序不变量**（P2-1）：窗口必须在 `FiberData.inject` 变更**之前**打开，否则并发 cascade 可能读到「可信索引 + 旧反向边」，漏掉刚变成 dependent 的 fiber（`fiber.rs:534-541`、回归测试 `registry.rs:2959-3035`）。cascade 进入时快照 `dep_index_epoch`，每轮/收尾对比 epoch；窗口中途开过（哪怕已关）即整体退化为全量扫描（`fiber.rs:2860-2918`）。

**S3 cascade 候选前沿**（`fiber.rs:2840-3011`）：

1. 从 root frame 的 `withdrawn` 服务名出发，`dependents_of_services(&frontier)` 取候选 id（`:2866-2901`）；
2. 每个候选重新做 `fiber_is_unready_active`（state==Active && sync && `!dependencies_satisfied`）判定，绝不只看索引（`:2888-2895`）；
3. 候选按 fiber id 排序保证确定性（`:2883`）；把新 frame 的 withdrawn 名字并入 frontier 继续下一轮，直到不动点或 `fiber_count+2` 上界；
4. 索引不可信/中途失效 → `index_failed=true` → 用 `find_unready_active()` 全量扫描重跑（`:2958-2984`）；
5. debug 可选交叉校验：候选前沿必须覆盖全量扫描能发现的所有 unready fiber，否则 `debug_assert!(false)`（`:2920-2957`、`registry.rs:1658-1669`）。

**S4 脏服务快路径**：`note_service_changed` 建 superset 集合（`:1594-1604`）；`refresh_all`（内部路径）用 `refresh_candidates()`：`take_dirty_services()` 为 `None`（索引不可用）或空集（没有信息）→ 全量扫描；否则仅重评 dirty 服务的 indexed dependents（`:1631-1651,2110-2165`）。**用户可见的 `Context::refresh/refresh_async` 走 `refresh_all_full`，永远全量**（`:2114-2121`，`context.rs:775-793`），保证 availability-gate 翻转总被看到（0.4.4 semantics）。

### 2.7 pending_ops / registration window / idle waiter

- **fiber 级**：`pending_ops` 由 `begin_operation` 增减（`fiber.rs:3852-3894`）；`dispose/restart/update/replace/refresh-unload` 与 `ctx.plugin` 的「注册→激活」窗口都占位（FIX-11）。
- **registry 级两个计数**：
  - `pending_registrations`（FIX-11b）：从 `ctx.plugin*` 入口**开始**（`begin_registration`，`registry.rs:1940-1956`）到 activation 开始后 guard drop；覆盖 `internal/plugin` create hook 运行期（那时 fiber 级 guard 还不存在）。
  - `registration_owners: HashMap<ThreadId, usize>`（C4）：记录**哪个线程创建**了 in-flight 注册；`RegistrationGuard` 可能跨 `.await` 并被别的 worker 线程 drop，所以必须按创建线程扣减（`registry.rs:36-54,694-715`）。
- **一致性快照**：`quiescence_snapshot()` 在一次持锁中取 `(own, total)`（`:1971-1990`）；`total.saturating_sub(own)` 只能少减、不能多减，跨更新窗口是保守的。F3 修复了两次读之间的 TOCTOU。
- **`is_idle()` 严格**（`:863-873`）：`total != 0` → false；再快照 `fibers` 并逐 fiber 检查 `state ∉ {Loading,Unloading} && pending_ops == 0`，最后检查 root fiber（`is_idle_excluding_with_total`，`:888-917`）。`is_idle_excluding(own)` 仅供 `IdleWait` 使用，用来排除调用线程自己的注册（`:875-883`）。
- **`when_idle` 状态机**（`registry.rs:587-651,950-1008`）：
  - 公开类型 `IdleWait<'_>`（crate-root 导出，`lib.rs:97`），内部 boxed future = `when_idle_recovering()`。
  - `when_idle_recovering`（`:963-996`）：循环上界 `max(fiber_count+8, 16)`；每轮取快照 → 若 idle 且无 parked → 返回；若有 parked：`active_unloads` 忙则等，否则 `recover_parked_unloads_async`；否则 `wait_idle_or_recoverable().await`。耗尽后记 error 并退回 `recover+wait`。
  - `IdleOrRecoverableWait`（`:587-628`）：poll 条件 = `is_recoverable_or_idle()`（`:942-948`：真 idle 或 `!active_unload && has_parked`）；否则把 `(IDLE_SEQ 自增 id, waker)` 注册进 `idle_waiters`（先删除同 id 再 push），注册后**再检查一遍**防丢唤醒；Drop 按 id 注销。
  - `notify_idle()`（`:927-937`）：在锁内 `mem::take` 走全部 waiters，锁外逐个 `wake()`；调用点 = RegistrationGuard::drop（`:713`）、`OperationGuard` 归零（`fiber.rs:3890-3893`）、状态迁移（`fiber.rs:1204-1207`）、`park_unload`（`:2011`）、`end_unload_activity`（`:2079-2083`）。
  - `Context::when_idle()` 直接 await 同一个 `Registry::when_idle()`（`context.rs:759-761` → `fiber.rs:2628-2634`）。
- **边界**：两个线程互相等待对方注册窗口仍可死锁（内核不检测环，spec §2.6）；`is_idle` 对任何观察者都严格（窗口内恒 false），只有 waiter 排除**自己线程**的计数。

### 2.8 parked cascade 恢复（F1/F4/F5）

背景：cancelled `dispose_async` 可能把非 root cascade 帧留在 `Unloading`；这些 fiber 的 async cleanup 必须由另一个有 reactor 的时机恢复，否则永久 `Unloading` 会卡死 quiescence（A-9）。

- **park**：`Registry::park_unload(data, target)`（`registry.rs:1996-2012`）把 `(Weak<Mutex<FiberData>>, 原始目标 state)` 去重后入 `parked_unloads`；入表前 prune 死 weak；随后 `notify_idle`。target 语义：dependency dependents → `Pending`，ownership children → `Disposed`（`fiber.rs:3189`、spec §2.6）。
- **take**：`take_parked_unloads()`（`:2024-2059`）整体取走并逐条升级；可恢复条件 = `state == Unloading` **或**（`state != Disposed` 且 `deferred_async` 非空，S7：恢复中途取消会留下「目标态 + 未 drain 的 deferred」）；按 fiber id 去重。
- **单 recoverer 闩锁**：`begin_recovery/end_recovery`（`:2061-2070`）用 `recovering_parked: AtomicBool`；`RecoveryGuard::drop` 释放并 `notify_idle`（`fiber.rs:2595-2613`）。RAII 保证取消/panic 不会永久占锁（F4）。
- **活跃 unload 排斥**：`begin_unload_activity/end_unload_activity`（`:2072-2083`）包住每个 unload/cascade body（`fiber.rs:3294-3310`）；`has_active_unload()` 为真时 recovery 直接返回 0（避免在 `Drop`/取消展栈里重入 reactor 清理）。
- **恢复循环**：`recover_parked_unloads_blocking`（`fiber.rs:2697-2765`）/`_async`（`:2767-2828`）：跳过 active unload → 取单 recoverer → 循环上界 `fiber_count+2`，每轮 `take` 一批，逐帧 `unload_fiber(_async)` 到原始 target；`ReparkGuard`（`:2636-2672`）在中途取消时把当前帧重新 park（含 deferred 非空者），`RecoveryBatch`（`:2674-2695`）把未处理批次重新 park——**取消恢复不丢帧**。
- **入口**：`Registry::dispose_all` 开头（`:1409-1411`）、`refresh_all_async_impl` 开头（`:2269-2282`，若 active unload 先等再重试，8 轮）、root `Fiber::dispose`（`fiber.rs:781-788,813-824`）、`dispose_async`（`:793-811`）、`when_idle` 循环（§2.7）。
- **故意不恢复**：同步 `refresh_all_impl` 明确跳过（`registry.rs:2123-2129`）——它可能运行在 reactor 线程，同步 drain async cleanup 会死锁；`Unloading` fiber 本就不在 Active/Pending 扫描范围。

### 2.9 refresh 固定点（`registry.rs:2090-2357`）

- 重入熔断：`begin_refresh` 用 `compare_exchange(0→1)`；失败仅置 `refresh_dirty=true` 并返回，外层循环消费 dirty（`:2094-2104`）；`RefreshGuard::drop` 在**所有退出路径**（含 panic/cancel）把 `refresh_depth` 归零（FIX-1，`:670-685`）。
- 同步 `refresh_all_impl(full)`（`:2123-2205`）：最多 64 轮；每轮取候选（full → 全量快照；否则 `refresh_candidates()`）→ 对每个 fiber 快照 `(ctx, plugin, config, deps, recorded)`（锁外释放）→
  - `Sync + Pending + dependencies_ready` → `activate_fiber`；
  - `Sync + Active + !dependencies_satisfied` → `unload_contained(Pending)`；
  - async fiber 一律跳过；
  - 若有进展或 dirty 被置位则继续。
- 异步 `refresh_all_async_impl`（`:2269-2357`）：先恢复 parked（≤8 轮），其余同上，但 Pending 按 `is_async` 分派 `activate_fiber_async`，Active 不满足时 `unload_contained_async(Pending)`。
- `find_unready_active`（`:2227-2252`）：扫描第一个 `Active` 且依赖不满足的 **sync** fiber；async dependent 由 refresh 重新评估（parity note `:2224-2226`）。
- `unload_contained`/`_async`（`fiber.rs:3441-3508`）：refresh 驱动的 unload 也 `begin_operation` + claim lifecycle slot（XT-02/P2-A），busy 时本轮跳过（返回 false），后续 refresh 重试；cleanup panic 被遏制记录，fiber 仍到达目标态。

---

## 3. Registry public API 全签名与「快照语义」

### 3.1 API 清单（`impl Registry`，`src/registry.rs:786-2358`）

| 签名（简化） | 语义 | 行号 |
|---|---|---|
| `pub fn new() -> Self` | 空 registry（`Default`） | `:788-790` |
| `pub fn is_idle(&self) -> bool` | 严格 quiescence（不含自身豁免） | `:863-873` |
| `pub fn when_idle(&self) -> IdleWait<'_>` | recovery-aware 等待 quiescence；导出为 crate-root `IdleWait` | `:998-1008` |
| `pub fn size(&self) -> usize` | **runtime 数**（不是 fiber 数） | `:1044-1053` |
| `pub fn fiber_count(&self) -> usize` | 全局 live fiber 数 | `:1055-1058` |
| `pub fn get(&self, key: PluginKey) -> Option<Arc<PluginRuntime>>` | 克隆 Arc 返回 | `:1060-1066` |
| `pub fn has(&self, key: PluginKey) -> bool` | 表内有 key | `:1068-1074` |
| `pub fn delete(&self, key: PluginKey) -> Option<Arc<PluginRuntime>>` | 先删表项，再逐个 `fiber.dispose()`（错误被忽略），返回被删 runtime | `:1076-1086` |
| `pub fn replace(&self, old, new: Arc<dyn Plugin>) -> Result<Vec<Fiber>>` | 全 runtime 原地换实现 + reload | `:1088-1129` |
| `pub async fn replace_async(&self, old, new: Arc<dyn AsyncPlugin>) -> Result<Vec<Fiber>>` | 异步版本 | `:1131-1165` |
| `pub fn dispose_all(&self) -> Result<()>` | 关机路径：恢复 parked → 逐个 dispose（含 root）→ prune | `:1405-1463` |
| `pub fn keys(&self) -> Vec<PluginKey>` | 快照 | `:1465-1471` |
| `pub fn values(&self) -> Vec<Arc<PluginRuntime>>` | 快照 | `:1473-1479` |
| `pub fn entries(&self) -> Vec<(PluginKey, Arc<PluginRuntime>)>` | 快照 | `:1481-1492` |
| `pub fn for_each<F: FnMut(PluginKey, &Arc<PluginRuntime>)>(&self, cb)` | 先 `entries()` 再在**锁外**回调 | `:1494-1503` |
| `pub fn fibers(&self) -> Vec<Fiber>` | 全局 fiber 表快照 | `:1505-1512` |
| supervision only：`set_quota/quota/stalled_fibers` | 配额与协作看门狗 | `:798-861` |

`PluginRuntime` 诊断 API：`fibers()`/`fiber_count()`/`is_empty()`（`:91-107`）。

### 3.2 快照语义（CORE-022）

- 每个方法只锁**一个**结构并把结果复制出来；`keys()` 与 `size()` 之间可能发生注册/销毁，因而返回量互相矛盾是允许的。规范明确：这些是**诊断视图**，需要一致视图时先 `is_idle()/when_idle()`（spec §2.6、`docs/CORDIS-RUST.md` §4）。
- `for_each` 不会在持锁时调用回调（`:1494-1503`），因此回调里做生命周期操作是安全的，但也不保证回调看到期间的新增/删除。
- `fibers()` 的 doc comment 写着「includes disposed ones still awaiting pruning」，但当前实现在 `finish_unload` 就调用 `unregister_runtime_fiber` 立即移除（`fiber.rs:2421-2432`）；**注释与实现存在陈旧风险**，Go 侧应以「live fiber 表」为准，不要依赖 disposed 仍可见。
- `get/values/entries` 返回 `Arc<PluginRuntime>` 克隆：调用后即使 runtime 被移出表，句柄仍可读（旧快照）。

### 3.3 `dispose_all()` 完整流程（CORE-051）

1. `recover_parked_unloads_blocking(self)`——先把 cancelled cascade 遗留帧处理完（`:1411`）。
2. 取 `fibers` 快照（`:1412-1416`），对每个非 Disposed fiber 执行 `catch_unwind(|| fiber.dispose())`，收集**第一个** error 与**第一个** panic payload（`:1419-1435`）。
3. root fiber 从 `root_fiber` weak 升级后同样处理（`:1436-1443`）。
4. `prune_empty_runtimes()`：清掉 rollback 残留的空 runtime（`:1444-1446`）。
5. 结果合并：error+panic → 记 error 日志后 `resume_unwind(panic)`（**panic 优先**）；只有 error → `Err(first_error)`；只有 panic → `resume_unwind`；否则 `Ok(())`（`:1447-1462`）。
6. 成功返回时 registry 为空且 `is_idle()==true`；若某 fiber 正被其他 lifecycle 操作占用，`dispose()` 返回 `LifecycleBusy`，该 fiber 可能残留，调用方需在其操作结束后重试（XT-10，spec §2.6）。实现上错误被收集而非中止，所以「其余 fiber 仍被尝试」。

### 3.4 `replace/replace_async` 的完整原子流程（CORE-048，FIX-7/D-2/XT-05）

**阶段 A：`rebind_runtime(old, kind)`（`registry.rs:1334-1403`）**

1. `kind.key()/kind.name()` 先求值（元数据可能 panic，已被 C2 遏制包装）。
2. 锁 `runtimes`：`remove(old)`，不存在 → `Err(InvalidPlugin("replace: no plugin runtime registered ..."))`（`:1341-1345`）。
3. 若 `new_key` 已存在 runtime 则 **merge**（复用目标 runtime），否则新建并插入（`:1347-1356`）。
4. 释放 `runtimes` 锁后：对每个旧 fiber 快照 `(Fiber, 旧 PluginKind, 旧 runtime_key)` 形成 `RebindUndo`；任一 fiber 锁 poison → `undo_rebind` 并返回 `Disposed`（`:1358-1382`）。
5. 把 fibers push 进目标 runtime 的 `FiberSlots`（`:1383-1387`）。
6. 对每个 fiber `swap_plugin(kind)`（内部会开/关索引窗口并 reindex）+ 写 `runtime_key = Some(new_key)`；失败 → `undo_rebind` 并返回错误（`:1388-1398`）。
7. `heal_disposed_registrations(&fibers)`：迁移窗口内被并发 dispose 的 fiber 不能被新 runtime 持有（D-1，`:1399-1401`）。
8. 返回 `(fibers, undo)`。

**阶段 B：逐 fiber reload（`replace` `:1101-1129` / `replace_async` `:1132-1165`）**

- 每个 fiber 先查 `state()==Disposed` → `heal_disposed_registration` 并跳过（A-6）。
- `catch_unwind(|| fiber.restart())`（async 用 `catch_unwind(restart_async())`）：
  - `Ok(Ok(()))` → 下一个；
  - `Ok(Err(e))` → `rollback_replace(undo, fibers, index, e)`；
  - `Err(payload)` → `PluginFailed("<op>: reload panicked: ...")`（`reload_panic` `:747-754`）后同样回滚（D-2）。
- 全部成功后：`heal_disposed_registrations` + `prune_empty_runtimes`，返回 `Vec<Fiber>`。

**阶段 C：回滚 `rollback_replace`（`:1167-1201`，async `:1203-1228`）**

1. `undo_rebind(undo)`（`:1287-1332`）：
   - 锁 `runtimes` → 从 target runtime 中按 fiber id 删除本事务迁移过去的 fibers；若 target 变空**且是本次创建的或共享的**都 prune（D-1 residual s28：否则空 runtime 永久留在 `keys/size` 中）；
   - 若旧 runtime 仍有 fiber 则插回 `old_key`；
   - 释放锁后对 undo 中每个 fiber `swap_plugin(旧 kind)` + 恢复 `runtime_key = 旧值`；
   - `heal_disposed_registration` + `prune_empty_runtimes`。
2. 重载目标 = `rollback_targets(fibers, reloaded)`：`0..reloaded`（已换到新实现的）+ `reloaded`（失败的那个，其 reload 可能已 unload 它）（`:756-766`）。
3. 逐目标 `restart_contained`（`catch_unwind` 包裹，panic → `PluginFailed("replace rollback: reload panicked: ...")`，`:768-784`）；跳过 Disposed；**特例**：失败 fiber 若已被依赖刷新重新激活成 `Active`，不再二次 reload（避免 drain 新效果/重抛清理 panic，`:1186-1193`）；只记录第一个 rollback error。
4. `heal_disposed_registrations` + `prune_empty_runtimes`。
5. `replace_rollback_result(original, rollback, reloaded)`（`:717-745`）：
   - 无 rollback error → 原错误；
   - `reloaded == 0`（还没有 fiber 真正迁移）或 rollback error 是 `LifecycleBusy` → 记 error 日志，返回**原始错误**（结构性恢复已完成，只是某 fiber 被别的线程占用）；
   - 否则 → `CordisError::Rollback("original error: ...; rollback error: ...")`。

**语义要点（XT-05）**：`Ok` 只表示「swap/rebind/reload 机器跑完」。若 reload 的 **activation** 失败，`restart()` 收敛为 `Ok(())`、fiber 落 `Failed`（错误在 `Fiber::error_detail()`），runtime 保持绑定新实现、**旧实现不恢复**；只有 `LifecycleBusy`/校验类错误才触发阶段 C。宿主热替换必须自行检查 `Fiber::state()/error_detail()`（spec §2.6、`fiber.rs:573-589`）。

### 3.5 单 fiber `replace_plugin` 与 registry `replace` 的差异

| 维度 | `Fiber::replace_plugin(_async)`（`fiber.rs:590-666`） | `Registry::replace(_async)`（`registry.rs:1101-1165`） |
|---|---|---|
| 作用域 | 一个 fiber | 旧 runtime 的全部 fibers + runtime 表迁移/merge |
| lifecycle 排他 | 先 `begin_operation` + claim fiber lifecycle slot（`fiber.rs:591-593,629-632`），并发调用得 `LifecycleBusy` | 自身不占全局锁；逐 fiber `restart()` 各自 claim slot，busy 即触发回滚 |
| 绑定变更 | `begin_replace` 改 `inject/plugin`（窗口+reindex）→ `registry.rebind_single_fiber(self, old_key, new_key)`（`registry.rs:1231-1285`） | `rebind_runtime` 批量改、并合并到已存在的目标 runtime |
| 回滚 | `restore_replace`（`swap_plugin(旧)` + `rebind_single_fiber` 反向迁移；root 无旧 key 时 detach 临时 runtime，`fiber.rs:728-757`）+ `reload_after_failed_replace(_async)` 重新激活旧实现 | `undo_rebind` + 对已迁移 targets 用旧实现 reload（§3.4 C） |
| 失败错误 | reload 失败/panic → `Err` 并回滚绑定；`Ok` 也可能 `Failed` | 同上；部分迁移+rollback 失败 → `Rollback` |
| root fiber 特例 | 初始 `runtime_key=None`，replace 失败必须移除临时 runtime 且不残留注册（FIX-8/D-1） | root fiber 不在 `fibers` 表，`replace` 不涉及它 |
| dedupe 副作用 | 移动单个 fiber 到目标 key（可能合并进已有 runtime） | 整个 runtime 迁到新 key；`undo` 时 target 若空则 prune |

**共同点**：都保持 fiber identity/uid/raw_config；panic 在 `Result` API 边界被遏制为 `PluginFailed` 并回滚；都通过 `heal_disposed_registration` 处理「迁移期间被并发 dispose」的竞态（D-1）。

### 3.6 lifecycle 排他（CORE-045）

- `claim_lifecycle(data, token)`（`fiber.rs:3743-3758`）：`lifecycle_owner==0` → 认领并 depth=1；`owner==token` → 重入 depth+1；否则 `Err(LifecycleBusy)`。**slot 属于 operation token 而非线程**：async 生命周期在 `.await` 挂起时，来自任何线程（包括上次 poll 的线程）的同步 dispose 都得到 `LifecycleBusy`（`fiber.rs:3727-3742`）。
- `begin_lifecycle`（sync）用当前 operation token 或线程 token（`:3760-3767`）；`begin_async_lifecycle` 优先用 `CURRENT_OP_TOKEN`（嵌套 async），否则若 owner 是当前线程 token 则复用，否则分配全新 token（`:3769-3795`）；`WithOperationToken`（`:3797-3838`）在 poll 期间安装 token 并 RAII 还原。
- 未获得 slot 的调用**不排队**，直接返回 `LifecycleBusy`，调用方稍后重试（refresh/recovery 会重新评估）。
- 这是 Go 侧最容易被漏掉的语义：Go 没有「取消排队」概念，若无脑用 Mutex 串行化会把 busy 变成阻塞等待，与 CORE-045 冲突。

---

## 4. Events 事件总线

### 4.1 存储结构与 listener 记录（`src/events.rs:113-171`）

- 三个独立 `Mutex<HashMap<String, Vec<…>>>`：`listeners`、`waterfall_listeners`、`async_waterfall_listeners`；一次派发只锁对应 map 取快照，之后无锁执行。
- `next_id: AtomicU64`（`:169`）：listener/waterfall entry 共用一个递增 id（`fetch_add+1`，`:589,620,651`），Disposer 按 id 精确移除。
- `internal: Arc<InternalBus>`（`:170`）：typed `internal/*` hooks，见 §4.6。
- `Listener` 字段（`:119-128`）：

| 字段 | 语义 |
|---|---|
| `id: u64` | 全局总线内唯一，移除键 |
| `owner: u64` | 注册时 `ctx.fiber_id()`；`remove_owner` 按此清空 |
| `body: ListenerBody::{Sync(ListenerFn), Async(AsyncListenerFn)}` | 两条 lane 分开存、分开派发 |
| `scope: ContextScope` | 注册时 `inner.isolate.clone()` 快照（`HashMap<&'static str,u64>`） |
| `global: bool` | 绕过 scope 过滤 |
| `once: bool` + `called: Arc<AtomicBool>` | 一次性监听器；`swap(true)` 保证并发下只执行一次 |
- `WaterfallEntry`/`AsyncWaterfallEntry`（`:130-146`）：`id/owner/callback/scope/global`，**没有 once/called**（waterfall 不支持 once）。
- 类型别名：`ContextScope`（`:76-77`）、`ListenerFn = Arc<dyn Fn(&[Value]) -> EventResult + Send + Sync>`（`:79`）、`AsyncListenerFn = Arc<dyn Fn(&[Value]) -> BoxFuture<'static, EventResult>>`（`:80-81`）。
- `EventResult`：`Continue | Bail(Value)`，`#[non_exhaustive]`；`into_bail()` 把 `Bail(Null)`/`Bail(false)` 视为「不 bail」（`:19-38`）。
- `EventOptions { prepend, global, once }` + builder `prepend()/global()/once()`（`:40-74`）。

### 4.2 isolate 过滤规则（CORE-019）

`passes(global, listener_scope, dispatch_scope)`（`:200-209`）：

- `global == true` → 放行；
- 否则 `dispatch_scope == None`（直接调 `Events::*`）→ 放行；
- 否则 **映射相等**才放行：`dispatch_scope == listener_scope`（HashMap 整体相等 = 所有 name→scope_id 完全相同）。

要点：

- 这是「isolate map 相等」，不是「子树遍历」；`root` 与 `root.isolate("x")` 是两个不同 scope，互不收事件（测试 `tests/events.rs:81-117`）。
- scope 是注册时快照：listener 注册在某 context 上，之后用同 context 派发即可命中；**子 context 的 isolate 变化不影响已注册 listener**。
- `Context` 层派发始终传 `Some(&inner.isolate)`（`context.rs:1785-1835`）；`Events` 层直接调用可传 `None` 得到「不过滤」语义。
- waterfall 的 scope 过滤在取快照时完成（`waterfall_snapshot` `:219-233`、`async_waterfall_snapshot` `:235-249`），被过滤的 middleware 不会出现在链里；`remaining()` 只统计过滤后的链。
- 强制 `global` 的唯一公开路径：`EventOptions`（`ctx.on_with_options`、`ctx.on_with`）；`on_async*` 只有 `prepend`（没有 global/once，`context.rs:1594-1618`），waterfall 也没有 options。

### 4.3 注册路径

**sync lane**：

```
ctx.on(name, l)                  → on_with(name,false,false,l)             context.rs:1480-1506
ctx.on_with(name, prepend, global, l)                                       context.rs:1487-1506
ctx.on_with_options(name, opts, l)                                          context.rs:1510-1536
  owner  = fiber_id()
  (callback, options) = resolve_listener(...)   ← internal/listener bail hook 可替换（见 §4.6）
  slot   = reserve_listener_slot()              ← supervision 配额
  callback = wrap_once_listener(callback, once, slot)  ← once 自动移除时释放配额
  disposer = Events::add_listener(events, name, owner, options, isolate.clone(), callback)
  adopt_listener(disposer, slot, "ctx.on")      ← 把 disposer 记为 fiber 的 kernel-internal effect
ctx.once(name, l) = on_with_options(name, EventOptions::default().once(), l)  context.rs:1539-1544
```

- `Events::add_listener`（`:559-568`）→ `add_listener_body`（`:581-610`）：`id = next_id+1`；`prepend` 则 `insert(0, listener)`，否则 `push`；返回 `Disposer`，其 cleanup = `remove_listener(name, id)`。
- **owner 归属 = 注册时的 fiber id**（不是 context 对象）；listener 的 disposer 又被注册为 fiber effect，因此 fiber unload/dispose 时随 effect 列表清理；即使 effect 列表因异常路径丢失，还有 §4.7 的两道兜底。
- `on_async/on_async_with`（`context.rs:1584-1623`）：直接 `Events::add_async_listener`，**绕过 `internal/listener` hook**（XT-06），options 固定 `{prepend, global:false, once:false}`；仍是 fiber-owned effect。
- waterfall 注册：`ctx.on_waterfall`（`context.rs:1745-1761`）→ `Events::add_waterfall_listener`（`:612-641`）；`ctx.on_waterfall_async`（`:1767-1783`）→ `add_async_waterfall_listener`（`:643-673`）。两者传 `EventOptions::default()`，因此公开 API 无法 prepend/global/once waterfall（raw `pub(crate)` 入口支持）。
- `Events::internal()` 的 raw 注册（`events.rs:178-198`）不归 fiber 所有，必须手动 dispose（`tests/api_surface.rs` 有对应 case）。

**async lane**：注册路径相同但有两条硬规则（spec §2.5、XT-06、`docs/CORDIS-RUST.md` §1）：

1. `internal/listener` 只拦截 sync 注册；async 注册直接进 async lane。
2. async lane 没有 `once`/`global`；`parallel_async/serial_async` 只按存储字段派发。

### 4.4 sync 派发算法（emit / bail / parallel / serial）

四个函数结构相同（`:293-371`）：`internal.emit_dispatch(mode, name, args)`（先发 `internal/dispatch`）→ `listener_snapshot(name)`（克隆 Vec，释放锁）→ 遍历过滤 `passes` → 只处理 `ListenerBody::Sync` → once 判断 → `call_sync`。

- `emit`（`:297-313`）：忽略返回值（`Bail` 也不停）。
- `bail`（`:315-340`）：`result.into_bail()` 为 `Some` 立即返回该值（**第一个非 null/false 的 Bail**）。
- `parallel`（`:342-361`）：与 emit 相同（sync 无 await，语义等价）。
- `serial`（`:363-371`）：同步版直接复用 `bail`。
- `call_sync`（`:83-100`）：`catch_unwind(AssertUnwindSafe(callback))`；panic → `tracing::error!` 并当作 `EventResult::Continue`（CORE-031）。
- once 语义：`if listener.once && listener.called.swap(true, SeqCst) { continue; }` → 执行 → 若 once 再 `remove_listener(name, id)`（`:304-310`）。`swap` 保证**并发派发下最多执行一次**；先 `swap` 后执行意味着「另一个并发派发已经执行/正在执行」时本线程跳过（不等待）。
- 快照语义：派发期间新增的 listener 不参与本次；派发期间被移除的 listener 仍可能在本轮快照里被调用一次（按注册顺序快照执行）。因此 listener 的回调必须容忍「注册后立即被 dispose 但事件已快照」。
- 顺序 = **注册顺序**；`prepend` 只是插入到 Vec 头部，影响后续所有派发。

### 4.5 async 派发算法（parallel_async / serial_async）

`parallel_async`（`:373-414`）：

1. `internal.emit_dispatch(Parallel, ...)`；
2. 按注册顺序遍历快照：sync body **立即 inline 执行**（含 once 处理与 panic 遏制）；async body 在 `catch_unwind` 中**构造 future**（构造 panic → 记录，视为 Continue），`boxed(catch_panic(future))` push 进 `pending`，once listener 在**构造后立即移除**（无论 future 是否已 poll）；
3. 循环结束后 `exec::join_all(pending).await`：手写轮转 poll（`exec.rs:131-161`），每轮按输入顺序 poll 每个未完成 future 一次；任一 future panic 被 `catch_panic` 捕获并记录，派发继续；`Bail` 结果被**忽略**（parallel 不 bail）。
4. 关键顺序保证：**所有 sync listener 都先于任何 async listener 的 body 执行**；async listener 的 future 构造顺序 = 注册顺序，但 body 并发交错（conformance：`tests/async_conformance.rs:517-565`，断言 `["a:in","b:in","a:out","b:out"]` 来自单线程轮转 poll，而非真并行）。

`serial_async`（`:415-468`）：

1. `internal.emit_dispatch(Serial, ...)`；按注册顺序遍历快照；
2. sync body inline 执行；async body 构造 future 后**立即 `.await`**（`catch_panic` 包裹）；
3. 二者交错严格按注册顺序；`into_bail()` 命中即返回，后续 listener 不再执行（含 async；测试 `:476-515` 计数=2）；
4. 每个 once 在调用/await 完成后移除；panic → 记录并 `Continue`。

两条 lane 的互斥：sync 派发**绝不 poll** async listener（`:303,326,351` 只 match Sync）；async 派发会跑 sync listener（inline），这是规范要求的（spec §2.5）。

### 4.6 `internal/*` 介入点

所有公开派发在**取 listener 快照前**调用 `InternalBus::emit_dispatch`（`events.rs:298,321,345,376,423,478,500`）：

- `emit_dispatch`（`internal.rs:645-664`）：`name.starts_with("internal/")` 或没有 dispatch hook → 直接返回（防递归）；`dispatch_depth.fetch_add(1) > 0` 时只记数并返回——**只有最外层公开派发会触发 `internal/dispatch`**；payload `DispatchInfo { mode, name, args: Vec<Value> }`（`internal.rs:140-147`）；mode 枚举 `Emit/Parallel/Serial/Bail/Waterfall`（`:118-136`），`parallel_async` 用 `Parallel`、`serial_async` 用 `Serial`、sync/async waterfall 都用 `Waterfall`。
- `internal/listener`（`internal.rs:149-175,750-763`）：仅 sync 注册路径经过 `Context::resolve_listener`（`context.rs:1546-1573`）调用 `intercept_listener`；hook 收到 `ListenerRequest { name, prepend, global, once, owner }`，返回 `Some(ListenerOverride { callback, prepend, global, once })` 时**替换整条注册**；`find_map` 语义 = 第一个非 None 的 hook 生效；hook panic 被 `contained` 捕获当作 None。
- 其他 typed hooks（`internal/plugin/status/service/config/update/get/set/lifecycle`）**不经过 Events 的三张表**，而是 `InternalBus` 自己的 `Hooks<T>` 列表（`internal.rs:459-500`：`next AtomicU64 + Mutex<Vec<(id, Arc<T>)>>`，注册顺序、Disposer 按 id 移除）；`Context::on_internal_*` 注册时同时作为 fiber effect（fiber-owned），`Events::internal()` raw API 则不（`events.rs:178-198`）。
- `internal/plugin` 在 fiber 创建后、激活前广播（`context.rs:1352`）；`internal/status`/lifecycle 广播在状态迁移提交后（`fiber.rs:1183-1208`）；`internal/dispatch` 是**观察**而非拦截（无返回值）。

### 4.7 owner 回收与 panic 遏制

owner 回收三道防线：

1. 正常 unload：`finish_unload` → `events.remove_owner(fiber_id)`（`fiber.rs:2409-2413`）；
2. 激活失败/取消回滚：`fail_fiber_*`、`discard_activation_effects*`、`rollback_activation_to_pending*` 等路径也显式 `remove_owner`（`fiber.rs:1269,1278,2014,2062` 等）；
3. 兜底：`FiberData::drop` 时若还能解析 context，则 `catch_unwind(remove_owner)`（FIX-10，`fiber.rs:234-246`）。

`remove_owner(owner)`（`events.rs:275-291`）：分别锁三张 map，对每个 event 的 Vec 做 `retain(owner != id)`。复杂度 O(事件名×listener 数)，只在 unload 时发生。

诊断计数：`listener_count()`（sync+waterfall 两表合计，`:523-529`）、`async_listener_count()`（仅 async body，`:532-542`）、`waterfall_listener_count()`（两 waterfall 表，`:545-557`）。

panic 遏制矩阵（events 部分）：

| 边界 | 行为 | 行号 |
|---|---|---|
| sync listener panic（emit/bail/parallel/serial/parallel_async 的 sync 部分） | 记录并视为 `Continue`，派发继续 | `events.rs:83-100` |
| async listener future **构造** panic | 记录，跳过该 listener，继续 | `:397-402,444-456` |
| async listener **poll** panic | `catch_panic` 捕获，记录，继续 | `:409-413,445-451` |
| internal hook panic | `contained` 降级到 fallback（None/继续） | `internal.rs:83-105` |
| waterfall middleware panic | **不遏制**：记录后 `resume_unwind`（fail-closed，见 §5.5） | `waterfall.rs:105-115` |
| 同步 waterfall terminal panic | 注意：terminal 调用不在 `catch_unwind` 内（`waterfall.rs:117`），panic 会原样上抛（未记录）；async terminal 在 `events.rs:505` 的 `catch_panic` 内 | — |

### 4.8 语义判定表（Go 移植应逐条测试）

| 维度 | 规则 |
|---|---|
| `Bail(Null)` / `Bail(false)` | 不 bail，继续（`into_bail`，`events.rs:27-38`）；测试 `tests/events.rs:156-188` |
| `emit` 返回值 | 忽略，所有 sync listener 都跑完 |
| `bail` 遇到真 bail | 立即停止并返回该值（含 `serial`） |
| `global` | 绕过 scope 相等过滤 |
| `prepend` | 插队首，仅影响之后的派发 |
| `once` | 并发下至多执行一次；执行后自动移除；返回的 Disposer 仍幂等 |
| async listener + sync 派发 | 完全不被调用 |
| sync listener + async 派发 | inline 立即执行（parallel/serial 均如此） |
| `parallel_async` 中 Bail | 忽略 |
| `serial_async` 中 Bail | 停止 |
| 派发时 listener 集合 | 取自快照；执行期间注册/移除不影响本轮（移除者可能仍被调用一次） |

---

## 5. Waterfall 中间件（`src/waterfall.rs` 全文）

### 5.1 continuation 结构

| 类型 | 字段 | 说明 |
|---|---|---|
| `WaterfallNext`（`:56-62`） | `remaining: Arc<[WaterfallListenerFn]>`、`args: Arc<[Value]>`、`terminal: WaterfallTerminal` | 值类型 continuation，`Clone`；`remaining` 是**当前 middleware 之后**的链 |
| `AsyncWaterfallNext`（`:131-136`） | 同上，元素为 `AsyncWaterfallListenerFn` | `next()` 返回 `BoxFuture<'static, Value>` |
| `WaterfallListenerFn`（`:43`） | `Arc<dyn Fn(&[Value], &WaterfallNext) -> Value + Send + Sync>` | 中间件看到的是 `&WaterfallNext` |
| `AsyncWaterfallListenerFn`（`:50-51`） | `Arc<dyn Fn(&[Value], AsyncWaterfallNext) -> BoxFuture<'static, Value> + Send + Sync>` | async 中间件拿到**拥有所有权**的 next |
| `WaterfallTerminal` / `AsyncWaterfallTerminal`（`:45-46,53-54`） | `Arc<dyn Fn(&[Value]) -> Value>` / `-> BoxFuture` | 链尾行为 |
| 构造函数 | `WaterfallNext::new(listeners, args, terminal)`（`:65-75`） | `Vec → Arc<[T]>`；`args` 复制一份 |

辅助 API：`args()` 返回 dispatch 参数（对每个 middleware 相同）、`remaining()` 返回当前之后剩余中间件数（`:77-85,151-159`）；`terminal()` 跳过剩余链直接跑链尾（`:121-124,180-184`）。`null_waterfall_terminal` / `null_async_waterfall_terminal` 默认返回 `Value::Null`（`:220-229`），`Events::waterfall*` 在 `terminal=None` 时使用它（`events.rs:481,503`）。

### 5.2 执行算法与顺序

`WaterfallNext::next()`（`:94-119`）：

```
match remaining.split_first():
  Some((listener, rest)) =>
      next = WaterfallNext { remaining: Arc::from(rest), args: clone, terminal: clone }
      catch_unwind(listener(args, &next))  → panic: log + resume_unwind
  None => (terminal)(args)                  // 不在 catch_unwind 内
```

- 链顺序 = 注册顺序：第一个注册的是**最外层** middleware，先执行、后返回（洋葱模型）；conformance `tests/async_conformance.rs:254-297`（`["outer:in","inner:in","inner:out","outer:out"]`）。
- `next()` 可以**多次调用**：每次从 `self.remaining` 重新 split，剩余链与 terminal 重跑（`tests/async_conformance.rs:331-373`：终端被调用 2 次，返回 `[1,2]`）。这支撑 retry/rewrite 中间件。
- 每次 `next()` 都用 `Arc::from(rest)` 复制一份剩余 slice（O(k) Arc clone），链深 k 时总代价 O(k²) 引用拷贝，但无数据拷贝——对典型事件中间件可忽略。
- `args` 在构造时复制为 `Arc<[Value]>`，所有 middleware 看到同一份参数（不可通过 next 篡改链的参数，只能改写返回值）。
- async 版 `next()`（`:161-178`）：`let this = self.clone(); boxed(async move { match split_first { Some => listener(&args, child).await, None => terminal(&args).await } })`；克隆开销是 Arc 级；顺序/洋葱模型与 sync 一致（`tests/async_conformance.rs:401-445`）。

### 5.3 veto 语义

- middleware **返回但不调用 `next`** ⇒ 剩余 middleware 与 terminal 都不执行；其返回值就是整条链的结果（CORE-020）。conformance `tests/async_conformance.rs:298-329`（veto 后 later middleware 与 terminal 均未运行）。
- Waterfall lane 没有 `EventResult::Bail`：不是「bail 停链」，而是「continuation 未被调用」；二者在 JS 参考里也不同（spec §2.5）。
- `next.terminal()` 是「跳过剩余中间件，只跑链尾」的正规入口（`tests/async_conformance.rs:799-817`：later middleware 未运行，terminal 返回 `"base"`）。
- middleware 的返回值可被外层 middleware 改写/包裹（返回 `json!({"outer": value})` 的例子 `:254-297`）——这是「返回值改写」的中间件模式。

### 5.4 sync / async 两个 lane 与注册

- sync lane：`ctx.on_waterfall`（`context.rs:1745-1761`）注册 `WaterfallListenerFn`；派发 `ctx.waterfall`（`:1812-1821`）→ `Events::waterfall`（`events.rs:471-483`）：先 `internal.emit_dispatch(Waterfall)`，再 `waterfall_snapshot(name, scope)`（过滤 + 只取 callback），再 `WaterfallNext::new(...).next()`。
- async lane：`ctx.on_waterfall_async`（`:1767-1783`）注册 `AsyncWaterfallListenerFn`；派发 `ctx.waterfall_async`（`:1824-1835`）→ `Events::waterfall_async`（`events.rs:493-520`）：同样先 dispatch hook，再 async 快照 → `AsyncWaterfallNext::new(...).next()`，整条 future 用 `exec::catch_panic` 包裹。
- **两条 lane 的监听器不互相混用**：sync `on_waterfall` 的中间件不会被 `waterfall_async` 执行，反之亦然（类型就不同）。仓库里没有把二者合并的 API。
- 过滤在快照时进行（`events.rs:219-249`），`scope` 为 `None` 时全量。`global` 只在 raw `add_*_waterfall_listener` 可设置，公开 API 默认 false。
- owner 回收走 §4.7 同一套 `remove_owner`；`waterfall_listener_count` 可诊断。

### 5.5 panic 策略：fail-closed（v0.4.2，两 lane 一致）

- sync：`waterfall.rs:102-115` — `catch_unwind(listener(...))` 失败 → `tracing::error!` → `std::panic::resume_unwind(payload)`。**剩余 middleware 与 terminal 不执行**。
- async：`events.rs:493-519` — 整条 `AsyncWaterfallNext::next()` future 被 `exec::catch_panic` 包裹；`Err(payload)` → 记录 `event`/`panic` → `resume_unwind`。同样中止剩余链。
- 为什么 fail-closed：waterfall 常被宿主用作策略/鉴权中间件；把 panic 降级为 `Value::Null` 会让「策略失败」变成「允许」，所以内核选择重抛（`docs/CORDIS-RUST.md:501-573` 的 panic 矩阵最后一行）。
- 在 `apply/apply_async` 内被触发的 waterfall panic 会沿激活路径上抛，被 activation 的 panic 遏制捕获 → fiber `Failed` + rollback（CORE-029；spec §2.5 最后一条）。
- **Go 移植注意**：`recover` 后必须按原值**重新 panic**（而不是返回零值/错误），并且日志先行；不能用「返回 error」替代，因为接口签名返回的是 `Value` 而非 `Result`。
- 注意 nuance：sync 的 terminal 调用不在 catch_unwind 内（`waterfall.rs:117`），terminal panic 不会被记录但仍会上抛（效果仍是 fail-closed，只是缺日志）；async 的 terminal 在 catch_panic 内。若 Go 侧想统一日志，需显式包 terminal。

### 5.6 与 bail lane 的对比

| 维度 | bail lane（emit/bail/parallel/serial） | waterfall lane |
|---|---|---|
| 控制流 | 由 listener 返回值决定是否停链 | 由是否调用 `next` 决定是否继续 |
| 停止方式 | `Bail(非 null/false)` | 返回而不调用 `next` |
| 结果 | 首个 bail 值 / 忽略（emit） | 最外层 middleware 的返回值 / terminal 值 |
| 中间件能力 | 无（不能改写后续结果） | 完整洋葱模型，可改写返回值、可重复 `next` |
| panic | 遏制为 Continue | 记录并重抛（fail-closed） |
| 类型 | `EventResult` | 无 Bail 类型，直接 `Value` |

---

## 6. 锁纪律与并发不变量

### 6.1 锁清单与获取顺序

| 锁 | 位置 | 保护 | 纪律 |
|---|---|---|---|
| `runtimes: Mutex<HashMap<...>>` | `registry.rs:406` | runtime 表 | 最外层拓扑锁；持有它时只允许再取 `runtime.fibers`（`registry.rs:1818-1822` 注释）；**不在持有时执行 embedder 代码**；`fiber.data` 的批量读取在释放 `runtimes` 后进行（`rebind_runtime` `:1357→1361`） |
| `PluginRuntime.fibers: Mutex<FiberSlots>` | `registry.rs:78` | 单 runtime 成员 | 允许在持有 `runtimes` 时嵌套；`fiber.id()` 读取遵循 `runtime.fibers → fiber.data` 顺序 |
| `Registry.fibers: Mutex<FiberSlots>` | `registry.rs:405` | 全局 fiber 表 | 与 `runtimes` 不嵌套：`unregister_runtime_fiber` 先释放 runtimes 再取它（`registry.rs:1880-1887`） |
| `FiberData` 锁（每 fiber） | `fiber.rs:148` | 状态/效果/runtime_key/… | 叶子级；持有时不做 embedder 调用（元数据回调在锁外先求值，`fiber.rs:529-534,695-707`） |
| `dep_index: Mutex<DepIndex>` | `registry.rs:458` | 反向索引 | **叶子锁**，绝不在持 `fibers/runtimes/fiber.data` 时持有或反之（`registry.rs:454-458`） |
| `service_dirty: Mutex<HashSet<String>>` | `registry.rs:481` | 脏服务集 | 短临界区 |
| `idle_waiters: Mutex<Vec<...>>` | `registry.rs:409` | idle waker 表 | `notify_idle` 在锁内 take、锁外 wake |
| `label_scopes` / `counter` / `scope_counter` | `registry.rs:403-413` | id 分配 | 只做 map/整数操作；label→scope_counter 嵌套顺序固定 |
| `parked_unloads: Mutex<Vec<...>>` | `registry.rs:442` | parked 帧 | 短临界区；weak upgrade 后释放锁再操作 fiber |
| `registration_owners: Mutex<HashMap<ThreadId,usize>>` | `registry.rs:435` | C4 自身注册计数 | 与 `pending_registrations` 一起构成 F3 快照；更新顺序固定（begin 先 atomic 后 owners，Drop 先 owners 后 atomic），使快照保守 |
| `Events.listeners/waterfall_*/async_waterfall_*` 三把 | `events.rs:166-168` | 注册表 | 只用于取快照/增删；**绝不跨 await 持锁**（async 派发先克隆再 await） |
| `InternalBus` 的 `Hooks<T>.hooks`（10 把） | `internal.rs:459-467` | typed hook 列表 | snapshot 后锁外执行 |
| root fiber / `service_store` / `effect 列表` 等 | `context.rs:49` 等 | — | Go 侧需保持「服务 store 锁内不跑 embedder 回调」 |

- **绝不跨 await 持锁**：所有 async 函数（`refresh_all_async_impl`、`parallel_async`、`serial_async`、`unload_fiber_async`、`when_idle_recovering`）都是「锁内取快照/改状态 → 释放 → await」。`WithOperationToken` 只装 token 不持锁。
- **poison 容错**：Rust `Mutex` 会 poison；代码普遍 `.lock().ok()/unwrap_or_default()` 降级为「空/无人/继续」，只有影响索引正确性的路径才 `mark_dep_index_broken()`（`registry.rs:1541-1543`）。Go `sync.Mutex` 无 poison 概念，但等价风险是「panic 后未解锁」——Go 侧必须用 `defer Unlock()`/封装保证。
- **原子量**：`IDLE_SEQ`（`registry.rs:33`）、`SETTLED_SEQ`（`fiber.rs:3897`）、`events.next_id`（`:169`）、`internal Hooks.next`（`internal.rs:460`）、`refresh_depth/dirty`、`pending_registrations`、`dep_index_*`、`active_unloads`、`recovering_parked`、`dispatch_depth/get_depth`（`internal.rs:530-531`）。除 id 分配类原子用 `Relaxed` 外，其余多为 `SeqCst`。

### 6.2 并发不变量（Go 实现必须逐条满足）

- **I1（拓扑一致）**：fiber 在 `Registry.fibers` 中 ⇔ 其 `runtime_key` 指向的 runtime 成员包含它（允许迁移窗口内的短暂不一致，由 `heal_disposed_registration` + id-based unregister 兜底，D-1）。
- **I2（runtime 回收）**：runtime 只在「成员表为空」时被移除；空 runtime 不得长期残留（`prune_empty_runtimes`，D-1 residual s28）。
- **I3（dedupe）**：同一 `PluginKey` 在任一时刻至多一个 `PluginRuntime`；同 runtime 的 fiber 数可 >1；fiber 数归零即删除 runtime（CORE-022）。
- **I4（索引派生）**：`DepIndex` 永不做可用性权威；`dep_index_broken` 或窗口打开时全量扫描（C2 fail-safe）。
- **I5（索引失效先于声明变更）**：`FiberData.inject` 变更前必须已开 reindex 窗口，变更后同锁路径内 `index_fiber_deps`；cascade 用 epoch 检测（P2-1）。
- **I6（quiescence 定义）**：`is_idle = total_registrations==own(严格时 0) && 所有 fiber 与 root 的 state∉{Loading,Unloading} && pending_ops==0`（CORE-049）。
- **I7（lifecycle 排他）**：每个 fiber 同时至多一个 operation token 持有 lifecycle slot；其他调用 `LifecycleBusy`，不排队（CORE-045）。
- **I8（Disposed 终态）**：Disposed 后不再发生 activation/restart/rebind；迟到 transition 被忽略（CORE-040/044）。
- **I9（parked 可恢复）**：cancelled cascade 留下的帧必须能在后续 root 操作/`when_idle` 中被恢复；恢复单 recoverer、有界、幂等、取消不丢帧（F1/F4/F5）。
- **I10（once 至多一次）**：`called.swap(true)` 原子判定；自动移除与 disposer 幂等（CORE-018）。
- **I11（listener 归属）**：owner=注册 fiber id；fiber unload/dispose 后其 listener 必须全部消失（CORE-017）。
- **I12（waterfall fail-closed）**：middleware panic ⇒ 剩余链与 terminal 不执行且 panic 重抛（CORE-020 + panic 策略）。
- **I13（快照而非事务）**：诊断 API 允许多次调用间不一致；一致性只能通过 quiescence 获得（CORE-022/049）。
- **I14（不跨 await 持锁）**：所有 async 路径锁内只做快照/原子更新。
- **I15（panic 收敛矩阵）**：除 waterfall 外，embedder 代码 panic 不得逃出内核 API；各 API 的 `Result` 边界不 unwind（`docs/CORDIS-RUST.md` §3）。
- **I16（index 一致性断言）**：debug 构建 `dep_index_validate` 可校验 `inject_by_fiber`/`dependents_by_service`/`runtime_by_fiber` 与 live fibers 完全一致（`registry.rs:1691-1754`）。

### 6.3 CORE 对照

| CORE/XT | 本层落点 |
|---|---|
| CORE-017 | owner sweep（`fiber.rs:2409-2413` + Drop 兜底）；测试 `tests/lifecycle.rs`、`tests/async_conformance.rs:566-600` |
| CORE-018 | `EventOptions` + once/prepend/global 实现（§4.1/4.4）；测试 `tests/events.rs:24-78,156-188` |
| CORE-019 | `passes` 映射相等（§4.2）；测试 `tests/events.rs:81-117` |
| CORE-020 | waterfall 顺序 + veto（§5）；8 个 conformance case |
| CORE-021 | `InternalBus` 9+ hooks（§4.6）；`tests/internal.rs` |
| CORE-022 | 地图 API + dedupe/回收（§2.2/§3.1）；`tests/registry.rs` |
| CORE-031 | sync listener panic 遏制（§4.4） |
| CORE-044 | Disposed 终态 + transition 忽略（`fiber.rs:1129-1134`） |
| CORE-045 | operation-token 排他（§3.6）；`tests/concurrency.rs`、`property.rs` |
| CORE-048 | replace identity + reload（§3.4/3.5）；`tests/replace.rs` |
| CORE-049 | quiescence + F5 recovery-aware wait（§2.7/2.8） |
| CORE-051 | dispose_all + panic 优先（§3.3）；`tests/miri_leaks.rs` |
| XT-04 | `when_idle` 只豁免 `internal/plugin` create hook（§2.7） |
| XT-05 | replace 后 activation 失败不恢复旧实现（§3.4） |
| XT-06 | async 注册绕过 `internal/listener`（§4.3） |
| XT-10 | dispose_all 遇 `LifecycleBusy` 需重试（§3.3） |

---

## 7. Rust 专有机制 → Go 移植含义

| Rust 机制 | 语义作用 | Go 落点/风险 |
|---|---|---|
| `Arc<dyn Plugin>` / `Arc<PluginRuntime>` / `Weak` | 共享所有权；weak 断环（root self-link CORE-031）、`root_fiber`、`children`、`parked_unloads`、service provider | Go 有 GC：循环引用自动回收，**可去掉大部分 Weak 设计**；`root_fiber`/`children`/parked 的 weak 语义改为「id + registry 查表」或「显式注销标记」，注意 disposed 后不得复活 |
| `Mutex` + poison 容错 | 状态一致性；poison 降级 | Go `sync.Mutex` 无 poison；用 `defer Unlock` 避免 panic 漏锁；「降级为空」需显式判断（如索引失效 flag） |
| `catch_unwind` / `resume_unwind` / `AssertUnwindSafe` | panic 遏制矩阵、waterfall fail-closed | Go `recover()` + 重新 `panic(v)`；**函数签名返回 `Value` 的 waterfall 必须 re-panic**；`runtime.Goexit` 不可 recover，`defer` 中的 recover 同样能捕获 map 写 nil 等 runtime panic |
| `Future/Waker/poll` + `IdleWait` 自定义 Future | 无 runtime 依赖的 quiescence 等待 | Go：`chan struct{}` 广播（close 一次）+ 订阅者表；或 `sync.Cond.Broadcast`；`Drop` 注销 waker → 显式 `defer unsubscribe`（Go 无 Drop） |
| `AtomicU64/Usize/Bool`（SeqCst） | quiescence/epoch/latch | `sync/atomic`（Go 1.19+ 泛型）；id 分配可用 `atomic.Uint64.Add` |
| `ThreadId` 作为 C4 owner | `when_idle` 排除调用线程自己的注册 | Go **拿不到可靠 goroutine id**：改为显式 token（`context.Value`/`*Registration` 句柄传给 `when_idle`），或在注册窗口内禁止 wait；`is_idle` 保持严格 |
| `OnceLock`（`exec::BLOCKING_EXECUTOR`） | 同步清理的 embedder 执行器 | `sync.Once` / `atomic.Value`；Go 侧若全同步 API 可不需要 |
| `'static str` + `TypeId` → PluginKey | dedupe 身份 | Go：`Key` 接口（`Named(string)` / `Type(reflect.Type)`）；`Named` 需自行 intern（`map[string]`）保证稳定；`reflect.Type` 可直接作 map key |
| `&'static [&'static str]` inject | 零拷贝声明 | Go `[]string` 不可比较：scope map 相等需逐项比较（或预计算 hash）；inject 需拷贝防外部修改 |
| `serde_json::Value` | 事件参数/插件配置 | Go `any` 或自定义 Value；**Go map 无序**，config merge/JSON 序列化需确定性策略；事件 args 是数组，顺序保留 |
| `Exec::join_all`（单线程轮转 poll，确定性交错） | `parallel_async` 的并发语义与可复现测试 | Go goroutine 真并行，**无法复现 `["a:in","b:in","a:out","b:out"]` 式确定性交错**：可能需自建「每轮 poll 一次」的调度器（用 channel/状态机）或把测试改为「并发但不保证交错」；这是 conformance 风险最高的点 |
| `block_on` / `BlockingExecutor` / sync 处置边界 | 同步 API 上等待 async 清理 | Go 无 async 模型：建议 Go 版只保留同步 `Dispose()`（内部直接执行 cleanup），或 async API 返回 `<-chan error`；**不要**用 goroutine park 模拟 block_on（会与 reactor 死锁同类问题） |
| `Drop`（`FiberData::drop`、`Disposer::drop`、RAII guards） | 兜底清理 / 取消安全 | Go 无 Drop；`runtime.SetFinalizer` 不可靠。必须**显式 Close/Dispose**，并把取消安全（ReparkGuard/RecoveryBatch 的 re-park）写成 `defer` + 显式 cancel 路径 |
| `ReparkGuard`/`RecoveryBatch`/`CancelSafeUnload` | 取消/panic 时不丢帧、不把 fiber 卡在 Unloading | Go：`defer` 中判断「未完成则重新注册 parked 帧」；取消通过 `context.Context`，需保证 defer 在 cancel 路径执行 |
| `tracing::error!` 携带 event/plugin 字段 | 可观测性 | `log/slog` 结构化字段；保留「contained」「fail-closed」等关键字便于运维检索 |
| `#[cfg(feature="supervision")]` | 编译期可选配额 | Go build tag（`//go:build supervision`）或运行期可选开关 |
| `#![forbid(unsafe_code)]` + `panic="unwind"` 编译期契约 | panic 遏制可行 | Go 天然 recover；但**goroutine 内 panic 不捕获会 terminate 进程**，所有内核派发入口都需 defer recover |
| `Arc<[T]>` 快照克隆 | 无锁派发 | Go：`append([]T(nil), list...)` 复制；注意拷贝的是接口/指针（共享底层对象），listener 对象本身需不可变或加锁 |

---

## 8. 建议的 Go 类型骨架（描述，不写代码）

- `Registry`：`mu sync.RWMutex` + `runtimes map[PluginKey]*PluginRuntime`；`fibersMu sync.Mutex` + `slots []*Fiber` + `pos map[uint64]int` + `live int`（复刻 FiberSlots tombstone 语义，不要用 `[]*Fiber` 线性删除）；`counter atomic.Uint64`；`scopeCounter atomic.Uint64`；`labelScopes sync.Map`；`idleWaiters` 订阅表；`parked` 帧列表；`depIndex`（map + leaf mutex）；`activationRegistry`（pending 注册 + 每 owner token 计数）。
- `PluginKey`：接口 `{ kind() ; named() (string,bool); typ() (reflect.Type,bool) }` 或 tagged struct；实现 `==`/hash 时需要可比较（string + reflect.Type 均可用作 map key）。
- `PluginRuntime`：`Name string; Key PluginKey; mu sync.Mutex; slots *fiberSlots`。
- `Fiber`：`data *FiberData`（或直接指针）；`FiberData` 含 `mu sync.Mutex`、`state FiberState`（原子读可选）、`pendingOps atomic.Int64`、`lifecycleOwner atomic.Uint64`、`lifecycleDepth atomic.Int64`、`plugin Plugin` + `isAsync bool`、`inject []string`、`rawConfig Value`、`runtimeKey *PluginKey`、`effects []Effect`、`deferredAsync []AsyncCleanup`、`children []uint64`（或 `[]*FiberData` + 显式注销）、`settleWaiters`。
- `Events`：三张 `map[string][]*Listener` 各自 `sync.RWMutex`；`nextID atomic.Uint64`；`internal *InternalBus`。
- `Listener`：`ID uint64; Owner uint64; Sync func([]Value) EventResult; Async func([]Value) Future; Scope map[string]uint64; Global, Once bool; Called *atomic.Bool`（Go 用 `atomic.Bool`，或对 Once 用 `CompareAndSwap`）。
- `WaterfallNext`：`remaining []WaterfallListener; args []Value; terminal WaterfallTerminal`——Go 值语义要小心：Rust 的 `Arc<[T]>` 相当于共享只读 slice；Go 可直接 `chain[1:]`（slice 共享底层数组，且不复制），但要防外部 append 改写；`terminal()`/`next()`/`remaining()`/`args()` 方法对齐。
- 并发派发：`parallel_async` 若要保确定性可自建调度器；若接受真并发则 `sync.WaitGroup` + 收集 error/bail，但**必须保证 sync listener 先 inline 跑完**（顺序语义），且所有 listener panic 被 recover。
- 必须显式建模而不是交给 GC 的：`Disposed` 终态、once 并发、parked 恢复、LifecycleBusy、waterfall fail-closed。

---

## 9. 证据索引（复核用）

| 主题 | 主要位置 | 规范/测试 |
|---|---|---|
| Registry 字段与 API | `src/registry.rs:401-482,786-2358` | spec §2.6、`tests/registry.rs` |
| PluginKey / dedupe / C2 | `src/registry.rs:56-398,1780-1805` | spec §2.6、`tests/registry.rs:44-99`、`tests/redteam.rs`（panicking key） |
| FiberSlots（S2） | `src/registry.rs:484-558` | `docs/CORDIS-RUST.md` §4（性能） |
| 反向依赖索引 S1–S4 | `src/registry.rs:454-585,1518-1754`；`src/fiber.rs:2840-3063` | `src/registry.rs:2360-3035` 内联测试（pending dependents / replace reindex / randomized topology / P2 window） |
| quiescence / idle waiter | `src/registry.rs:33,407-435,587-651,863-1008,1940-1990` | spec §2.6、`tests/quiescence.rs`、`tests/regression_core.rs`（c4/f5） |
| parked cascade 恢复 | `src/registry.rs:436-466,1992-2088`；`src/fiber.rs:2595-2828,3140-3432` | spec §2.6、`tests/regression_core.rs`（f1/f4）、`tests/regression_semantics.rs` |
| replace / rollback | `src/registry.rs:662-784,1088-1403`；`src/fiber.rs:525-757` | spec §2.6（XT-05）、`tests/replace.rs`、`tests/regression_core.rs`（D-1/D-2） |
| dispose_all | `src/registry.rs:1405-1463` | CORE-051、`tests/miri_leaks.rs` |
| lifecycle 排他 | `src/fiber.rs:3680-3838,3840-3894` | CORE-045、`tests/concurrency.rs` |
| Events 存储/注册 | `src/events.rs:113-171,559-673`；`src/context.rs:1479-1623` | CORE-017/018、`tests/events.rs` |
| scope 过滤 | `src/events.rs:200-249`；`src/context.rs:241-256,296-306` | CORE-019、`tests/events.rs:81-117` |
| sync 派发 | `src/events.rs:83-100,251-371` | CORE-018/031、`tests/events.rs` |
| async 派发 | `src/events.rs:373-468`；`src/exec.rs:131-187` | spec §2.5、`tests/async_conformance.rs:476-600` |
| internal 介入 | `src/internal.rs:83-105,459-500,645-663,750-763`；`src/events.rs` 各派发首行 | CORE-021、`tests/internal.rs` |
| waterfall | `src/waterfall.rs:1-229`；`src/events.rs:470-520`；`src/context.rs:1745-1835` | CORE-020、`tests/async_conformance.rs:254-500,795-817` |
| panic 策略 | `docs/CORDIS-RUST.md:501-573`；`src/events.rs:83-111,493-520`；`src/waterfall.rs:102-115` | CORE-029/030/031、panic 矩阵 |

---

## 10. 风险 / 待确认问题（交给 Lead 汇总）

1. **Go 并发语义差异**：`parallel_async` 的「先 inline 全部 sync、再并发 poll async」与 `join_all` 的确定性轮转，在 Go 的 goroutine 模型下无法 1:1 复现。需要 Lead 决策：a) 引入自建确定性调度器（保 conformance），或 b) 接受真并发并放宽测试断言（需在 Go 侧文档声明差异）。这是本层最大移植风险。
2. **`when_idle` 的 C4 线程豁免**：Go 无 goroutine id。可选方案：显式 `RegistrationScope` token（调用方把 token 传入 `when_idle`）、或 Go 版 `when_idle` 不接受豁免而要求宿主不要在注册窗口内 wait。需与 spec 的 XT-04 对齐。
3. **`fibers()` doc comment 与实现不一致**（注释说包含待 prune 的 disposed fiber，实际 `finish_unload` 立即移除，`fiber.rs:2421-2432`）：Go 侧以「live fiber 表」为准，建议同步修正 spec 表述。
4. **sync waterfall terminal panic 未记录**：`waterfall.rs:117` 的 terminal 调用在 `catch_unwind` 之外（async 在内）。行为仍 fail-closed，但日志缺失；Go 侧建议统一包裹。
5. **`delete` 忽略 dispose 错误**（`registry.rs:1078-1086`）：与 `dispose_all` 的错误收集策略不同。若 Go 侧要求可观测性，建议加返回值或日志，并在 spec 标注差异风险。
6. **`Registry::replace` 无 registry 级事务锁**：并发 replace/delete/delete_all 依赖逐 fiber lifecycle slot + `heal_disposed_registration` 收敛；Go 侧若引入全局 registry 锁串行化，会改变 `LifecycleBusy` 的可观测行为，需要 conformance 复核。
7. **`label_scopes` 只增不减**：Rust 侧受 `&'static str` 约束天然有界；Go 若允许动态 label，需要明确 lifecycle/清理策略。
8. **Go `map` 遍历无序**：`keys/values/entries/for_each` 在 Rust 是 HashMap 无序快照，Go 同样无序——测试不要依赖顺序；但 `fibers`/cascade 依赖 **fiber id 排序**（`registry.rs:2154,2307`、`fiber.rs:2883`），Go 侧必须显式 sort。
9. **`EventResult` 语义**：Go 若用 `(value any, bailed bool)` 表达，务必保留「`nil`/`false` 视为不 bail」的判定（等价 JS `isBailed`）。
10. **Owner 回收的兜底**：Rust 有 `FiberData::drop` 最后兜底；Go 无 Finalizer 保证。Go 侧必须确保所有 dispose 路径（含 panic 回滚、取消）都调用 `events.removeOwner`，并加静态检查/测试覆盖。
