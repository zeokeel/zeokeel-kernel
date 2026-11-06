# 02 · Fiber 生命周期、Effect 所有权与 Internal Hooks

> 上游基线：GitHub [`zeokeel/zeokeel-cordis`](https://github.com/zeokeel/zeokeel-cordis) v0.6.0。
> 本文 `file:line` 引用均相对上游仓库根目录（如 `src/fiber.rs:152`）。目标：为 Go 重实现提取 fiber 生命周期 +
> effect 所有权 + internal hooks 的可移植语义；不写 Go 代码，§9.4/§9.5 给出移植含义与必须重新定义的语义。
> 证据：`src/fiber.rs`(4347)、`src/effect.rs`(80)、`src/internal.rs`(787)、`src/async_effects.rs`(733)、
> `src/registry.rs`(3036)、`src/context.rs`(2359)、`src/exec.rs`(234)、`spec/CORDIS-CORE.md`、
> `docs/DESIGN.md`、`docs/CORDIS-RUST.md`。

## 0. 结论速览

1. **所有权**：每条 fiber 一个 `FiberRef = Arc<Mutex<FiberData>>`；fiber 拥有**一条有序 effect 列表**，
   sync `Disposer` 与 async `AsyncDisposer` 混排在同一个 `Vec<FiberEffect>`（`src/fiber.rs:92-105`），
   卸载严格**逆注册序** drain，双 lane 共用一个顺序。
2. **状态机**：`Pending/Loading/Active/Failed/Unloading/Disposed`（`src/fiber.rs:116-125`）；唯一写入口
   `transition`（`:1123-1143`）与 `transition_if_epoch`（`:1153-1179`）。每次真实迁移在**状态锁内**捕获
   `(old, new, seq)`，锁外依次发 raw `internal/status`（非快照）和 lifecycle broadcast（快照 from/to/seq）。
   `Disposed` 终态不可复活。
3. **生命周期互斥是 operation-level token**，不是线程身份（`:3649-3795`）：同 token 重入允许
   （cleanup 内 dispose、update→restart），其他 operation 一律 `LifecycleBusy`，**不排队**。
   异步 token 由 `WithOperationToken` 每次 poll 安装/还原，跨 executor 迁移正确。
4. **激活 epoch**：每次尝试 `activation_epoch+=1`，经 thread-local `ACTIVATION_FRAMES`（async 用
   per-poll `EpochFrameFuture`）给 effect 打标，使 stale 尝试只回收自己的 effect；XT-02 回滚用原子
   `(state,epoch)` check-and-store 落 `Pending`（`:1805-1963`）。
5. **卸载 = 两阶段迭代级联**：先 provider-before-dependent 地 begin（置 Unloading、撤服务、取走 effect），
   再**逆 begin 序** drain（dependent-before-provider、child-before-parent），调用栈 O(1)（`:2502-3011`）。
6. **静默**：`pending_ops` + `Loading/Unloading` + `pending_registrations`；`when_idle` 排除调用线程自己的
   注册帧并按 recovery-aware 循环回收被取消的级联帧；sync `refresh` 故意不 recovery（可能在 reactor
   线程上同步 drain async cleanup → 死锁）。
7. **取消**是协作式、per-fiber（`cancel_requested` + `ctx.checkpoint()`）；Rust 真正的「中止」原语是
   **drop future**（超时/select），由 `CancelSafeUnload`/`park_cancelled_frame`/`AsyncActivationGuard` 收尾。
8. **internal.rs = 9 个 typed `internal/*` hook + 1 个独立 lifecycle bus**；全部 `contained()`
   （catch_unwind + log + fallback）per-hook 遏制，不存在 internal hook panic 让链失败的路径（唯一
   fail-closed 例外是 events 的 waterfall listener，不属于 internal bus）。fiber-owned 与 raw
   `InternalBus` 两种所有权并存。
9. **Go 最大风险**：Rust 的 4 处 `thread_local`（activation frame、lifecycle thread token、
   CURRENT_OP_TOKEN、disposer drain frame）以及 `Arc::strong_count==1`、确定性 `Drop`、
   `future drop = cancel` 三个原语在 Go 无对应物 → 必须重新设计 API（token/frame 入
   `context.Context` 或显式 operation 句柄；`Abandon`/`DisposeAll` 取代 Drop 安全网；
   `ctx.Done()` + 恢复未启动 cleanup 取代 future-drop）。详见 §9.4/§9.5。

## 1. 数据结构、句柄图与状态机

### 1.1 句柄与引用图

| 对象 | 定义/位置 | 持有关系 |
|---|---|---|
| `Fiber`（Clone 句柄） | `{ data: FiberRef }` `src/fiber.rs:332-336` | 用户/registry/ContextInner 各持 clone |
| `FiberRef` | `Arc<Mutex<FiberData>>` `:148` | 唯一 fiber 状态锁（粗粒度） |
| `ContextInner.fiber` | 强 `Fiber` `src/context.rs:122-124,164-176` | context → fiber |
| `FiberData.ctx`/`.parent` | `Option<FiberLink>` `:169-171` | 子 fiber：`Strong(Context)`；根 fiber：`Weak` 自引用（避免 Context↔FiberData 强环，CORE-031，`src/context.rs:178-184`）；Disposed 时置 None（`:2434-2437`） |
| `FiberData.children` | `Mutex<Vec<Weak<FiberData>>>` `:189` | 父 → 子**弱**；级联 DFS/child sweep 用 |
| child ownership effect | 捕获 `Weak<FiberData>` `:1470-1543` | 父 effect 列表 → 子弱；父卸载逆序 drain 时 dispose 子，无父子 Arc 环 |
| `Registry.fibers`（FiberSlots） | 强 `Fiber` 槽 `src/registry.rs:484-560` | registry → fiber 强；Disposed 时 unregister 移除，**已注册 fiber 不因用户丢句柄而释放** |
| `Registry.root_fiber` | `Weak<FiberData>` `src/registry.rs:414-416` | 根 fiber 不在 `fibers` 里但参与 quiescence |
| `parked_unloads` | `Vec<(Weak<FiberData>, FiberState)>` `src/registry.rs:436-442` | 取消级联停摆帧，弱引用+去重剪枝 |

### 1.2 `FiberData` 全字段（`src/fiber.rs:152-220`）

「保护」：**M**=由 `FiberData` 自身 Mutex 保护（`state` 是普通字段，非原子）；**A**=原子；**子锁**=字段内部另有 Mutex。

| 字段（行） | 类型 | 保护 | 作用 |
|---|---|---|---|
| `id`(153) | `u64` | M | 身份；根=0 |
| `state`(154) | `FiberState` | M | 唯一权威状态，迁移只在锁内 |
| `plugin`(156) | `PluginKind` | M | 实现；`name/inject/key/config_schema` 在锁外调用（embedder 代码） |
| `inject`(157) | `&'static [&str]` | M | 依赖名门控 |
| `config`(159) | `Value` | M | 本次激活已解析/校验的配置 |
| `raw_config`(162) | `Value` | M | 调用方原始配置，每次激活重 resolve（CORE-040） |
| `error`(167) | `Option<CordisError>` | M | 最近激活失败的结构化错误；激活开始清空（`:2102`/`:2212`） |
| `parent`(169)/`ctx`(171) | `Option<FiberLink>` | M | 父 Context / 自己的 Context；Disposed 断边 |
| `effects`(172) | `Vec<FiberEffect>` | M | 唯一有序 effect 列表（sync+async 混排） |
| `deferred_async`(177) | `Vec<AsyncDisposer>` | M | 无法 inline await 的 async cleanup 尾（newest-first，从头 drain） |
| `runtime_key`(179) | `Option<PluginKey>` | M | 所属 runtime；replace 迁移/回滚 |
| `effect_stack`(182) | `Vec<Arc<Mutex<EffectNode>>>` | M | 正在执行 body 的 effect 节点栈（嵌套诊断） |
| `dep_versions`(184) | `Mutex<Vec<(&'static str,u64)>>` | 子锁 | 激活时依赖版本；refresh 判定需重算 |
| `registry`(187) | `Option<Arc<Registry>>` | M | 卸载后无 ctx 仍能 notify_idle |
| `children`(189) | `Mutex<Vec<Weak<...>>>` | 子锁 | 直接子 fiber 弱列表 |
| `dispose_requested`(192) | `AtomicBool` | A | 父卸载遇 busy child 打标，由 child 当前 operation 收尾 finalize |
| `cancel_requested`(194) | `AtomicBool` | A | `cancel()` 置位，`checkpoint()` 观察（I11） |
| `listener_count`(197) | `usize` | M | supervision 监听器配额（非 supervision 时占位） |
| `clock`(200) | `Arc<dyn Clock>` | M | 注入时钟 |
| `last_activity_ms`(203) | `AtomicU64` | A | watchdog；begin/end_operation 刷新 |
| `activation_epoch`(206) | `AtomicU64` | A | 激活尝试代数（stale/XT-02 核心） |
| `status_seq`(210) | `AtomicU64` | A | 迁移序号；lifecycle broadcast 的 seq |
| `lifecycle_owner`(212) | `AtomicU64` | A | 持有 lifecycle slot 的 operation token，0=idle |
| `lifecycle_depth`(214) | `AtomicUsize` | A | 同 token 重入深度 |
| `pending_ops`(216) | `AtomicUsize` | A | 在飞 lifecycle operation 计数 |
| `settle_waiters`(219) | `Mutex<Vec<(u64,Waker)>>` | 子锁 | await_settled waker，带 future id 便于注销 |

### 1.3 锁分组与锁纪律

- **L1 `FiberData` 粗锁**保护全部非原子字段；持锁时**不调用 embedder 代码**（name/inject/schema/hook/
  cleanup 全部锁外：`:343-350`、`:529-559`、`:694-726`、`:1183-1208`）。
- L2 子锁：`children`、`dep_versions`、`settle_waiters`、`EffectNode`、`Registry.quota`；无「L2→L1」反向。
- `Registry` 的各结构各自独立锁；`dep_index` 是叶子锁（`src/registry.rs:454-458`）。
- 锁中毒：所有 `.lock()` 失败一律降级（`map_err(|_| Disposed)`、`.ok()`、`unwrap_or_default`），内核永不
  因毒锁 panic。消费者/事件回调在 `snapshot()`（克隆 Arc、释放锁）之后执行。
- `mem::take` 出 effect/children 列表后再在锁外 drain/回调（`:1985-1989`、`:2362-2382`）。

### 1.4 `FiberState` 六态与迁移图

```text
(create_fiber_handle → Pending，无迁移事件)        根 fiber 直接构造为 Active (src/context.rs:134)
Pending ──activate──► Loading ──apply ok──► Active ──unload begin──► Unloading ──► Pending / Disposed
                       │  │                                          ▲
                       │  └─ apply Err/panic / 校验失败 / cancel ─► Failed ─restart─┘
                       └─ XT-02 回滚 (epoch 精确) ─► Pending
```

- 六态 `#[non_exhaustive]` `Copy`（`:116-125`）。`Failed` 是**稳定态**：refresh 只对 `Pending` 激活、对
  `Active` 卸载（`src/registry.rs:2183-2198`），Failed 只能经 restart/update/replace 复活。
- **Disposed 终态**：`transition` 发现当前 Disposed 直接返回（`:1129-1134`）；`transition_if_epoch` 同样拒绝
  （`:1161-1166`）；`begin_restart`/`resolve_update`/`begin_replace` 显式 `Err(Disposed)`
  （`:891-894`、`:1010-1013`、`:709-711`）。
- `begin_restart` 对 Active/Failed/Pending 一律先 `transition(Unloading)`（`:906`）。
- 例外：Disposed 仍有 deferred async cleanup 时，重试 dispose 走 `take_effects_begin` 的 Disposed 分支
  （`:2362-2376`）：不迁移、不 withdraw，只排干 tail（A-9 retry）。

### 1.5 迁移写入口与事件发布

`transition(data,next)`（`:1123-1143`）：锁 → 同态 no-op → 当前 Disposed no-op → `old=state`，
`seq=status_seq.fetch_add(1)`（返回旧值，**首个事件 seq=0**）→ `state=next`，快照 `ctx_ref` → 释放锁 →
`publish_transition`。

`transition_if_epoch(data,expected,next,epoch)`（`:1153-1179`）：同一把锁内检查 `state==expected &&
state!=Disposed && activation_epoch==epoch && state!=next`，通过才提交；返回 bool。专用于 XT-02 回滚。

`publish_transition`（`:1183-1208`，锁外）顺序：① `emit_status(&fiber, old)`（raw hook，`to` 由 hook
自读当前 fiber，可能 torn）；② `emit_lifecycle(&fiber, old, next, seq)`（快照 from/to/seq，XT-03/08）；
③ 重新锁 data 取 registry 快照 → `registry.notify_idle()`（Disposed 后 ctx 已清空，仍需唤醒 quiescence）。

### 1.6 状态迁移触发点全表

| 迁移 | 触发 | 位置 |
|---|---|---|
| 创建→`Pending` | `create_fiber_handle` 构造 | `src/context.rs:1285-1313`（无迁移事件） |
| 根构造→`Active` | `Context::root` | `src/context.rs:132-162` |
| `Pending→Loading` | `activate_fiber`/`_async` | `src/fiber.rs:2114`/`:2219` |
| `Loading→Active` | 激活成功 + dep 复检通过 | `:2173`/`:2305` |
| `Loading→Failed` | 校验失败/apply Err/apply panic/async cancel guard | `:2129`、`:2022`、`:2068`、`:2240`、`:1351`、`:1367` |
| `Loading→Pending` | XT-02 依赖消失回滚 | `transition_if_epoch` `:1946`/`:1960` |
| `*→Unloading` | `take_effects_begin`（级联每帧）/`begin_restart` | `:2383`/`:906`（FIX-6：restart 也发事件） |
| `Unloading→Pending` | `finish_unload(target=Pending)` | `:2408-2415` |
| `Unloading→Disposed` | `finish_unload(target=Disposed)` | 同上 |
| 任何迁移出 `Disposed` | 被 `transition`/`transition_if_epoch` 拒绝 | `:1129-1134`、`:1161-1166` |

全部触发点集中在 fiber.rs，无绕过 helper 的裸 `state` 赋值（`grep 'transition('` 可核对）。

### 1.7 其他原子/字段语义

- `activation_epoch`：`begin_activation_epoch` 返回 `fetch_add(1)+1`（`:1800-1803`）。
- `status_seq` 只在迁移锁内 `fetch_add`；lifecycle broadcast 的 `seq`。
- `pending_ops` 由 `begin_operation`/`end_operation` 维护（§2.4）；`error` 由 `set_error` 写（`:1230-1234`），
  激活开始清空；`await_settled` settled 后重抛。
- `config`/`raw_config`：每次激活重新 resolve+validate，restart/update 不叠加派生值（CORE-040）。
- `last_activity_ms` + 注入 `Clock`：watchdog（supervision）的 `idle_for`/`stalled_fibers` 依据
  （`:433-479`、`src/registry.rs:823-861`）。

## 2. Lifecycle 互斥（operation-level token）

### 2.1 表示与生成

| 符号 | 行号 | 语义 |
|---|---|---|
| `NEXT_LIFECYCLE_TOKEN` | 3656 | 全局 `AtomicU64`（从 1 起），`fetch_add` 发新 token |
| `THREAD_TOKEN` | 3658-3661 | thread-local，每线程惰性领一个唯一 token（同步入口默认） |
| `CURRENT_OP_TOKEN` | 3663 | thread-local `Cell<u64>`：当前被 poll 的 async operation token，0=none |
| `current_lifecycle_token()` | 3669-3676 | op token 非 0 用它，否则用 thread token |

`claim_lifecycle(data,token)`（`:3743-3758`）在 FiberData 锁内：owner==0 → 写 token、depth=1；
owner==token → depth+=1（**重入**）；否则 `Err(LifecycleBusy)`（不阻塞不排队）。
`LifecycleGuard::drop`（`:3694-3725`）：仅 owner 可释放；depth>1 递减，否则清零；owner 不匹配时 log +
debug_assert 且**不**误清别人的槽。

`begin_async_lifecycle`（`:3779-3795`）：嵌套在 async op 内（CURRENT_OP_TOKEN≠0）→ 复用；
外层同步 op（owner==本线程 THREAD_TOKEN）→ 复用 thread token；否则领**全新 op token**（于是同线程在
挂起期间发起的同步调用得到 Busy 而非被误判重入）。`WithOperationToken`（`:3803-3838`）每次 poll
`Cell::replace` 安装、RAII `Restore` 还原（panic 安全、嵌套正确；回归测试 `:4235-4263`）。

### 2.2 重入判定矩阵

| 场景 | 同步入口 | 异步入口 |
|---|---|---|
| 外部/其他 operation，fiber 空闲 | 领取（thread token） | 领取（新 op token） |
| 同线程同同步 op 内嵌套（cleanup 里 dispose、update→restart） | 允许 | owner==thread token 时复用 |
| async op 被 poll 期间 cleanup 内重入 | 允许（CURRENT_OP_TOKEN 已安装） | 允许（复用 op token） |
| async op **挂起**期间同线程同步调用 | `LifecycleBusy` | 新 op token → `LifecycleBusy` |
| async op 挂起期间其他线程调用 | `LifecycleBusy` | `LifecycleBusy` |
| 父卸载 sweep 子 fiber（子 busy） | Busy → 打 `dispose_requested`，延迟回收 | 同左 |

### 2.3 `LifecycleBusy` 返回点

`dispose`(`:786`)、`dispose_async`(`:804`)、`restart`(`:846`)、`restart_async`(`:861`)、
`update_with_no_save`(`:964`)、`update_async_with_no_save`(`:980`)、`replace_plugin`(`:592`)、
`replace_plugin_async`(`:632`)；refresh 卸载 `unload_contained(_async)` 遇 Busy 返回 false 本轮跳过
（`:3441-3453`/`:3474-3484`）；sweep/ownership cleanup 的 Busy 转为 `dispose_requested`（`:1443-1449`、
`:1500-1508`）；`dispose_all` 记 first_error 后继续、最终 `Err(LifecycleBusy)`（`src/registry.rs:1409-1463`，
XT-10）。被跳过操作**不排队**，调用方重试（`src/error.rs:33-51`）。

### 2.4 `pending_ops` / `OperationGuard` / `await_settled` / `settled`

- `begin_operation`（`:3856-3863`）：`pending_ops+=1` + 刷新 last_activity；`end_operation`（`:3865-3894`）：
  递减，归零时取走并唤醒全部 settle waiters，并 `registry.notify_idle()`。
- 计入入口：dispose/dispose_async/restart(_async)/update*/replace*/unload_contained*，以及
  **`activate_fiber`/`_async` 本身**；`ctx.plugin*` 还在 create→activate 窗口额外 begin_operation
  （`src/context.rs:1391`/`:1415`/`:1439`）。
- `Settled`（`:3900-3947`）：poll 读 pending_ops；>0 时注册 waker（id 来自 `SETTLED_SEQ` `:3897`）并
  **注册后二次检查**防丢唤醒；ready 时返回 `fiber.error.clone()`（有错 Err，否则 Ok）；Drop 注销 waker。
  条件 = `pending_ops==0`，不要求 Active（Failed 也能 settle 并返回错误），不等注册窗口。
- 边界：`internal/plugin` create hook 内 `await_settled()` 立即返回（hook 在 `src/context.rs:1352` 先发、
  `:1391` 才 begin_operation；spec 367-371）。
- v0.6.0 起 refresh 触发的卸载也 begin_operation（P3），`await_settled` 不会在 refresh unload 中提前返回。

### 2.5 refresh 驱动 unload 持有 lifecycle slot

`refresh_all_impl` 对 Active 且依赖不满足的 Sync fiber 调 `unload_contained(data,Pending)`
（`src/registry.rs:2190-2194`）；async 版用 `unload_contained_async`（`:2345-2348`）。
`unload_contained`（`src/fiber.rs:3441-3471`）：begin_operation + begin_lifecycle；Busy→false 跳过；
`catch_unwind(unload_fiber)` 错误/panic 只 log（dependent cleanup panic 不能把 provider 卡在 Unloading，
`docs/CORDIS-RUST.md:191-195`）。`unload_contained_async`（`:3474-3508`）用 begin_async_lifecycle +
with_operation_token，保证任意 executor 线程上 cleanup 重入语义一致（XT-02/P2-A）。

## 3. Activation epoch（CORE-045 / XT-02）

### 3.1 机制组成

1. `begin_activation_epoch`（`:1800-1803`）：`activation_epoch = fetch_add(1)+1`。
2. 同步激活：`EpochFrame::enter(fiber_id, epoch)` 包住 `catch_unwind(apply)`（`:2141-2146`），RAII 弹出。
3. 异步激活：`EpochFrameFuture`（`:1868-1898`）在**每次 poll** 安装 frame → future 迁移到其他 executor
   线程仍正确（普通栈帧会错线程）。
4. `ACTIVATION_FRAMES: RefCell<Vec<(fiber_id, epoch)>>`（`:1810-1817`）；`current_activation_epoch(fiber_id)`
   取**最内层匹配** frame（`:1857-1866`）；`EpochFrame::drop` 用 `rposition` 精确移除本帧而非盲 pop
   （embedder panic 可乱序 drop，`:1833-1848`）；嵌套激活（父 apply 里加载子）自然叠栈。
5. 兜底：不在 frame 内注册的 effect 回退到 `activation_epoch` 最新值（`src/context.rs:1038-1048`）。

### 3.2 effect 的 epoch 标签与 stale/invalidated

- `push_effect` 写 `effect.activation = current_activation_epoch(fiber.id).unwrap_or(latest)`
  （`src/context.rs:1038-1048`）；child ownership effect 同样打标（`src/fiber.rs:1535-1542`）。
  **推论**：stale 尝试创建的子 fiber 的 ownership effect 也带该 epoch → `rollback_activation_to_pending`
  的 epoch 精确 drain 连带回收子 fiber，同时不碰新尝试的 child（这正是它不做 blanket sweep 的原因）。

| 概念 | 判定 | 处理 |
|---|---|---|
| **stale**（被新尝试取代） | `activation_epoch != epoch`（`activation_stale` `:1792-1797`） | `take_stale_effects` 按 `effect.activation==epoch` partition（`:1900-1912`）→ drain（sync inline/async await，`:1916-1924`）；**不**改 state、**不** remove_owner |
| **invalidated**（fiber 被 dispose/restart） | state ∈ {Disposed, Unloading}（`activation_invalidated` `:1242-1247`） | `discard_activation_effects`：取走全部 effects+deferred drain、children sweep（contained）、`remove_owner`（`:1261-1279`） |

`AsyncActivationGuard`（`:1286-1381`，async 激活 future 被 host drop 时跑）用「state==Loading &&
epoch==self.epoch」双判定只回收自己，回滚后再次检查仍 current 才置 Failed；`thread::panicking()` 时只
打标 child 不阻塞；结束 `registry.notify_idle()` 且**不**在 Drop 里 `refresh_dependents`（`:1368-1379`）。

### 3.3 XT-02 精确回滚

`rollback_activation_to_pending(data,ctx,epoch)`（`:1935-1949`）：
1. `discard_stale_activation(data,epoch)`：只 drain 本尝试 effects；listener 注销 disposer 与 ownership
   effect 都带本 epoch，**drain 即完成回收**，不能再额外 remove_owner/sweep（否则拆掉并发新尝试的注册）；
2. `transition_if_epoch(data, Loading, Pending, epoch)`：原子 `(state,epoch)` 校验+提交；成功才
   `ctx.refresh_dependents()`。async 版（`:1954-1963`）在 await discard 后用同一原子迁移。

### 3.4 并发场景期望

| 场景 | 结果 |
|---|---|
| restart 在 apply 执行中开始 | 旧尝试返回时 stale → 丢弃自己的 effect，不发布 Active；新尝试最终 Active/Failed |
| dispose 在 apply 执行中开始 | invalidated → 丢弃全部 effect/children/listeners，state 保持 Disposed |
| apply 成功但 inject provider 中途撤下 | `dependencies_ready` false → epoch 精确回滚到 Pending（XT-02） |
| apply panic + 并发新尝试 | stale 优先 → discard，不写 Failed/error（`:2185-2196`） |
| async apply 跨线程 poll | EpochFrameFuture 在 poll 线程装 frame，标签正确 |
| 激活 future 被 host cancel | AsyncActivationGuard 回收自己 epoch 的 effect + Failed（或 invalidated 仅回收） |

## 4. Effect 子系统

### 4.1 数据结构

| 类型 | 定义 | 说明 |
|---|---|---|
| `EffectMeta`（公开） | `{label, children: Vec<EffectMeta>}` `:19-26` | get_effects 诊断树 |
| `EffectNode`（内部） | `{label, children: Vec<Arc<Mutex<EffectNode>>>, parent: Option<Weak<...>>, owner_child: Option<u64>}` `:28-42` | 树节点；`owner_child` 标记 ownership effect 以便按 child id 精确摘除（R3-A4）；`new`:45、`new_owned`:55、`snapshot_filtered`:71-81 |
| `EffectKind` | `Sync(Disposer)` / `Async(AsyncDisposer)` `:84-88` | 双 lane |
| `FiberEffect` | `{kind, meta, activation: u64, internal: bool}` `:90-105` | `activation`=epoch 标签；`internal`=内核记账（不占用户 max_effects）；`is_disposed()` `:107-113` |
| 有序列表 | `FiberData.effects: Vec<FiberEffect>` `:172` | **sync/async 共享一条列表**；push 顺序=注册完成顺序；drain 用 rev |
| deferred | `FiberData.deferred_async: Vec<AsyncDisposer>` `:177` | 无法 inline await 的 async cleanup 尾（newest-first，从头 drain） |
| 诊断栈 | `FiberData.effect_stack` `:182` | body 执行期间的「当前父节点」；RAII `EffectStackScope`（`src/context.rs:62-72`） |

### 4.2 注册路径

| 入口 | 流程 | 位置 |
|---|---|---|
| `ctx.effect`/`effect_labeled` | `run_labeled_effect`：建 node → `attach_effect_node`（挂栈顶）→ state 检查（Disposed/Unloading → `InactiveEffect`）→ push effect_stack → 执行 `setup()` → 弹栈 → `push_effect(internal=false)` → 返回 Disposer | `src/context.rs:1066-1096`、`:1132-1146` |
| `ctx.effect_async*` | 同上，`AsyncDisposer` | `:1098-1128`、`:1170-1183` |
| `ctx.effect_future`/`effect_stream` | future/stream 包装成 async cleanup，同路径 | `:1648-1740` |
| `ctx.attach_effect(disposer,label)` | 不执行 body，把已有 Disposer 作为 **user** effect 挂到当前 fiber（占 max_effects） | `:795-802`、`:935-966` |
| 内核记账 effect | `register_effect_labeled`：internal=true（provide/监听器/hook/ownership） | `:933-941` |
| child ownership effect | `register_child_ownership`：`EffectNode::new_owned("ctx.plugin", child_id)`，AsyncDisposer 捕获 Weak child，嵌套到当前 effect 栈顶，internal=true，epoch 打标 | `src/fiber.rs:1470-1544` |
| `push_effect` 统一守卫 | ① Disposed/Unloading → `InactiveEffect`；② `retain(effect_is_live)` 剪掉已 dispose 条目（R3B）；③ max_effects 只数 !internal；④ 打 epoch 标签；⑤ push | `src/context.rs:994-1051` |

node 在 body 前挂到父节点；body panic/push 失败会留「幽灵节点」，靠 get_effects 的 live 过滤隐藏（C3）。

### 4.3 get_effects 过滤

`Fiber::get_effects()`（`:372-396`）：锁 FiberData → `live` = 未 disposed 的 meta 集合 → 只返回
`meta.parent.is_none()` 的顶层节点，并 `snapshot_filtered(&live)` 递归：子节点仅当与 live 中某节点
`Arc::ptr_eq` 才保留（C3 修复）。即 EffectMeta 树只反映**仍持有资源**的 effect；嵌套关系来自注册时的
effect_stack。注意这里在 fiber 锁内再锁各 EffectNode（锁序 L1→L2）。

### 4.4 逆序 drain 算法与变体

统一模式：`for effect in effects.into_iter().rev()`（逆注册序）逐项执行；单个 cleanup panic 隔离并记录
**第一个** payload，全部跑完后在安全点重抛（CORE-011/030）；`deferred_async` 在所有普通 effect 之后
drain（newest-first 存、从头取）。

| 变体 | 位置 | 行为 |
|---|---|---|
| `drain_effects_blocking` | `:2446-2466` | sync；async 用 `dispose_blocking`（BlockingExecutor/thread-park block_on）；返回 first panic |
| `drain_effects_async` | `:2471-2500` | 逐个 await async cleanup（顺序确定不并发） |
| `CascadeFrame::drain_blocking` | `:2575-2580` | drain effects → sweep_children_sync → finish_unload(target)；返回 drain panic 优先 |
| `CancelSafeUnload::drain` | `:3520-3563` | 取消安全：in-flight effect 保留在 vec 直到完成；future drop 时保序重试；deferred 从头 drain |
| `AsyncFrameUnload::drain` | `:3210-3248` | 非根帧取消安全版；drop 时 park_cancelled_frame |
| `fail_fiber_sync_inner` | `:1991-2008` | 失败回滚：sync inline，async `dispose_blocking` 或推 deferred |
| `rollback_epoch_effects` | `:1391-1416` | epoch 精确回滚：sync inline，async 全 deferred（Drop 上下文不能 block） |
| `FiberData::drop` | `:234-330` | 最后安全网：逆序 sync cleanup（contained）；async 只在未 unwinding 且装了 BlockingExecutor 时 blocking，否则进 orphan queue |

顺序性质：① sync/async 混排 → 跨 lane 逆注册序成立（CORE-007/038）；② deferred 是更晚注册、更早
drain 的尾，下次 dispose 时先于主列表 drain；③ **嵌套 effect 的列表位置**：run_labeled_effect 先执行
body（其中嵌套 effect 已 push）再 push 自己 → 内层 effect 在列表更靠前 → fiber 级逆序 drain 先执行外层
cleanup 再执行内层 cleanup（诊断树嵌套 ≠ drain 嵌套）；Go 必须保持一致或由 Lead 明确改语义。

### 4.5 `Disposer`（sync）状态机

`src/effect.rs:22-79`，`inner: Arc<Mutex<Option<Vec<Cleanup>>>>`：

```text
Armed(slot=Some) ──dispose()──► slot=None，从尾部 pop（逆注册序）执行
                                 │ 每个 callback catch_unwind，记录第一个 panic
                                 ▼
                           全部跑完 → resume_unwind(first_panic)（CORE-011）
重复 dispose：slot=None → no-op（单次/幂等）
Drop（仅最后一个 handle）：strong_count(inner)==1 时调用 dispose()，panic 被 contained+log
```

`Cleanup = Box<dyn FnOnce()+Send>`、`Effect = Vec<Cleanup>`（`:9-20`）；`is_disposed()`=slot None
（`:55-58`）。CORE-030：panic 不跳过剩余 cleanup，drain 完再重抛第一个。

### 4.6 `AsyncDisposer` 状态机

字段：`inner: Arc<Mutex<Option<Vec<AsyncCleanup>>>>` + `state: Arc<DisposeState>`（`src/async_effects.rs:74-78`）。
`DisposeState`（`:152-159`）：`done/cancelled/claimed: AtomicBool + waiters: Mutex<Vec<Waker>>`；
`is_settled()=done&&!claimed`；`claim()` CAS(false→true) 取得 teardown 所有权并清 done/cancelled；
`finish_drain()` 置 done 清 cancelled release+wake；`cancel()` 置 cancelled+done release+wake
（cancelled 表示「有 restored cleanups 待重试」）。

```text
Idle(claimed=false,done=false)
 ├─ dispose()/dispose_blocking() → claim 成功 → Draining（唯一 owner）
 │    ├─ 正常：逐项 await（first panic 记录）→ finish_drain → resume_unwind(first)
 │    ├─ future 被 drop（cancel）：CancelSafeDrain::drop 把未启动 cleanup 放回 slot（slot 为空时）、
 │    │   state.cancel()；in-flight cleanup 丢失（不可恢复，log）→ 下次 dispose 重新 claim 继续逆序跑
 │    └─ 列表空：finish_drain → no-op
 ├─ 他人正在 Draining（claim 失败）
 │    ├─ 本调用栈正处于该 disposer 的 drain frame（self-join）→ 立即返回
 │    └─ 普通 joiner → wait_done()；正常 finish → 返回；对方 cancelled → 重新 claim 重试
 └─ 已 settled（done&&!claimed）：重复 dispose no-op（cancelled 时除外，需重试）
```

关键代码：
- `dispose()` 循环（`:459-517`）：claim 失败先查 `drained_by_current_thread()`（self-join→return），
  否则 `wait_done().await`，正常返回、cancelled 则 `continue` 重试；claim 成功后 `take()`，
  `CancelSafeDrain` 包住列表，`pop` 逐项执行（`catch_unwind(cleanup)` 建 future +
  `catch_panic(DrainScopeFuture)` 逐 poll），完成后 `finish_drain` + `resume_unwind` 第一 panic。
- **drain frame / self-join（S-2/R2-1）**：thread-local `DRAIN_FRAMES: RefCell<Vec<usize>>`（`:80-92`），
  key=`&DisposeState` 地址（稳定身份 `:210-214`）；`DrainScopeFuture` 每个 poll 进入 frame（`:122-139`）；
  `drained_by_current_thread()`=当前线程栈含该 key（`:216-223`）。**不是 thread id 判断**：同一 worker
  上的无关 joiner 必须继续等。
- `dispose_blocking()`（`:572-587`）：先 self-join 短路；`is_settled()&&!cancelled` 才 no-op；否则
  `block_on_unit(clone.dispose())` —— **join 语义**：阻塞到全部 cleanup 真正完成。
- `CancelSafeDrain::drop`（`:239-266`）：remaining 非空且 slot 空 → 放回（保序，下次 pop 继续逆序）；
  `state.cancel()`；in-flight cleanup future 无法恢复（Rust 无 cancel-safe await），log。
- `discard()`（`:529-534`，R3-A4 专用）：取走 cleanups 丢弃 + finish_drain，使 Drop 安静 no-op。
- `Drop`（`:598-629`）：cancelled→只 log（restored cleanups 随句柄消失，不从 Drop 跑）；最后一个 handle
  且未 disposed 且线程未 unwinding → contained `dispose_blocking()`；unwinding → 取走丢弃+log。
- fiber 级 deferred：取消/同步失败时 AsyncDisposer 直接挪进 fiber 的 `deferred_async`（不在 disposer
  内部），由下次 fiber dispose/drain 处理。

### 4.7 Orphan queue 与公开 API（XT-01）

`src/async_effects.rs:268-434`：`ORPHAN_CAPACITY=256`；`ORPHANS: Mutex<Vec<AsyncDisposer>>`；
3 个原子计量（累计泄漏、累计失败、队列 gauge）。
`orphan_async_disposer`（`:305-324`）仅在 `FiberData::drop` 无法安全跑 cleanup 时调用：队列未满入队；
满/中毒则 `mem::forget`（绝不就地 Drop，Drop 可能 block）+ 累计计数；从不阻塞、从不 unwind。
公开 API（crate root 导出 `src/lib.rs:75-79`）：`orphan_async_cleanups_pending()`（queue gauge）、
`orphan_async_cleanup_count()`（容量外累计泄漏，健康进程为 0）、
`orphan_async_cleanup_failure_count()`（drain 时 panic 计数）、
`drain_orphan_async_cleanups()`（先整体 take 再逐个 `dispose_blocking`，幂等、返回处理数、失败
contained+计数；必须在非 reactor 线程调用）。角色：`FiberData::drop` 在无 BlockingExecutor 或线程
unwinding 时把 async cleanup 交孤儿队列而非静默丢弃；sync cleanup 仍 inline（`:279-328`）。

### 4.8 墓碑/剪枝/嵌套/过滤

- **disposed 条目惰性剪枝**：`push_effect` 每次注册前 `effects.retain(effect_is_live)`
  （`src/context.rs:1014-1020`、`:1053-1062`）→ 不占配额、列表不因 register/dispose 循环无界增长。
- **ownership 摊销剪枝**：`OWNERSHIP_PRUNE_THRESHOLD=1024`（`:1457`），注册新 ownership 时若
  `effects.len()>=1024` 删除「owner_child 已标记且已 dispose」的条目（`:1550-1562`），只动 ownership。
- **精确摘除**：`detach_child_ownership`（`:1570-1639`）在子自行 dispose 后从 parent.effects 摘除
  `owner_child==child_id` 项、从 children 移除弱条目、unlink 诊断节点；随后**锁外**
  `AsyncDisposer::discard()`（不是 drop）。幂等。
- **嵌套**：effect_stack + EffectNode.parent 只用于 diagnostics；资源归属仍在 fiber 平铺列表。
- **过滤**：get_effects 的 live+顶层过滤；is_disposed 是唯一活性判据。

## 5. 生命周期入口完整流程

### 5.1 `dispose` / `dispose_async`

`dispose()`（`:781-788`）：① `recover_parked_before_root_operation()`（先同步恢复被取消级联停摆的帧，
`:815-824`）→ ② `begin_operation` → ③ `begin_lifecycle`（Busy→Err）→ ④ `unload_fiber(data,Disposed)`。
`dispose_async()`（`:793-811`）：① `recover_parked_unloads_async(registry).await` → ② begin_operation →
③ begin_async_lifecycle + with_operation_token → ④ `unload_fiber_async(...,Disposed)` → ⑤ `drop(lifecycle)`。
幂等：已 Disposed 且无 deferred → `begin_cascade` None → Ok；已 Disposed 有 deferred → 只排干 tail（不迁移）。
sync 版经 BlockingExecutor 跑 async cleanup（确定性完成，非 fire-and-forget）。cleanup panic 契约：
drain 全部后根帧 `resume_unwind` 第一 payload（`:3324-3326`），dependent 的 panic 只 log（FIX-2）。

### 5.2 `restart` / `restart_async`

`restart`（`:844-856`）/`restart_async`（`:859-876`）骨架：
1. begin_operation → begin_(async_)lifecycle → `FinalizeGuard`（覆盖所有返回路径）；
2. `begin_restart(allow_async)`（`:888-911`）：Disposed→`Err(Disposed)`；Unloading→`Err(InactiveEffect)`；
   sync 入口+async 插件→`Err(AsyncPluginNeedsAsync)`；`transition(Unloading)`（**发 status/lifecycle 事件**）；
   锁内重读 `(ctx, plugin, raw_config)`；
3. `unload_fiber(_async)(..., Pending)` 完整级联卸载；
4. `if dependencies_ready(ctx, plugin.inject()) { let _ = activate_*_by_kind(...) }` —— 激活错误被**忽略**
   （fiber 落 Failed，错误在 error）：**Ok(()) 不代表 Active**（`:838-843`、spec 554-559）；依赖不满足则
   保持 Pending 等 refresh；
5. FinalizeGuard drop 时如父卸载打过 `dispose_requested` → `finalize_requested_dispose`。

### 5.3 `update` / `update_with_no_save` / `update_async(_with_no_save)`

`update(config)=update_with_no_save(config,false)`；`update_with_no_save`（`:962-970`）：
begin_operation → begin_lifecycle → FinalizeGuard → `resolve_update(config, allow_async, no_save)`
（`:1009-1078`）→ 若需 restart 则 `self.restart()`/`restart_async().await`（**重入同一 lifecycle token**）。

`resolve_update` 顺序：
① 状态检查（Disposed→Err、Unloading→InactiveEffect、sync+async→AsyncPluginNeedsAsync、ctx 丢失→Disposed）；
② `internal/config` waterfall（`:1029`）；
③ schema 校验（失败→`Err(Validation)`，不 restart、raw_config 不变）；
④ `internal/update` waterfall（`:1044-1070`），terminal **最多一次**（`ran: AtomicBool`）：
   - hook 返回与传入不同的 config（replacement）→ 先校验 replacement（失败→记录 rejection、return false
     veto）→ 通过才 `store_raw_config(replaced)`；
   - pass-through → `store_raw_config(调用方原始 raw)`（不是 resolved，防派生复合 CORE-040/FIX-14）；
   - 不调 next = veto：`update` 返回 `Ok(())`、不 restart、raw_config 不变；
⑤ rejection 有值 → `Err(Validation)`（terminal 校验通过前不提交，无脏值）。
commit 顺序：**config waterfall → validate → update terminal 提交 raw → restart（其 activation 再
resolve+validate 一次）**。`no_save` 只是 persistence hook 的 hint，永不阻止内存 raw config 提交。

### 5.4 `replace_plugin` / `replace_plugin_async`

`replace_plugin`（`:590-622`）：
1. begin_operation + begin_lifecycle + FinalizeGuard；
2. `begin_replace(kind)`（`:691-726`）：**锁外**先算 `inject()`（B-3 防 embedder panic 毒锁）；打开 registry
   的 **dep-index window**（S1/C2：先冻结候选索引再改声明）；锁内 Disposed→`Err(Disposed)`，存旧
   kind/runtime_key，写新 plugin+inject；锁外 `index_fiber_deps` 重建反边；关 window；
3. `registry.rebind_single_fiber(...)`（`src/registry.rs:1231-1286`）迁移 runtime（失败→`restore_replace`
   回滚 Err）；
4. `catch_unwind(restart())`（D-2：重载会 drain 旧实现 cleanup，panic 不能逃出 Result API）：
   `Ok(Ok)`→Ok（**仍 Ok≠Active**，restart 自己吞激活失败）；`Ok(Err)`→restore_replace +
   `reload_after_failed_replace`（非 Active 再 restart 旧实现，contained）→Err；`Err(payload)`→
   `Err(PluginFailed("replace_plugin: reload panicked: …"))` + 同样回滚/重载；
5. `restore_replace`（`:728-757`）：swap_plugin 恢复实现/inject/index；old_key 存在则 rebind 回旧 runtime；
   `old_key==None`（root 特例）→ `unregister_runtime_fiber(id)`+`runtime_key=None`，不留临时 runtime（FIX-8/D-1）。
`replace_plugin_async`（`:628-666`）：同流程，begin_async_lifecycle + with_operation_token +
`FutureExt::catch_unwind(restart_async())`，最后 drop(lifecycle)。`Registry::replace` 多 fiber 版对已
reload 的 fiber 逐个用旧实现回滚，回滚失败且确有部分迁移→`CordisError::Rollback`
（`src/registry.rs:1101-1305`、`replace_rollback_result` `:717-745`）。

### 5.5 共享卸载管线（`unload_fiber` / `unload_fiber_async`）

1. `begin_unload_activity`（registry active_unloads++，RAII `UnloadActivity` 保证 future 被 drop 也递减并
   notify，`src/fiber.rs:3296-3310`、`src/registry.rs:2073-2088`）——active unload 期间 recovery 跳过；
2. `begin_cascade(data,target)` 两阶段 begin（§6.3）；
3. drain：sync（`:3312-3328`）`drain_cascade_dependents` 拆出 root、dependents 逆序 `drain_blocking`
   （panic contained、记 first），root 最后 drain → 有 panic `resume_unwind`；async（`:3330-3432`）
   `RootFrameGuard`+`RemainingFrames`+`AsyncFrameUnload` 逐帧取消安全 drain+sweep，root 用
   `CancelSafeUnload`，最后 `sweep_children_async(root)`+`finish_unload(root)`；
4. `finish_unload(data,ctx,target)`（`:2408-2440`）：`remove_owner(listeners)` → `transition(target)`
   （发事件）→ 若 Disposed：`detach_child_ownership` → `registry.unregister_runtime_fiber(id)`（D-1 按
   id 从所有 runtime 摘除）→ `ctx=None`、`parent=None`（断边 CORE-040）；
5. provider 撤回先于 binding 移除：`take_effects_begin` 在 drain **之前** `ctx.withdraw_owner_services(owner)`
   把这些 service entry 标 `withdrawn=true`（`is_available()` 直接 false，`src/service.rs:99-122`）+
   `note_service_changed`；随后 drain 中 provide disposer 才 `store.remove`+emit None+refresh
   （`src/fiber.rs:2384-2403`、`src/context.rs:526-540`；CORE-042）。

### 5.6 失败回滚顺序（activation failure）

`fail_fiber_sync_inner(...,defer_async)`（`:1983-2035`）/`fail_fiber_async`（`:2038-2079`）：
① `set_error`；② 锁内 `mem::take(effects)`；③ **逆注册序**：sync `dispose()`（catch）、async
`dispose_blocking()`（catch）或 defer_async 推 deferred（记录 first panic）；④ deferred 存回
`fiber.deferred_async`；⑤ `events.remove_owner(fiber_id)` 兜底；⑥ `sweep_children_sync/async`（panic
只 log，FIX-4）；⑦ **若 `!activation_invalidated`** → `transition(Failed)` + `refresh_dependents()`
（若激活期间已被 dispose/restart 抢先则保持 Disposed/Unloading，不回写 Failed）；⑧ cleanup panic 只 log
（失败激活的回滚不能再抛）。apply Err 原样保留结构化错误；apply panic → `PluginFailed("panic: …")`；
校验失败 → `Validation`。

### 5.7 pending-dispose finalisation（全覆盖）

`FinalizeGuard`（`:1774-1789`）+`finalize_requested_dispose`（`:1752-1765`）：
- 在 `replace_plugin`(:593)、`replace_plugin_async`(:634)、`restart`(:847)、`restart_async`(:863)、
  `update_with_no_save`(:965)、`update_async_with_no_save`(:982) 创建；
- Drop 时若 `dispose_requested && state!=Disposed` → 清 flag → `Fiber.dispose()`（同 token 重入），
  外层再 `catch_unwind`（guard 自身绝不 unwind，`:1782-1787`）；
- 覆盖正常返回、提前 Err、`?`、cleanup panic 及 Disposed/Unloading/AsyncPluginNeedsAsync/bind 失败/
  validation/veto 全部退出路径（spec 265-270）——确保「父已卸载 + 子当时 busy」的子 fiber 不会永久 Active；
- flag 来源：`sweep_children_*` 中 child.dispose Err/panic（`:1676-1697`/`:1710-1738`）与 ownership
  cleanup 中 `child.dispose_async()` Err（`:1500-1508`）。

### 5.8 `dispose_all`（关闭路径）

`Registry::dispose_all`（`src/registry.rs:1409-1463`）：① `recover_parked_unloads_blocking`；② snapshot
fibers 逐条 `catch_unwind(fiber.dispose())` 记录 first_error/first_panic、永不中断；③ 对 root 同样尝试；
④ `prune_empty_runtimes`；⑤ 收尾：panic 优先（error 先 log 再 resume_unwind 第一个 panic）；只有 err → Err；
只有 panic → 重抛；否则 Ok。Ok 后 registry 空且 is_idle；Busy fiber → `Err(LifecycleBusy)` 且可能留未清理
fiber（调用方重试，XT-10）。

## 6. Child ownership

### 6.1 三条并列线索

① `FiberData.children: Vec<Weak<FiberData>>`（父→子弱，级联 DFS/child sweep 用）；
② 父 effect 列表中的 ownership effect（internal async，`owner_child=child_id`，cleanup 调
`child.dispose_async()`）——父卸载逆序 drain 时生效，busy 则打标；
③ 子→父强 `Context`（parent）+ 父→子弱 children 构成 fiber 树。级联 begin 先按 children DFS 纳入子帧；
drain 时子帧先完成 → 父 ownership effect 执行时子通常已 Disposed，短路返回（`:1494-1502`）；
未覆盖的（并发新建）由 ownership effect 与 `sweep_children_*` 兜底。

### 6.2 注册顺序与 `MAX_PLUGIN_DEPTH`

`ctx.plugin`→`create_fiber_handle`（`src/context.rs:1207-1354`）：
1. 父 Disposed→`Err(Disposed)`；2. **深度守卫**（`:1229-1248`）：从加载者 fiber 沿 `parent()` 链数数
（含自身与根；根 weak 自引用用 `parent.id==fiber.id` 停止），`depth>=MAX_PLUGIN_DEPTH(256)` →
`Err(InvalidPlugin)`。合法 fiber 链最多 root+255 个 plugin（root 级 plugin 之下再嵌约 254 层）；
只按 fiber 父链计数，extend/intercept/extend_meta 不消耗；3. 分配 id/clock，构造 FiberData
（Pending、parent=Strong(self)、registry、runtime_key）；4. `Context::child` 写回 `fiber.ctx=Strong(child)`；
5. `register_fiber`（registry 强句柄）+`register_runtime_fiber`；6. 父状态须 ∈{Active,Loading,Pending}
否则 `fiber.dispose()`+`Err(Disposed)`；7. `parent.children.push(Weak<child>)`；8. `register_child_ownership`
（失败→fiber.dispose()+Err）；9. `emit_plugin(&fiber)`（internal/plugin；此时 Pending、无 pending_ops）。

### 6.3 两阶段迭代级联（`begin_cascade` → 逆序 drain）

`begin_cascade(data,target)`（`:2840-3011`）返回 **begin 序** frames，调用方逆序 drain。

**Phase 1a：依赖 dependent（provider-before-dependent）**：`begin_frame(root, root=true)`（root 允许
Unloading/Disposed 以支持 A-9 retry）→ `take_effects_begin`（置 Unloading、取 effects+deferred、记 ctx、
withdraw_owner_services+note_service_changed）。从 root 的 `withdrawn` 服务名开始 frontier 驱动：
- `dep_index_ready()` 时用 `dependents_of_services(frontier)` 取候选（不可用→置 index_failed 回退），候选按
  fiber id 排序（确定性），对 `fiber_is_unready_active`（Active+Sync+依赖不满足，`:3042-3063`）执行
  `begin_frame(candidate,Pending,non-root)`，新 frame 的 withdrawn 扩展 frontier；有界轮数
  `limit=fiber_count+2`；中途索引窗口打开（replace 改 inject）→ 检查 `dep_index_epoch` 变化，回退 full scan
  （debug 构建另有 cross-check，`:2924-2957`）；
- 索引不可用→**full scan 兜底**：每轮 `find_unready_active()`（`src/registry.rs:2227-2252`）取第一个 begin，
  直到固定点/上限（C2 正确性 backstop，`:2960-2984`）；
- `begin_frame(non-root)` 对 Disposed/Unloading 直接 `Ok(None)`（**永不重复取 effect**，`:3069-3078`）。
任一 begin 失败→`rollback_cascade(frames)`：已 begin 帧**逆序** drain（contained），避免卡 Unloading
（`:3013-3024`；测试 `:4134-4191`）。

**Phase 1b：ownership descendants（显式 DFS stack，O(1) 栈）**：`stack=child_fibers(data)`（children 弱列表
upgrade）→ while pop：`begin_frame(child,Disposed,non-root)` → `stack.extend(child_fibers(child))`；帧数超
limit→log+break（剩余 child 存活可重试）。begin 序=DFS 前序 ⇒ **逆序 drain = 子先于父**；依赖 dependent 在
child 之前 begin ⇒ 逆序时 child 先于 dependent 先于 provider，同时满足 child-before-parent 与
dependent-before-provider。

**Phase 2：逆序 drain**：sync `drain_cascade_dependents`（`:3099-3127`）拆出 root，dependents 逆序
`drain_blocking`（drain effects→sweep children→finish_unload），panic 记 first 只 log；root 最后 drain，
其 panic 在状态落地后 `resume_unwind`。async 见 §5.5（每帧取消安全 + park 兜底）。

### 6.4 busy child：标记延迟 dispose

`sweep_children_sync/async`（`:1669-1701`/`:1703-1738`）LIFO 遍历 children（后创建先处理，与 effect 逆序
一致）：`child.dispose()/dispose_async()`：Ok→已回收；`Err(LifecycleBusy)`→`request_child_dispose` 打
`dispose_requested` 并继续；panic→contained、打标、记 first payload 并继续；first payload 等 parent 到达
目标状态后才重抛（FIX-4；测试 `:4030-4089`）。持 child lifecycle 的 operation 结束前由 `FinalizeGuard`
finalize（§5.7）——这就是 busy child 延迟 dispose 的闭环。被取消的级联停摆帧由 recovery 恢复（§6.6）；
`ReparkGuard` S7：recovery 把 fiber 落到 target（Pending）但仍有 restored deferred cleanup 时继续 re-park
直到排干（`:2650-2672`；`take_parked_unloads` 的 recoverable 判定 `:2042-2051`）。
实现事实（Go 需定夺）：级联**非根帧** begin 不 claim 该 fiber 的 lifecycle slot，只靠 Disposed/Unloading
跳过 + finish_unload 幂等；若 child 正并发 Loading 存在被父级联取走 effect 的窗口。

### 6.5 动态记账上界

| 机制 | 位置 | 作用 |
|---|---|---|
| 子自行 dispose 时摘除 parent ownership effect + children 弱条目 + 诊断节点链接 | `detach_child_ownership` `:1570-1639`，finish_unload(Disposed) 调 `:2416-2422` | 长寿命父动态 load/unload 不累积 |
| ownership effect 摊销剪枝（阈值 1024） | `:1457`、`:1550-1562` | 父 effect 列表有界兜底 |
| 每次注册前 retain(live) | `src/context.rs:1014-1020` | disposed 条目及时清除 |
| max_child_fibers 只数活 child（剪死 weak） | `src/context.rs:1249-1280` | 不被陈旧条目占用 |
| max_effects 只数 user effect（Q11） | `FiberEffect.internal`、`src/context.rs:1021-1036` | 内核记账不耗宿主预算 |
| `AsyncDisposer::discard`（摘除时丢弃不运行） | `src/async_effects.rs:529-534` | 避免 re-dispose 已 dispose 的 child / 触发 blocking executor |

### 6.6 取消级联恢复 / parked frames（F1/F4/F5）

- **产生**：async 级联 future 被 host drop 时：非根未处理帧→`park_cancelled_frame`（`:3144-3195`）：
  sync cleanup inline（contained）、async 全部推入 `deferred_async`、fiber 保持 `Unloading` 且**仍注册**
  （不能在此完成 teardown：可能丢最后一个 FiberData 引用，而 deferred cleanup 需 reactor），
  `registry.park_unload(&data,target)`；root 帧→`CancelSafeUnload::drop`→`complete_teardown`（sync inline、
  async 进 deferred、`finish_unload(target)`，A-9：完成而非回滚，避免 fake Active/永久 Unloading）；
  `RemainingFrames` drop→park；`RootFrameGuard` drop→complete。
- **恢复**：`parked_unloads` 去重剪死（`:1996-2012`）；`take_parked_unloads`（`:2028-2059`）只返回仍
  recoverable（Unloading，或非 Disposed 且有 deferred）的 fiber 并按 id 去重；`begin_recovery` 单恢复者
  （`:2063-2070`）+ `RecoveryGuard` RAII 保证取消/panic 也释放 latch（`:2595-2613`）；`ReparkGuard`
  （`:2638-2672`）/`RecoveryBatch`（`:2678-2695`）保证批次被取消时不丢帧。
- `recover_parked_unloads_blocking`（`:2709-2765`）/`_async`（`:2770-2828`）：`has_active_unload()` 时返回 0
  （**绝不在别的 unload/cascade 内 recovery**）；轮数上限 `fiber_count+2`；对每个 parked fiber 以**原 target**
  `unload_fiber(_async)`（Disposed=ownership child、Pending=依赖 dependent），错误/panic contained log，剩余
  仍可重试。
- **恢复入口**：`dispose` 前置（`:782-784`）、`dispose_async` 前置（`:793-802`）、`dispose_all`
  （`src/registry.rs:1410-1411`）、`refresh_all_async_impl`（`:2269-2282`）、`when_idle` recovery 循环
  （`:963-996`）；sync refresh 故意不 recovery（§7.2）。
- 边界：取消落在**正在执行**的 async cleanup 上时该 in-flight future 不可恢复（A-9 边界），但 fiber 不会
  永久 Unloading；`is_idle()` 严格 false 直到 recovery 跑完。

## 7. Quiescence 与协同取消

### 7.1 `is_idle`（严格）与 `when_idle`（排除自身注册帧）

Registry 字段（`src/registry.rs:402-482`）：`pending_registrations: AtomicUsize`、
`registration_owners: Mutex<HashMap<ThreadId,usize>>`、`idle_waiters: Mutex<Vec<(u64,Waker)>>`、
`parked_unloads`、`active_unloads`、`recovering_parked`。

- `is_idle()`（`:871-873`）+`is_idle_excluding_with_total(own,total)`（`:888-917`）：`total-own==0`（严格版
  own=0），且所有 fiber+root：state ∉ {Loading,Unloading} 且 pending_ops==0。
- 注册窗口：`Context::plugin*` 在 `create_fiber_handle` **之前** `begin_registration()`
  （`src/context.rs:1379-1384`、`:1404-1409`、`:1428-1433`），guard 覆盖到 activation；`begin_registration`
  同时 `pending_registrations++` 与 `registration_owners[当前线程]++`（`:1942-1956`）；
  `RegistrationGuard::drop` 按**创建线程**释放 owner（跨线程 drop 也不错扣，C4）+ pending-- + notify_idle
  （`:694-715`、`:1958-1969`）。
- `when_idle()`（`Registry::when_idle` `:998-1008`；`Context::when_idle` 包装 `src/context.rs:759-761`）的
  recovery-aware 循环 `when_idle_recovering`（`:963-996`）：每轮 `quiescence_snapshot()` 在**同一把
  owner-map 锁内**取 `(own,total)`（`:1982-1990`，防 TOCTOU）；`is_idle_excluding_with_total(own,total) &&
  !has_parked_unloads()`→return（S7：recovery 取消后可能已到 target 但仍有 restored deferred，必须等排干）；
  有 parked 时 active unload→等 `wait_idle_or_recoverable()` 后重试，否则 `recover_parked_unloads_async` 后
  重试；否则等 `wait_idle_or_recoverable()`（被状态迁移/注册释放/park/卸载结束 notify 唤醒）；轮数上限
  `fiber_count+8`（≥16），超限 log 后回退纯可恢复等待（不 hang）。
- `is_recoverable_or_idle()`（`:942-948`）= idle-excluding **或** `(!has_active_unload() &&
  has_parked_unloads())`；`IdleOrRecoverableWait`（`:594-628`）带 id 注册/drop 注销/注册后二次检查；
  `IdleWait` 只是包住 recovery 循环的 Future（`:630-651`）。
- `notify_idle` 调用点：publish_transition、end_operation、RegistrationGuard::drop、RecoveryGuard::drop、
  park_unload、end_unload_activity。
- 边界：两个线程的注册窗口互相 `when_idle()` 会死锁（内核不检测循环注册依赖）；`internal/plugin` create
  hook 内可安全 when_idle（自己注册被扣除）；`apply`/`apply_async` 内**不可**同步等待 idle
  （自己 pending_ops>0 会等自己，XT-04；spec 490-504）。

### 7.2 recovery-aware 等待；sync refresh 不 recovery

- `refresh_all_async_impl` 开头最多 8 轮「有 parked →（active unload 则等）→ recover」（`:2269-2282`）。
- **sync `refresh_all_impl`（`:2123-2132`）明确不 recovery**：recovery 可能要在调用线程上 drain
  reactor-backed async cleanup，而 sync refresh 可能正跑在 reactor 线程（如 tokio current_thread）上，
  同步 block 会死锁；sync refresh 把 Unloading 帧当「不在扫描集合」（find_unready_active 只扫 Active）跳过，
  恢复留给 awaitable 入口（when_idle/refresh_async/dispose_all/dispose(_async)）；见
  `docs/CORDIS-RUST.md:191-195`、spec 505-530。
- recovery 单恢复者 + 跳过 active unload + RAII guard 见 §6.6。

### 7.3 协同取消（I11）

- `Fiber::cancel()`（`:483-487`）：`cancel_requested=true`（per-fiber，不误伤兄弟）；
  `Context::checkpoint()`（`src/context.rs:713-730`）：读 flag，true→`Err(Cancelled)`；内核从不抢占/强杀，
  长任务必须自行 checkpoint 并沿正常 failure/rollback 退出（CORE-050）；`is_cancelled()`（`:490-495`）。
- Rust 真正「中止」机制是 **drop future**（卸载/激活 future 被 host 取消），由 `CancelSafeUnload`/
  `park_cancelled_frame`/`AsyncActivationGuard` 收尾；`cancel()` 本身只置位。
- `await_settled`/`Settled` 汇总（§2.4）：条件 = `pending_ops==0`，然后 error 有值→Err 否则 Ok；不要求
  Active、不等注册窗口；waker 带 id 注册、drop 注销、注册后二次检查防丢唤醒（`:3900-3947`）。

## 8. `internal.rs`：9 个 typed hook + lifecycle bus

### 8.1 九个 `internal/*` typed hook（映射表见 `src/internal.rs:12-22`）

| # | event | 签名 | payload / 语义 | 调用时机 | panic 策略 |
|---|---|---|---|---|---|
| 1 | `internal/plugin` | `PluginHook=Arc<dyn Fn(&Fiber)+Send+Sync>`(`:177-178`) | `&Fiber`（新 fiber，Pending、无 pending_ops） | `create_fiber_handle` 末尾、ownership effect 后、激活前：`emit_plugin`(`:599-605`；call `src/context.rs:1352`) | contained 逐 hook |
| 2 | `internal/status` | `StatusHook=Arc<dyn Fn(&Fiber,FiberState /*old*/)+Send+Sync>`(`:179-180`) | `&Fiber`+旧态；`to` 由 hook 自读（可能 torn） | 每次真实迁移（publish_transition 第一步，call `src/fiber.rs:1192`） | contained |
| 3 | `internal/config` | `ConfigHook=Arc<dyn for<'a> Fn(&Fiber,Value,&ConfigNext<'a>)->Value+Send+Sync>`(`:183-184`) | 配置 waterfall；terminal 返回最终 config | 每次激活（`src/fiber.rs:2118-2121`/`:2229-2232`）与 update 的 `resolve_update`(`:1029`)；`waterfall_config`(`:666-677`) | contained；panic→链继续、fallback=传入 config |
| 4 | `internal/service` | `ServiceHook=Arc<dyn Fn(&Context,&str,Option<&ServiceValue>)+Send+Sync>`(`:181-182`) | `Some`=provide / `None`=remove | insert_service 成功后 `Some`(`src/context.rs:512-515`)；provide disposer `None`(`:532-535`) | contained |
| 5 | `internal/update` | `UpdateHook=Arc<dyn for<'a> Fn(&Fiber,Value,bool,&UpdateNext<'a>)+Send+Sync>`(`:185-186`) | update waterfall；terminal 提交 raw config+请求 restart | `resolve_update`(`src/fiber.rs:1044-1070`，`waterfall_update` `:679-700`) | **panic=allow**：链继续、terminal 仍提交/重启；策略 hook 必须不调 next 才能 veto(`:338-342`) |
| 6 | `internal/get` | `GetHook=Arc<dyn for<'a> Fn(&Context,&str,&GetNext<'a>)->Option<ServiceValue>+Send+Sync>`(`:187-189`) | 读 waterfall；`None`=服务不存在/不可见，**不是** skip | `Context::get/require/get_any` 读路径 `waterfall_get`(`:702-726`；call `src/context.rs:574`/`:607`/`:1981`) | contained；panic→`next.next()` |
| 7 | `internal/set` | `SetHook=Arc<dyn for<'a> Fn(&Context,&str,&ServiceValue,&SetNext<'a>)->bool+Send+Sync>`(`:190-192`) | 写 waterfall；返回是否已写 | `set_revertible` 的 `waterfall_set`(`:728-748`；call `src/context.rs:2045`) | contained；panic→继续 |
| 8 | `internal/listener` | `ListenerHook=Arc<dyn for<'a> Fn(&ListenerRequest<'a>)->Option<ListenerOverride>+Send+Sync>`(`:297-299`) | bail：`ListenerRequest{name,prepend,global,once,owner}`(`:150-162`)→`ListenerOverride{callback,prepend,global,once}`(`:164-175`)，**第一个 Some 生效** | listener 注册前 `intercept_listener`(`:750-763`；call `src/context.rs:1562`) | contained；panic→None 继续 |
| 9 | `internal/dispatch` | `DispatchHook=Arc<dyn Fn(&DispatchInfo)+Send+Sync>`(`:300-301`) | `DispatchInfo{mode: DispatchMode,name,args}`(`:138-147`)；mode=Emit/Parallel/Serial/Bail/Waterfall(`:115-136`) | 公共事件分发前 `emit_dispatch`(`:643-664`；calls `src/events.rs:298/321/346/377/424/479/501`) | contained；递归门闩 dispatch_depth；`internal/` 前缀直接跳过 |

**额外：lifecycle bus（不是第 10 个 `internal/*`）**：`LifecycleHookDyn =
dyn Fn(&Fiber,FiberState /*from*/,FiberState /*to*/,u64 /*seq*/)+Send+Sync`(`:506`)，注册
`on_lifecycle_hook`(`:615-621`)，发送 `emit_lifecycle`(`:623-633`)，入口 `Context::on_lifecycle`
(`src/context.rs:2252-2268`)。与 `internal/status` 区别：lifecycle 在状态锁内快照 from/to/seq 再锁外投递
（XT-03/08），raw status 不保证；lifecycle 订阅**不属于 fiber**、不随安装 fiber 卸载。`LifecycleEvent`
（`:243-257`）`#[non_exhaustive]`：字段 `fiber_id/name/from/to/seq`，下游只能 `LifecycleEvent::new(...)`
构造（`:265-273`）+ 匹配加 `..`；`name` 在投递时由 `fiber.name()` 取（root="root"）；`seq` per-fiber 单调
迁移序号（0 起、可乱序到达，按 (fiber_id, seq) 重排还原链）；每个 listener 单独 contained。

### 8.2 所有权：fiber-owned vs raw `InternalBus`

| 注册方式 | 所有权 | 卸载自动移除 | 适用 |
|---|---|---|---|
| `Context::on_internal_*`（`src/context.rs:2200-2337`） | 调用方 fiber：写共享 bus + `register_effect_labeled(disposer.clone(),"ctx.on_internal_*")` 作为 internal effect | **是**（随 fiber 卸载/restart 逆序 drain 注销） | 普通插件 |
| `InternalBus::on_*`（`:534-583`） | 调用方自持返回的 Disposer | **否**（必须手动 dispose） | 比 fiber 长寿的 embedder 工具 |
| `Context::on_lifecycle` | 非 fiber 绑定，返回 Disposer 是唯一 owner | **否**（by design） | metrics/审计/关闭协调 |

两种入口写**同一有序 hook 列表**（注册顺序交错）；`internal/*` 不反灌 dispatch bus。返回的 Disposer
单次幂等；fiber-owned 的 Disposer 被用户提前 dispose 后，effect 里的 clone 只是惰性条目（drain no-op）。

### 8.3 waterfall 语义小结

| hook | `next` | terminal / 关键坑 |
|---|---|---|
| config | `next(config)->Value`，递归 split_first；panic 用 fallback 继续 | 空 hooks 返回传入值；panic 不拦截 |
| update | `next(config,no_save)->bool`；不调 next = veto | terminal 由 `ran.swap(true)` 保证最多一次，提交 raw config 并 true；hook 返回 false 只表示 veto；panic=allow |
| get | `next()->Option<ServiceValue>` | None=服务缺失；全局 `get_depth` 门闩：任一 get hook 执行期间再 ctx.get 直读 store、跳过所有 get hook |
| set | `next()->bool` | 空 hooks 写 store（version++）；类型校验在 set_revertible 前置；panic 继续 |
| listener | 无 next（bail） | 第一个 replacement Some 生效 |

`contained()`(`:83-102`)：`catch_unwind(AssertUnwindSafe(body))`→`tracing::error!`（target
`zeokeel_cordis::containment`）→ 返回 fallback。所有 emit/waterfall 先 `snapshot()`（克隆 Arc 列表、释放
hooks 锁）再锁外调用。`Hooks<T>`(`:458-500`)：`{next:AtomicU64, hooks:Mutex<Vec<(u64,Arc<T>)>>}`，注册
返回按 id 移除的 Disposer，`snapshot()` 克隆，`is_empty()` 快路径。

## 9. 锁纪律、不变量与 Go 移植含义

### 9.1 锁清单与纪律

| 层 | 锁 | 顺序约束 |
|---|---|---|
| L1 | `FiberData`（每 fiber 一把） | 先于 L2；持锁时**不调用 embedder 代码**（name/inject/schema/hook/cleanup 全锁外） |
| L2 | `children`、`dep_versions`、`settle_waiters`、`EffectNode`、`Registry.quota` | L1 内获取，不反向 |
| L3 | `Disposer.inner`、`AsyncDisposer.inner`、`DisposeState.waiters` | 在 fiber 锁外（drain 前已 take 列表） |
| R | Registry 各结构独立锁 | `dep_index` 叶子锁（`src/registry.rs:454-458`） |
| C | `ContextInner.service_store/accessors/clock/meta` | 不在 fiber 锁内获取 |
| E | `Events` 监听器 map | 回调前释放锁（snapshot） |

纪律要点：持 fiber 锁不回调 embedder；`mem::take` 列表后在锁外 drain/回调；`publish_transition` 先释放
fiber 锁再发 hook（hook 可合法 re-enter 读 fiber）；Registry 快照式 API 允许瞬时不一致（diagnostics）。
锁中毒一律降级（`map_err(|_| Disposed)`、`.ok()`），内核不因毒锁 panic。

### 9.2 死锁边界（已声明）

| 边界 | 机制 |
|---|---|
| Disposer self-join（sync/async） | drain frame（按 DisposeState 地址）短路，非 thread id；同 worker 无关 joiner 仍等待（R2-1/S-2） |
| 生命周期重入 | 同 token 允许；跨 operation 一律 Busy 不阻塞 |
| `when_idle` 在注册窗口内 | 排除调用线程自己的注册；两线程互相等待注册窗口仍可死锁（不检测） |
| when_idle/await_settled 在 apply 内 | 自己 pending_ops>0 → 等自己，文档禁止（XT-04） |
| sync refresh 遇 parked 帧 | 不 recovery（可能在 reactor 线程同步 drain async cleanup） |
| recovery 与 active unload | has_active_unload 时跳过 recovery，防嵌套 blocking |
| latch 泄漏 | RefreshGuard/RecoveryGuard/各 Guard 全 RAII，panic/取消不永久禁用 refresh/recovery |
| 锁内 cleanup panic | cleanup 全在 fiber 锁外执行；Drop 路径 catch_unwind 包裹 |

### 9.3 不变量 I1–I12 → 实现（`docs/DESIGN.md:155-172`）

| # | 不变量 | 主要实现点 |
|---|---|---|
| I1 | effect 逆序单次；卸载后不留服务/监听器 | 单有序列表+rev drain（`src/fiber.rs:2446-2500`）；Disposer/AsyncDisposer 单次；finish_unload remove_owner（`:2408-2414`）；provide/listener 均为 effect |
| I2 | 激活失败回滚一切 | `fail_fiber_sync/async`(`:1973-2079`)；deferred async 兜底 CORE-038 |
| I3 | panic 不跨内核边界 | contained(`src/internal.rs:83-102`)；apply catch_unwind(`:2141-2196`)；cleanup 逐个 catch；child sweep 逐个 catch；FinalizeGuard/Drop catch；唯一 fail-closed 例外是 events waterfall listener（不在 internal bus） |
| I4 | 依赖变化驱动激活/停用/重算 | inject gate(`:1081-1088`)、dep_versions(`:1091-1099`)、dependencies_satisfied(`:1105-1118`)、refresh 固定点(`src/registry.rs:2123-2357`)、级联撤服务 |
| I5 | 服务身份 (scope,name) 唯一、类型错误报错 | ServiceKey/scope(`src/context.rs:467-541`)；ServiceTypeMismatch(`src/context.rs:2130-2159`) |
| I6 | ctx.set 只改自己的服务；撤销逆序 | provider owner 检查 + provide effect 逆序(`src/context.rs:526-540`、`:1993-2022`) |
| I7 | 生命周期互斥；Disposed 终态 | operation token(§2)；transition 终态仲裁；Failed 稳定、restart 才复活 |
| I8 | 子 fiber 属于父、级联回收 | ownership effect+children DFS+两阶段级联(§6)；busy child 打标+finalize |
| I9 | 替换/重载保身份、失败原子回滚 | replace_plugin*(§5.4)；restore_replace；Registry::replace 多 fiber 回滚+Rollback |
| I10 | 静默立即返回且不漏唤醒 | registry 三原子+waiter 注册后二次检查+recovery-aware 循环 |
| I11 | 取消协作式 per-fiber | cancel_requested+checkpoint(§7.3)；不误伤兄弟 |
| I12 | dispose_all 遇 panic 仍尝试全部 | `src/registry.rs:1409-1463`；panic 优先于 error |

### 9.4 Rust 专有机制 → Go 对应物

| Rust 机制 | 位置 | Go 对应 | 必须重新定义的点 |
|---|---|---|---|
| `Arc<Mutex<FiberData>>` | `src/fiber.rs:148` | `*fiberData`+`sync.Mutex`，Fiber 指针句柄 | 无锁中毒；靠 defer Unlock+不变量 |
| `Weak`（children/parked/root/ownership） | `:1472`、`src/registry.rs:414-442` | `weak.Pointer[T]` 或 id→fiber+活跃判定 | GC 回收时机不同；列表惰性剪枝；disposed 后 parent/uid 不可见需显式清字段 |
| `thread_local ACTIVATION_FRAMES` | `:1810-1817` | activation frame 放进传给 Apply 的 ctx/句柄 | 嵌套激活链式携带；同 fiber 并发嵌套要显式栈 |
| `THREAD_TOKEN`/`CURRENT_OP_TOKEN` | `:3658-3676` | token 入 operation ctx 或显式 `*Operation`；或所有入口发新 token | 「同 goroutine 重入」无法探测→公开 API 需 `Reentrant` 约定（§9.5 R1） |
| `DRAIN_FRAMES`（disposer self-join） | `src/async_effects.rs:80-139` | drain frame 入 cleanup 的 ctx / `DisposeFrom(ctx)` | 无 goroutine id，外部不带 ctx 调 DisposeBlocking 无法识别 self-join→死锁 |
| `catch_unwind`/`resume_unwind` | 遍布 | defer recover；drain 后 `panic(first)` | panic 值任意类型需归一化；Go runtime fatal（throw/OOM）不可 recover（对应 Rust abort 清单） |
| `BoxFuture`/async-await/drop 取消 | `src/exec.rs:26-27`、unload 系列 | `context.Context`+goroutine；取消=ctx.Done()；cleanup 带 ctx | 无 future drop 等价物；必须显式定义未启动恢复/in-flight 不可中断/完成 teardown 而非回滚 |
| Waker/手写 Future/block_on | `src/exec.rs:49-187`、Settled | channel/Cond+WaitGroup；WhenIdle(ctx)/WhenSettled(ctx) | join 进行中 teardown 用 channel/cond 显式实现+self-join 短路 |
| Mutex poisoning | 遍布 | 无此概念 | 去降级分支；需 -race+不变量测试 |
| Drop（FiberData/Disposer 安全网） | `src/fiber.rs:234-330`、`src/effect.rs:60-79` | 显式 Dispose/DisposeAll/Abandon；SetFinalizer 仅 orphan 兜底（非确定） | 「丢最后一个句柄自动清理」无法保留；Lead 已定 Abandon 对应（见 doc.go） |
| `#[non_exhaustive]` enum | `src/fiber.rs:116-125`、`src/internal.rs:243` | 封闭枚举+unknown 约定 | 观测者不能穷举未来状态 |
| AtomicU64/Bool + Ordering | 遍布 | `sync/atomic`（顺序一致） | Go 原子简化 |
| `TypeId`/`Arc<dyn Plugin>` | `src/registry.rs` | `reflect.Type`/接口值；具名 key | 去重键语义重定义 |
| `Send+Sync` 约束 | 编译期 | 无编译期约束 | 回调可能并发；-race+文档承诺 |

### 9.5 Go 移植必须重新定义的关键语义（Top 风险）

- **R1 lifecycle token / activation frame 的「当前执行上下文」**：Rust 用 3 处 thread-local + 1 处
  drain-frame thread-local，Go 无 goroutine id 可移植读取。方案三选一/组合：(a) token/frame 放进
  `context.Context`（生命周期 API 加 ctx 参数，回调传回）；(b) 显式 `*Operation` 贯穿内部
  （update 内部调 restart(op)）；(c) 所有入口一律新 token，仅内部显式 op handle 实现重入——外部
  cleanup 里 `fiber.Dispose()` 将得 LifecycleBusy（与 Rust 同线程 cleanup 可重入不同），需 Lead 裁决。
  activation frame 随 `Apply(ctx,cfg)` 的 ctx 携带（子 `ctx.Plugin` 派生链式继承）。
- **R2 Drop 安全网 / last-handle 语义**：Disposer::drop、AsyncDisposer::drop、FiberData::drop 三处依赖
  「最后强引用 drop」；registry 强持有 fiber + GC ⇒ Go 丢句柄不触发清理。建议显式
  `Dispose/DisposeAsync/DisposeAll` 唯一支持路径；`Abandon()` 对应 FiberData::drop（sync inline、
  async 进 orphan queue）；SetFinalizer 仅极端兜底且文档标注非确定。
- **R3 取消与 join**：AsyncDisposer 的 cancel-restore、join、self-join、fiber unload 的 cancel-safe 收尾
  必须逐条移植；Go ctx 取消协作式、in-flight cleanup 不可中断 → 需显式「未启动恢复/已启动等待或放行」，
  否则泄漏或死锁。
- **R4 panic 归一化**：recover 值任意类型；apply panic→CodePluginFailed（含 `panic: <msg>`）、cleanup panic
  drain 后重抛第一并入 log；FinalizeGuard 类改 defer（注意「先 finalize 后释放 lifecycle 槽」），defer 不可
  panic。
- **R5 级联非根帧仲裁**（§6.4 残余窗口）：建议级联帧也尝试 claim lifecycle slot，Busy 则打
  dispose_requested 跳过，与 sweep 语义统一。
- **R6 弱引用/孤儿/停摆列表**：weak.Pointer+id 兜底；parked fiber 必须在 registry 强持有直到
  recovered/disposed（对应 Rust 注释「stays registered」）。
- **R7 阻塞 executor**：Rust BlockingExecutor 用于 sync disposal 跑 async cleanup；Go 里 async cleanup 即
  `func(ctx) error`，sync 路径直接调用即可，但要保留「sync 上下文跑 async cleanup 可能阻塞」告警和
  self-join 短路。
- **R8 顺序语义逐条核对**：effect 逆序（跨 lane）、ownership effect 位置（ctx.plugin 当时立即 push）、
  deferred newest-first、级联 begin/drain 顺序、finish_unload 的 withdraw→drain→transition、dispose_all
  的 panic 优先——都是观测语义，最易无意改掉。
- **R9 quiescence 记账**：pending_registrations+创建线程排除+pending_ops+parked 判定+recovery 循环必须
  一起移植；「创建线程」不要 goroutine id hack，用显式 operation 结构传递。
- **R10 测试基线**：`src/fiber.rs:3958-4347` 回归测试（sweep 遏制、drop owner 清理、级联回滚、XT-02
  epoch 精确回滚、operation token 安装/还原、同线程假重入拒绝、transition_if_epoch）、`src/registry.rs`
  registration/quiescence 测试、`src/async_effects.rs` orphan 测试是 Go 侧对应测试 baseline。

### 9.6 建议落地顺序

① FiberState+事件（seq、status raw vs lifecycle snapshot 双通道）→ ② effect 有序列表+Disposer+逆序 drain+
panic 归一化 → ③ AsyncDisposer（claim/join/cancel-restore/self-join/blocking 别名）+deferred tail →
④ lifecycle token（先「全入口新 token+内部 op handle」，外部对齐 LifecycleBusy，再评估 ctx-token 补齐
cleanup 重入）→ ⑤ activation epoch（ctx 携带 frame）+XT-02 原子迁移 → ⑥ 两阶段级联+child ownership+
busy child 打标+finalize defer → ⑦ quiescence（registration guard/pending_ops/when_idle recovery 循环）+
parked/recovery → ⑧ 生命周期入口（dispose/restart/update/replace+失败回滚+provider 撤回顺序）→
⑨ internal hooks（9 typed+lifecycle bus+contained+waterfall 语义）→ ⑩ orphan+Abandon+dispose_all →
⑪ 按 upstream 一致性用例逐条对照（CORE-007/008/009/011/017/028/029/030/035/036/038/040/043/044/045/
046/047/049/050/051、XT-01/02/03/04/05/08/10/11、R2-1/S-1/S-2、C1/C2/C3/C4、F1/F4/F5、D-1/D-2、P2-A、
R3-A1/A3/A4）。

## 附录 A：关键函数索引

| 主题 | 位置 |
|---|---|
| effect 树 / 逆序 drain / Disposer | `src/fiber.rs:19-82`、`2446-2500`；`src/effect.rs:24-79` |
| AsyncDisposer / orphan | `src/async_effects.rs:74-629`、`268-424` |
| 状态迁移 / epoch / stale 回滚 | `src/fiber.rs:1123-1208`、`1805-1963` |
| 激活 / 卸载 / 级联 | `src/fiber.rs:2091-2345`、`2347-2440`、`3312-3432`、`2502-3127` |
| child ownership / sweep | `src/fiber.rs:1457-1738` |
| recovery / parked | `src/fiber.rs:2595-2828`、`3144-3292` |
| lifecycle token / pending / settled | `src/fiber.rs:3649-3947` |
| quiescence / registration | `src/registry.rs:687-715`、`863-1008`、`1940-2090` |
| internal hooks / lifecycle event | `src/internal.rs:83-763` |
| 生命周期入口 | `src/fiber.rs:781-1078`、`590-666` |

## 附录 B：语义边界速查

- `Ok(())` **不代表** Active：restart/update/replace 的激活失败是 fiber state，调用方必须查
  `state`/`error_detail`（`src/fiber.rs:838-843`、spec 554-559）。
- 生命周期操作**不排队**：Busy→调用方重试。
- cleanup panic 两套策略：用户直接入口（dispose 等）drain 后**重抛第一个**；refresh/retire/recovery/
  失败激活回滚/child sweep 中 **contained+log**（`docs/CORDIS-RUST.md:503-518`）。
- Disposed 终态；但「Disposed+deferred async cleanup」可再 dispose/dispose_async 排干。
- provider 撤回先于 binding 移除：dependents 在 binding 仍可见时停止注入，之后 provide disposer 才删
  entry/emit None（CORE-042）。
- register/activation 窗口对 `is_idle()` 严格可见，对 `when_idle()` 排除调用方自身注册。
- sync refresh 不 recovery；refresh async/when_idle/dispose_all 才 recovery；parked 帧永不丢。
- `internal/update` hook panic=allow（terminal 仍执行）；策略 hook 必须显式不调 next 才能 veto。
