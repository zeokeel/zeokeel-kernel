# Cordis（Go 内核）

**纯 Go 的可插拔内核**：实现 Cordis 的「时空可组合」上下文范式——Context 树提供作用域，插件运行在 fiber 上，所有副作用可按逆序完整撤销，依赖声明驱动插件的激活与停用。

> 这是 **Go 重实现**，上游是纯 Rust 的 [zeokeel/zeokeel-cordis](https://github.com/zeokeel/zeokeel-cordis)（v0.6.0，MIT）。上游仓库的完整源码已作为只读参考放在 [`reference/zeokeel-cordis-rust/`](reference/zeokeel-cordis-rust/) 下，用于逐项对照语义。
>
> 这是**内核**，不是框架：只提供组合原语与生命周期语义。模块加载、热更新、权限、沙箱、进程/WASM 隔离都属于使用它的宿主（embedder）。

| | 状态 |
| --- | --- |
| 语言 | Go 1.27（仅标准库，零第三方依赖） |
| 语义基线 | 上游 Rust v0.6.0 的 `spec/CORDIS-CORE.md`（CORE-001…051） |
| 实现方式 | 按 Go 语言特性重写（泛型、`context.Context`、goroutine、`recover`、GC），不是逐行直译 |
| 一致性测试 | `conformance/` 目录，逐条对应 CORE 用例，`-race` 稳定 |

---

## 分析结论：Go 能不能做到？

**能，且语义可以对齐到可观察行为级别。** 详细论证见：

| 文档 | 内容 |
| --- | --- |
| [`docs/ARCHITECTURE-ANALYSIS.md`](docs/ARCHITECTURE-ANALYSIS.md) | Rust 内核架构全解：分层、数据结构、状态机、锁纪律、不变量、CORE 映射 |
| [`docs/GO-FEASIBILITY.md`](docs/GO-FEASIBILITY.md) | Rust→Go 逐项语言特性映射、语义差异、风险与缓解、可行性结论 |
| [`docs/GO-DESIGN.md`](docs/GO-DESIGN.md) | Go 侧设计：包布局、API 约定、并发模型、生命周期实现、已知简化与路线图 |
| [`docs/analysis/`](docs/analysis/) | 5 份上游源码深度分析笔记（带 `file:line` 引用） |

核心判断（摘要）：

| Rust 机制 | Go 方案 | 结论 |
| --- | --- | --- |
| 所有权 / `Arc` / `Weak` | GC + （可选）`weak.Pointer` | 等价：GC 负责存活，*确定性清理*改由显式生命周期 API 承担 |
| `Drop` 兜底清理 | `Dispose`/`DisposeAll`/`Abandon` + orphan 队列 | **语义重新定义**：Go 无析构器，必须显式 dispose（文档中列为与 Rust 的差异） |
| `catch_unwind` panic 遏制 | `recover`（同 goroutine） | 等价（水文流 waterfall 保持 fail-closed 重抛） |
| `thread_local`（激活帧/操作 token/drain frame） | goroutine id 注册表 + `context.Context` 携带 drain frame | 等价到用例级（见 `runtime_goid.go` 的说明） |
| future / `BoxFuture` / 取消=drop | `context.Context` + 阻塞函数 | 语义映射：`ctx` 取消 ≈ drop future；A-9 取消完成 teardown 可复现 |
| `TypeId` / `?Sized` 双 lane | `reflect.Type` + 泛型 `ServiceKey[T]` + 包级泛型函数（Go 不支持泛型方法） | 等价且更简洁（Go 接口天然「unsized」） |
| feature gate（`supervision`） | 常开（记账成本是每 fiber 几个原子量） | 有意简化 |
| `panic = "unwind"` 编译期 guard | Go 无等价（`recover` 只对同 goroutine 的 panic 有效） | 文档声明边界 |

---

## 设计思想（与上游一致）

Cordis 把「动态组合」拆成两个正交维度：

| 维度 | 含义 | Go 内核中的落地 |
| --- | --- | --- |
| **时间可组合性**（revertible effects） | 组件的每个副作用都有逆操作，卸载时按逆序执行，系统回到组件加载前 | 每 fiber 一条有序 effect 列表；`Disposer`/`AsyncDisposer` 单次幂等；激活失败回滚；cancel-safe 卸载 |
| **空间可组合性**（reactive coeffects） | 组件声明自己依赖哪些上下文，上下文变化驱动组件的激活/停用/重算 | `Plugin.Inject()` 依赖门控、`Pending` 状态、服务变化触发 refresh、provider epoch/version |
| **统一 Context**（context paradigm） | effect 与 coeffect 都经过同一个上下文，作用域隔离使不同组件互不干扰 | `Context` 树、`Extend`/`Isolate`/`IsolateLabeled`/`Intercept`、按作用域隔离的服务与事件 |

组件（插件 + fiber）是动态组合的单位：激活是事务（失败全回滚），生命周期操作互斥，子插件属于父 fiber（级联卸载）。

### fiber 状态机

```
Pending ──(依赖满足)──► Loading ──(apply 成功)──► Active
   ▲                       │                        │
   │                       ├─(apply Err/panic)──► Failed
   │                       │(生效期间被 dispose)     │
   └───────────────────────┴──────────────────────► Unloading ──► Disposed
```

### 12 条不变量

上游 `docs/DESIGN.md` §5 的 I1–I12 在 Go 实现中逐条保留（effect 逆序单次、激活失败回滚、panic 不跨越内核边界、依赖变化驱动重算、服务身份 `(scope,name)` 唯一且类型错误必须报错、`set` 可逆、生命周期互斥、子 fiber 级联回收、replace 身份保持与原子回滚、quiescence、协作取消、`dispose_all` 全量尝试）。

---

## 快速开始

```bash
go get github.com/zeokeel/cordis-go
```

```go
package main

import (
	"fmt"

	cordis "github.com/zeokeel/cordis-go"
)

type Greeter struct{}

func (Greeter) Name() string { return "greeter" }

func (Greeter) Activate(ctx cordis.Context, config cordis.Value) error {
	who := "world"
	if object, ok := config.(map[string]cordis.Value); ok {
		if name, ok := object["name"].(string); ok {
			who = name
		}
	}
	fmt.Printf("hello, %s\n", who)
	// 清理逻辑归 fiber 所有：卸载时逆序、恰好一次执行。
	_, err := ctx.Effect(func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() { fmt.Printf("bye, %s\n", who) })}
	})
	return err
}

func main() {
	root := cordis.Root()
	fiber, err := root.Plugin(Greeter{}, map[string]cordis.Value{"name": "Cordis"})
	if err != nil {
		panic(err)
	}
	_ = fiber.Dispose()
	_ = root.DisposeAll() // 宿主退出时的标准关闭路径
}
```

可运行示例：[`examples/hello`](examples/hello)、[`examples/services`](examples/services)、[`examples/events`](examples/events)、[`examples/async_plugin`](examples/async_plugin)：

```bash
go run ./examples/hello
go run ./examples/services
go run ./examples/events
go run ./examples/async_plugin
```

### 核心 API 一览

| 概念 | 入口 |
| --- | --- |
| Context / 作用域 | `cordis.Root()`、`ctx.Extend()`、`ctx.Isolate(name)`、`ctx.IsolateLabeled(name, label)`、`ctx.Intercept(name, config)` |
| 插件 | `Plugin` / `AsyncPlugin` 接口；可选 `PluginKeyer`/`PluginInjector`/`PluginConfigSchema`；`ctx.Plugin(p, config)`、`cordis.PluginTyped[P]`、`ctx.PluginAsync` |
| 服务 | `cordis.NewServiceKey[T]`、`cordis.Provide/Get/Require/ProvideChecked/ProvideGated`、`ctx.SetNamed`、`ctx.SetRevertibleNamed`、`ctx.ServiceAvailable`、`ctx.GetNamed` |
| Effect | `ctx.Effect/EffectLabeled/EffectAsync/EffectFuture/EffectStream`、`ctx.AttachEffect`、`cordis.Disposer`/`AsyncDisposer` |
| 事件 | `ctx.On/Once/OnWithOptions/OnAsync/OnWaterfall/OnWaterfallAsync`、`ctx.Emit/Bail/ParallelAsync/SerialAsync/Waterfall` |
| fiber 生命周期 | `Fiber.{State,Restart,RestartAsync,Update,UpdateAsync,ReplacePlugin,Dispose,DisposeAsync,Abandon,AwaitSettled,Cancel}` |
| 注册表 | `Registry.{Size,Get,Keys,Values,Entries,ForEach,Fibers,Delete,Replace,ReplaceAsync,IsIdle,WhenIdle,DisposeAll}` |
| 内部 hook | `ctx.OnInternal{Plugin,Status,Service,Config,Update,Get,Set,Listener,Dispatch}`、`ctx.OnLifecycle` |
| 日志 / 配置 / 时钟 | `ctx.Logger()`、`ctx.LogExporter()`、`cordis.NewJsonConfigSchema()`、`ctx.ResolveConfig`、`ctx.Accessor`、`ctx.SetClock` |
| 监督（L2） | `cordis.FiberQuota`、`registry.SetQuota`、`fiber.Heartbeat/IdleFor`、`registry.StalledFibers`、`cordis.DetectIsolationCapabilities` |

Go 的硬约束：**方法不能带类型参数**，因此 typed 服务读写是包级泛型函数：

```go
key := cordis.NewServiceKey[*Config]("config")
cordis.Provide(ctx, key, cfg)
cfg, ok := cordis.Get(ctx, key)
cfg, err := cordis.Require(ctx, key)
```

---

## 与 Rust 上游的差异（Go 语义适配）

完整清单见 [`docs/GO-FEASIBILITY.md`](docs/GO-FEASIBILITY.md) §4，要点：

- **无 `Drop` 兜底**：未 dispose 的 fiber 不会因句柄被 GC 而清理。用 `Dispose`/`DisposeAll`；无法显式清理时用 `Fiber.Abandon()`（同步清理立即执行，异步清理进入有界 orphan 队列，可用 `cordis.DrainOrphanAsyncCleanups()` 回收）。
- **异步 = `context` + goroutine**：`AsyncPlugin.ActivateAsync(ctx, ...)`、`AsyncCleanup func(ctx) error`；取消 `ctx` 是 Rust「drop future」的 Go 等价物。A-9（取消卸载仍完成 teardown、异步清理 defer 到下次 dispose）已实现。
- **自 join 检测**：Rust 用线程/任务本地 drain frame；Go 用 `context.Context` 携带 drain frame —— 清理函数调用自身 disposer 时，**必须把它收到的 ctx 传回去**才能触发 self-join 短路（传 `nil`/`context.Background()` 会等待）。
- **生命周期重入判定**：Rust 用 operation token + 线程本地帧；Go 用 goroutine id 注册表（`runtime_goid.go`，唯一一处读取 runtime 栈的位置，只在生命周期入口的冷路径调用）。
- **refresh 实现**：依赖变化后的 dependents 刷新用简化版反向依赖索引（`inject 名 → fiber`）+ `dirtyServices` 快路径，只重算受影响的 fiber；显式 `Refresh/RefreshAsync` 仍是全量 fiber 扫描 + 有界固定点（≤64 轮）。上游的 parked-cascade 恢复（F1/F4/F5）未移植：A-9 取消语义让取消卸载直接完成 teardown，因此不会产生 parked 帧。
- **supervision 常开**：上游是 cargo feature；Go 记账成本低，不做编译期开关。
- **内核诊断日志**：上游用 `tracing`；Go 用标准库 `log/slog`（`slog.Default()`，宿主可替换 handler）。服务日志（`LoggerService`）仍是独立的 ring buffer + exporter 通道，默认无 exporter。

---

## 内核 / 宿主边界

**内核负责**：context/作用域、服务注入、fiber 生命周期、effect 所有权与回滚、事件、配置 schema、日志、静态插件注册表。

**宿主负责**：模块加载器与热更新、定时器/事件循环、权限与审计、资源配额策略、线程/fd 计量、进程隔离（L3）或 WASM 沙箱（L4）、UI/产品契约。

内核只依赖 Go 标准库；边界由 [`internal/archtest`](internal/archtest) 的架构测试在 CI 中强制（禁止 `net`/`os`/`io`/`unsafe`/第三方依赖等），对应上游 `tests/kernel_boundary.rs` 的思路。

---

## 质量与严格构建

```bash
make check   # gofmt + go vet
make build   # go build ./...
make test    # go test ./...
make race    # go test -race ./... -timeout 10m
make ci      # check + build + test
```

当前基线（本地实测）：

- `go test ./...`：内核根包（55 个 smoke/edge 测试）+ `conformance/lifecycle`（67）、
  `conformance/services`（36）、`conformance/runtime`（51）+ `internal/archtest`（2）全部通过
- `go test -race ./...`：通过；并发/异步/生命周期用例 `-count=5..20` 稳定
- 合并覆盖率 **80.7%**（`go test -coverpkg=github.com/zeokeel/cordis-go -coverprofile ... . ./conformance/...`）
- `go vet ./...`、`gofmt -l` 干净；`scripts/ci.sh`（gofmt/vet/build/test/race）全绿
- 一致性测试逐条对应 `conformance/` 下的 `TestCoreNNN_*`：**CORE-001…051 全覆盖**
  （映射表见 [`docs/GO-DESIGN.md`](docs/GO-DESIGN.md) §10）

---

## 目录结构

```
.
├── *.go                      # package cordis：内核
│   ├── context.go            #   Context / 作用域 / 服务 / effect / 事件入口 / 插件加载
│   ├── fiber.go              #   fiber 状态机 / effect 所有权 / 生命周期 / 级联卸载
│   ├── registry.go           #   PluginKey / PluginRuntime / Registry / quiescence / replace
│   ├── service.go            #   ServiceKey[T] / 服务条目 / ServiceGate / accessor / meta
│   ├── events.go waterfall.go internal.go
│   ├── effect.go async_effects.go exec.go
│   ├── config.go logger.go l2.go value.go error.go
│   └── op_token.go runtime_goid.go contains.go doc.go
├── conformance/              # 一致性测试（package *_test）
│   ├── lifecycle/            #   CORE-001/007…012/026…030/035…039/043…051
│   ├── services/             #   CORE-002…006/010/013…016/022/025/032/033/040/041/042/048
│   └── runtime/              #   CORE-017…021/023/024/031/049
├── examples/                 # 可运行示例（hello/services/events/async_plugin）
├── internal/archtest/        # 内核边界架构测试（go/parser）
├── docs/                     # 架构分析 / 可行性 / Go 设计 / 上游分析笔记
├── reference/zeokeel-cordis-rust/  # 上游 Rust 源码只读参考（v0.6.0）
├── Makefile  scripts/ci.sh  .github/workflows/ci.yml
└── go.mod
```

---

## 上游、论文与许可证

- 上游实现：<https://github.com/zeokeel/zeokeel-cordis>（MIT）
- 论文：**A Programming Paradigm for Spatiotemporal Composability**，Yifan Shi、Wei Zhang、Tianyi Cui，arXiv:2608.25512，<https://arxiv.org/abs/2608.25512>

本项目为 MIT 许可（见 [LICENSE](LICENSE)）。`reference/` 下为上游源码副本，版权归上游作者；论文版权归原作者。
