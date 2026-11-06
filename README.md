# Zeokeel Kernel

[![CI](https://github.com/zeokeel/zeokeel-kernel/actions/workflows/ci.yml/badge.svg)](https://github.com/zeokeel/zeokeel-kernel/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/zeokeel/zeokeel-kernel)](https://github.com/zeokeel/zeokeel-kernel/releases)
[![Go](https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go)](https://go.dev/doc/go1.27)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/zeokeel/zeokeel-kernel.svg)](https://pkg.go.dev/github.com/zeokeel/zeokeel-kernel)

**纯 Go 的可插拔内核**：实现 Cordis 的「时空可组合」上下文范式——Context 树提供作用域，
插件运行在 fiber 上，所有副作用按逆序完整撤销，依赖声明驱动插件的激活与停用。

> **Zeokeel Kernel** 是 Cordis 范式的纯 Go 内核实现：语义对齐 CORE-001…051，Go 包名保留
> `cordis` 以保持与 Cordis 的语义连续性。
>
> 这是**内核**，不是框架：只提供组合原语与生命周期语义。模块装配、权限、沙箱、进程/WASM
> 隔离属于使用它的宿主（embedder）；本仓库额外提供可选的编译期模块注册表 `module/`。

| | 状态 |
| --- | --- |
| 语言 | Go 1.27（泛型方法、`weak`、`sync.WaitGroup.Go`；仅标准库，零第三方依赖） |
| 语义基线 | Cordis CORE-001…051 |
| 测试 | 233 个（内核 56、conformance 154、module 21、archtest 2），**CORE-001…051 全覆盖** |
| 覆盖率 | 聚合约 **79.9%**（`-coverpkg=...kernel,module`） |
| 发布 | 语义化版本 tag；严格 CI；`main` 分支受保护，只允许 PR |

---

## 为什么是「内核」而不是框架

Cordis 把「动态组合」拆成两个正交维度，Zeokeel Kernel 在 Go 中完整保留：

| 维度 | 含义 | Go 实现 |
| --- | --- | --- |
| **时间可组合性**（revertible effects） | 每个副作用都有逆操作，卸载时按逆序执行 | 每 fiber 一条有序 effect 列表；`Disposer`/`AsyncDisposer` 单次幂等；激活失败回滚；cancel-safe 卸载 |
| **空间可组合性**（reactive coeffects） | 组件声明依赖，上下文变化驱动激活/停用/重算 | `Plugin.Inject()` 依赖门控、`Pending` 状态、服务变化触发 refresh、provider epoch/version |
| **统一 Context** | effect 与 coeffect 经过同一个上下文，作用域隔离 | `Context` 树、`Extend`/`Isolate`/`IsolateLabeled`/`Intercept`、按作用域隔离的服务与事件 |

组件（插件 + fiber）是动态组合单位：激活是事务（失败全回滚），生命周期操作互斥，
子插件属于父 fiber（级联卸载）。

### 核心保证

- **生命周期六态机**：`Pending → Loading → Active → Unloading → Disposed`（+ `Failed`），
  更新/替换/卸载互斥、可等待静默、可回滚。
- **结构化清理**：服务、监听器、effect、子插件都归 fiber 所有，逆序、单次、可异步。
- **panic 遏制**：所有调用模块代码的内核边界 `recover`，单模块 panic 不拖垮宿主。
- **静态可审计**：仅标准库；`internal/archtest` 在 CI 中强制内核边界（禁止 `net`/`os`/`io`/`unsafe`/第三方依赖）。

## 快速开始

```bash
go get github.com/zeokeel/zeokeel-kernel
```

```go
package main

import (
	"fmt"
	"os"

	cordis "github.com/zeokeel/zeokeel-kernel"
)

type Greeter struct{}

func (Greeter) Name() string { return "greeter" }

func (Greeter) Activate(ctx cordis.Context, _ cordis.Value) error {
	_, err := ctx.Effect(func() cordis.Effect {
		return cordis.Effect{cordis.CleanupOf(func() { fmt.Println("bye") })}
	})
	return err
}

func main() {
	root := cordis.Root()
	fiber, err := root.Plugin(Greeter{}, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("state:", fiber.State())
	_ = fiber.Dispose()
	_ = root.DisposeAll()
}
```

更多可运行示例见 [`examples/`](examples/)（hello/services/events/async_plugin）。

### 编译期模块注册表（可选宿主层）

内核不是模块系统。多产品共享同一内核、按配置装配模块时，用 `module/` 这一可选层：

```go
import (
	cordis "github.com/zeokeel/zeokeel-kernel"
	"github.com/zeokeel/zeokeel-kernel/module"
)

registry := module.NewRegistry()
_ = registry.Register(
	module.Define("app/db", func(ctx cordis.Context, cfg DBConfig) error {
		_, err := ctx.Provide(DBKey, openDB(cfg))
		return err
	}).AddProvides(module.Provides(DBKey)).AddRequires(module.Requires(cordis.LoggerKey)))

loader, err := module.LoadFile(root, registry, "assembly.json") // 启动时读配置 + 预检 + 装配
```

`module` 层提供：编译期注册表、typed 清单、启动预检（缺失/重复 provider、类型不匹配、
依赖环、坏配置）、required/optional 失败策略与整体回滚、JSON 启动配置、`SafeGo/SafeCall`
panic 遏制。**刻意不做进程内热配置更新**（配置变更=重启），详见
[`docs/COMPILE-TIME-REGISTRY.md`](docs/COMPILE-TIME-REGISTRY.md)。

## 文档

| 文档 | 内容 |
| --- | --- |
| [`docs/GO-DESIGN.md`](docs/GO-DESIGN.md) | Go 侧设计：包布局、API 约定、并发模型、生命周期实现、简化与路线图 |
| [`docs/COMPILE-TIME-REGISTRY.md`](docs/COMPILE-TIME-REGISTRY.md) | 编译期模块注册表设计审查、热配置更新取舍、崩溃隔离边界 |
| [`docs/RELEASE-PREFLIGHT.md`](docs/RELEASE-PREFLIGHT.md) | Go module 发布预检报告与发布步骤 |
| [`docs/REPOSITORY-SETUP.md`](docs/REPOSITORY-SETUP.md) | 仓库配置：分支保护、必需 CI 检查、合并策略、发布流程 |

## 开发

```bash
make check      # gofmt + go vet
make build      # go build ./...
make test       # go test ./...
make race       # go test -race ./... -timeout 10m
make ci         # check + build + test
make preflight  # 发布预检：CI 门禁 + module 卫生 + 覆盖率 + 交叉编译 + API 文档 + 发布元数据
```

要求：Go **1.27+**；仅使用标准库；提交前 `scripts/ci.sh` 必须全绿。

### 目录结构

```
.
├── *.go                  # package cordis：内核
│   ├── context.go        #   Context / 作用域 / 服务 / effect / 事件 / 插件加载
│   ├── fiber.go          #   fiber 状态机 / effect 所有权 / 生命周期 / 级联卸载
│   ├── registry.go       #   PluginKey / PluginRuntime / Registry / quiescence / replace
│   ├── service.go        #   ServiceKey[T] / 服务条目 / ServiceGate / accessor / meta
│   ├── events.go waterfall.go internal.go
│   ├── effect.go async_effects.go exec.go
│   ├── config.go logger.go l2.go value.go error.go
│   └── op_token.go runtime_goid.go contains.go doc.go
├── conformance/          # 一致性测试（CORE-001…051 全覆盖）
│   ├── lifecycle/  services/  runtime/
├── module/               # 可选宿主层：编译期模块注册表 + 启动装配 + L0 崩溃契约
├── examples/             # 可运行示例（hello/services/events/async_plugin）
├── docs/                 # 架构/可行性/设计/对照/发布与仓库配置文档
├── internal/archtest/    # 内核边界与导出 API 静态检查
├── scripts/              # ci.sh、release-preflight.sh、doccheck.go、setup-github.sh
├── .github/              # CI / release workflow、PR/issue 模板、CODEOWNERS、dependabot
├── Makefile  go.mod  LICENSE
```

## CI 与分支策略

`main` 为受保护分支：**禁止直接 push，所有变更必须通过 PR**，且合并前必须通过以下
GitHub Actions 检查：

| 必需检查 | 内容 |
| --- | --- |
| `lint` | `gofmt`、`go vet`、`go mod tidy` diff、`go mod verify`、零外部依赖、导出 API doc 检查 |
| `test` | `go test ./... -count=1` |
| `race` | `go test -race ./... -count=1` |
| `coverage` | 聚合覆盖率 ≥ 75% |
| `cross` | linux/arm64、darwin/arm64、windows/amd64 等交叉编译 |
| `govulncheck` | 标准库/依赖漏洞扫描 |

分支保护通过 GitHub API 配置（需要仓库 admin）：

```bash
./scripts/setup-github.sh            # 配置合并策略 + main 分支保护/规则
```

细节见 [`docs/REPOSITORY-SETUP.md`](docs/REPOSITORY-SETUP.md)、[`CONTRIBUTING.md`](CONTRIBUTING.md)。
发布：推送 `v*` tag 触发 [`release.yml`](.github/workflows/release.yml)，先跑严格 CI 再创建
GitHub Release（发布步骤见 [`docs/RELEASE-PREFLIGHT.md`](docs/RELEASE-PREFLIGHT.md)）。

## 已知边界（Cordis 语义的有意简化）

- **Go 无析构器**：确定性清理通过显式 `Dispose/DisposeAll/Abandon` + 有界 orphan 队列承担；
- **panic 遏制只到同 goroutine**：模块自己的 goroutine 必须用 `module.SafeGo/SafeCall`；
  fatal/OOM/死循环只能靠进程 supervisor（L1 隔离按需再加，见 COMPILE-TIME-REGISTRY §6）；
- **无进程内热配置更新**：配置变更=重启进程（编译期路线的有意简化）；
- **模块加载器是最小子集**：启动装配 + 预检 + 定位；无动态 ESM/HMR、无 include/group 完整语义；
- **部分官方 4.1 特性未实现**：Proxy 服务属性、callable services、`ctx.mixin`、装饰器、
  volatile config、`internal/commit`、loader 生态等（后续可补）。

## License

- 本仓库：[MIT](LICENSE)，Copyright (c) 2026 Zeokeel Kernel contributors。
- Zeokeel Kernel 是 Cordis 范式的独立 Go 实现。
