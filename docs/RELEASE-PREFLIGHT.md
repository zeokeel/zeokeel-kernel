# 发布预检与发布状态（Go module）

> 模块：`github.com/zeokeel/zeokeel-kernel` ｜ 状态：**已发布 `v0.1.0`**
> （https://github.com/zeokeel/zeokeel-kernel/releases/tag/v0.1.0）
> 预检脚本：[`scripts/release-preflight.sh`](../scripts/release-preflight.sh)（也可 `make preflight`）。

## 1. 模块信息

| 项 | 值 |
| --- | --- |
| module path | `github.com/zeokeel/zeokeel-kernel` |
| `go.mod` go directive | `go 1.27` |
| 外部依赖 | 无（仅标准库；`go mod verify` / `go mod tidy -diff` 干净） |
| 发布包 | `github.com/zeokeel/zeokeel-kernel`、`github.com/zeokeel/zeokeel-kernel/module` |
| 测试 | 233 个（root 56、conformance 154、module 21、archtest 2） |
| 覆盖率（`-coverpkg=...kernel,.../module`） | 79.9% |
| 已验证的消费方式 | 干净临时模块 `go get github.com/zeokeel/zeokeel-kernel@v0.1.0` 成功 |

## 2. 本地门禁结果（clean worktree）

| 门禁 | 结果 |
| --- | --- |
| `gofmt -l`（全仓库 Go 文件） | ✅ clean |
| `go vet ./...` | ✅ |
| `go build ./...` | ✅ |
| `go mod verify` / `go mod tidy -diff` | ✅ |
| `go.mod` 无 `replace/exclude/retract` | ✅ |
| 零外部模块依赖（stdlib only） | ✅ |
| `go test ./... -count=1` | ✅ |
| `go test -race ./... -count=1` | ✅ |
| 聚合覆盖率 ≥ 75% | ✅ 79.9% |
| 交叉编译 linux/amd64、linux/arm64、darwin/amd64、darwin/arm64、windows/amd64 | ✅ |
| 示例运行 smoke（hello/services/events/async_plugin） | ✅ |
| 导出 API doc comment（`scripts/doccheck.go`） | ✅ 485/485 |
| README / LICENSE / go.mod / CI workflow / docs | ✅ |
| git remote 与 module path 匹配且可达 | ✅ |
| 版本 tag | ✅ `v0.1.0` |

最终预检：**passed=29 / warnings=0 / failed=0 → READY**。

## 3. 仓库与发布状态

| 项 | 值 |
| --- | --- |
| 仓库 | https://github.com/zeokeel/zeokeel-kernel（public） |
| 默认分支 | `main`（已从 `master` 重命名） |
| 合并策略 | squash-only、线性历史、合并后自动删除分支 |
| 分支保护 | PR-only；`enforce_admins: true`；禁止 force push / 删除；strict required checks；对话必须 resolved |
| 必需检查 | `lint`、`test`、`race`、`coverage`、`cross`、`govulncheck`（全部在 GitHub Actions 上通过） |
| 批准数 | 0（单人维护；增加第二名维护者后 `MIN_APPROVALS=1 ./scripts/setup-github.sh`） |
| 发布 tag | `v0.1.0`（annotated），release workflow 已创建 GitHub Release |
| 发布自动化 | `.github/workflows/release.yml`：tag 触发 → `scripts/ci.sh` → `gh release create --generate-notes` |

分支保护/合并策略配置脚本：[`scripts/setup-github.sh`](../scripts/setup-github.sh)；说明见
[REPOSITORY-SETUP.md](REPOSITORY-SETUP.md)。

## 4. 后续发布流程

1. 所有变更走 PR 到 `main`，必需 CI 检查全绿后 squash merge；
2. 本地 `make preflight`（应 0 warning / 0 fail；tag 前会在 “no semantic-version tag” 处提示）；
3. 打 annotated tag 并推送：`git tag -a vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`；
4. release workflow 重跑严格 CI 并自动创建 GitHub Release；
5. 在干净临时模块验证 `go get github.com/zeokeel/zeokeel-kernel@vX.Y.Z`。

## 5. 非阻塞提醒

1. **Go 1.27 工具链门槛**：模块使用 Go 1.27 泛型方法，消费者必须 ≥1.27；README 与 release notes 已注明。
2. **无 CHANGELOG**：发布说明由 release workflow 自动生成（`--generate-notes`）；如需人工
   changelog 可后续补充。
3. **默认批准数 0**：仅适用于单人维护；增加维护者后应把 `MIN_APPROVALS` 调到 ≥1 并重新应用脚本。

## 6. 发布检查清单（v0.1.0 已全部完成）

- [x] `main` clean，`make ci` + `make preflight` 通过
- [x] module path / go directive / LICENSE / README 与发布仓库一致
- [x] 远端仓库创建并推送，GitHub Actions 严格 CI 全绿
- [x] `main` 分支保护 + 必需检查 + squash-only 生效
- [x] annotated tag `v0.1.0` 已推送
- [x] release workflow 创建 GitHub Release
- [x] 干净环境 `go get ...@v0.1.0` 成功
