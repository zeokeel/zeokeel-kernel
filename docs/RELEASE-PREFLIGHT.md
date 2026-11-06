# 发布预检报告（Go module）

> 状态（2026-11-06，`go1.27.3 linux/amd64`）：**本地门禁全部通过；仓库已完成本地配置
> （module path / remote / 分支 / CI），剩余发布动作是创建远端仓库、推送 `main` 与打 tag。**
> 预检脚本：
> [`scripts/release-preflight.sh`](../scripts/release-preflight.sh)（也可 `make preflight`）。

## 1. 模块信息

| 项 | 值 |
| --- | --- |
| module path | `github.com/zeokeel/zeokeel-kernel` |
| `go.mod` go directive | `go 1.27` |
| 外部依赖 | 无（仅标准库，`go mod verify` / `go mod tidy -diff` 均干净） |
| 发布包 | `github.com/zeokeel/zeokeel-kernel`、`github.com/zeokeel/zeokeel-kernel/module` |
| 测试 | 233 个（root 56、conformance 154、module 21、archtest 2） |
| 覆盖率（`-coverpkg=github.com/zeokeel/zeokeel-kernel,.../module`） | 79.9% |

## 2. 门禁结果

| 门禁 | 结果 |
| --- | --- |
| `gofmt -l`（全仓库 Go 文件，排除 `reference/`） | ✅ clean |
| `go vet ./...` | ✅ |
| `go build ./...` | ✅ |
| `go mod verify` | ✅ all modules verified |
| `go mod tidy -diff` | ✅ 无 diff |
| `go.mod` 无 `replace/exclude/retract` | ✅ |
| 零外部模块依赖（stdlib only） | ✅ |
| `go test ./... -count=1` | ✅ |
| `go test -race ./... -count=1` | ✅ |
| 聚合覆盖率 ≥ 75% | ✅ 79.9% |
| 交叉编译 linux/amd64、linux/arm64、darwin/amd64、darwin/arm64、windows/amd64 | ✅ |
| 示例运行 smoke（hello/services/events/async_plugin） | ✅ |
| 导出 API doc comment（`scripts/doccheck.go`） | ✅ 485/485 |
| README / LICENSE / go.mod / CI workflow / docs 存在 | ✅ |
| git remote 与 module path 匹配 | ✅ `origin` = `https://github.com/zeokeel/zeokeel-kernel.git`（尚未 push） |
| git remote 可访问（已发布） | ⚠️ 仓库尚未创建/推送 |
| 语义化版本 tag | ⚠️ 无（首次发布前创建，如 `v0.1.0`） |
| `reference/` 体积 | ⚠️ 2.1MB 位于 module root，会被打进 Go module zip |

汇总：**本地代码门禁全部通过**；`make preflight` 在推送前会把“远端不可达 / 无 tag”作为
非阻塞提醒（远端尚未 push），发布前以实际运行结果为准。

## 3. 阻塞项与修复

### 3.1 模块尚未发布到远端（剩余发布动作）

`go.mod` 声明 `github.com/zeokeel/zeokeel-kernel`，本地 `origin` 已配置为该仓库地址；
但远端仓库尚未创建/推送，因此 `go get` 仍无法解析。发布顺序：

```bash
# 0) 在本地预检（应只剩“远端不可达/无 tag”提醒）
make preflight

# 1) 在 github.com/zeokeel/zeokeel-kernel 创建仓库并推送 main
git push -u origin main

# 2) 确认 GitHub Actions（strict CI）全绿

# 3) 打 tag（首次发布建议 v0.1.0；v0 阶段 API 仍可调整）
git tag -a v0.1.0 -m "zeokeel-kernel v0.1.0"
git push origin v0.1.0
#    release workflow 会再次跑 scripts/ci.sh 并创建 GitHub Release

# 4) 从干净环境验证模块可被消费（等 Go proxy 索引，通常几分钟）
cd "$(mktemp -d)" && go mod init example.com/check
go get github.com/zeokeel/zeokeel-kernel@v0.1.0
go doc github.com/zeokeel/zeokeel-kernel | head
```

推送前可在预检输出中看到 `WARN git remote is not reachable yet` 与 `WARN no semantic-version git tag`，
这两项在完成上述步骤后消失。

## 4. 非阻塞提醒

1. **`reference/` 2.1MB 在 module root**：Go module zip 会包含它，消费者会下载 Rust 参考源码。
   可接受（文档价值），但若想减小模块体积，可在发布前移出 module（独立 repo/submodule）或接受该成本。
   注意 module zip 上限远大于此，不影响可发布性。
2. **Go 1.27 工具链门槛**：模块使用 Go 1.27 泛型方法，`go.mod` 声明 `go 1.27`，消费者必须使用
   ≥1.27 的工具链；README 已注明。发布说明里应再次强调。
3. **CI 覆盖**：strict CI 已包含 `cross`（5 个 GOOS/GOARCH）与 `govulncheck` job；
   本地未安装 `staticcheck`/`govulncheck`，本地以 `make preflight` 的交叉编译覆盖为主。
4. **无 CHANGELOG**：首个 tag 前可选补充；不影响模块可消费性。
5. **仓库配置**：分支保护/squash-only/必需检查由 `scripts/setup-github.sh` 配置，
   见 [REPOSITORY-SETUP.md](REPOSITORY-SETUP.md)；需在仓库创建并推送 main 后执行。

## 5. 发布检查清单（执行顺序）

- [ ] 本地 `git status` 干净；`make ci` + `make preflight` 通过（除未发布 remote/tag 提示）
- [ ] `go.mod` module path、`go` directive、LICENSE、README 与发布仓库一致
- [ ] 创建远端仓库并推送；GitHub Actions 全绿
- [ ] 打 annotated tag `vX.Y.Z` 并推送
- [ ] 干净临时模块 `go get <module>@vX.Y.Z` 成功，`go doc` 可见
- [ ] 在 release notes 注明 Go ≥1.27、stdlib-only、模块契约（启动装配 + L0，无热配置更新）
