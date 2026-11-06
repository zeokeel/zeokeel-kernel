# 发布预检报告（Go module）

> 状态（2026-11-06，`go1.27.3 linux/amd64`）：**本地门禁全部通过；发布仍有 1 个阻塞项
> （仓库未发布到 module path），2 个非阻塞提醒。** 预检脚本：
> [`scripts/release-preflight.sh`](../scripts/release-preflight.sh)（也可 `make preflight`）。

## 1. 模块信息

| 项 | 值 |
| --- | --- |
| module path | `github.com/zeokeel/cordis-go` |
| `go.mod` go directive | `go 1.27` |
| 外部依赖 | 无（仅标准库，`go mod verify` / `go mod tidy -diff` 均干净） |
| 发布包 | `github.com/zeokeel/cordis-go`、`github.com/zeokeel/cordis-go/module` |
| 测试 | 233 个（root 56、conformance 154、module 21、archtest 2） |
| 覆盖率（`-coverpkg=cordis,cordis/module`） | 79.9% |

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
| git remote 与 module path 匹配 | ❌ **阻塞**（未配置 remote，module 未发布） |
| 语义化版本 tag | ⚠️ 无（首次发布前创建，如 `v0.1.0`） |
| `reference/` 体积 | ⚠️ 2.1MB 位于 module root，会被打进 Go module zip |

汇总：**26 passed / 2 warnings / 1 failed**（failed = 发布元数据阻塞，本地代码门禁全绿）。

## 3. 阻塞项与修复

### 3.1 模块未发布（唯一 blocker）

`go.mod` 声明 `github.com/zeokeel/cordis-go`，但仓库没有 `origin` remote，无法 `go get`。
发布顺序：

```bash
# 1) 在 github.com/zeokeel/cordis-go 创建仓库并推送（module path 必须与仓库地址一致）
git remote add origin git@github.com:zeokeel/cordis-go.git
git push -u origin master

# 2) 确认 GitHub CI 通过（.github/workflows/ci.yml 使用 Go 1.27.x）

# 3) 打 tag（首次发布建议 v0.1.0；v0 阶段 API 仍可调整）
git tag -a v0.1.0 -m "cordis-go v0.1.0"
git push origin v0.1.0

# 4) 从干净环境验证模块可被消费（需等 Go proxy 索引，通常几分钟）
cd "$(mktemp -d)" && go mod init example.com/check
go get github.com/zeokeel/cordis-go@v0.1.0
go doc github.com/zeokeel/cordis-go | head
```

本地预检（`make preflight`）在发布前应保持全绿（除“无 remote/tag”之外）。

## 4. 非阻塞提醒

1. **`reference/` 2.1MB 在 module root**：Go module zip 会包含它，消费者会下载 Rust 参考源码。
   可接受（文档价值），但若想减小模块体积，可在发布前移出 module（独立 repo/submodule）或接受该成本。
   注意 module zip 上限远大于此，不影响可发布性。
2. **Go 1.27 工具链门槛**：模块使用 Go 1.27 泛型方法，`go.mod` 声明 `go 1.27`，消费者必须使用
   ≥1.27 的工具链；README 已注明。发布说明里应再次强调。
3. **CI 只跑 Linux**：交叉编译由发布预检脚本覆盖；如需持续保证，可在 workflow 增加
   `GOOS/GOARCH` build matrix。
4. **未安装 staticcheck / govulncheck**：本机与 CI 都未运行；零第三方依赖降低了风险，
   但标准库漏洞扫描（govulncheck）建议在发布 CI 中补一条（需要 vuln DB 网络）。
5. **无 CHANGELOG**：首个 tag 前可选补充；不影响模块可消费性。

## 5. 发布检查清单（执行顺序）

- [ ] 本地 `git status` 干净；`make ci` + `make preflight` 通过（除未发布 remote/tag 提示）
- [ ] `go.mod` module path、`go` directive、LICENSE、README 与发布仓库一致
- [ ] 创建远端仓库并推送；GitHub Actions 全绿
- [ ] 打 annotated tag `vX.Y.Z` 并推送
- [ ] 干净临时模块 `go get <module>@vX.Y.Z` 成功，`go doc` 可见
- [ ] 在 release notes 注明 Go ≥1.27、stdlib-only、模块契约（启动装配 + L0，无热配置更新）
