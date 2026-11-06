# 仓库配置（main 保护 / 只允许 PR / 严格 CI）

目标：`main` 是唯一长期分支；**任何人（包括 admin）都不能直接 push**，所有变更必须经过 PR，
且 PR 必须通过严格 CI。配置以脚本 + 文档形式固化，便于仓库迁移/重建时复现。

## 1. 分支与合并策略

| 项 | 配置 |
| --- | --- |
| 默认分支 | `main` |
| 直接 push / force push / 删除 `main` | ❌ 禁止 |
| 允许的合并方式 | Squash merge（保持线性历史；禁用 merge commit 与 rebase merge） |
| 合并后删除分支 | ✅ |
| 必需 CI 检查 | `lint`、`test`、`race`、`coverage`、`cross`、`govulncheck` |
| 分支最新性（strict） | ✅ PR 必须基于最新 `main` 且 CI 在最新提交上通过 |
| 对话未解决 | ❌ 禁止合并（required conversation resolution） |
| 审查批准数 | 默认 0（单人维护可合并自己的 PR）；有第二名维护者后设 `MIN_APPROVALS=1` |
| Admin 绕过 | ❌ 不绕过（`enforce_admins: true`） |

> 单人维护时的取舍：GitHub 不允许作者批准自己的 PR。默认批准数为 0 仍强制 **PR 流程**；
> 若要求 1 个批准，请先增加一名 collaborator，或临时用 admin 关闭保护处理安全事件。

## 2. 一键配置

先决条件：仓库已创建并推送 `main`；CI workflow 至少成功运行过一次（必需检查的 context 才会出现）；
本地 `gh` 已登录且有 admin 权限。

```bash
REPO=zeokeel/zeokeel-kernel ./scripts/setup-github.sh
# 有第二名维护者后：
MIN_APPROVALS=1 ./scripts/setup-github.sh
```

脚本执行：

1. `PATCH repos/{owner}/{repo}`：默认分支 `main`、squash-only、合并后删分支；
2. `PUT repos/{owner}/{repo}/branches/main/protection`：
   PR-only、strict required checks、linear history、禁止 force push/删除、启用对话解决、enforce admins；
3. 尽力开启 vulnerability alerts / automated security fixes。

校验：

```bash
gh api repos/zeokeel/zeokeel-kernel/branches/main/protection | jq
gh api repos/zeokeel/zeokeel-kernel | jq '.default_branch,.allow_squash_merge,.allow_merge_commit'
```

## 3. 严格 CI（`.github/workflows/ci.yml`）

触发：`pull_request`/`push` 到 `main`、手动 `workflow_dispatch`；同一 ref 并发时取消旧运行。

| Job | 检查 |
| --- | --- |
| `lint` | `gofmt`、`go vet`、`go mod verify`、`go mod tidy -diff`、禁止 replace/exclude/retract、零外部依赖、导出 API doccheck |
| `test` | `go test ./... -count=1` |
| `race` | `go test -race ./... -timeout 10m` |
| `coverage` | 聚合覆盖率 ≥ 75%（`-coverpkg` 覆盖 kernel + module） |
| `cross` | linux/amd64、linux/arm64、darwin/amd64、darwin/arm64、windows/amd64 交叉编译 |
| `govulncheck` | `golang.org/x/vuln/cmd/govulncheck@latest ./...` |

`main` 的 required checks 必须与 job 名一致。修改 workflow 的 job 名时要同步更新
`scripts/setup-github.sh` 的 `REQUIRED_CHECKS` 并重新应用分支保护。

## 4. 发布流程

1. 合并 PR 到 `main`，确认 CI 全绿；
2. 本地运行 `make preflight`（见 [RELEASE-PREFLIGHT.md](RELEASE-PREFLIGHT.md)），确认只剩发布元数据提示；
3. 打 annotated tag 并推送：`git tag -a v0.1.0 -m "v0.1.0" && git push origin v0.1.0`；
4. `.github/workflows/release.yml` 在 tag 上重跑 `scripts/ci.sh` 并创建 GitHub Release（自动生成 notes）；
5. 在干净目录验证 `go get github.com/zeokeel/zeokeel-kernel@v0.1.0` 可用。

## 5. 仓库文件清单

- `.github/workflows/ci.yml`：严格 CI；
- `.github/workflows/release.yml`：tag 发布；
- `.github/dependabot.yml`：GitHub Actions 依赖周更；
- `.github/CODEOWNERS`：默认 owner `@zeokeel`；
- `.github/pull_request_template.md`、`.github/ISSUE_TEMPLATE/*`：PR/issue 模板；
- `SECURITY.md`、`CONTRIBUTING.md`：安全与贡献规范；
- `scripts/setup-github.sh`：本仓库服务端配置脚本。
