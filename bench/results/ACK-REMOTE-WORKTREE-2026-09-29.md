# ACK 集群实测（2026-09-29）— 跨节点远端工作区

**目的**：本机所有测量都是 loopback（mega2 与 daemon 同机）。架构主张「`libra status` 只查 upper 层、与树规模解耦」只有在**服务端真的远端**时才可观测。本次在 ACK 上把两者**分到不同节点**，专门补这一维。

## 环境

| 项 | 值 |
|---|---|
| 集群 | ACK ManagedKubernetes `ack.standard`，cn-hangzhou，k8s 1.34.10 |
| 节点 | 2 × `ecs.e-c1m2.xlarge`（4c8g） |
| 拓扑 | **mega2 在 `172.18.16.80`，bench pod 在 `172.18.16.81`**（podAntiAffinity 强制分开） |
| 镜像 | `crpi-…/gitmono/gitmono1:v0.3.0-ack-libra`（自建）：daemon 来自 `bench/gitmono-vs-git` 分支（含 pinned-store 性能提交）+ 本轮修复过的 libra（restore/add/ls-files/commit）+ git + python3 |
| 被测树 | 在集群内生成并推入 mega2 的合成树，**20,001 个文件**（svc??/pkg???/mod??/f?????.rs） |
| 跨节点 RTT | **一次 `GET /worktrees/{id}/state` = 12 ms**（本机 loopback 为 ~0.05 ms）|

## 结果

| 指标 | 值 | 说明 |
|---|---|---|
| **`git clone`（远端，20k 文件）** | **62,217 ms** | git 侧"拿到可工作副本"的代价 |
| **`libra worktree add`（attach）** | **1,044 ms** | mono 侧同一件事 |
| → 就绪时间之比 | **60×** | **核心结论** |
| `libra status` | 580 / 600 / 552 ms | fast path（只查 upper） |
| `git status`（本地 clone） | 229 / 218 ms | git 仍更快（见下"诚实边界"） |
| `libra add -A`（0 改动 / 1 改动） | 155 / 167 ms | A 修复生效 |
| **`libra ls-files`** | **88 ms** | B#1 生效（纯 index dump） |
| **`libra ls-files -t`**（需工作区状态，对照） | **130,176 ms** | 同一条代码路径，**1,479× 差距** |
| `libra commit` | 10,637 ms | B#2 生效但本树仅 20k 条，收益不如 124k 时显著 |
| 工作区占用 | mount 1,320,115 B + store 11,607,219 B | |
| 播种 | 10 批 × 2000 文件，全部成功 | |

## 结论

1. **就绪时间是最大赢面，且在远端被放大**：同一棵 20k 文件树，`git clone` 62.2 s vs `libra worktree add` **1.04 s（60×）**。git 必须把整棵树物化到本地，mono 只钉一个 revision。
2. **B#1 在真实集群上被直接证实**：`ls-files` 88 ms vs `ls-files -t` 130 s —— 同一条代码路径，唯一差别是"要不要算工作区状态"。修复前两者都是 130 s 量级。
3. **A 生效**：`add -A` 155/167 ms（本机 124k 树上修复前是 100–121 s）。
4. **restore 修复生效**：`worktree add` 1.04 s（修复前本机 41 s，且本机数字与树规模无关，量级可直接对照）。
5. **诚实边界**：`git status`（218–229 ms）**仍然快于** `libra status`（552–600 ms）——在这棵 20k 本地 clone 上。这与本机结论一致：libra 的 ~0.55 s 常数打不过 git 在**本地盘**上的 O(树) 成本。mono 的赢面在"不物化"和"树很大/工作区不在本地盘"时，不在这条。

## 踩到的坑（都记在这里，避免重复）

| 坑 | 症状 | 修法 |
|---|---|---|
| `snat_entry` + 反亲和 | — | `snat_entry: true`；bench pod 用 `podAntiAffinity` 对 `app: mega2` 强制异构 |
| ACK 自动节点池 | 上次多一个池 → 4 台 ECS 成本翻倍 | 本次 `num_of_nodes: 0` 后**只建一个池**，验证 pools=1 |
| **HTTP push 超 1 MiB 即断连** | `git push` 报 `the remote end hung up unexpectedly`，1.4 s 即失败 | git 超过 `http.postBuffer`（默认 1 MiB）改用 **chunked 传输**，服务端不接受 → **调大 `http.postBuffer` + 分批推送**（每批 2000 文件）|
| WSL 里调 Windows CLI | `/c/Users/...` 不存在 | 用 `/mnt/c/Users/...` |
| `--body @file` 不生效 | 服务端收到字面 `@` | CLI 是 Windows 二进制，不认 `/mnt/...` 路径 → **内联 `--body "$(cat f)"`** |
| YAML 里内嵌脚本 | heredoc 内容顶格 → 块标量提前终止 → `could not find expected ':'` | 脚本放独立文件，用生成器产 YAML（缩进由程序保证）；且**不能用 heredoc**（ConfigMap 统一缩进后 `PY` 终止符不再顶格）→ 改 `python3 -c '...'` |
| scorpiofs 运行时镜像缺工具 | 没有 `git` / `python3` | overlay 层 `apt-get install git python3`（旧方案因此需要独立 `base-git` sidecar）|

## 成本

2 节点 × 4c8g + NAT ≈ **¥1.9/小时**。实测用完即删；删除后核对 clusters/ECS/NAT/EIP/SLB 全部归零。
