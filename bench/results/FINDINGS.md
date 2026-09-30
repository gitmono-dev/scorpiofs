# 三项对比实验 — 结果解读

配套：`RESULTS-SUMMARY.md`（聚合表）、`raw/*.jsonl`（原始记录，可回溯重算）。
环境：WSL2 / 23 GiB RAM；mega2 跑在本地 docker（`127.0.0.1:19000`），ScorpioFS daemon 原生跑
（`127.0.0.1:37251`），libra 0.19.63。被测仓库 `/project` ≈ 124k 文件。

---

## 1. 一句话结论

**mono 侧赢在「不复制」：工作区磁盘占用、多工作区、基线切换都是它的强项；输在「每次
sync 都要摊平整棵树」——`libra sync` 的 finalize 是当前唯一的、也是量级最大的性能瓶颈。**
在本地盘上，git 读/写小改动的延迟全面占优；mono 的优势要么体现在空间，要么需要「远端
工作区」这个前提才成立。

## 2. 实验三（大仓库，124k 文件）——最完整的一组

| 指标 | git | mono | 谁赢 |
|---|---|---|---|
| **INIT** 初次可用 | 17.0 s | 59.2 s | git（3.5×） |
| **INIT** 落盘 | 266 MB | —（见下） | — |
| **READ** 100 文件 · 热 | 121 ms | 267 ms | git（2.2×） |
| **READ** 100 文件 · 真冷首轮 | 262 ms | **25,901 ms** | git（99×） |
| **MULTI** 5 个工作区 · 时间 | **29.0 s** | 265.4 s | git（9.2×） |
| **MULTI** 5 个工作区 · **真实落盘** | 1.50 GB | **94.5 MB** | **mono（16×）** |
| **SWITCH** 基线切换 | 184 ms | **212 ms** | 量级相当（git 略快） |

- `--depth=1` 在 mega2 上无收益（不支持 shallow）。
- **MULTI 的磁盘结论是本轮唯一一处处在同一量纲上的 mono 大胜**：5 次 chain fork 各只产生
  **4 KB** 的 `sealed-*` 层，零拷贝名副其实。代价是 fork 本身约 40 s/个，合计把时间拖到 9.2×。
- **SWITCH 两边都只有第 1 轮是真切换**（之后是 `already_at_target` 空转），所以有效值
  是 git 184ms vs mono 212ms；mono 且不触碰工作树。

## 3. 实验一（当前 124k 树）

| 指标 | git | mono | 备注 |
|---|---|---|---|
| TTFW 零→首次可写 | 18.5 s | **469 ms** | mono 冷首轮 60.2 s，预热后快 ~40× |
| STATUS 全树零改动 | 128 ms | 708 ms | |
| READ 100 文件 | 273 ms | 冷 9,747 / 热 **260** ms | 冷读要付网络抓取 |
| COMMIT 改 1 文件→远端 | **94.9 s** | 375.4 s | 见下 |
| DISK 工作区占用 | 267.7 MB | **268.5 MB（真实）** | 记录值 495.9MB 是量错了，见 §5 |
| NET | — | — | **无效**，见 §5 |

- **COMMIT 两边都慢，但原因不同**：git 的 94.9 s 几乎全是 `git add -A` 对 12 万个文件做
  全树 stat（O(树)）；mono 的 375 s 是 `libra sync` 的 finalize（同样是 O(树)，但常数更大）。
- **E1-STATUS-SCALE（早年 <2k 树）** 与今天的大树结果方向一致：mono 的 fast path 与树规模
  **解耦**（2k≈627ms、20k≈603ms、100k≈523ms），但常数 ~0.5–0.6 s 打不过本地盘的 git
  （18/36/51 ms）。**这条赢面要在「远端工作区」才成立**——git 工作区不在本地盘时，它的
  每文件 stat 会变成网络往返。

## 4. 实验二（多仓库）

| 指标 | git | mono | 备注 |
|---|---|---|---|
| FETCH | 180 ms（仅 svc-a）<br>**600 ms（全部 6 仓 + submodule）** | 40,688 ms | 见下的不对称说明 |
| PARTIAL 只要 svc-a | 186 ms | 39,526 ms | |
| UPGRADE common 升级 | **2,274 ms（22 步，6 服务）** | 324,997 ms（1 次原子 sync） | mono 慢 **117×** |

**必须说明的不对称**：exp2 的 mono 侧「attach 一次」拿到的是**整个 124k 文件的 monorepo**，
而 git 侧取的是**26 个文件的 multirepo 内容**。两者规模差约 5000 倍，所以 FETCH/PARTIAL
的数字**不能当作「同样工作量谁快」**来读——它衡量的是「mono 的 attach 成本随整个 monorepo
规模走」。我已补测 git 拉全部 6 个仓库（600 ms）作为同口径对照，即便如此差距仍在两个数量级，
原因就是上述规模差异。

**UPGRADE 是这组里最有价值的**：同样是「common 升级 → 全部服务跟上」，git 走 22 步、2.3 s；
mono 一次 sync 理论上更优雅（原子），却花了 325 s。差距不是「步骤多」造成的，而是 finalize。

## 5. 测量陷阱（不修就会读错数）

1. **`du -sb` 在 FUSE 挂载点上量的是逻辑大小**，不是真实占用。它会把 12 万个可见文件的
   表观大小全加起来。E3-MULTI 因此把 mono 记成 1.46 GB（真实 94.5 MB，差 16×）；E1-DISK
   记成 495.9 MB（真实 268.5 MB）。两处都已补记 `disk_bytes_real`（= upper + sealed + store），
   原值保留可追溯。
2. **`cold_cache` 只清 OS page cache，不清 daemon 自己的 store**。所以 E3-READ 的
   「cold」只有第 1 轮是真冷，后两轮是「冷 page cache + 热 store」。
3. **E1-NET 无效**：`net_bytes` 采样 `eth8`，而本机 WSL 用 mirrored 网络，发往
   `127.0.0.1:19000` 的 loopback 流量不计在该接口上（记录值只有 KB 级）。
4. **结果分两代**：2026-09-24/25/26 的数据测的是 <2k 文件的过滤树，2026-09-27 起才是
   124k monorepo。汇总按代分表，**不可混算**。

## 6. 唯一值得立刻动手的优化点

**`libra sync` 的 finalize 是 O(全树)**：改 1 个文件要 375 s，改 6 个服务的 common 要 325 s，
两者几乎相同——说明耗时与「改了多少」无关，而与「树有多大」成正比。这同时解释了
E2-UPGRADE 的 117× 和 E1-COMMIT 的 4×。

嫌疑环节（按 memory 里记录的 finalize 流程）：`commit-finalize` 的**逐路径哈希验证**、
以及**把 chain 摊平进 upper** 这两步都在遍历整棵树。若能把它们限定在「本次提交的
committed_paths ∪ 受影响子树」上，sync 就能回到 fast path 的量级。

其次：**MULTI 的 chain fork 每次约 40 s**，与「零拷贝应接近瞬时」的预期不符，值得单独 profile。

## 7. 过程中修掉的 bench 脚本 bug（都会静默产出错数）

| bug | 位置 | 后果 |
|---|---|---|
| `printf '%c' 97` 得到 `'9'` 而非 `'a'` | `gen-multirepo.sh`、`exp2.sh:71` | 生成 svc-9/svc-1，仓库互相覆盖、升级循环全部 `cd` 失败，**却仍记录 steps=22** |
| `run_metric` 找 `${metric}_ms`，而调用方传的已是 `status_ms` | `common.sh` | read/status/commit **静默不落盘**（已从 stdout 恢复） |
| `case_multi`/`attach_mono` 直接用未绑定的 `$ROUND` | `exp3.sh:102`、`exp2.sh:29` | `set -u` 下整个 case 崩溃 |
| `case_init` 无论 `fresh_mono` 成败都 `record` | `exp3.sh` | 失败的 attach 被记成"可行的 init 时间" |
| `case_switch` 推 `variant` 分支并用 git tip 作 revision | `exp3.sh` | mega2 只有 main、且 revision 需用内部 OID → refresh 是空转 |
