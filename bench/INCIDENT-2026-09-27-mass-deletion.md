# 事故报告：`libra sync` 误删 124,008 个文件

**日期**：2026-09-27
**影响**：本地 dev mega2（`127.0.0.1:19000`）的 `/project` 单棵 monorepo，124,008 个文件被一次 `libra sync` 提交删除并推送。
**恢复**：已完全恢复，无永久数据丢失。
**状态**：根因已重新定位（v2），初版报告的归因**是错的**。

---

## 1. 发生了什么

在 ScorpioFS 挂载的工作区上执行一次「clone → 改代码 → sync」流程：

```
libra status  →  deleted: .gitkeep / bench-notes.txt / dev-lab/... （几乎全部）
libra sync    →  124009 files changed (new: 1, modified: 0, deleted: 124008)
```

推送后 `/project` 只剩 1 个文件（新写入的 `.libraignore`）。

## 2. 根因（v2，已用对照实验定论）

**真正的根因：工作流脚本在错误的工作目录里执行了 `status` 和 `sync`。**

`mst2-impl/full-workflow.sh` 的流程是：

```bash
libra clone -q -b main --no-checkout  <mega2>/project  "$MAIN"   # ← 注意 --no-checkout
cd "$MAIN"                                                        # ← 停在这里
...
libra worktree add --backend scorpiofs -b flow-$RANDOM "$WT"      # 挂载建在 $WT
...
libra status    # ← 在 $MAIN 里跑，不在 $WT 里！
libra sync      # ← 同上
```

`$MAIN` 是用 `--no-checkout` 克隆的，**它的工作区本来就是空的**。于是：

- `libra status` 在 `$MAIN` 里如实报告：所有 124,008 个已跟踪路径**都不在磁盘上 → deleted**
- `libra sync` 忠实地把这批删除提交并推送

**这不是 FUSE 的问题，也不是 libra 的问题**——从 `$MAIN` 的视角看，报告完全正确。真正错的是「在一个刻意留空的工作区里执行破坏性 sync」这个脚本设计。

### 对照实验（决定性证据）

daemon 健康、挂载完整的前提下（124,008 个文件都能 `find` 到），同一时刻对两个目录跑 `libra status`：

| 目录 | 说明 | 可见条目 | `status` 删除数 |
|---|---|---|---|
| `$MAIN` | `--no-checkout` 克隆 | 1 | **124,008** |
| `$WT` | ScorpioFS FUSE 挂载点 | 7（顶层），全树 124,008 个文件 | **0**（`working tree clean`） |

```text
=== A) $MAIN (cloned with --no-checkout) ===
  visible entries    : 1
  libra status deleted: 124008
=== B) $WT (the ScorpioFS FUSE mount) ===
  is a fuse mount    : 1
  files (full walk)  : 124008
  libra status deleted: 0
=== C) raw status output from $WT ===
On branch flow2-279
nothing to commit, working tree clean
```

→ **ScorpioFS + Libra 的读路径是健康的**；124,008 这个数字来自 `$MAIN`。

### 那 fd 耗尽呢？

fd 耗尽是**真实存在的、但是另一件事**，是初版报告的错误归因：

- mega2 的 soft nofile 确实只有 1024，日志里确实有成片的 `Too many open files (os error 24)` + `accept error`
- 这确实导致 `libra worktree add` 有时 502 失败，只留下一个空的 `$WT`

但它**不是** 124,008 删除的原因，而且它让一个**必然致命**的设计缺陷看起来像一次偶发的基础设施抖动：无论 fd 上限提到多高，在 `$MAIN` 里跑 `sync` 都会删光整棵树。

初版报告里还有一条自证其伪的证据被误读了：「空工作区时 `libra status` **1 秒**返回 124008 条删除（真扫 124k 文件不可能这么快）」。1 秒是对的——因为扫的就是一个空目录。这不是「挂载坏了」的征兆，这就是「cwd 错了」的征兆。

## 3. 证据

| 证据 | 内容 |
|---|---|
| fd 限制（真实但非本次根因） | `docker exec mega2-e2e-mega2-1 cat /proc/1/limits` → `Max open files 1024 1048576` |
| mega2 错误 | 日志中成片的 `Too many open files (os error 24)` + `accept error` |
| **决定性对照** | 见上表：健康挂载下 `$MAIN` 报 124008、`$WT` 报 0 |
| attach 偶发失败 | `LBR-IO-002 502 Bad Gateway`（fd 耗尽所致，留下只有 `.libra` 的空 `$WT`） |
| 恢复时树哈希一致 | 恢复提交的 tree hash 与 `f164940^{tree}` 逐字节相同（`75335dbe…`） |

## 4. 恢复过程

mega2 拒绝非快进推送（`push chain from f164940 never reached old_id e0df005; fetch and rebase`），所以**不能回退**，只能做一个**内容等价的新提交**：

```bash
git clone <mega2>/project recover && cd recover
git rm -rq --ignore-unmatch .
git checkout f164940 -- .          # 上一个好提交的整棵树
git add -A
git write-tree                     # 与 f164940^{tree} 哈希一致 → 75335dbe...
git commit -m "revert: restore the monorepo tree after an accidental mass deletion"
git push origin main               # 快进，mega2 接受
```

**结果**：`new tree == old tree`（哈希逐字节一致），`/project` 内容全部回来。
（教训：**mega2 的 trunk 策略不允许回退**，所以「恢复」= 造一个内容相同的正向提交；好在对象从未被删，这一步是安全的。）

## 5. 已做的修复

| 修复 | 位置 | 状态 |
|---|---|---|
| **G1：`status`/`sync` 必须跑在已挂载的 ScorpioFS 工作树内**（`cd "$WT"` + 校验 mount marker + 校验是活 FUSE 挂载），杜绝「空 `--no-checkout` 工作区」被当成删除源 | `mst2-impl/full-workflow-guarded.sh` | ✅ 本次修复 |
| G2：挂载必须非空（>1000 文件）才继续 | 同上 | ✅ |
| G3：扫描报告的删除数 > 阈值（默认 50）则 **ABORT 不 sync** | 同上 | ✅ |
| mega2 fd 上限 1024 → 1048576 | `e2e-mega2-local.override.yml` 加 `ulimits.nofile` | ✅ 已验证生效（真实修复，只是与本次删除无关） |

## 6. 建议的根治项（未做）

1. **`libra sync` 必须有破坏性操作的量级闸门**——例如删除占比超过某阈值（或绝对数超过 N）时拒绝提交，除非显式 `--allow-mass-delete`。这是最后一道防线，成本极低。**注意**：这条闸门只能防「部分损坏」，防不住本次这种「在空工作区里整体 sync」——那种情况下删除是 100%，任何阈值都只是把事故从「静默删除」变成「报错退出」。所以闸门 + 正确的 cwd 校验，两者都要。
2. **区分「工作区是空的」与「文件被删了」**——若被跟踪路径在磁盘上一个都不存在，几乎不可能是用户的真实意图。`sync` 检测到「删除数 == 已跟踪文件总数」时应当直接拒绝，并提示用户检查工作目录是否正确。
3. **扫描要区分「不存在」与「取不到」**——后端 5xx/超时应让 status 显式失败，而不是静默归入 deleted。
4. **`libra worktree add` 失败必须让后续命令感知**——attach 失败后工作区是空的，此时 `status` 会报满屏删除。应在工作区记录挂载状态，`status`/`sync` 检测到「本应是 ScorpioFS 工作区但没有挂载」时直接报错退出。
5. **mega2 应把 EMFILE 表现为可重试错误**（503 + Retry-After），而不是 500。
6. 评测脚本里不要 `>/dev/null` 吞掉 attach 错误——本次排查中我多次因此误判（操作教训）。

## 7. 给评测脚本的推论

`libra clone --no-checkout` + `libra worktree add` 这个组合，**任何**在 `$MAIN` 里跑 `status`/`sync` 的脚本都会 100% 删光仓库。评测/基准脚本必须：

- `cd` 进 `$WT` 之后再跑任何 libra 命令；并且
- 在做破坏性操作前断言「cwd 是活的 FUSE 挂载 + mount_id 匹配」。
