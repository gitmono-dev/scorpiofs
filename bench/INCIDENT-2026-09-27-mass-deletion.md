# 事故报告：`libra sync` 误删 124,008 个文件

**日期**：2026-09-27
**影响**：本地 dev mega2（`127.0.0.1:19000`）的 `/project` 单棵 monorepo，124,008 个文件被一次 `libra sync` 提交删除并推送。
**恢复**：已完全恢复（见下），无永久数据丢失。

---

## 1. 发生了什么

在 ScorpioFS 挂载的工作区上执行一次正常的「clone → 改代码 → sync」流程：

```
libra status  →  deleted: .gitkeep / bench-notes.txt / dev-lab/... （几乎全部）
libra sync    →  124009 files changed (new: 1, modified: 0, deleted: 124008)
                  3 objects pushed
```

推送后 `/project` 只剩 1 个文件（新写入的 `.libraignore`）。

## 2. 因果链（已逐层验证）

```
① mega2 的 soft nofile = 1024（容器内实测 /proc/1/limits）
       ↓  124k 文件的 monorepo + 懒加载目录 → 瞬间打满
② mega2 报错：
     Application error: Database error: Connection error: Too many open files (os error 24)
     axum::serve::listener: accept error: Too many open files (os error 24)
       ↓  对 tree 请求返回 HTTP 500，甚至停止 accept 新连接
③ ScorpioFS 挂载把这些路径呈现为「空目录」
   （libra worktree add 也会因此失败，502 Bad Gateway，留下一个只有 .libra 的空工作区）
       ↓
④ libra 的扫描把「取不到」当成「不存在」→ 报 deleted
       ↓
⑤ libra sync 未做任何量级校验，直接提交并推送这批删除
```

**关键在第 ④/⑤ 步**：无论②③为何失败，**扫描无法区分「文件确实不存在」与「后端取不到」，而 sync 对后者也执行破坏性动作**。这才是真正的事故根因——上游抖动不该导致数据删除。

## 3. 证据

| 证据 | 内容 |
|---|---|
| fd 限制 | `docker exec mega2-e2e-mega2-1 cat /proc/1/limits` → `Max open files 1024 1048576` |
| mega2 错误 | 日志中成片的 `Too many open files (os error 24)` + `accept error` |
| 挂载为空 | attach 成功后 `mount_id` 存在但工作区仅 8 项 / 或 attach 直接失败（`LBR-IO-002 502 Bad Gateway`） |
| status 耗时异常 | 空工作区时 `libra status` **1 秒**返回 124008 条删除（真扫 124k 文件不可能这么快）→ 说明扫的是一个空目录 |
| 对照：git 正常 | 同一挂载点上 `git status` 报 **0** 删除 → 挂载本身没问题时是对的 |
| 对照：修 fd 后 | mega2 fd 提到 1048576 后，daemon `changes=0`、`libra status` **clean** |

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
| mega2 fd 上限 1024 → 1048576 | `e2e-mega2-local.override.yml` 加 `ulimits.nofile` | ✅ 已验证生效 |
| 工作流前置安全阀 | `mst2-impl/full-workflow-guarded.sh`：baseline 删除数 > 阈值就 **ABORT 不 sync** | ✅ 已落地 |

## 6. 建议的根治项（未做）

1. **`libra sync` 必须有破坏性操作的量级闸门**——例如删除占比超过某阈值（或绝对数超过 N）时拒绝提交，除非显式 `--allow-mass-delete`。这是防止同类事故的最后一道防线，成本极低。
2. **扫描要区分「不存在」与「取不到」**——后端 5xx/超时应让 status **显式失败**（或在输出中标记为 blocked），而不是静默归入 deleted。daemon 侧已有 `io_blocked` 概念，应贯通到 FUSE/lower 取数失败。
3. **`libra worktree add` 失败必须让后续命令感知**——attach 失败后工作区是空的，此时 `status` 报满屏删除、`sync` 会照单执行。应在工作区记录挂载状态，`status`/`sync` 检测到「本应是 ScorpioFS 工作区但没有挂载」时直接报错退出。
4. **mega2 应把 EMFILE 表现为可重试错误**（503 + Retry-After），而不是 500 让客户端当成「内容为空」。
5. 评测脚本里不要 `>/dev/null` 吞掉 attach 错误——本次排查中我多次因此误判（这是操作教训，不是产品缺陷）。
