# 重大更正：W3 的 mono status/add 数字全部作废（2026-10-01）

## 事实更正

**`libra <dir> <subcommand>` 不是合法的 libra 调用形式。** 它立即以
`rc=129 LBR-CLI-001 "'<dir>' is not a libra command"` 退出，耗时约 11 ms。

W3 计时脚本把 mono 侧写成 `$LIBRA /tmp/mwt status` / `$LIBRA /tmp/mwt add -A`，
于是**这些"操作"从未执行**，11 ms 的快速失败被计时器记录为"成功"。

```
form A: libra <dir> status      → rc=129,  11 ms   ← W3 测的（假）
form B: (cd dir && libra status) → rc=0,   29-31 ms（真实，小仓库）
```

**连锁后果**：W3 的 mono commit 报 rc=128，根因是 add 从未 stage（假成功）→
commit 面对空暂存区报 "nothing to commit"。commit 本体在诊断重放中 rc=0。

## 作废的结论

以下曾经报告的结论**全部无效**，不应引用：

- "mono status 快 12.5× / add 快 14×"（本地 174k 轮）
- "mono status 快 6.2× / add 快 8.1×"（云端 50k 轮）
- 基于这些数字的"workspace 操作优于 git"表述

## 修正后的真实数字

### 云端（ACK 跨节点，50k bench50k，release daemon）

| 操作 | git | mono | 比值 |
|---|---|---|---|
| status | 69 ms | **623 ms** | 慢 9.0× |
| add -A | 90 ms | **284 ms** | 慢 3.2× |
| commit | 64 ms | **1083 ms** | 慢 16.9× |
| W1 clone | 6,530 ms | attach 21,237 ms（+clone ~30s） | 慢 3.7–7.8× |

### 本地（同机环回，174k fixture，debug daemon）

| 操作 | git | mono | 比值 |
|---|---|---|---|
| status | 134 ms | **976 ms** | 慢 7.3× |
| add -A | 116 ms | **488 ms** | 慢 4.2× |
| commit | 90 ms | **1618 ms** | 慢 18.0× |
| W1 就绪 | 34.2 s | 126.5 s（clone 33.1 + attach 93.5） | 慢 3.7× |

**同机与跨节点都是慢的** —— 不是网络拓扑能解释的差距；挂载路径的每次操作
有实打实的额外成本（FUSE 往返 + index/元数据读取的逐项开销）。

## 仍然成立的结果（经独立验证，不受本次更正影响）

| 项 | 数字 | 验证方式 |
|---|---|---|
| **commit 性能修复本身** | 53.8 s → 0.3–1.6 s（**45–170×**） | 云端 rc=0/1083 ms；本地 316–1618 ms，分解插桩 |
| **W2 多工作区磁盘** | 约 **800×**（真实落盘） | 独立运行，逐挂载文件数门禁 |
| **filter 克隆** | libra 7.7 s → **1.2 s**（6.4×） | 用 `libra clone`（合法形式）测量，字节数对照 |
| 首字节修复 | 81.28 s → **0.0014 s** | curl ttfb |
| commit 分解 | validate_staged 11 ms、persist 6 ms、staged 55–97 ms | `COMMIT_PHASE` 插桩 |

## 目标状态（对照 GOAL-MONOREPO-WIN）

**"mega2+scorpiofs 在中等规模上性能优于 git" —— 当前不成立。**

| 判据 | 状态 |
|---|---|
| W1 就绪 ≤1.5× | ❌ 3.7×（depth-1 口径；filter 口径待 attach 兼容修复后复测） |
| W2 多工作区磁盘 ≤0.2× | ✅ 约 0.001 |
| W3 日常操作 ≤3× | ❌ status 7–9×、add 3.2–4.2×、commit 17–18× |
| W4 全量可比 | ✅（双方全量 rc=0） |

## 教训（测量工具失效第 4 例）

前 3 例：want-oid 来源错误、pkt-line 前缀污染、换计时工具未察觉。
本例：**命令形式无效 + 快速失败被当作成功**。

**新规则**：计时 harness 必须**同时校验退出码与输出语义** ——
`rc != 0` 的样本一律作废；对"好得可疑"的数字（如 11 ms 的 50k status），
必须用输出内容（而非耗时）证明操作真的执行了。

## 下一步方向（若继续追求该目标）

1. **剖析 status 的真实 976 ms**：upper 启发式为何仍慢 —— 疑似 index 读取
   （50k 条目在挂载内）与 sidecar/HEAD 元数据的逐项 FUSE 往返
2. **commit 剩余 17×**：create_tree 全量重建（entry 假象外仍有 ~500 ms 真实成本）
   与 staged diff 的挂载开销
3. **attach 93 s（本地 debug）/ 21 s（云端 release）**：挂载初始化是 W1 大头
4. **filter+attach 集成**：filter 克隆 1.2 s 的红利目前因 populate 读本地 blob 而无法
   用于工作区
