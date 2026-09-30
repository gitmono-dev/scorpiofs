# commit 慢的定位：本地分解完成（2026-09-30）

## 方法转折（按"本地优先"规则执行）

插桩版（`COMMIT_PHASE` eprintln）在云端跑时被测试脚本的 stderr 重定向吞掉，
白跑一轮。改在**本地纯客户端环境**复现（50k 文件、`libra init` + add + commit，
无 mega2/FUSE/网络）—— commit 是本地操作，本就该能本地复现。

## 本地分解（50k 文件，两次 commit 取一致值）

| 阶段 | 耗时 | 说明 |
|---|---|---|
| validate2（批量修复后） | **3.4 s** | 批量方法内部仍逐个 `storage.get`（全对象读） |
| staged_changes | 0.03 s | 快 |
| **create_tree（线性化后）** | **9.3 s** | 仍偏慢：每文件 `index.get(name)` + from_tree_items |
| 其余（写对象/ref 等） | ~4 s | 未插桩 |
| **commit 合计** | **16.7 s** | |

## 与云端 49 s 的对账 —— 剩余 32 s 在挂载路径

```
云端（MST/2 挂载工作区）: 49 s
本地（纯客户端）        : 16.7 s
差额                    : 32 s   ← MST/2 挂载环境独有
```

已排除的候选（云端实测）：status 模板、FUSE 全树扫描（daemon lookup 仅 193 行）、
index 验证、树构建递归。32 s 差额的下一定位手段：**本地起 FUSE 栈复现**（WSL 有
docker mega2 栈 + 完整配方），插桩会直接指认。

## 已确认可再压的部分（本地就有 13 s 空间）

1. **validate 3.4 s → <1 s**：批量方法改用 header-only 探测（上游
   `perf/index-linear-scan-fixes` 分支的 `object_types_bounded_probe` 机制，
   不读对象体）
2. **create_tree 9.3 s → 目标 <2 s**：profile 每文件的 `index.get(name)`
   与 `Tree::from_tree_items`；50k 文件建树理论上应 <1 s

## 已交付的修复（已推送 libra `c2608fd5d`）

- 批量类型探测：100k 次异步边界穿越 → 1 次（云端实测贡献 ~1 s）
- 树构建分组线性化：~5.3 亿次路径比较 → 线性（实测贡献 ~1.5 s）
- `COMMIT_PHASE` 阶段插桩保留在代码中（下轮直接用）

## 附带记录

- 本地 `add -A`（无 scorpiofs 快路径的普通仓库）= 544 s —— 上限层启发式
  在 ScorpioFS 挂载上快 45×（12 ms），再次印证 add 快路径的价值
- 本地复现脚本：`mst2-impl/local-commit-repro.sh`（`COMMIT_PHASE` stderr 可见）
