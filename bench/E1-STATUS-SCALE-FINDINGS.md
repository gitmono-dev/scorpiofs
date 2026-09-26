# E1-STATUS-SCALE 实测结论（本地 WSL，2026-09-26）

## 测量设置

- **git 侧**：本地盘 bare repo + `git clone` 工作区（git 的最佳条件），
  `core.untrackedCache=true`；fsmonitor on/off 双线。
- **mono 侧**：合成树 seed 进 mega2 → `libra worktree add --backend scorpiofs`
  → `libra status`（ScorpioFS upper 层 fast path，release 构建）。
- 变量：树规模 2k / 20k / 100k 文件；改动量 0/1/100 个文件；
  冷（每轮 drop_caches，两侧对称）/热缓存。
- 每格 5 轮取中位数；正确性抽查两侧一致（改动数一致）。

## 数据（中位数，ms）

| 树规模 | 缓存 | 改动 | git-off | git-on | libra(fast path) |
|---|---|---|---|---|---|
| 2k | 热 | 0 | 6 | 6 | 497 |
| 2k | 热 | 100 | 7 | 6 | 571 |
| 20k | 热 | 0 | 15 | 15 | 488 |
| 20k | 热 | 100 | 16 | 15 | 528 |
| **100k** | **热** | **0** | **51** | **48** | **523** |
| **100k** | **热** | **100** | **63** | **58** | **558** |
| 2k | 冷 | 0 | 28 | 27 | 715 |
| 20k | 冷 | 0 | 76 | 56 | 709 |
| 20k | 冷 | 100 | 57 | 56 | 764 |

注：本表的 libra 列（约 500ms）高于早期的 250ms 单点测量，原因是
`/project` 中此时已 seed 了 100k 子树——**每个挂载点的 index 都包含全部
12 万 entry**，libra 的 O(index) 成本被整体推高。这本身就是"瓶颈在
index 层"的又一证据：树（lower）规模翻 50 倍时，libra 仅从 497→523ms
（+5%，架构解耦），但 index 的绝对大小直接抬升常数项。

## 结论（诚实版）

1. **架构层赢面成立**：fast path 的候选集是 upper 触碰集——`libra status`
   从 2k 到 100k（**50 倍树规模**）仅从 497ms 变为 523ms（**+5%**），
   与树规模基本解耦；daemon 的 state API 裸延迟 1.6ms（O(upper)）。
   对照 git 的干净线性（6→15→48ms，fsmonitor on）。
   fast path 的语义也强于 mtime 启发式：touch（内容不变）正确判 clean。

2. **当前实现层吃掉架构优势**：libra 的固定成本 ~500ms 在
   **index 加载 + staged 计算（index vs HEAD）**上——该 worktree 的 index
   为 **13.8MB（12 万 entry）**，属 O(index) 的本地工作，与挂载/upper
   无关。同规模内容的 **git status 仅 48–60ms**（本地盘、二进制 index、
   fsmonitor）。小树上 libra 常数 ~50ms（tiny repo 实测），但 index 一大
   即膨胀——本轮 /project 增加 100k 子树后 libra 常数从 250ms 抬到 ~500ms。

3. **对评测的含义与交叉点推演**：
   - 现状下 status 项在**本地盘热缓存 + 中小树**上 git 全面占优；
   - 交叉点估算：git 热缓存约 0.5ms/文件、libra 恒定 ~500ms →
     **约 100 万文件处交叉**（且 index 层优化后 ~500ms→60ms 可把交叉点
     提前到 ~20 万文件）；
   - mono 的架构赢面在**网络/远端工作区**（git 每次 per-file stat 升到
     ms 级，其 O(树) 斜率放大 10 倍以上）——那才是 GitMono 的真实使用
     场景，需要单独设计"远端工作区"对照（E1-REMOTE）；
   - **明确的新优化目标**：libra index 层的加载与 staged 比对
     （格式/批量访问/懒加载），这是把 status 从 ~500ms 拉到 git 同档的路径。

## 后续动作

- [ ] libra index 加载剖析（13.8MB → 700ms 的构成：解析/对象访问/DB）
- [ ] 设计 E1-REMOTE：git 工作区置于网络盘/FUSE 时的 status 对照
- [ ] 大树上再验证 fast path 的改动量斜率（100 改动的 +60ms 是否符合 O(触碰)）
