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
| 2k | 热 | 0 | 6 | 6 | 250 |
| 2k | 热 | 100 | 7 | 6 | 316 |
| 20k | 热 | 0 | 14 | 14 | 247 |
| 20k | 热 | 100 | 16 | 16 | 311 |
| 2k | 冷 | 0 | 28 | 25 | 380 |
| 20k | 冷 | 0 | 53 | 50 | 430 |
| 10 万（git 本地 vs libra 子树化挂载上的 status） | 热 | 0 | **60** | — | **700–920** |

## 结论（诚实版）

1. **架构层赢面已成立**：fast path 的候选集是 upper 触碰集——`libra status`
   在 2k 与 20k 树上耗时几乎相同（250 vs 247ms），**与树规模解耦**；
   daemon 的 state API 裸延迟 1.6ms（O(upper)，非 O(树)）。daemon 侧不再是瓶颈。
   同时 fast path 的语义强于 mtime 启发式：touch（内容不变）正确判 clean。

2. **当前实现层吃掉架构优势**：libra 的固定成本 ~700ms 出现在
   **index 加载 + staged 计算（index vs HEAD）**上——该 worktree 的 index
   为 **13.8MB（12 万 entry）**，属 O(index) 的本地工作，与挂载/upper 无关。
   同规模内容的 **git status 仅 60ms**（本地盘、二进制 index、fsmonitor）。
   小树上 libra 常数 ~50ms（tiny repo 实测），但 index 一大即线性膨胀。

3. **对评测的含义**：
   - 现状下 status 项在**本地盘热缓存**条件下 git 全面占优；
   - mono 的架构赢面要在**网络/远端工作区**（git 工作区不在本地盘）或
     **超大树上 git 线性项超过 libra 常数**时体现——前者才是 GitMono 的
     真实使用场景，需要单独设计"远端工作区"对照（E1-REMOTE）；
   - **明确的新优化目标**：libra index 层的加载与 staged 比对
     （格式/批量访问/懒加载），这是把 status 从 700ms 拉到 git 同档的路径。

## 后续动作

- [ ] libra index 加载剖析（13.8MB → 700ms 的构成：解析/对象访问/DB）
- [ ] 设计 E1-REMOTE：git 工作区置于网络盘/FUSE 时的 status 对照
- [ ] 大树上再验证 fast path 的改动量斜率（100 改动的 +60ms 是否符合 O(触碰)）
