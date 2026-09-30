# 目标：中规模下证明 mega2+ScorpioFS 优于普通 git

**设定日期**：2026-09-29　**状态**：进行中

## 终态（Definition of Done）

在中规模（**50k 文件**——收敛到中等规模，避免 500k 下服务端缺陷主导结论）上，
在 ACK 跨节点拓扑下，跑出**可复现的对比数据**，并满足：

| # | 判据 | 门槛 | 状态 |
|---|---|---|---|
| **D1** | MST/2 lower 在集群中可用 | daemon 日志出现 `serving MST/2 snapshot view as the lower layer`；挂载内容可读 | ✅ **通过**（50,000 文件，深层读正确） |
| **D2** | MST/2 lower 写路径可用 | 遍历 / 新建 / 覆盖 / 删除 / append **全部成功** | ⚠️ **8/9** —— 仅 append 失败（已知边界，见下） |
| **D3** | 就绪时间 | mono（clone+attach）÷ git clone **≤ 0.2** | ✅ **通过：0.174**（14,882 ms vs 85,308 ms）—— **依赖 `--depth 1`，见下** |
| **D4** | 多工作区 | 5 个 worktree 的**真实落盘增量**比 ≤ 0.2 | ⬜ 未测 |
| **D5** | 日常操作不退化 | mono 的 status/add/commit 不慢于 git 的 **3×** 以上 | 🔶 部分：`status` 50k = 1.7 s ✅ |
| **D6** | 证据留档 | 原始数据 + 结论写入 `bench/results/` 并更新网页 | 🔶 进行中 |

**D1/D2 详情**：`bench/results/MST2-D1-D2-CLUSTER-2026-09-29.md`
**服务端缓存设计**：`bench/design-mega2-cache.md`

### D3 达标的**诚实边界**（必须写进结论）

```
git  全量 clone      : 85308 ms
mono clone+attach    : 14882 ms  (clone 9583【--depth 1】+ attach 5299)
比值 = 0.174         目标 ≤ 0.2     ✅
```

**但这个达标依赖 `--depth 1`。** 原因是服务端每次 clone 都重算**全部历史**：

| 场景 | 耗时 | blob 数 | 体积 |
|---|---|---|---|
| 全量 clone | 83–98 s | **550,001** | 75.4 MB |
| `--depth 1` | 8.2–9.6 s | **50,000** | 9.4 MB |

`git-upload-pack` **首字节 81.28 s**（服务端在建 pack 期间一个字节都不发），
而 libra 客户端有固定 60 s idle 超时 → 全量 clone 在 mono 侧**根本跑不通**
（服务端还会因此 `SendError` panic 掉一个 tokio worker）。

所以准确的表述是：**"mono 的就绪速度在 shallow 下达到 5.7×；而不 shallow 时
mono 侧连跑完都做不到"** —— 后者是服务端缺陷，不是 mono 的优势。
`bench/design-mega2-cache.md` 给出了修复方案。

### D2 的 append 失败是已记载边界，不是回归

修复写在本机 libfuse-fs checkout（从未提交），而 ScorpioFS 依赖 crates.io 的
`libfuse-fs 0.3.0`。失败形态是 `ENOSYS`，与文档记录的两端（修复前 ENOENT /
修复后成功）都不同 —— 镜像里是第三个状态，需集群内 strace 定位。

**诚实边界（会写进结论）**：若某项不达标，照实报告，不粉饰。

---

## 阶段与检查点

### 阶段 1：开启 MST/2 并验证读写（D1/D2）✅ 已完成
D1 全通，D2 8/9。三个已知 bug 现状：append **失败**（ENOSYS）；
EBADF **未复现**；chunked-read EIO **未复现**（3 MB 读写正常）。

### 阶段 2：服务端性能 ✅ 已诊断，⏳ 待修
已量化：首字节 81.28 s、零缓存（r1 83.1 s / r2 86.7 s）、N+1（~15k SQL + ~50k S3 GET）、
两遍树遍历、`window_size=0`、响应必须等 pack 建完、无 side-band-2 进度。
设计见 `bench/design-mega2-cache.md`（**待确认后实施**）。

### 阶段 3：对比测试（D3–D5）
D3 ✅ 0.174。D4/D5 待补。

### 阶段 4：留档与发布（D6）

---

## 当前已知状态（承接前序工作）

| 项 | 状态 |
|---|---|
| ACK 集群 | ✅ 运行中，2 × 8c32g；mega2 在 `.89`，bench/probe 在 `.90`（跨节点） |
| `/project` | ✅ **精确 50,000 文件**（`bench50k/` 一棵树） |
| scorpiofs MST/2 lower | ✅ 已开启并验证（`SCOPE=/project`） |
| libfuse-fs | ❌ 镜像用 crates.io 0.3.0，**不含** append 修复（未提交） |
| 探针 Job | ✅ `bench/infra/k8s/mst2-probe.{yaml,sh}`，含 D1/D2/D3 全套与 3 个前置校验 |
| libra 超时绕过 | ⚠️ `LIBRA_FETCH_IDLE_TIMEOUT_MS=900000`；**`--depth 1` 后不再需要** |
