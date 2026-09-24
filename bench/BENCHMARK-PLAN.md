# ScorpioFS+Mega2 vs Git Stack 优越性评测规划

> 版本 0.1（规划稿，待评审）2026-09-24
> 目标：通过三组可控实验，证明 GitMono 栈（Mega2 服务端 + ScorpioFS FUSE + Libra）相比传统 git 栈的结构性优势。

---

## 0. 要证明的主张（与诚实边界）

| # | 主张 | 依据的结构性优势 |
|---|------|----------------| 
| 1 | 普通项目：获得可编辑工作区的成本（时间/磁盘/流量）远低于 git clone | attach 秒级、lower 零拷贝按需拉取 vs clone 全量解包 |
| 2 | 多仓库关联：跨仓原子修改是**一次 commit**，无需 submodule/subtree 工具链 | 单 monorepo 单树，路径即仓库边界 |
| 3 | 大仓库：首次可用时间、多工作区磁盘、分支/快照切换时间是数量级差异 | pinned lower + sealed chain 零拷贝 vs checkout 全树重写 |

**诚实边界（报告里必须写，避免打稻草人质疑）**：
- git 的强项保持承认：离线完整历史、生态工具链、status 在 fsmonitor 优化后很快；
- 对照组给 git 用最佳配置（partial clone `--filter=blob:none`、`--depth` 变体、`core.fsmonitor`、untracked cache），指标里注明配置；
- FUSE 路径读延迟不是处处占优（首次冷读有网络 RT），如实记录冷/热两套数据。

---

## 1. 总体架构（Aliyun K8s）

### 1.1 部署拓扑

```
阿里云 单可用区（内网 RT <1ms，排除网络变量）

  ┌── k8s 集群（ACK 或 ECS 自建 k3s）──────────────────────┐
  │                                                        │
  │  [node-a] mega2 服务端        [node-b] git 对照服务端    │
  │   deployment: mega2            deployment: gitea        │
  │   svc: 19000/19700             svc: 30022/30080         │
  │   pvc: 云盘 ESSD               pvc: 云盘 ESSD           │
  │                                                        │
  │  [node-c] 观测栈                                        │
  │   prometheus + grafana + node-exporter(DS)              │
  └────────────────────────────────────────────────────────┘

  ┌── 裸 ECS runner（不进 k8s）────────────────────────────┐
  │  [runner-git]   git/gitea-cli + opencode + workload    │
  │  [runner-mono]  scorpiofs daemon(FUSE) + libra         │
  │                 + opencode + workload                   │
  │  两台同规格、同镜像基线、同 sysctl、同 page-cache 策略     │
  └────────────────────────────────────────────────────────┘
```

**为什么 FUSE runner 用裸 ECS 而不是 pod**：容器化 FUSE 需要 privileged + `/dev/fuse` + `user_allow_other`，且容器 overlayfs 嵌套会引入额外变量。runner 直接用 ECS + systemd 跑 daemon，对照组同样裸跑 git，公平且省坑。

### 1.2 资源与成本估算

| 组件 | 规格 | 数量 | 按量估价 |
|------|------|------|---------|
| mega2 / gitea 节点 | 4c8g + 200GB ESSD | 2 | ~0.6 元/h |
| 观测节点 | 2c4g | 1 | ~0.2 元/h |
| runner-git / runner-mono | 8c16g + 300GB ESSD（实验 3 用 16c32g） | 2 | ~1.6 元/h |
| 合计（实验期 5 天 × 8h 有效时段，用完即停机） | | | **~150–250 元** |

k8s 选择建议：实验期短，用 **ECS 自建 k3s**（免 ACK 托管费）；若要演示"生产形态"再补一版 ACK 部署。

---

## 2. 实验一：普通项目对比（3rd-party workload）

### 2.1 变量与假设

- **H1**：TTFW（Time To First Write，从零到可编辑）attach ≪ clone；
- **H2**：工作区磁盘占用 mono ≪ git（lower 零拷贝 vs .git 全量）；
- **H3**：status 延迟同量级（诚实记录，git 有 fsmonitor）；
- **H4**：agent 执行简单任务的端到端 wall time mono ≤ git（grep/read 密集的任务对文件可用性敏感）。

### 2.2 workload 项目（3rd party）

| 项目 | 规模 | 用途 |
|------|------|------|
| tokio（rust） | ~5k 文件 / ~80MB | 中型热身 |
| cpython | ~25k 文件 / ~500MB | 中大型主测 |

（每个项目分别 seed 进 mega2、push 进 gitea，**同一 commit 基线**。）

### 2.3 测量项（每项目 ≥5 轮取中位数 + p95）

| 指标 | git 侧命令 | mono 侧命令 |
|------|-----------|------------|
| TTFW 冷 | `drop_caches; time git clone --filter=blob:none … && git checkout` | `drop_caches; time (seed 已备) attach → 首次写入探测` |
| TTFW 热 | 同上去掉 drop_caches | 同上 |
| 工作区磁盘 | `du -sh` repo + .git | `du -sh` mount 点 + 缓存目录 + size.db |
| 首读延迟 | 读 10 个随机文件 | 读同 10 个文件（冷/热各测） |
| status | `git status`（fsmonitor on/off 两套） | `libra status`（effective diff） |
| 小改动提交 | edit → `git commit && git push` | edit → `libra sync` |
| 网络总流量 | vnstat per-run | 同 |

### 2.4 opencode 任务集（简单、机械、可判分）

免费模型只跑**可自动判分**的小任务，避免模型能力成为噪声：
- T1：在指定文件新增一个带签名的函数（判分：编译过 + 文本 diff 匹配）
- T2：全仓 grep 某符号并统计出现次数写入文件（判分：数字正确）
- T3：把某常量值替换为给定值（判分：diff 精确）

每个任务在两种栈上用同 prompt、同模型、同温度跑 5 次，报 wall time + agent 内部 I/O 工具调用耗时分解（opencode 会记录 bash 工具输出，用时间戳切分）。

---

## 3. 实验二：多仓库关联关系

### 3.1 变量与假设

- **H5**：跨仓原子修改（改公共库 + 全部下游升级）git 需要 N+1 次 commit/push + submodule 指针同步，mono 一次 commit；
- **H6**：新人获取全部关联代码：git N 次 clone（含 submodule init）vs mono 一次 attach；
- **H7**：submodule 指针漂移类错误在 mono 结构上不存在（演示性，不算分）。

### 3.2 场景构造

生成 1 库 + 5 服务的合成拓扑（脚本 `gen-multirepo.sh`）：
- `common`：被 5 个服务 import 的库（v1.0）
- `svc-a … svc-e`：各自依赖 common（git 侧以 submodule 挂载；mono 侧为 monorepo 下 `common/` + `svc-*/` 子树）

### 3.3 测量任务

| 任务 | git 侧 | mono 侧 |
|------|--------|---------|
| 全量获取 | `git clone --recurse-submodules` ×1 | `attach` ×1 |
| 跨仓升级 common v1.0→v1.1 | 改 common → push → 5 个服务各 `submodule update --remote` + commit + push（脚本化执行，计总时与人工步骤数） | 改 common/ → `libra sync` 一次 |
| 原子性验证 | 制造"服务升级了但忘了推 submodule 指针"（opencode 任务 T4），检查 CI 拉取失败 | 结构上不可能，记录为定性优势 |
| 部分获取 | 只需要 svc-a：仍要拉 common 完整对象 | attach 子路径 `svc-a` + `common`，按需 |

### 3.4 opencode 任务

- T4：在 git 栈上执行"升级 common 并同步所有服务"——观察 agent 是否会漏掉 submodule 指针步骤（真实痛点演示）；同一任务在 mono 上完成。
- T5：跨仓 rename（common 里改一个 API 名，5 个服务全部跟随）——判分：编译/引用一致性校验通过，比较 wall time。

---

## 4. 实验三：大仓库性能

### 4.1 变量与假设

- **H8**：git clone 时间/磁盘随规模线性恶化（full 与 partial 都要拉对象），attach 时间近常数（按需）；
- **H9**：多工作区成本：git worktree N 份 = N×检出磁盘；mono N 个 mount 共享 lower，各 upper 接近零；
- **H10**：切换基线（branch/快照）：git checkout 重写全树（秒~分钟），mono refresh/switch 只换 lower 引用；
- **H11**：fd/内存压力在 10 万+ 文件规模下 daemon 稳定（fd 上限 65536 已在 compose 验证过）。

### 4.2 大仓库构造

| 仓库 | 来源 | 规模 |
|------|------|------|
| linux-kernel | 真实镜像 | ~80k 文件 / pack ~3GB |
| synth-100k | `gen-repo.sh` 合成 | 100k 文件 / ~1.2GB（模拟微服务 monorepo：大量小文件 + 深目录） |
| synth-200k | 合成 | 200k 文件 / ~2.5GB（压力档） |

合成生成器可控（文件大小分布、目录深度、重复率），保证 git/mono 两侧输入完全一致——**这是选合成的原因**。

### 4.3 测量矩阵

| 指标 | 说明 |
|------|------|
| 初次可用时间 | full clone / partial clone / depth=1 三档 vs attach（冷/热） |
| 磁盘占用 | `.git` + worktree vs lower 缓存 + upper + size.db；**分母同口径 du** |
| 随机读 100 文件 | 冷/热、命中率 |
| status/切换 | `git switch` 到平行分支（全树变化 10%）vs `refresh`/chain fork |
| 多工作区 ×5 | `git worktree add` ×5 vs mount ×5：总磁盘、创建时间 |
| 并发 agent ×4 | 4 个 opencode 并发小任务（模拟团队），daemon 与 git 各自稳定性/吞吐 |
| 资源曲线 | prometheus 抓 CPU/RSS/fd，附 grafana 截图 |

### 4.4 预期主战场

**多工作区与切换**是结构性赢面（git worktree 也要每个 checkout 全量文件，ScorpioFS sealed chain 零拷贝）；clone 时间是数量级赢面；status 争取平手（如实报告 fsmonitor 版本）。

---

## 5. 公平性与统计方法

1. **同基线**：两侧同 commit 内容、同硬件、同 AZ、同 sysctl（`vm.dirty_*`、swappiness）、每次 run 前 `echo 3 > /proc/sys/vm/drop_caches`（冷测）；
2. **git 侧最优配置清单**（写入报告附录）：partial clone、fsmonitor、untracked cache、parallel checkout（`git 2.4x` 默认）；
3. **取样**：≥5 轮，报中位数 ± IQR + p95；总时间超 10 分钟的实验（clone 大仓）跑 3 轮；
4. **变量隔离**：网络流量用 vnstat per-interface 记录；CPU/RAM 用 node_exporter 对齐时间窗；
5. **agent 公平性**：同模型/同 prompt/同 max-turns；任务判分脚本拒绝放水；失败 run 记录原因但不剔除（如实报成功率）。

---

## 6. 里程碑

| 阶段 | 内容 | 产出 | 预估 |
|------|------|------|------|
| P1 | 云环境脚手架：k3s 起集群、mega2/gitea/prometheus 部署 manifests、runner ECS 初始化脚本 | `bench/infra/`（terraform/aliyun CLI + k8s yaml + 起停脚本） | 1 天 |
| P2 | workload 准备：seed 导入脚本、gitea 镜像脚本、`gen-repo.sh`、`gen-multirepo.sh`、判分脚本 | `bench/workload/` | 0.5 天 |
| P3 | 实验一执行 + 数据收集 | 结果 JSON + 初表 | 0.5 天 |
| P4 | 实验二执行 | 同上 | 1 天 |
| P5 | 实验三执行（大仓，最耗时） | 同上 | 1.5 天 |
| P6 | 报告：指标矩阵 + grafana 图 + 结论与边界 | `bench/REPORT.md` | 0.5 天 |

**关键脚本清单**：`bench-run.sh`（单指标采样循环）、`bench-agent.sh`（opencode 任务驱动+计时）、`collect.sh`（du/vnstat/prometheus 对齐抓取）、`judge.sh`（T1–T5 判分）。

---

## 7. 风险与规避

| 风险 | 规避 |
|------|------|
| 免费模型限流/不稳定 | 任务设计成短任务；失败重试 ≤2 次；预留本地 ollama 小模型做 fallback（仅计时参考，不进主表） |
| k8s 里 FUSE 的坑 | runner 用裸 ECS systemd（已定），k8s 只跑无 FUSE 依赖的服务 |
| mega2 在云盘 IOPS 下表现未知 | 先跑 P2 冒烟：seed + attach + 读写压测 10 分钟，异常调 ESSD PL 等级 |
| git 侧被质疑配置不优 | 附录披露完整配置 + 版本号；fsmonitor on/off 双份结果 |
| daemon 长跑稳定性（fd/内存） | 实验 3 并发档前先单机 soak 30 分钟；fd 上限 65536（compose 已验证） |
| 数据偶发异常 | 全部原始 JSON 落盘 `bench/results/raw/`，报告可回溯重算 |

---

## 8. 待你确认的决策点

1. **对照组 git 服务端**：Gitea（轻量推荐）还是 GitLab（重但更"企业"）？
2. **k8s 形态**：ECS 自建 k3s（省成本，推荐）还是 ACK 托管（演示价值）？
3. **实验 3 真实大仓**：linux kernel（3GB，可接受）够不够？要不要再上更大（如 nixpkgs）？
4. **免费模型**：用哪家的免费额度（qwen/glm/deepseek）？opencode 配置由你提供还是我默认配 qwen？
5. **agent 任务规模**：T1–T5 是否够，还是要加"中型任务"（如让 agent 完成一个跨 3 文件的重构）以展示 agent 与文件系统交互的放大效应？
