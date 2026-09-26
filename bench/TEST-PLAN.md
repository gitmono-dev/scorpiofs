# GitMono vs Git Stack — 详细测试计划

> v0.2 2026-09-24。配套脚本全部在 `bench/` 下；所有 API 调用方式与 `demo/demo.sh`、`bench-scorpio-vs-git.sh` 中已验证的一致。
> 核心设计：**mega2 作为测试基准服务端**——它同时暴露 git 协议端点（`git clone $M2/project`）和 ScorpioFS FUSE 路径，两侧读取同一份存储，服务端差异变量归零；Gitea 仅在实验二（submodule 需要 N 个独立 repo）与实验一的第三方对照线出现。

---

## 1. 环境与角色

| 角色 | 部署形态 | 说明 |
|------|---------|------|
| mega2 | docker（k8s deployment 可选） | 唯一数据源：`$M2` = `http://<node>:19000`；git 端点 `$M2/project` |
| gitea | docker/k8s | 实验 2 的 N 个独立 repo；实验 1 第三方 git 对照 |
| runner-mono | **裸 ECS + systemd** | scorpio daemon(:37251, sudo/FUSE) + libra + opencode |
| runner-git | 裸 ECS | git 客户端 + opencode；与 runner-mono 同规格同 AZ |
| 观测 | docker（prometheus + grafana + node-exporter） | CPU/RSS/fd/网络曲线 |

**变量控制**：
- 冷测前 `sync; echo 3 > /proc/sys/vm/drop_caches`（common.sh::cold_cache）
- 两侧同 commit 基线（seed 脚本保证同一棵过滤后的树）
- 每指标 ≥5 轮（>10min 的实验 3 轮），输出 JSONL 到 `results/raw/`
- mega2 git 端点不支持 `--filter=blob:none`（已验证），git 侧以 full clone 为主口径，gitea 线补 partial clone 变体

---

## 2. 用例矩阵

标记：⏱=计时指标 💾=体积指标 📈=资源曲线 🤖=agent 任务

### 实验一：普通项目（workload: tokio / cpython 过滤版）

| ID | 指标 | git 侧（mega2 git 端点） | git 侧（gitea 对照） | mono 侧 | 判定 |
|----|------|------------------------|---------------------|---------|------|
| E1-TTFW ⏱ | 零→可编辑 | `cold; git clone $M2/project && git checkout`（+checkout 后首 touch 探测） | 同左，指向 gitea | `cold; attach；首写探测`（`libra worktree add` + `touch` 成功） | 记录型，报告 ms |
| E1-DISK 💾 | 工作区占用 | `du -sb`（clone 目录含 .git） | 同左 | `du -sb` mount + scorpio store + size.db 之和 | 记录型 |
| E1-READ ⏱ | 100 文件随机首读 | `cat` 100 随机文件（clone 后，本地） | 同左 | 同 100 文件（冷=drop_cache 后，热=第二次） | 记录型，冷/热分开 |
| E1-STATUS ⏱ | status 延迟（全树、零改动） | `git status`（fsmonitor on / off 两套） | — | `libra status`（effective diff） | 记录型 |
| E1-STATUS-1F ⏱ | status 延迟（全树、单文件改动） | 同上（改动后） | — | 同上 | 记录型 |
| E1-STATUS-SCALE ⏱ | **结构性命中集对比**：树规模 × {0,1,100} 个改动 | `git status` 必须遍历/校验全树（fsmonitor on/off 双线） | — | `libra status` **ScorpioFS fast path**：只查 upper 触碰集，延迟与树大小无关 | **核心赢面**，随树规模放大 |
| E1-COMMIT ⏱ | 小改动→远端 | edit+`git add/commit/push` | 同左 | edit+`libra sync` | 记录型 |
| E1-NET ⏱📈 | 全程网络流量 | vnstat 差分 | 同左 | vnstat 差分 | 记录型 |
| E1-A1 🤖 | D1 新功能：parse_timeout | opencode on clone | — | opencode on mount | 判分通过 + wall time |
| E1-A2 🤖 | D2 补测试：parse_kv ×3 | 同上 | — | 同上 | 判分 + wall time |
| E1-A3 🤖 | D3 修 bug：分钟分支 | 同上 | — | 同上 | 判分 + wall time |

**E1-STATUS-SCALE 的设计（结构性对比，非调参对比）**：
- git 侧：bare repo（本地盘，git 的最佳条件）+ 工作区 clone 到本地盘；
  `git status` 的校验成本随树规模线性增长（fsmonitor 只省 stat、仍需遍历与
  失效校验），三档树规模 × 三种改动量各测 5 轮。
- mono 侧：同一棵树 seed 进 mega2 → attach → `libra status`（fast path）：
  daemon 的 upper 触碰集即候选集，**延迟与树规模解耦**（O(触碰) + 一次 HTTP）。
- 预期形状：git 是 O(树) 斜线，mono 是 O(触碰) 近水平线——交叉点与放大倍数
  就是本项的结论。同时记录 fast path 的语义正确性（touch 判 clean 等，见
  `verify-clean-fastpath.sh` 的四场景）。
- 诚实边界：树很小时 git 的常数项更低（无 HTTP/index 加载）；报告如实给出
  小树场景 git 持平或占优的数据点。

**开发型任务 D1–D3**（`gen-devlab.sh` 生成有真实逻辑的 Rust 配置解析 CLI seed 进
mega2）：三个任务共享 `src/config.rs` 演进语境（加功能 → 补测试 → 修 bug），
互不依赖、独立判分（静态内容校验）。D3 特意留一条"锁定 bug"的过时测试断言，
正确的修复必须同时改实现与测试。prompt/判分内置在 `bin/bench-agent.sh`。

### 实验二：多仓库关联（workload: gen-multirepo 生成的 1 库 + 5 服务）

| ID | 指标 | git 侧（gitea + submodule） | mono 侧（monorepo 子树） | 判定 |
|----|------|---------------------------|--------------------------|------|
| E2-FETCH ⏱💾 | 全量获取 | `git clone --recurse-submodules`（含子模块 init/update） | `attach`（一次） | 记录型 |
| E2-UPGRADE ⏱ | common v1.0→v1.1 全链升级 | 改 common→push→5 服务各 `submodule update --remote && commit && push`（脚本化） | 改 `common/`→`libra sync` 一次 | 记录型 + 步骤数 |
| E2-PARTIAL ⏱💾 | 只要 svc-a | clone svc-a + submodule（common 仍要全量对象） | attach 子路径按需 | 记录型 |
| E2-DRIFT 🤖 | T4 升级任务交 agent | agent 跑升级脚本任务，检查指针漂移（CI 视角 `git ls-remote` 校验） | 同任务 | 定性演示 + 成功率 |
| E2-RENAME 🤖 | T5 跨仓 rename | agent 完成跨 6 repo 改名 | 同任务 | 判分（引用一致性）+ wall time |

### 实验三：大仓库（workload: linux-kernel 真实 / synth-100k / synth-200k 合成）

| ID | 指标 | git 侧 | mono 侧 | 判定 |
|----|------|--------|---------|------|
| E3-INIT ⏱💾 | 初次可用 | full clone（3 轮）+ `--depth=1` 变体 | `cold; attach`（3 轮） | 记录型 |
| E3-READ ⏱ | 100 随机文件冷读 | clone 后 | attach 后（含目录惰性 fetch 成本） | 记录型 |
| E3-MULTI 💾⏱ | 多工作区 ×5 | `git worktree add` ×5：总磁盘+创建时间 | mount ×5（chain fork）：总磁盘+创建时间 | **核心赢面** |
| E3-SWITCH ⏱ | 基线切换 | 制造 10% 变更平行分支，`git switch`（全树重写 10%+） | 新 revision `refresh` / `fork`（lower 换引用） | 记录型 |
| E3-CONCUR 📈⏱ | 4 并发 agent 小任务 | ×4 opencode | ×4 opencode | wall time + daemon 稳定性 |
| E3-SOAK 📈 | 30min 单机压测 | `git status` 循环 | find/status/sync 循环 | fd/RSS 曲线平稳 |

---

## 3. 脚本清单与调用方式

```
bench/
├── TEST-PLAN.md              ← 本文件
├── bin/
│   ├── common.sh             公共库：计时/冷cache/JSONL落盘/env探测
│   ├── bench-agent.sh        opencode 任务驱动（T1-T5 + D1-D3 prompt/判分内置）
│   ├── collect.sh            du/vnstat/prom 快照
│   └── report.py             results/raw/*.jsonl → REPORT.md
├── workload/
│   ├── gen-repo.sh           合成仓库（--files --depth --avg-size --seed）
│   ├── gen-multirepo.sh      1库+5服务（git submodule 版 / 平铺版）
│   ├── seed-mono.sh          评测树导入 mega2（照 demo.sh 已验证路径）
│   └── seed-git.sh           同一棵树 push 到 gitea
├── cases/
│   ├── exp1.sh               E1-TTFW|DISK|READ|STATUS|COMMIT|NET|A1|A2|A3
│   ├── exp2.sh               E2-FETCH|UPGRADE|PARTIAL|DRIFT|RENAME
│   └── exp3.sh               E3-INIT|READ|MULTI|SWITCH|CONCUR|SOAK
├── infra/
│   ├── README.md             阿里云开机器→部署全流程
│   ├── docker-compose.infra.yml   gitea+prometheus+grafana+node-exporter
│   ├── mega2-k8s.yaml        mega2/gitea/观测 的 k8s manifests（可选形态）
│   ├── init-runner-mono.sh   runner-mono 初始化（依赖+scorpio systemd unit）
│   └── init-runner-git.sh    runner-git 初始化
└── results/raw/              JSONL 原始数据（报告可回溯重算）
```

**单用例运行示例**：

```bash
bash cases/exp1.sh ttfw        # 单指标
bash cases/exp1.sh all         # 实验一全部
ROUNDS=5 bash cases/exp3.sh multi switch
bash bin/bench-agent.sh T1 git # 在 git 侧跑 T1
python3 bin/report.py          # 汇总生成 REPORT.md
```

---

## 4. 数据格式

`results/raw/<EXP>-<CASE>.jsonl`，每行：

```json
{"exp":"E1","case":"TTFW","side":"mono","round":3,"metric":"ttfw_ms","value":4120,
 "meta":{"repo":"tokio","cold":true,"ts":"2026-09-24T10:00:00Z"}}
```

`report.py` 按分组聚合中位数/IQR/p95 生成矩阵表，全部原始行保留。

---

## 5. 通过/报告准则

- benchmark 用例本身是**记录型**（无 pass/fail），判分型仅 agent 任务（judge 输出 0/1 + 理由）
- 报告必须披露：git 版本与全部优化配置、mega2/libra/scorpiofs 版本号（`git describe`）、机器规格、AZ、每指标轮数
- 任何异常 run 不剔除、不重跑，附原因记录在 meta.note
