# ScorpioFS 系统论文实施 Spec

状态：Draft v0.2，2026-09-06。代码基线 `6a2e7f2ddb7d913b497167f76ff6539bfbb983c2`。用户附件是需求草案；本 spec 根据源码核对修正接口假设。文中的模块名、API 和测试 ID 是拟议交付物。

总 Epic：[#39](https://github.com/gitmono-dev/scorpiofs/issues/39)。已有路线 issue 全部保持原编号；#45–#48 是已关闭的重复项，不能作为功能已完成的证据。本次没有实施 Rust 代码或运行论文实验。

## 1. 目标、范围与核心约束

目标是在多个 build/agent workspace 中共享不可变文件树、内容与远端请求，并使每个 workspace 的写入保持独立。真实收益通过实验判断，不能把架构复杂度当作论文贡献。

版本协议是第一前置：Mega 原生目录、scope clone、import 仓库和聚合目录需要共同组成可重放视图。详见 [monorepo-versioning.md](monorepo-versioning.md)，该文件规定 source/view/generation、服务端接口、更新及错误语义。

Mega 已在独立 WSL checkout 核对，基线 `c4c79bc195541a13ac1505b94728c81a8ff3d603`；配套服务端草案为 Mega `docs/spec/namespace-snapshot-spec.md`。新增 G01–G06 实施包、MG01–MG17 验收组，覆盖 scope proof、同库 publication txn、push/网页编辑写入口、初始回填与保留。后续两仓已加入固定 source reader、共享 codec、服务端索引及事务存储基础；完整 writer/HTTP/FUSE 链路和论文实验仍未完成，具体证据见两仓 Draft PR。

约束：

- 所有 immutable read 可追溯到固定 view/source/OID；旧 workspace 不随 latest 改变。
- 用户态 lower 共享、物理 CAS 去重、remote single-flight、kernel cache/session 共享分别计量，不相互代指。
- view_id、projection_key、workspace generation、delta_seq、publication_seq 分工明确。
- Libra 负责 VCS，Mega 负责 namespace 发布，ScorpioFS 负责投影和 upper；嵌入模式的 desired state 只有一个 owner。
- 不支持的能力明确拒绝。兼容 mutable mode、CL build 和 interactive worktree 使用独立语义标识。
- 普通进程 crash、daemon crash、机器断电与数据损坏是不同实验；持久化保证以规定的 fsync 边界为准。

## 2. Issue 与工作包映射

| 草案 | GitHub | 本 spec 交付边界 |
| --- | --- | --- |
| 0 Epic | [#39](https://github.com/gitmono-dev/scorpiofs/issues/39) | 研究假设、依赖、实验 claim 映射 |
| 1 Telemetry | [#40](https://github.com/gitmono-dev/scorpiofs/issues/40) | 版本化 events、可控采样、计量开销 |
| 2 Harness | [#41](https://github.com/gitmono-dev/scorpiofs/issues/41) | fixtures、同等 workload、cache state、raw results |
| 新增前置 Namespace contract | [#55](https://github.com/gitmono-dev/scorpiofs/issues/55) | 原生/import/聚合目录的固定路由与发布契约 |
| 3 Snapshot | [#42](https://github.com/gitmono-dev/scorpiofs/issues/42) | SourceSnapshot + NamespaceView，不再仅单一 base_revision |
| 4 CAS | [#43](https://github.com/gitmono-dev/scorpiofs/issues/43) | verified objects、metadata namespace、GC/pins |
| 5 Refresh | [#44](https://github.com/gitmono-dev/scorpiofs/issues/44) | 受控 generation 切换、journal、恢复 |
| 6 Delta | [#49](https://github.com/gitmono-dev/scorpiofs/issues/49) | provenance、增量变化、条件清理、repair |
| 7 Fetch | [#50](https://github.com/gitmono-dev/scorpiofs/issues/50) | single-flight、优先级、公平、有界资源 |
| 8 Shared lower | [#51](https://github.com/gitmono-dev/scorpiofs/issues/51) | A/B/C 验证后选型；依赖版本/观测正确性 |
| 9 Prefetch | [#52](https://github.com/gitmono-dev/scorpiofs/issues/52) | 可选、严格预算、邻居干扰门槛 |
| 10 Integration | [#53](https://github.com/gitmono-dev/scorpiofs/issues/53) | 唯一 state owner、Libra/Orion E2E、独立 oracle |
| 11 Evaluation | [#54](https://github.com/gitmono-dev/scorpiofs/issues/54) | 全矩阵、ablation、可公开 artifact |

关键路径：namespace contract → immutable source/view → CAS → refresh/delta → shared architecture → E2E。M0 与 namespace contract 并行；FetchCoordinator 可在 CAS 后与 refresh/delta 并行；prefetch 不阻塞主线。

## 3. M0：测量基础

### 3.1 Telemetry contract

拟新增 `src/telemetry/{event,context,sink,metrics}.rs`，只通过轻量 context/sink 埋点；禁用时不创建写盘线程。事件字段：`schema_version,event_id,trace_id,span_id,workspace_id,view_id,projection_key,generation,delta_seq,operation,result,data_source,queue_us,latency_us,bytes,object_type,path_id`。

耗时用 monotonic clock，墙上时间只用于关联。并发事件不承诺全局顺序，使用 request/span 关系重建因果。一个共享 fetch 只有一次 physical fetch 计数，所有 workspace 的 logical requests 分开；共享 bytes 不向每个 waiter重复计为物理网络字节。

HMAC path key 在一次实验内稳定，跨实验默认轮换；raw path 显式开启。输出有界队列、采样、rotation、丢弃计数；观测不能阻塞 demand read。指标标签限制基数，路径/OID 不作为无界 histogram 标签。schema 区分 `unknown` 与 0，并声明单位。

验收 T01：trace 关联 create→ready→read/write→refresh→delete；T02：同 blob 64 logical reads 的 physical 计数正确；T03：disabled 无后台写盘；T04：固定 replay 的开启/关闭 overhead 报告，目标中位吞吐损失 ≤5%；T05：旧 schema fixture 兼容。

### 3.2 Harness contract

拟新增 `bench/`，入口及布局在首个实现 PR 固定；建议 CLI 为 `python3 bench/run.py --manifest bench/manifests/smoke.json`。该命令目前未实现。

manifest 必填：schema、fixture seed/commit、view descriptor、workload、baseline、workspace count、unique views/sources、cache state、network profile、repeats、timeout、hardware/kernel/config。产物 `manifest.json, events.jsonl, samples.csv, summary.json`；每次运行单独目录，失败不覆盖原始日志。

baseline adapter 负责 prepare/cache-precondition/run/collect/cleanup；校验所有 baseline 的可见树 digest 相同，partial/sparse baseline 提供 workload 必需路径和依赖。Git worktree baseline 的共享 object store 占用只计一次，per-workspace 文件占用单列。

| 状态 | 前提检查 |
| --- | --- |
| C0 | 新 daemon + 空专用 disk cache，记录 kernel cache 处理策略 |
| C1 | daemon 已热但目标 snapshot 未访问，CAS 状态明确记录 |
| C2 | 目标 shared snapshot 已热，新 workspace 未访问 |
| C3 | 目标 workspace 重复执行 |
| C4 | content cache 保留，kernel/FUSE 状态通过隔离环境重置并验证 |

不能只通过重启 daemon 宣称 kernel cold。可能影响整机的 drop_caches 只在专用实验 VM 中执行。precondition 无法验证则标记 invalid，不与通过验证的 cache 状态混合。

M0 smoke：两版本公开 fixture，native/import/placeholder/conflict，1/4 workspace，metadata/read/write；full 扩至 1/4/16/64。至少 10 次独立重复用于主要性能点；不要拿 10 次样本的一个最大值当可靠 P99，尾延迟另收集足够操作样本并说明嵌套相关性与 CI 方法。WSL 可用于功能 smoke，论文主结果的内核/宿主资源噪声需在专用 Linux 环境控制。

## 4. M1：版本、存储、切换与 delta

规范细节以 [版本 spec §3–8](monorepo-versioning.md#3-拟议身份模型) 为准，模块边界如下：

| 拟新增模块 | 输入/输出 | 禁止承担的职责 |
| --- | --- | --- |
| `src/snapshot/identity.rs` | typed source/view IDs、canonical vectors | 解析用户凭据、隐式 latest |
| `src/snapshot/backend.rs` | capabilities、resolve、immutable tree/blob | 假定所有 scope 同一 commit |
| `src/snapshot/resolver.rs` | view + path → fixed source/object | 在读路径查实时 ref/registry |
| `src/cache/` | verified CAS、metadata、pins、配额 | 把 inode 当内容 ID |
| `src/workspace/transaction.rs` | generation/operation journal | 双写 Libra desired state |
| `src/workspace/delta.rs` | mutation intent、entry seq、changes cursor | 以路径集合 hash 代表内容版本 |

第一 PR 先引入 backend trait 和 fake backend，允许旧 Dicfuse adapter 并存。错误类型和 descriptor 确定后再迁移实际 fetch，不一次性改动所有 daemon 逻辑。新版数据写到独立 schema 根，旧 state 显式标注 legacy。

refresh 先支持清洁、受控、可暂停 workspace；dirty commit 通过 owner 协议和 selective cleanup 单独交付。协议 v2 暴露 `generation` 与 `delta_seq`；旧 `/changes` 的 path fingerprint 字段保持旧语义。

M1 退出：版本 spec V01–V14、V16–V17 correctness 测试通过；真实 Mega adapter 至少完成 native A/B + import A/B + old namespace routing；未支持 namespace 发布时只能交付 source-snapshot capability。

## 5. M2：共享 fetch、架构对照与可选预取

### 5.1 FetchCoordinator

队列按 workspace 做有界 admission，并给 demand metadata/read 高于 speculative prefetch 的优先级。每类队列采用带权公平调度；后台任务也有受控最低进展/aging，避免严格优先级的永久饥饿。跨域请求不错误合并。

single-flight state：Absent→Queued→InFlight→Published/Failed；每个 waiter 独立 deadline/cancel。leader 的执行寿命与某个首发 workspace 解耦。对象大小未知时按块预留带宽与 in-flight bytes，超过单对象预算用磁盘流或明确拒绝，不能突破内存上限。

相同对象的 prefetch 后来遇 demand 时升级队列优先级；已开始的 HTTP 不宣称可以无成本抢占，只通过 demand 预留并发位保证进展。全局和 per-workspace 限额同时生效。永久错误的短负缓存限定 source/OID/权限语境；鉴权失败不变为全局 missing。

验收 F01：64 同对象冷读一次后端请求；F02：取消一个 waiter 不影响其余；F03：retry 和 publish 幂等；F04：队列和内存硬上限；F05：noisy neighbor 下有进展及 demand 延迟报告。用可确定性 fake backend 控制完成顺序与失败点。

传输子课题使用 Mega 的 [研究设计](https://github.com/gitmono-dev/mega/blob/codex/namespace-snapshot-spec/docs/spec/scorpiofs-transfer-research.md) 和 [协议 Spec](https://github.com/gitmono-dev/mega/blob/codex/namespace-snapshot-spec/docs/spec/scorpiofs-transfer-v1.md)。它补充三类动作：并发单对象、精确缺失包、服务端缓存包。先以固定策略建立实测基线，再决定是否实现按缓存和成本选择的算法；不将 tar.zstd、CAS 或固定包大小本身作为论文创新。

H1/H2/H3 分别检验传输选择、跨版本复用和多 workspace 共享。每项都要求正确性及成本分解；REAPI 批量 CAS、工作集聚合和版本化惰性文件系统是必须核对的先例。未运行实际实现时只做机制比较，不宣称胜过原系统。

### 5.2 A/B/C 架构 bakeoff

三个方案定义见 [版本 spec §9](monorepo-versioning.md#9-架构-adr先验证再选择共享挂载方案)。先在 C 上提供正确性基线；A/B 的首轮原型时间箱暂定各 3–5 人日（工程估算，不是排期承诺）。到期产出 ADR：可以进入生产、继续调查或淘汰；没有功能/隔离/恢复证据不扩展为生产重构。

分别记录 unique SourceSnapshot 数、unique NamespaceView/projection 数、FUSE connections、mounts、upper、RSS、CPU、kernel slab/page cache。组合 view 的数量可能大于 source snapshot 数；不能把 O(U) 结论里的 U 随实验更换定义。

功能门槛：shared lower 不变、私有 upper 隔离、unmount 一个不伤其他、refresh 不修改活动 lower、delta index 完整且 crash 可修复。性能保留门槛沿用 #51：64 workspace 的 P95 provision ≥5×改善，或预注册的 CPU/RSS/session 成本指标 ≥3×改善，或真实并发 build throughput ≥25% 改善；全部报告，不能事后换指标挑赢家。

### 5.3 预取

统一 planner 输入 context/hints/budget，输出有序对象候选和可解释 score；budget 同时限制网络 bytes、objects、time、in-flight。已命中 CAS 不计远端预算。准确率以实际后续 demand 事件判断，定义 unused/late/evicted-before-use，取消后只保留仍有需求的 fetch。

先 explicit paths 和 trace hotset，再有证据时做 build hints。#52 为可选：两个真实 workload 的 P95 first-successful-action ≥20%改善、waste ≤ demand bytes 30%、邻居 P95 退化 <5% 才入主论文。静态模式、no-prefetch 和预算变化保留作为对照。

## 6. M3：集成、实验与 artifact

Libra attach 传入 scope commit 或 published view；Mega 提供验证后的 immutable descriptor；ScorpioFS resolve/mount；readiness 后 Libra 才放行进程。Orion 每个 task 记录 view_id 和 CL base/head，重试使用原身份，不能重新读取 latest。

唯一 owner 保存 desired state 和跨步骤 receipt；ScorpioFS 保存 runtime handles 及必要执行 journal。嵌入 External 模式不得另写独立 desired-state 文件；事务 journal 的持有者/持久化 callback 在集成 PR 明确。HEAD/index 与 mount 可能短暂处于恢复中，但恢复屏障完成前不允许任务访问。

公开 correctness oracle 单独物化期望树，验证 path/type/mode/content（额外元数据的合成规则固定）；每次 attach/refresh/commit 都比对。LFS/submodule 未支持时使用显式 fixture 或 fail，不把错误忽略计为成功。

实验矩阵：B0 full checkout、B1 warm bare+worktree、B2 partial+sparse、B3 materialized+kernel OverlayFS、B4 固定现有 ScorpioFS、B5 snapshot+CAS、B6 +fetch、B7 选定 shared architecture、B8 可选 prefetch。组合命名空间 baseline 必须物化同一 bindings，不能把 B4 的浮动 latest 当作正确性对照。

workload 至少覆盖 Buck2、Bazel、Cargo、CMake/Ninja、百万文件 sparse 合成、scripted agent、parallel patch。每个真实仓库先 smoke，再固定公开 commit/target/toolchain。M0 用小 fixture 启动，完整矩阵在机制通过正确性后展开。

每个 claim 对应 experiment ID、原始文件、聚合脚本和图表；预注册 outlier/失败处理，报告绝对值、相对值、median/tail/CI 和失败率。agent 同时报告任务成功率。存储报告 resident/cached/upper/object logical/physical，root-trie 创建成本和服务器租约/GC 成本单列。

传输实验额外控制 Mega 包缓存与客户端对象/kernel cache 的冷热；统计构包 CPU、后端 IO、首次对象完成和整个构建时间。纯按需、执行前已有提示、完整未来 trace 的 offline oracle 分开；按项目及时间切分调优/测试数据，不能用本次测试访问记录预先生成自己的包。既有 1,000 文件/8 包数字只适用于需求已知且可按 128 个聚合的示例，不是在线访问保证。

artifact 包含一键 smoke、fake backend、固定 namespace fixtures、环境 manifest、故障矩阵、raw JSONL/CSV 和画图脚本。需要 FUSE/mount namespace 的测试在具备条件的 runner 中执行，普通 CI 运行无 FUSE 的协议/schema/调度/恢复模型测试。

## 7. 第一轮可执行拆分与验证

| PR 工作包 | 改动范围 | 验收 | 依赖 |
| --- | --- | --- | --- |
| P01 Contract + fixtures | `src/snapshot/` 类型草图、fixtures、API/schema 文档 | canonical vectors、scope mismatch、old import refs 回归预期 | D1；Mega 接口审阅 |
| P02 M0 event core | `src/telemetry/`、最小 create/fetch 埋点 | T01/T02/T03/T05，先得到 physical/logical 计数 | 可与 P01 并行 |
| P03 Smoke harness | `bench/`、baseline adapter、oracle | 相同 native/import A/B 树、失败返回非零、manifest 完整 | fixtures，可与 P02 并行 |
| P04 Immutable read slice | Dicfuse manager/store + fake/Mega adapter | V01/V02/V03/V09，两个版本并存且旧版不漂移 | P01、服务端 source-snapshot |
| P05 CAS + metadata namespace | `src/cache/`、迁移 marker | V08/V14、损坏恢复、配额、旧 schema 不误读 | P04 |
| P06 Namespace composition | bindings resolver + publication adapter | V04–V07/V15/V17 | Mega namespace contract、P05 |
| P07 Refresh + delta | journal/manifest/observer、API v2 | V10–V12/V16、每阶段 crash | D3、P05，namespace 模式还依赖 P06 |
| P08 Scheduler + architecture ADR | fetch coordinator，A/B 最小原型 | F01–F05、#51 功能/性能 gate | M0、P05/P07 |

跨仓衔接：Mega G01 与 P01 共用 canonical/目录 fixture；G02 对接 P04；G03/G04 提供 P06 的固定索引和发布读链路；G05 完成真实部署保留/迁移后，G06 与 P06/P07 联调。G02 可先交付单 source，但 P06 的全 namespace 退出条件不能据此勾选完成。Mega writer 覆盖和 rollout gate 不属于 ScorpioFS 单仓可代办的事项。

后续集成/全量实验在 P04–P08 的实测风险明确后估算；目前没有人力和截止日期，不提供虚假的日历交付时间。

文档变更本次检查：本地相对链接、Issue 编号映射、schema 示例、diff whitespace。实现 PR 应运行相应 unit/integration/FUSE tests；拟议 tests/CLI 尚不存在，不能把它们列为已经通过。

## 8. 决策记录

D1/D2/D3 见 [版本 spec §12](monorepo-versioning.md#12-待确认决策与明确假设)。在收到确认之前，方案按推荐方向写成可审阅草案，不锁定服务端发布政策或 live-refresh 承诺。

上一轮建议的“Option A 直接定为生产首选”撤回为待 bakeoff 的候选。原因是完整 namespace 版本、kernel lower 换代和 delta observer 是联合约束；现有 per-job FUSE 继续承担第一阶段正确性基线。
