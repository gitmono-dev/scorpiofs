# ScorpioFS 计划模板

本文是 `docs/plan/` 下新建计划的标准模板，改编自 Libra 项目 `docs/development/plan/plan-template.md`（源模板版本 `v2.2`，2026-08-27 生效）。新计划应复制本文件结构，替换 `<...>` 占位符，并删除不适用的说明性文字；强制章节不得删除，不适用时写 `N/A` 和原因。

**模板版本:** `v1.1`（2026-08-31 起生效；`v1.0` 同日生效，改编自 Libra plan-template v2.2；`v1.0`→`v1.1` 的规范性变更见下方「模板版本与迁移政策」的版本历史）。相对源模板的结构性差异——ScorpioFS 是单一包 workspace（`cargo metadata --no-deps` 的 `workspace_members` 只有根包自己这一个成员，不是多 crate workspace；无 `web/`/`worker/` 前端与 Cloudflare Worker 表面）：

1. **发布产物不同：** 发布是 `Cargo.toml` 版本 bump → 推 `v*` tag → `.github/workflows/release.yml` 编译 `x86_64-unknown-linux-gnu`（必须成功）与 `aarch64-unknown-linux-musl`（best-effort）→ 发布 GitHub Release（tarball + sha256）→ `cargo publish` 到 crates.io（`publish-crate` job 声明了 `environment: crates-io`；按 workflow 注释的意图，需要人工审批才会执行——但该保护规则本身配置在 GitHub 网页端，不写在仓库文件里，无法仅凭仓库内容验证是否真的已配置）。没有 Cloudflare R2/D1、没有 Homebrew tap、没有 CodeQL 工作流。
2. **版本面只有一处权威源：** 只有 `Cargo.toml` 的 `[package].version` 会被 CI/工具链解析；仓库内未发现第二处需要与它保持数值一致、由工具强制同步的版本面（`install.sh` 通过 `SCORPIO_VERSION` 环境变量或 GitHub Releases API 解析版本，不写死版本号）。但仓库里确实存在把版本号写成示例文本、不受任何工具约束的位置（例如 `README.md` 的 `/health` 输出示例、`install.sh` 帮助文本里的 `--version` 用法示例），这些会随发布自然过期，属于文档同步层面要留意的东西，不构成 ER-08 意义上「必须同步」的版本面。因此 ER-08 不需要 Libra 式的「五处同步」，但 GC-05 文档同步在处理版本相关改动时应顺手检查这类示例是否已经过期。
3. **测试基线不同：** 没有 `.env.test`、没有 L1/L2/L3 分级，全量套件是 `cargo fmt --all -- --check` + `cargo clippy --all-targets --all-features -- -D warnings` + `cargo test --verbose --all-features` + `cargo test --doc`（均为 `dtolnay/rust-toolchain@stable`，非 nightly）。另有一条不计入常规全量、只在改动 `install.sh`/打包/systemd 表面时触发的 `qlean-ci` 门（QEMU/libvirt 隔离安装冒烟，`cargo test --locked --features qlean-ci --test qlean_installer -- --ignored`），见 ER-13。
4. **没有 `tests/INDEX.md`：** 普通 `tests/*.rs` 由 Cargo 自动发现；只有 feature-gated target（如 `qlean_installer`）需要在 `Cargo.toml` 显式登记 `[[test]]` + `required-features`。
5. **没有 `docs/commands/<cmd>.md` EN+zh 结构、没有 `COMPATIBILITY.md`、没有 `LBR-*` 风格稳定错误码：** 用户可见契约是 CLI 稳定退出码（0/1/2/3/4，README 已文档化）与 HTTP JSON 错误响应（`src/daemon/antares.rs`、`src/daemon/mod.rs`）；文档集是 `README.md`、`deploy/README.md`、`docs/*.md`（扁平结构，非按命令拆分的双语页面）。
6. **没有 `AGENTS.md`/`CLAUDE.md`：** 本模板是当前唯一的书面质量契约；一旦仓库新增这两份文件，必须回来同步本模板与其内容（记一条修订历史）。
7. **VCS 不变：** 本仓库用 `.libra`（非 `.git`）做版本控制——ScorpioFS 自身也是 Libra/Mega 生态的被服务对象之一，dogfood 了 `libra` 命令行。ER-07（`libra status/add/commit/push` 与提交签名）与 GC-12（精确暂存）照抄源模板，因为它们描述的是 `libra` 这个工具本身的行为，与哪个仓库使用它无关；只删除了源模板里针对 Libra 仓库自身 `AGENTS.md` 措辞漂移的说明段（本仓库没有该文件，不存在漂移）。

其余方法论——ER-*/GC-*/G-*/ADR-*/GAP-*/DEP-*/REL-*/EX-*/FIX-*/DEFER-*/M-* 的编号体系、粒度规则、发布切片纪律、并发与串行发布边界、测试分层——原样保留，因为这些是与产品表面无关的过程约束。

### 模板版本与迁移政策

- 生效日期之后**新建**的计划必须整份符合本版模板。
- 生效日期之前成稿的计划（本仓库目前没有）按**增量迁移**处理：只有本次被新增或做规范性修改的任务卡需要满足本版 `G-*` 与新增字段；未触碰的卡保持原样，不构成违规。
- 本模板的后续修订必须在「模板版本与迁移政策」登记一行版本历史，说明相对上一版的规范性变更；不得直接覆盖旧版本号。
- 若某份计划因迁移成本暂时保留与本版冲突的口径，必须在该计划的「修订历史」登记一行例外与预期迁移时机。

**版本历史:**

| 版本 | 生效日期 | 相对上一版的规范性变更 |
|---|---|---|
| `v1.0` | 2026-08-31 | 初版，改编自 Libra plan-template `v2.2`（见上方「相对源模板的结构性差异」7 条） |
| `v1.1` | 2026-08-31 | 「字段全局默认与例外」新增明确例外：`Task type` 字段不适用「取默认值可整行省略」规则——G-11 要求每张卡无论取值是否为默认都必须单独写出 `**Task type:**` 这一行；原文把 `Task type` 也列进可省略清单，与 G-11 的强制声明要求矛盾（`docs/plan/plan-20260831.md` 的 Codex Review R25 发现并修正，见该计划「修订历史」2026-08-31 R25 行） |

## 使用规则

- 日期计划命名为 `plan-YYYYMMDD.md`，用于可执行的实现、迁移、打包、发布或文档收敛任务。
- 长期能力进入 `plan-long.md`（本仓库尚未创建；首次需要登记跨计划长期能力时新建，不得把长期路线图复制进日期计划）。
- 每个计划必须以当前 checkout 的源码、测试、`docs/*.md`、`README.md`、`deploy/README.md`、`.github/workflows/*.yml` 为事实基线。历史的 `docs/improvement/*.md` 评估文档、旧 issue、截图或口头描述只能作为线索，不能作为「已实现」的证据——`docs/improvement/deployment.md` 就是一个反例：其中列出的多项 `[P0]` 缺口（`Dockerfile`、systemd unit、`install.sh`、`release.yml`）在写作当时确实缺失，但截至本模板生效日已经全部实现，若不核实现状会误判为待办。
- 每个任务卡必须能交给 Agent 独立执行：范围明确、依赖明确、文件落点明确、验收标准明确、验证命令明确。
- 每个任务卡必须满足「任务卡粒度规则」全部 `G-*` 条款：单一可独立恢复的行为轴、条目与规模在上限内、默认一张卡一个发布切片。粒度不合格的卡不得进入开工态，必须先拆分或合并。
- 涉及 CLI 参数/子命令、`scorpio.toml`/`config.toml` schema、HTTP API（`/antares/*`、已废弃的 `/api/fs/*`）、systemd unit、`Dockerfile`/`docker-compose.yml`、`install.sh`/打包脚本、FUSE 挂载语义或权限边界的计划，必须包含测试、文档、回滚和兼容处理。
- 测试成本按阶段分层（ER-13）：任务卡执行阶段默认只跑与本卡相关的 focused 测试，命中全量触发条件或另有特定要求时才跑全量；全部任务卡完成后必须跑一次全量收口门，并修复该次全量暴露的全部 Bug。
- 若计划引用外部项目/仓库作为参照或依赖（例如 `libra-tools/libra`、Omarchy 上游仓库、AUR 包仓库），必须 pin 具体 revision、文件路径和核对日期；不得把浮动 `main`/`HEAD` 当作规范，并在「依赖登记表」登记为 `DEP-*`。
- 新增或重命名 `--test` target 时，若该 target 是 feature-gated（如 `qlean-ci`），必须在 `Cargo.toml` 注册 `[[test]]` + `required-features`，并在 `script/README.md` 或本计划的「测试矩阵」登记用途；普通 `tests/*.rs` 无需额外登记（Cargo 自动发现）。
- 生产代码不得新增未解释的 `unwrap()`、`expect()` 或 `panic!()`；如确属不可失败逻辑，必须有 `// INVARIANT:` 注释并在任务验收中说明。

### 规范性 ID 与术语

计划正文引用规范条款时一律用具体 ID（例如「按 G-03 拆卡」），不要用「上一节」「前面那条」或会随条款增删失效的范围表述。新增条款时必须同步下表。

| 前缀 | 含义 | 定义位置 |
|---|---|---|
| `ER-*` | 执行检查必备需求（开工、验收、发布、证据） | 「执行检查必备需求」 |
| `GC-*` | 全局工程约束（对全部任务生效） | 「全局工程约束」 |
| `G-*` | 任务卡粒度规则 | 「任务卡粒度规则」 |
| `ADR-*` | 已决议设计决策 | 「已决议设计决策」 |
| `GAP-*` | 事实基线缺口 | 「当前缺口」 |
| `DEP-*` | 依赖登记项（含跨计划、跨仓库与外部前置） | 「依赖登记表」 |
| `REL-*` | 发布分组（含家族卡窗口） | 「发布分组与并发窗口」 |
| `EX-*` | 白名单内的规则 waiver（需具名审批） | 「字段全局默认与例外」 |
| `FIX-*` | 执行期发现的越界修复卡（ER-10） | 对应 Phase 末尾 |
| `DEFER-*` | 延后项 | 「非目标与延后项」 |
| `M-*` | 里程碑 | 「里程碑验收与回滚」 |

术语（全文统一，不要混用同义词）：

- **行为轴**：一个可独立恢复、对外语义自洽的变化方向（例如「scorpiofs 的 AUR `-bin` 打包」是一个轴，「systemd unit 的用户/权限模型」是另一个轴）。
- **落点**：一个可枚举的代码或文档归属域，粒度为**一个具体目录**（如 `packaging/archlinux/scorpiofs-bin/`）或**一组同名文档**（如 `deploy/README.md` + 本计划新增的打包说明）。仓库根、`src/`、`tests/`、`docs/` 这类顶层目录**不算**一个落点。
- **写集**：会被修改的文件/目录集合，分三类（G-10）——**实现写集 I**（每卡字段，决定能否并发）、**发布写集 R**（每卡字段，`Cargo.toml` 版本行 + `Cargo.lock` + release artifact；不用于实现阶段的并发分组，但进入发布窗口后按 I–R / R–R 规则串行化）、**协调写集 C**（计划级，发布顺序与窗口记录，不进任务卡字段、不参与并发判定）。
- **发布切片**：一次独立的 review + 验收 + **本卡版本 bump（默认 `patch + 1`）** + 提交 + 推送 + **`gh` 触发 GitHub Release**（`Task type = release`/`independent` 才需要；纯打包脚本/文档类任务可能是 `no-release`，见下）。
- **focused 测试**：只覆盖本卡行为的测试集合——ER-04 A 组按本卡实际改动表面选出的命令，加上本卡 `Verification` 登记的指定用例。
- **全量测试**：`cargo fmt --all -- --check` + `cargo clippy --all-targets --all-features -- -D warnings` + `cargo test --verbose --all-features` + `cargo test --doc`；改动 `install.sh`/`script/test_installer*.sh`/systemd unit/`Dockerfile`/`docker-compose.yml` 时并入这些脚本自身的验证方式（见 ER-04 A 组表）。fmt 与 clippy 是独立的静态门，不计入「测试」，任何会推送的卡都必须跑（ER-13）。
- **家族卡**：共用唯一发布点的一组子卡（G-08）。
- **恢复模式（字段名 `Rollback mode`）**：`revert` / `forward-only` / `compensating` / `immutable-release` 四种之一（G-01）。不可逆变更用后三种表达，不要求「一次 revert 撤销」。

## 标题

`# <主题>计划（<YYYY-MM-DD>）`

## 文档职责

本文解决 `<问题/能力>`，目标是 `<可交付结果>`。

本文只规划任务，不宣称实现完成。落地时每个任务都必须先刷新源码/文档锚点，再按任务卡验收。

### 适用范围

- `<包含的命令/模块/打包表面/部署表面>`
- `<包含的配置 schema、HTTP API 或 FUSE 挂载表面>`
- `<包含的测试、文档、兼容矩阵或发布动作>`

### 非目标

- `<明确不做的能力>`
- `<延后到其它计划/ADR 的范围>`
- `<容易被误解但本计划不承诺的行为>`

### 成功定义

- `<用户或系统行为变化>`
- `<机器接口、退出码或部署产物变化>`
- `<文档、测试、发布证据>`
- `<何时可标记计划完成>`

## 事实基线

> 所有行号和源码/文档锚点必须在开工当天刷新。过期锚点只能作为历史线索。

| 类别 | 当前事实 | 证据 |
|---|---|---|
| 代码入口 | `<src/...>` | `<file:line>` |
| CLI 表面 | `<scorpio ... / antares ...>` | `<src/cli.rs:line 或 src/bin/antares.rs:line>` |
| 配置/状态 | `<scorpio.toml / config.toml key>` | `<file:line>` |
| HTTP/机器输出 | `<Antares API /antares/*、legacy /api/fs/*、/health>` | `<src/daemon/...:line>` |
| 退出码 | `<0/1/2/3/4 契约>` | `<src/main.rs 或 src/cli.rs:line>` |
| 部署表面 | `<install.sh / deploy/systemd/*.service / Dockerfile / docker-compose.yml>` | `<file:line>` |
| 文档 | `<README.md / deploy/README.md / docs/...>` | `<file:line>` |
| 测试 | `<tests/...>` | `<target::test_fn>` |
| 外部参照 | `<repo@sha>` | `<path + date>` |

### 当前缺口

| ID | 缺口 | 影响 | 证据 | 计划动作 |
|---|---|---|---|---|
| GAP-01 | `<问题>` | `<用户/生产影响>` | `<file:line 或外部证据>` | `<任务 ID>` |

## 与其它计划的关系

| 计划/文档 | 关系 | 本计划处理 |
|---|---|---|
| `plan-long.md` | `<本仓库尚未创建 / 关联长期能力编号>` | `<链接、消费、更新状态或不触碰；首次登记时新建>` |
| `plan-YYYYMMDD.md` | `<前置/并行/替代/冲突>` | `<复用、不重做、迁移、关闭>` |
| `docs/improvement/*.md` | `<历史评估文档，可能已过期>` | `<核实后引用或标记过期>` |

## 评审结论与修订记录

计划成稿前必须从以下维度做一次自审；如果有阻断项，先修计划再开工。

| 维度 | 结论 | 修订动作 |
|---|---|---|
| 合理性 | `<目标是否值得做>` | `<调整>` |
| 可行性 | `<任务是否可拆、可交付>` | `<调整>` |
| 任务卡粒度 | `<是否存在多轴卡、L/XL 卡、碎片卡、未登记的合并发布>` | `<按 G-* 拆分/合并/登记例外>` |
| 依赖与顺序 | `<DAG 是否无环、是否缺边、跨仓库依赖是否已登记 DEP-*>` | `<调整>` |
| 完整性 | `<测试/文档/迁移/回滚是否齐全>` | `<调整>` |
| 安全性 | `<HTTP API 鉴权边界、FUSE 权限、secret、路径、systemd 特权>` | `<调整>` |
| 功能正确性 | `<挂载/卸载状态机、边界条件、错误路径>` | `<调整>` |
| 接口兼容 | `<CLI 参数/退出码/HTTP 契约/systemd unit>` | `<调整>` |
| 数据流与控制流 | `<挂载并发、Antares CL 生命周期、幂等>` | `<调整>` |
| 性能与容量 | `<热路径、复杂度、安装耗时>` | `<调整>` |
| 可靠性与容错 | `<崩溃恢复、重试、残留挂载点释放>` | `<调整>` |
| 可维护性 | `<事实源、抽象边界、重复实现>` | `<调整>` |

### 修订历史

计划成稿后的每次规范性变更（任务卡拆分/合并、依赖调整、发布边界变化、决策反转）都必须在此登记一行；G-09 的拆分同步以本表为闭环凭证。

| 日期 | 触发 | 变更内容 | 原卡 → 新卡 | 受影响的引用 |
|---|---|---|---|---|
| `<YYYY-MM-DD>` | `<自审 / review / 现状核对>` | `<做了什么规范性修改>` | `<TASK-ID> → <TASK-ID>, <TASK-ID>` | `<实施顺序、依赖登记表、REL-*、追溯表、测试矩阵、里程碑、风险表>` |

## 已决议设计决策

实现时若需偏离本节，必须先修改计划并说明原因，不得在代码/脚本中静默改语义。

### ADR-<PREFIX>-01: <决策标题>

- **Status:** Accepted
- **Context:** `<为什么需要这个决策>`
- **Decision:** `<选定方案>`
- **Alternatives considered:** `<备选方案及拒绝理由>`
- **Consequences:** `<带来的约束、风险、后续工作>`
- **Revisit when:** `<何时应重审>`

## 全局工程约束

以下约束对本文所有任务生效。任务条目不再逐条重复，违反任一项即视为任务未完成。

- **GC-01 现状核实前置:** 每个任务开工前重新核对计划、`README.md`、`deploy/README.md`、相关 `docs/*.md`、当前代码、CI workflow 和测试。如果已实现，则任务改为补测试、补文档、更新状态或关闭，不重复实现（`docs/improvement/deployment.md` 的教训：不核实现状会把已完成的工作重新排上日程）。
- **GC-02 单一事实源:** 挂载状态机、`scorpio.toml`/`config.toml` schema、HTTP 响应结构、退出码、权限策略和共享 helper 必须有单一事实源。禁止 CLI、HTTP handler、systemd unit 或安装脚本各自复制路径/端口/用户名等常量。
- **GC-03 FUSE/Antares 语义边界:** 挂载表面必须说明与本地文件系统语义的一致点和有意差异（例如惰性加载 stat、`dicfuse_readable`、Antares CoW upper 层与 CL 生命周期）；HTTP API 表面必须说明幂等性、并发挂载语义与鉴权（或明示无鉴权）边界。
- **GC-04 输出与契约:** 用户可见的 CLI 退出码契约（0 成功、2 配置错误、3 挂载/卸载失败、4 HTTP 绑定失败、1 其它内部错误）与 HTTP JSON 错误响应结构必须保持稳定；新增错误路径需要在对应模块补齐响应分支并同步 `README.md`、`docs/antares.md`、`docs/api.md`。
- **GC-05 文档同步:** 命令、配置项、HTTP 端点、systemd unit 或安装脚本参数变化必须同步 `README.md`、`deploy/README.md`、相关 `docs/*.md`、`scorpio.toml.example`。
- **GC-06 测试发现（本仓库无 tests/INDEX.md）:** 新增普通 `tests/*.rs` 由 Cargo 自动发现，无需额外登记；新增或修改 feature-gated `--test` target（如 `qlean-ci`）必须在 `Cargo.toml` 注册 `[[test]]` + `required-features`，并在 `script/README.md` 登记用途。
- **GC-07 安全默认值:** 未满足认证、路径归属、权限或 sandbox 前置时默认 fail-closed。**注意实际现状**：`scorpio` 二进制自身的裸 CLI 默认值是 `--http-addr 0.0.0.0:2725`（`src/main.rs` 的 clap 默认值），且 HTTP API 无认证中间件；裸 CLI 默认值本身从未变安全过，现有的安全收紧靠**两种不同机制**、共三处落地——① **CLI 参数收紧**：`deploy/systemd/scorpiofs.service` 与 `install.sh` 生成的 `ExecStart`（`install.sh:29` 的 `HTTP_ADDR="${SCORPIO_HTTP_ADDR:-127.0.0.1:2725}"` 写进 `install.sh:1583` 生成的 `ExecStart`，选了非 loopback 地址还会先警告、非显式确认就强制拉回 loopback）都是显式传 `--http-addr 127.0.0.1:...`；② **宿主机端口映射收紧**：`Dockerfile` 的 `CMD` 其实是 `serve --http-addr 0.0.0.0:2725`（容器网络命名空间内部必须监听所有接口，端口转发才能工作），真正的安全边界来自 `docker-compose.yml`/`README.md` 建议的 `-p 127.0.0.1:2725:2725` 宿主机端口映射——这不是「给二进制传了个安全参数」，而是「用 Docker 的端口发布机制限制宿主机上谁能连到这个端口」，两种机制不能混为一谈。因此任何新增的部署/打包产物必须清楚自己用的是哪种机制：直接跑二进制（systemd/PKGBUILD 等）就必须**显式**传递 `--http-addr 127.0.0.1:...`；走容器化部署就必须确保宿主机端口映射收紧到 `127.0.0.1`，不能假设容器内的 `0.0.0.0` 监听本身有问题。不得依赖「CLI 自身默认值就是安全的」这一错误假设；修改 CLI 自身的全局默认值是一次独立的、影响所有现有部署方式的破坏性变更，需要单独的 ADR 和兼容窗口，不能顺带在打包类计划里做。
- **GC-08 原子性与恢复:** 修改挂载状态（FUSE mount/umount）、Antares job/CL 生命周期、`config.toml` 运行时状态文件或发布状态时，必须定义事务边界、幂等键、崩溃窗口和回滚/前滚策略。
- **GC-09 并发与资源生命周期:** 挂载点、`/dev/fuse` 句柄、systemd 服务重启、临时目录和子进程必须有释放/恢复语义；测试不得依赖未隔离的全局状态（真实 FUSE 挂载测试需要显式的隔离/清理）。
- **GC-10 性能预算:** 安装脚本、挂载/卸载路径、HTTP 热路径不得引入无界扫描、无界内存或不可预期的安装耗时。需要时写出数据规模和断言。
- **GC-11 生产 panic 禁止:** 生产路径不得新增裸 `unwrap()`、`expect()`、`panic!()`；必须用 `Result`、`anyhow`/`thiserror` 或领域错误返回可操作信息。
- **GC-12 精确暂存:** 提交前只 `libra add <相关路径>`，不得使用等价于 `commit -a` 的一次性全量提交。发现无关脏状态时保留并报告，不得清理、重置或混入提交。

## 执行检查必备需求（强制）

任一要求未满足，对应任务不得标记完成。条目使用稳定 ID，正文引用时用 ID 而不是序号，便于后续插入条目而不破坏交叉引用。

1. **ER-01 开工前安全检查:** 运行 `libra status --short --branch`，确认当前分支与计划指定分支一致、工作区脏状态、目标文件是否已有无关改动。若目标文件已有未确认用户改动，先报告并避免覆盖。
2. **ER-02 先核对后实现:** 刷新本任务相关源码锚点、文档锚点、CI workflow 和外部参照 revision，再决定实现、补测、补文档、关闭或降级。
3. **ER-03 粒度门禁:** 开工前按粒度规则 `G-*` 逐条复核本任务卡，并逐字段核对该卡的 `Granularity` 摘要行。若核对后发现范围已扩大，先修改计划拆卡再开工，不得在实现中静默扩张任务范围。
4. **ER-04 每卡验收门:** 门由 **A 表面 focused 门**（按实际改动的表面）+ **B 类型门**（按 `Task type`）+ **C 发布收口门**（覆盖要求对所有非延后卡生效，执行归属见下）+ **D 远端后置门**（有不可本地复现的 CI 语义时）四组组成，**所有适用行累加**，全部通过才算验收。权威口径：`cargo fmt --all -- --check` 与 `cargo clippy --all-targets --all-features -- -D warnings` 两门是**任何会推送的改动**的完成契约，本模板不得削弱、任何卡不得跳过；第三门（`cargo test --verbose --all-features` + `cargo test --doc`）在**计划驱动的多任务卡工作**中按 **ER-13** 分层执行。

   **门不计入条目上限：** 本条列出的门是全局强制门，**不计入**任务卡 `Verification` 的 G-03 条目计数；`Verification` 只登记本卡特有的判据。

   **两个正交状态字段（都在任务卡登记）:**
   - `Lifecycle`（执行生命周期）：`pending` | `in-progress` | `blocked`（ER-10 的越界故障置此值）| `done`。
   - `Acceptance`（验收状态）：空 | `locally-accepted` | `remote-pending`（仅当本卡有适用的 D 组远端后置门）| `complete`。
     - `locally-accepted` = 本卡适用的 **A 组 + B 组**门已过，但本卡的 **C 组覆盖**尚未取得。此状态下**不得**对外报告「完成 / done」。
     - `remote-pending` = A/B 已过且 C 组覆盖已取得，但本卡适用或继承的 **D 组**远端后置门尚未全绿。此状态同样**不得**报告完成。
     - `complete` = A + B 已过、C 组覆盖已取得、且适用或继承的 D 组已全绿。无 D 时 C 覆盖到手即可 `complete`；有 D 时必须先经 `remote-pending`。
     - 唯一状态转移路径：A/B 通过 → `locally-accepted` → review PASS → 取得 C 组覆盖 →（无 D：`complete`；有 D：`remote-pending` → D 全绿 → `complete`）。
   - 两者独立取值：`blocked` 卡的 `Acceptance` 可以已是 `locally-accepted` 甚至 `complete`。`Lifecycle=done` 必须以 `Acceptance=complete` 为前提；`blocked` 必须先回到 `in-progress` 并完成剩余动作才能进入 `done`。

   **C 组覆盖与执行归属:**
   - **独立发布卡（`Release boundary = independent`）与发布点卡（`release` / `family release point`）**：自行执行完整 C 组门。
   - **`family child`**：不 bump、不构建、不推送，**继承**其家族唯一发布点的 C 覆盖；家族发布点完成时按 ER-08 做一次 `patch + 1`（或 ADR 规定的 `minor`/`major`）并走完整 C/D。
   - **`no-release` 卡（`docs` / `audit` / `spike` / `handoff`）**：**继承**任务卡显式声明的承载发布点的 C 覆盖与其 D 组；该承载点必须在卡内写明 ID，不得留空。

   门分四组，全部适用者累加：

   **A 表面 focused 门**（覆盖全部合法生产表面；本仓库单一 Rust crate，无 `web/`/`worker/`/`sql/` 表面）：

   | 实际改动的表面 | focused 门 |
   |---|---|
   | Rust 内部单元逻辑（测试写在 `src/**` 的 `#[cfg(test)]` 里） | `cargo test --lib <filter>` |
   | Rust 集成 / CLI / HTTP 行为（测试在 `tests/**`，非 feature-gated） | `cargo test --test <target> [<filter>]` |
   | Rust 二进制入口（`src/bin/antares.rs` 等） | `cargo test --bin <name> [<filter>]` |
   | feature-gated 集成测试（`qlean-ci`） | `cargo test --locked --features qlean-ci --test qlean_installer -- --ignored [<filter>]`（需要 KVM/libvirt，本地或 CI 均可能不可用，见 T-CI 条件） |
   | `install.sh` / `script/test_installer*.sh` | `bash -n install.sh`（语法检查）+ `script/test_installer.sh`（安全校验冒烟，无系统改动）+（涉及 systemd 生成逻辑时）`sudo script/test_installer_systemd.sh <version> <release-url> <test-root>` |
   | `deploy/systemd/*.service` | `systemd-analyze verify deploy/systemd/<unit>.service`（语法与依赖检查）+ 手工证据：在测试环境 `systemctl start`/`status`/`stop` 一轮 |
   | `Dockerfile` / `docker-compose.yml` | `docker build -t scorpiofs-check .` + `docker compose -f docker-compose.yml config`（配置渲染检查） |
   | 新增打包表面（PKGBUILD / `.SRCINFO` / `.install` / `.sysusers` 等，本计划新增） | `namcap PKGBUILD`（若可用）+ `makepkg --printsrcinfo` 与已提交的 `.SRCINFO` 逐字节比对（无 diff）+（有 Arch/Omarchy 环境时）`makepkg -si --noconfirm` 冒烟安装 |
   | 只改文档 / 说明（无代码、无配置、无打包脚本） | 无表面 focused 门，只走 B 组的结构与链接门 |

   **Rust 行的修饰规则（不是独立表面）:** feature gate 与 env/线程约束是上面 Rust 行的**修饰条件**：先按测试归属唯一选中 `--lib` / `--test` / `--bin` 中的一行，再把该 target 实际需要的 `--features` 并入同一条命令。不得另跑一条不带 feature 的裸命令充当「集成行」。

   **不得**为了凑一条命令而制造与本卡无关的用例；也不得因为「C 组已有全量测试」就跳过 A 组某个表面的行。

   **仓库配置与 CI 展开规则:** 受影响 job 必须从本卡实际改动的那个 workflow 文件（`fmt.yml` / `clippy.yml` / `build.yml` / `test.yml` / `ci.yml` / `qlean.yml` / `release.yml`）现场提取：

   ```bash
   awk '/^jobs:/{in_jobs=1; next} in_jobs && /^[^[:space:]]/{exit} in_jobs && /^  [A-Za-z0-9_-]+:/{print FNR ":" $0}' .github/workflows/<file>.yml
   ```

   再读取受影响 job 的完整定义（`strategy`/`matrix`、`env`、`if`、`run`、`uses`、`with`）后据此写判据。**依赖 GitHub Actions runner 环境的步骤不得照抄为本地命令**（如 `qlean.yml` 里的 `libvirt`/KVM 初始化、`release.yml` 的 `softprops/action-gh-release`、`cargo publish` 的受保护环境审批）——这些归入下文 **D 组远端后置门**。

   **B 类型门**（按 `Task type` 取一行）：

   | Task type | 类型门 |
   |---|---|
   | `implementation` / `migration` / `removal` | 无额外类型门（由 A 组 + C 组构成完整验收；`family child` 自跑 A 组 + fmt/clippy，C 覆盖继承自家族唯一发布点） |
   | `docs` / `audit` / `handoff` | 结构与链接门：本卡产物文件存在且章节完整、内部链接与 `file:line` 锚点可解析、`libra status --short --branch` 无越界改动 |
   | `spike` | 产物门：结论文档或 ADR 已落盘、go/no-go 已判定、承接卡已登记；**allowlist diff 门**——`libra status --short --branch` 的全部变更必须落在本卡 `Deliverables` 声明的产物内，且生产表面（`src/**`、`Cargo.toml`/`Cargo.lock`、CI 与仓库配置、`install.sh`、`deploy/**`、新增打包目录）零改动 |
   | `release` | 聚合守卫（本组引入的全部新守卫用例）+ release note / 兼容证据 |

   **C 发布收口门（由会推送的卡执行，顺序强制）:** ① 版本预检（ER-08：核对 `Cargo.toml` 当前版本）→ ② 对本卡做版本 bump（默认 `patch + 1`）并同步 `Cargo.toml` → ③ **禁止手改 `Cargo.lock`**：执行 `cargo build`（或本卡等价的 `cargo build`/`cargo check`），由 Rust 工具链刷新 `Cargo.lock` → ④ 在**已 bump 的状态**上跑本卡收口测试门：`cargo fmt --all -- --check` 与 `cargo clippy --all-targets --all-features -- -D warnings` **恒定必跑**；测试面按 **ER-13** 判定 → ⑤ `cargo build --release` → ⑥ 本地/测试环境安装验证（`install.sh` 或 `makepkg -si`，视本卡表面而定）→ ⑦ `libra add <相关路径>` + `libra commit -s -m`（ER-07）→ ⑧ 推送并确认 branch ref：`libra push origin main` 成功 → ⑨ **发布步（仅 `Task type = release`/`Release boundary = independent` 且本卡确实要面向用户发行新版本时执行；纯打包脚本迭代若不需要立即切新版本，可在 `Release boundary` 声明 `no-release` 并说明将随哪次发布进入用户渠道）**：

   `.github/workflows/release.yml` **只由 `push` 到 `v*` tag 触发**（`on: push: tags: v*`），光推 `main` 不会触发它；触发之后，其 `release` job（`needs: build`）用 `softprops/action-gh-release@v3` **自动创建/更新 GitHub Release 并上传构建产物**。也就是说触发方向是「先有 tag push，`release.yml` 才会自己创建 Release」，不是反过来靠一条 `gh release create` 命令去建 Release 或触发 workflow——**推 tag 之后不要再手动跑 `gh release create`**：`release` job 已经在做同一件事，手动再跑一次要么和自动创建竞态（谁先谁后不确定），要么在自动创建已完成后因 Release 已存在而直接失败退出。发布步到创建并推送 tag为止（**C 组止步于此**，见下方「C / D 边界」）：

   ```bash
   VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n1)"
   libra tag -m "Release v${VERSION}" "v${VERSION}"   # libra 内建 tag 子命令（annotated tag）
   libra push origin "v${VERSION}"                     # 推送 tag——这一步本身就会触发 release.yml，无需再手动调用 gh release create
   ```

   **C / D 边界（唯一口径）:** C 组截止到已验证的 tag 推送（该推送本身即触发 `release.yml`）；推送之后由远端 `release.yml` 产生的一切证据——多平台编译、`release` job 自动创建的 GitHub Release 及其产物、`publish-crate`（声明了 `environment: crates-io`，按 workflow 注释的意图需人工审批——该保护规则本身配置在 GitHub 网页端、无法仅凭仓库内容验证是否已配置，但无论是否已配置都不阻塞 `complete`，见下方「`publish-crate` job 的例外」）——全部归 **D 组**，包括等待 workflow 跑完与核实 Release 已生成这两步（见下方 D 组命令）。

   **D 远端后置门（post-push）:** `release.yml` 的 `build`/`release` job，以及 `qlean.yml` 的 libvirt/KVM 隔离安装验证，只能由 push / tag 触发，本地无法产出证据。任务卡必须写明要看哪个 workflow 的哪个 job、触发事件与 ref、判据，并在推送后取得结果。**不要用 `gh run watch <run-id>` 等待整个 run**——`release.yml` 的 `publish-crate` job（`needs: release`）声明了 `environment: crates-io`（workflow 注释里写的意图是「配置一个必须人工批准的 reviewer，防止 tag push 自动发布到 crates.io」，但该保护规则本身是 GitHub 网页端的仓库设置，不写在 workflow YAML 里，无法仅凭本仓库内容验证是否真的配置了——本计划按注释所述的意图假设它已配置）。若已配置，该 job 会停在 `waiting` 状态直到人工批准；`gh run watch` 等待的是整个 run（含 `publish-crate`）到达终态，一旦真的停在 `waiting`，会被无限期挂起，与「`publish-crate` 不阻塞 `complete`」的既定口径自相矛盾——因此无论保护规则是否已实际配置，都不应依赖等整个 run 结束这件事。**全部会发版卡统一使用以下命令，只等待 `build`/`release` 两个 job（不含 `publish-crate`）**（同样不手动调用 `gh release create`）：

   ```bash
   RUN_ID="$(gh run list --workflow=release.yml --branch "v${VERSION}" --limit 1 --json databaseId --jq '.[0].databaseId')"
   test -n "$RUN_ID"                                   # 确认确实捕获到该 tag 触发的 run，而非空结果
   # 注意：until 判断的是命令的退出码，而 `gh ... | jq` 只要 gh/jq 本身没出错就退出码 0——
   # 哪怕 jq 打印的是字面 "false"，退出码仍是 0，这样写会导致 until 立刻当作"已完成"退出、根本不轮询。
   # 必须显式把 jq 输出的文本（"true"/"false"）拿来跟字符串 "true" 比较：
   until [ "$(gh run view "$RUN_ID" --json jobs \
       --jq '[.jobs[] | select(.name | startswith("build ") or . == "Publish GitHub Release") | .status == "completed"] | all')" = "true" ]; do
     sleep 15
   done                                                 # 只轮询 build/release 两个 job 是否跑完，不等待 publish-crate（是否真的需要人工审批见上方说明）
   gh run view "$RUN_ID" --json jobs \
     --jq '.jobs[] | select(.name | startswith("build ") or . == "Publish GitHub Release") | {name, conclusion}'
                                                         # 核对：build x86_64-unknown-linux-gnu 必须 conclusion=success；
                                                         # build aarch64-unknown-linux-musl 是 best-effort，允许 failure；
                                                         # Publish GitHub Release 必须 conclusion=success
   gh release view "v${VERSION}"                       # 确认 release job 已自动创建好 Release 及产物
   ```

   只有当适用的 D 组门也全绿，该卡的 `Acceptance` 才能到 `complete`。**`publish-crate` job 的例外：** 该 job 声明了 `environment: crates-io`，按 workflow 注释的意图需要人工在 `gh`/GitHub 网页上批准才会执行——但这条 GitHub 环境保护规则本身配置在网页端、不写在仓库文件里，无法仅凭仓库内容验证是否真的已配置。**无论是否已配置**，`publish-crate` 都是一次独立于本卡发布节奏的、可被有意推迟或跳过的决策，不是「这次发布是否成功」的判据——因此其运行结果**不计入**本卡 D 组的阻塞判据，`build`+`release` 全绿即满足该卡的 D 组要求；`publish-crate` 仍属于 D 组的一部分，只是不阻塞 `complete`，任务卡如果关心它，应单独登记为一条不阻塞验收的观察项。

5. **ER-05 Review 闭环:** 实现和本地验收完成后进行代码/脚本 review；review 问题修复后重跑相关验收，直到 review 明确给出 PASS。P0/P1 必须关闭，不得以「residual risk 已接受」替代 PASS（仅 P2 可由具名责任人书面接受）。
6. **ER-06 文档与兼容同步:** 涉及公开行为的任务必须同步 `README.md`、`deploy/README.md`、相关 `docs/*.md`、`scorpio.toml.example`。
7. **ER-07 Libra-native 工作流与提交签名:** 本仓库使用 Libra 工作流：`libra status`、`libra add <相关路径>`、`libra commit -s -m "<scope>: <summary>"`、`libra push origin main`。不要把仓库当普通 Git 仓库处理。`libra commit` **没有** `-S` 开关，`-s` 只添加 `Signed-off-by`；签名策略优先级是 `--no-gpg-sign`（最高）> `commit.gpgSign`（`false` 直接关闭签名）> `vault.signing` 默认。因此：
   - 预检必须**先读 `commit.gpgSign`**（`libra config get commit.gpgSign`），未设置时再回退 `libra config get vault.signing`。
   - 每次提交后强制校验：`libra cat-file -p HEAD | rg -q '^gpgsig'`。校验失败不得推送。
   - 本仓库目前没有 `AGENTS.md`/`CLAUDE.md`，不存在措辞漂移问题；一旦新增，必须回来核对是否与本条一致并记一条修订历史。
8. **ER-08 版本与发布（每卡 patch bump + 工具链 lockfile + `gh` 发布）:** 版本权威源是 `Cargo.toml` 的 `[package].version`（形如 `MAJOR.MINOR.PATCH`）。本仓库**只有这一处版本面**，不存在 Libra 式的「五处同步」。

   **每卡 bump（会发版卡强制）:** 计划文件内每一张会独立完成并走 C 组的任务卡（`implementation` / `migration` / `removal` / `release`，`Release boundary = independent`）在本卡行为验收通过、准备提交之前，若确实要面向用户发布新版本，须把 `PATCH` **加 1**；patch 位没有上限。不得为了「好看」把 patch 归零或改写 minor/major，除非本卡 `Version increment` 按下方例外显式声明 `minor` / `major`（须有 ADR + 兼容窗口证据）。

   **`Cargo.lock`（禁止手改）:** 不得手工编辑 `Cargo.lock`。流程是：先改 `Cargo.toml` → 执行 `cargo build`（或等价 `cargo check`）→ 由 Rust 工具链更新 `Cargo.lock` → 把工具链产生的 lockfile diff 一并 `libra add`。

   **提交后发布（会发版卡强制）:** 本卡代码提交并 `libra push origin main` 成功后，`libra tag -m "Release v${VERSION}" "v${VERSION}"` 创建 tag、`libra push origin "v${VERSION}"` 推送到远端——这一次 tag push 本身就是 `.github/workflows/release.yml` 的触发事件（`on: push: tags: v*`），之后 CI 编译 `x86_64-unknown-linux-gnu`（必须成功）与 `aarch64-unknown-linux-musl`（best-effort），`release` job（`needs: build`）用 `softprops/action-gh-release@v3` **自动创建** GitHub Release（tarball + sha256），随后 `publish-crate` job（受保护环境 `crates-io`，据 workflow 注释需人工审批——该保护规则本身配置在 GitHub 网页端，不在仓库文件里，无法从仓库内容单独验证，本计划按 workflow 注释的说明假设其已配置）决定是否同步发布到 crates.io。**推完 tag 之后不要再手动调用 `gh release create`**——Release 已经由 `release` job 自动生成，重复调用要么与自动创建竞态，要么在 Release 已存在后直接失败；本卡的动作到「推送 tag」为止，随后转为等待并核实 D 组远端证据（只轮询 `build`/`release` 两个 job，见「D 远端后置门」的统一命令，**不用** `gh run watch` 等整个 run）。只推 `main`、不创建并推送 `v*` tag，**不算**完成本卡发布义务。

   **`Version increment` 取值:**
   - `patch`（默认）：`PATCH + 1`，无上限。
   - `minor` / `major`：仅用于删除公开 CLI/HTTP 表面或破坏兼容的变更；级别由 ADR + 兼容窗口证据决定。
   - `N/A`：仅 `docs` / `audit` / `spike` / `handoff`（`no-release`）与 `family child`；必须写明产物随哪次发布进入用户渠道。**本计划范围内的纯打包脚本（PKGBUILD 等）新增本身不改变 `scorpiofs`/`libra` 的行为，通常不需要为此单独切一个 crates.io/GitHub Release 版本**——除非打包脚本依赖的某个二进制侧改动（例如为兼容 systemd unit 路径而调整 `ExecStart` 硬编码）确实需要新 release；具体取值由每张任务卡按 GC-01 现状核实后决定，不预设。

   其余步骤与顺序按 ER-04 的「发布收口门」执行（bump 后必须重跑三门），并记录安装/发布与 `gh run` 证据。
9. **ER-09 push 失败策略:** 非 fast-forward 需要 pull/merge 后重新验收再推；认证、权限、网络或服务端失败不 blind retry，记录原因，待下一次修复/发布窗口处理。
10. **ER-10 内部服务错误（有界重试）:** GitHub API/Release、AUR/`makepkg`、libvirt/QEMU（`qlean-ci`）等错误不得直接把任务宣告完成。确定性错误（4xx/权限/schema 不符/编译或配置缺陷）不重试，按范围决定归属——只有修复落在本卡行为轴内且重跑 ER-03 的全部 `G-*` 仍然通过时，才作为本卡修复项就地修；否则新建修复卡 `FIX-*`，并把当前卡置为 `blocked`。暂时性错误（超时、5xx、限流、网络中断）按指数退避重试，默认 ≤ 5 次、总计 ≤ 30 分钟；超预算后置为 `blocked` 并记录 sanitized 证据。发布类动作不自动重试，按 ER-09 处理。
11. **ER-11 证据卫生:** 验收证据不得保存 secret、API key、token、PII、未脱敏 transcript、绝对私有路径或原始 tool payload。需要留存时只写 sanitized summary（尤其注意：本计划涉及在真实笔记本上操作，截图/日志中不得包含个人主机名、私有网络地址、`whoami` 之外的账号信息）。
12. **ER-12 并发边界与串行发布:** 并发只适用于**实现与 review 阶段**：只有 `Implementation write set` 不相交（G-10）的卡可以并发推进。**发布动作一律串行，且由单一发布者执行**：
    - 计划必须在「发布分组与并发窗口」声明发布者。同一时刻只允许一个卡处于「已 bump 未完成推送 / 未触发 release」状态。
    - 进入发布前重新读取 `Cargo.toml` 权威版本，按顺序做完整套发布动作后才轮到下一张卡。
    - **禁止多 Agent 并发发布。** 本仓库没有仓库级发布锁；若某计划确实需要并发发布，必须先用独立 ADR + 独立计划落地一个仓库级发布锁，并在本计划以 `DEFER-*` 登记；在该机制落地前一律串行执行。
13. **ER-13 测试执行分层（执行阶段 focused，收口阶段全量）:** 计划的测试成本按阶段分层，默认**不**在每张任务卡上跑全量套件。

    **执行阶段（本卡开工到本卡推送）默认口径:** 只跑与本任务相关的测试——ER-04 A 组按本卡实际改动表面选出的 focused 命令，加上本卡 `Verification` 登记的指定用例。`cargo fmt --all -- --check` 与 `cargo clippy --all-targets --all-features -- -D warnings` 不属于「测试」，任何会推送的卡都必须跑。

    **必须跑全量的触发条件（命中任一条，本卡 C 组第 ④ 步就跑全量）:**
    - `T-1` **跨切面表面**：`src/cli.rs` 的命令注册或全局 flag、`Cargo.toml` 非版本行、`.github/workflows/**`、`install.sh`。
    - `T-2` **发布点与聚合卡**：`Task type = release`，或 `Release boundary` 为 `family release point`。
    - `T-3` **删除或重命名公开表面**：公开命令 / flag / HTTP 端点 / `--test` target 的删除或重命名。
    - `T-4` **共享测试基建**：改动被多个 `--test` target 复用的 fixture / helper。
    - `T-5` **显式要求**：计划正文、任务卡或用户明确要求本卡跑全量。
    - `T-6` **信号不足**：focused 门出现无法归因的失败，或本卡改动影响面无法用 A 组命令圈定时，先跑全量再判定。
    - `T-CI` **`qlean-ci` 门单独判定**：`qlean_installer` 需要 KVM/libvirt，本地沙箱或未启用嵌套虚拟化的环境很可能无法运行；命中该门但环境不支持时，任务卡必须记录「环境不支持，改由 CI `qlean.yml` 或真实 Omarchy 笔记本手工跑一遍」并给出手工证据要求，不得静默跳过且不留痕迹。

    **登记要求:** 每张会推送的卡在 `Full-suite trigger` 字段写 `none` 或 `T-<n>: <一句话依据>`；字段缺失按 `none` 处理。

    **收口阶段（全部任务卡完成后，强制）:** 计划必须跑一次「完成判据」的全量收口门（fmt + clippy + `cargo test --verbose --all-features` + `cargo test --doc`），并修复该次全量暴露的全部 Bug。

    **风险归属:** 本仓库 `ci.yml`/`test.yml`/`fmt.yml`/`clippy.yml` 由 `push`/`pull_request` 触发，直推 `main` 也会跑（与 Libra 仓库「直推不跑 CI」不同，风险敞口更小，但 `qlean.yml` 的 libvirt 隔离安装验证仍只在 CI runner 或真实 Linux 主机上有意义，本地沙箱通常跑不了）。

## 实施顺序

依赖边格式：`A -> B` 表示 A 必须先于 B。依赖图必须无环，且每条边都指向具体任务卡而不是整个 Phase（G-06）；任务卡拆分后本节必须同步更新。

- `<TASK-01> -> <TASK-02>`
- `<TASK-02> -> <TASK-03>`

### 依赖登记表

本计划外的一切依赖关系（其它日期计划、外部仓库/服务、人工审批、上游 release），以及本计划向外移交的范围，都必须在此登记后才能被任务卡引用（G-06）。计划内任务之间的依赖直接写任务 ID，不进本表。

`direction` 区分方向：`incoming` = 本计划等待外部产物；`outgoing` = 本计划把范围移交给别的计划（此时 Owner 是接收方，「超时与失败策略」写接收方未接手时的回落处理）。

| ID | direction | 类型 | 对象 | Owner | 产物与可用性判据 | 证据 | 超时与失败策略 |
|---|---|---|---|---|---|---|---|
| DEP-01 | `<incoming / outgoing>` | `<跨仓库 / 外部服务 / 审批 / 上游 release>` | `<plan-YYYYMMDD#TASK-ID 或外部对象，如 libra-tools/libra@<sha>>` | `<负责人/系统/接收方计划>` | `<交付什么、如何判定可用>` | `<file:line / commit / URL + 核对日期>` | `<等待上限、超时后降级或回落路径>` |

### 发布分组与并发窗口

默认每张卡独立发布（G-07）。只有需要合并发布或需要显式并发/串行窗口时才登记本表；登记项必须在任务卡 `Release boundary` 中被引用。

| ID | 成员 | 唯一发布点 | 窗口规则 | 失败回滚顺序 | 理由 |
|---|---|---|---|---|---|
| REL-01 | `<TASK-ID 列表>` | `<TASK-ID>` | `<窗口规则>` | `<按依赖逆序前滚修复或 abort 未完成的 release run>` | `<为何需要协调窗口>` |

**并发声明:** `<实现阶段可并发的卡组（实现写集互不相交）/ 全串行>`（G-10）

**发布者:** `<负责 C 组 bump/构建/安装/提交/branch 推送/tag 推送，并跟踪 D 组远端证据（`release.yml` 自动创建的 GitHub Release）的唯一 Agent/人>`（ER-12：发布一律串行，禁止多 Agent 并发发布）

**发布窗口顺序:** `<按依赖与 REL-* 分组列出发布顺序；同一时刻只允许一张卡处于「已 bump 未完成推送 / 未触发 release」状态>`

并发执行时另需满足：实现写集不相交（G-10）。

### Phase 0: <基线冻结和消歧>

**目标:** `<本阶段目标>`

**进入条件:**

- `<前置条件>`

**退出条件:**

- `<阶段完成判据>`

### Phase 1: <实现第一个可发布切片>

**目标:** `<本阶段目标>`

**进入条件:**

- `<前置条件>`

**退出条件:**

- `<阶段完成判据>`

## 任务卡

任务 ID 使用稳定前缀，例如 `A0-01`、`DR-01`、`P0-03`。编号被引用后不重排；拆分出的新卡在所属 Phase 末尾追加新编号，因此**编号顺序 ≠ 执行顺序**，执行以「实施顺序」的依赖边和各卡 `Dependencies` 为准。废弃的编号保留并标记替代关系。

### 任务卡粒度规则（强制）

粒度是任务卡质量的第一判据：卡过大则无法 review、无法回滚、无法交给单个 Agent 完成；卡过碎则实现、测试与文档脱节，且每片都要付一次发布成本。新增或修改任务卡时必须逐条满足下列 `G-*` 规则。任一条不满足即为「粒度不合格」，必须在开工前拆分或合并，并同步实施顺序、依赖登记表、发布分组、追溯表、测试矩阵、里程碑、风险表的任务归属和修订历史。

- **G-01 单一行为轴与恢复模式（上限）:** 一张卡只承担一个**可独立恢复**的行为轴——该卡失败或需要撤回时，存在**单一已声明的恢复动作**，执行后系统停在一个自洽状态，不留半吊子中间态。恢复形式必须在 `Rollback mode` 字段声明为四种模式之一：
  - `revert`：纯本地代码/文档/脚本变更，一次 revert 即完整撤销（默认）。
  - `forward-only`：已产生不可逆的系统改动（真实机器上已安装的用户/组、已修改的 `/etc` 配置、已创建的 systemd unit 并启动过一次），只能前滚修复；必须写出恢复验证命令和用户影响。
  - `compensating`：对外部服务已有副作用（GitHub Release、crates.io 发布、AUR 提交），撤销靠补偿动作；必须写出补偿命令与幂等键。
  - `immutable-release`：已发布 artifact 不可撤回，只能发新版本；必须写出降级指引与兼容窗口。
- **G-02 完整可交付（下限）:** 一张卡必须是一个自洽的可验收增量。同一行为轴的实现、测试、文档同步是**同一张卡**的验收内容，禁止拆成「实现卡 / 补测试卡 / 补文档卡」。
- **G-03 条目上限与计数口径:** 上限按 `Task type` 取值，计数按**独立判据**而非行数。ER-04 的强制门**不计入**本条计数。

  | Task type | AC 上限 | Verification 上限 |
  |---|---|---|
  | `implementation` / `migration` / `removal` | 8 | 8 |
  | `spike` | 8 | 8 |
  | `docs` / `audit` / `handoff` | 20 | 20 |
  | `release` | 12 | 12（只计聚合守卫、release note、兼容证据等本卡特有项） |

  计数细则：
  - AC 按「独立 pass/fail 谓词」计。一条 checklist 内用「且 / 并且 / 以及 / 同时」连接的多个可分别失败的断言按多条计；嵌套子列表逐项计；表格行逐行计。
  - Verification 按「独立验证门」计：环境准备前缀（`cd`、`export`）与其后的命令合计为一门；一条命令中的多个 `--test` target 分别计；`&&` 串联两个都会独立判定的验收命令按两门计；手工证据按项计。
  - 超限视为多轴信号，必须拆卡，不得通过合并长句、塞进表格或改写成「等等」来规避。
- **G-04 规模上限（可计数）:** `Estimated scope` 的开工态只允许 `S` 或 `M`。`L`/`XL` 只能作为「必须再拆」的中间标注，计划成稿后不得存在 L/XL 卡。计数只统计行为实现落点与生产文件：
  - **计入**：承载本卡行为变更的生产代码/脚本落点与文件（`src/**`、`install.sh`、`deploy/**`、新增打包目录如 `packaging/archlinux/**` 等）。
  - **不计入（随附同步集）**：本卡自己的测试文件、按 GC-05/ER-06 强制同步的文档、以及 ER-08 的版本面（本仓库只有 `Cargo.toml` 一处）。
  - `S`：≤ 2 个行为落点、≤ 3 个生产文件，无 CLI/HTTP/systemd unit 公开接口变更。
  - `M`：≤ 4 个行为落点、≤ 12 个生产文件，最多一处公开行为或接口变化，仍是单一行为轴。
  - 超出 `M` 的计数即为 L：默认必须拆分。确实不可拆的机械变更可在「字段全局默认与例外」的 waiver 白名单登记 `EX-*`。
- **G-05 Agent 可独立执行:** 一张卡必须能在不阅读其它卡正文的前提下被执行：`Current evidence` 给出可核对的 `file:line` 锚点，`Acceptance criteria` 自洽可判定，`Verification` 是可直接复制执行的确切命令，`Dependencies` 只引用「依赖登记表」中的 `DEP-*` / 任务 ID。
- **G-06 依赖闭合且无环:** 依赖必须有向无环。本计划内依赖直接引用任务 ID；跨仓库与外部前置必须先在「依赖登记表」登记为 `DEP-*` 再引用。
- **G-07 发布切片对齐（按任务类型）:** 默认「一张卡 = 一个发布切片」。`implementation` / `migration` / `removal` 若确实需要发新版本，必须走完整发布切片（含 ER-08）；`docs` / `audit` / `spike` / `handoff` 卡不 bump 版本、不产出 artifact，`Release boundary` 写 `no-release` 并说明其产物随哪次发布进入用户可见渠道；`release` 卡本身就是发布点。任何「多卡合并发布」都是例外，必须在「发布分组与并发窗口」登记 `REL-*`。
- **G-08 家族卡:** 当一次公开表面删除、或多个文件必须同时上线这类变更确实无法切成可独立发布的切片时，用「家族卡」表达：拆成多张各自 review、各自通过全部适用 ER-04 门、各自本地提交的子卡，共用一个唯一发布点卡（`Task type = release`）。
- **G-09 拆分协议:** 拆分已被引用的卡时，原编号保留给主轴，新子卡在所属 Phase 末尾追加新编号，不重排既有编号。
- **G-10 写集与并发:** 写集分三类，每张卡必须声明前两类：
  - **`Implementation write set`（I）**：承载本卡行为的代码、脚本、测试、文档文件。
  - **`Release write set`（R）**：`Cargo.toml` 版本行 + 由工具链刷新的 `Cargo.lock` + release artifact。`family child` 与 `no-release` 卡写 `N/A`。
  - **协调写集（C）**：计划级的发布顺序与窗口记录，由 ER-12 的单一发布者串行维护。

  冲突规则：
  - **I–I 相交** → 禁止并发，无豁免通道：补一条顺序依赖边，或把相交部分合并到唯一集成卡。
  - **I–R 相交** → 在已声明的串行发布窗口内（ER-12），该窗口对 R 内文件是写锁。
  - **R–R 相交** → 由 ER-12 的串行发布窗口顺序化。
- **G-11 任务类型:** 每张卡必须声明 `Task type`：
  - `implementation`：默认类型，全部 `G-*` 条款全量适用。
  - `migration`：数据/配置迁移（例如 `scorpio.toml` 字段迁移），`Rollback mode` 通常为 `forward-only`。
  - `removal`：公开表面删除，通常进入家族卡（G-08），必须先有 deprecation 窗口证据。
  - `spike`：探索/验证，不得改动生产代码/脚本。必须写出待回答的问题、时间箱、产物、go/no-go 退出标准与后续承接卡。
  - `audit` / `docs`：只读核对或文档收敛。
  - `release`：发布点卡，不引入新行为，只做版本、构建、安装、聚合守卫与发布证据。
  - `handoff`：跨计划/跨仓库移交，默认 `no-release`。**移入**（本计划承接他人，例如等待 libra 侧确认打包细节）在「依赖登记表」登记 `direction: incoming`；**移出**（本计划把范围交给别的计划/仓库，例如把 Omarchy fork 的接入交给未来的「Omarchy overlay 计划」）登记 `direction: outgoing`。

#### 推荐拆分维度

| 维度 | 切法 | 典型结果 |
|---|---|---|
| 打包表面轴 | PKGBUILD/`.install`/`.sysusers` → 依赖下载与校验 → 安装后验证 | 2–3 张卡 |
| 权限/身份轴 | 系统用户与组 → FUSE 设备/`allow_other` 边界 → systemd capability | 每轴一卡 |
| 仓库轴 | ScorpioFS 打包 → libra 打包 → Omarchy 侧集成产物 | 按仓库分卡 |
| 生命周期轴 | 首次安装 → 升级路径 → 卸载/回滚 | 按阶段分卡 |
| 清理轴 | 公开表面删除（家族卡） → 内部模块退场 → 依赖摘除 | 家族卡 + 普通卡 |

#### 粒度反模式速查

| 反模式 | 症状 | 处理 |
|---|---|---|
| 巨型卡 | `Estimated scope` = L；AC > 8；Description 含多个并列目标 | 按「推荐拆分维度」拆分（G-01/G-03/G-04） |
| 碎片卡 | 「补测试」「补文档」「改个字段名」单独成卡 | 合并回所属行为轴（G-02） |
| 多轴伪装 | 把多条 AC 合成一条长句、塞进表格或写「等等」以压到 8 条以内 | 按独立谓词还原计数后重新判定（G-03） |
| 落点注水 | 把 `src/` 或仓库根算作「一个落点」以保住 S/M | 按目录级落点重新计数（G-04） |
| 隐式依赖 | Description 写「按 X 卡的约定」而 X 卡未交付该约定 | 写进本卡，或提升为全局约束 / ADR（G-05） |
| 幽灵验收 | `Verification` 只写 `cargo test --verbose --all-features`，不区分本卡命中的具体表面 | 指定 target 与 test fn（G-05） |
| 悬空依赖 | `Dependencies` 写「Phase N 完成」或自由描述外部前置 | 收敛到具体前置卡 ID / `DEP-*`（G-06） |
| 假回滚 | 已在真实笔记本上安装/启用过的卡仍写「一次 revert 撤销」 | 按实际选 `forward-only` / `compensating`（G-01） |
| 并发冲撞 | 两张无依赖的卡实现写集相交 | 只有两条出路：补顺序边，或合并到唯一集成卡（G-10 不可豁免） |
| 顺手合并 | 多张卡凭开工笔记合成一次发布 | 登记为 `REL-*` 家族卡，或拆回独立发布切片（G-07/G-08） |

#### 字段全局默认与例外

计划在本节声明字段的全局默认值后，任务卡中**取默认值的字段可以整行省略**，或写 `Inherited`；只有偏离默认的字段才在卡内展开并在下表登记。**`Task type` 字段是本规则的唯一例外**——G-11 要求每张卡都必须单独写出 `**Task type:** \`<...>\`` 这一行，即使取值正好等于下面声明的默认值也不能省略整行或写 `Inherited`；这里声明的「默认」只是「多数卡预期会填的常见值」，不构成可省略的依据。

- **Release boundary 默认:** `<每张卡独立发布切片 / 其它>`
- **Task type 默认:** `<implementation / 其它>`（仅供参考「多数卡的常见取值」，不适用本节「可整行省略」的规则——见上方例外说明，每张卡必须照 G-11 单独写出该行）
- **Rollback mode 默认:** `<revert / 其它>`
- **Migration and rollback 默认:** `<N/A：无数据迁移 / 其它>`
- **Security and privacy 默认:** `<继承 GC-07、GC-11 / 其它>`
- **Performance budget 默认:** `<继承 GC-10 / 其它>`
- **Docs and compatibility impact 默认:** `<按 GC-05 同步 README/deploy/README/docs / 其它>`

**默认覆盖**（不是例外，只是取了非默认值，无需审批）：

| 任务 | 偏离的字段 | 取值与理由 |
|---|---|---|
| `<ID>` | `<Rollback mode>` | `<forward-only：真实机器上已创建系统用户，前滚修复>` |

**规则 waiver（`EX-*`，需具名审批）**：可豁免的规则是**白名单**，只有下表内容；`G-01`、`G-02`、`G-05`、`G-06`、`G-07`、`G-08`、`G-09`、`G-10`、`G-11` **永不可豁免**。

| 可豁免项 | 允许的理由范围 |
|---|---|
| G-03 条目上限 | 清单型产物（文档 / 审计）确实需要超过本类上限，且已写明产物文件清单 |
| G-03 条目上限（**门族型验收**） | `implementation` 卡的验收本质是**同一恢复轴上的机械门族/fixture 清单**（打包字段、绑定、投影、锁等），逐门计数必然超过本类上限，而按门拆卡会违反 G-01/G-02（同一行为轴的实现/测试/文档不得拆散）与「碎片卡」反模式。准入条件（缺一不可）：① 每个门都是**可复制执行的具名命令**，失败即整卡不达标；② 门族清单在卡内「判据规范（非计数正文）」块逐条枚举；③ 分子如实写作 `n/上限@EX-ID`，不得以合并长句掩盖；④ 门族增减时同批更新 waiver 行与粒度审计表 |
| G-04 规模上限（`L-exception`） | 不可拆的机械变更：全仓重命名、批量删除、格式化 |
| ER-07 签名要求 | 仓库策略层面的具名豁免（sign-off-only） |

| 例外 ID | 任务（或 `ALL/<作用域>`） | 豁免项 | 理由与补偿措施 | Approver | Review round | 证据 | 有效期 |
|---|---|---|---|---|---|---|---|
| EX-01 | `<ID>` | `<豁免项>` | `<理由与补偿>` | `<具名审批人>` | `<R-n>` | `<file:line / review 结论>` | `<有效期>` |

#### 任务卡粒度审计表

| 任务 | type | axis | recovery | complete | self-contained | AC | VER | landing / prod-files | scope | deps | writeset | release | split-from | exception |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `<ID>` | `<Task type>` | `<行为轴>` | `<恢复动作>` | `<yes>` | `<yes>` | `<n/上限[@EX-ID]>` | `<n/上限[@EX-ID]>` | `<n>/<n>` | `<S/M/L-exception:EX-n>` | `<TASK-ID/DEP-ID/none>` | `<no-overlap/序列化于 ID>` | `<independent/family child/family release point/no-release>` | `<ID/N/A>` | `<EX-ID/N/A>` |

#### Verification 判定口径

- **零命中守卫必须区分「无命中」与「命令失败」。** `rg` 的退出码是 `0` = 有命中、`1` = 零命中、`>1` = 执行失败。使用下列模板：

  ```bash
  if rg -n "<pattern>" <paths>; then
    echo "FAIL: forbidden pattern found"; exit 1
  else
    rc=$?
    if [ "$rc" -ne 1 ]; then echo "ERROR: rg failed with exit $rc"; exit "$rc"; fi
    echo "OK: zero hits"
  fi
  ```

- 「只允许 allowlist 命中」类守卫必须逐条比对固定 allowlist，并在任务记录中附命中 diff。
- 涉及真实系统改动（用户/组创建、`/etc` 配置、systemd 服务）的验证命令必须写明在什么环境执行（本地容器 / CI VM / 真实 Omarchy 笔记本），不可本地复现的部分归 D 组或标注为「需要真实笔记本手工验证」。
- `cargo test --verbose --all-features` 不能替代任务指定用例（ER-04）；反过来，指定用例也不能替代计划完成前的全量收口门。

### Task <ID>: <任务标题>

**Task type:** `<implementation | migration | removal | spike | audit | docs | release | handoff>`（G-11）

**Lifecycle / Acceptance:** `<pending | in-progress | blocked | done>` / `<空 | locally-accepted | remote-pending | complete>`（ER-04）

**Description:** `<要做什么、为什么、现实影响。一句话点明本卡唯一的行为轴。>`

**Out of scope:** `<逐项列出本卡明确不做的内容，每项标注状态：「由 <ID> 承接」/「尚未排期，重启条件 …」/「永久非目标，理由 …」。>`

**Current evidence:**

| 事实 | 证据 |
|---|---|
| `<当前实现或缺口>` | `<file:line / test / external repo@sha>` |

**Acceptance criteria:**

- [ ] `<用户可见或系统行为判据>`
- [ ] `<机器输出/退出码/HTTP 契约判据>`
- [ ] `<失败路径/边界条件判据>`
- [ ] `<文档/兼容同步判据>`

**Verification:**

- [ ] `<exact command>`
- [ ] `<exact command>`
- [ ] `<manual/sanitized evidence, if required>`

**Full-suite trigger:** `<none | T-<n>: <命中依据> | N/A（no-release / family child）>`（ER-13）

**Dependencies:** `<无 / 本计划 Task ID / 「依赖登记表」中的 DEP-ID>`（G-06）

**Deliverables:** `<docs / audit / spike / handoff 卡必填：产物文件清单。代码/脚本卡写 N/A 或 Inherited。>`

**Implementation write set:** `<承载本卡行为的代码/脚本/测试/文档文件或目录>`（G-10）

**Release write set:** `<Inherited（= Cargo.toml 版本行 + Cargo.lock + release artifact）/ N/A（family child / no-release 卡）>`

**Files likely touched:** `<src/...>, <tests/...>, <docs/...>`（估计值；并发判定以 `Implementation write set` 为准）

**Docs and compatibility impact:** `<Inherited / 具体文件>`

**Rollback mode:** `<revert | forward-only | compensating | immutable-release>`（G-01）

**Migration and rollback:** `<N/A 或前滚步骤、恢复验证命令、用户影响>`

**Security and privacy:** `<N/A 或权限、secret、path、systemd 特权约束>`

**Performance budget:** `<N/A 或数据规模、安装耗时断言>`

**Estimated scope:** `<S / M / L-exception:EX-<n>>`（G-04；`XL` 永不允许作为开工态）

**Version increment:** `<patch | minor | major | N/A>`（ER-08）

**Release boundary:** `<independent | family child of REL-<n> | family release point of REL-<n> | no-release>`

**C/D coverage from:** `<self | <TASK-ID>>`（ER-04）

**Granularity:** `type=<Task type>; axis=<本卡唯一的行为轴>; recovery=<失败/撤回时的单一恢复动作>; complete=<yes>; self-contained=<yes>; AC=<n>/<上限>[@EX-ID]; VER=<n>/<上限>[@EX-ID]; landing=<n>; prod-files=<n>; scope=<S|M|L-exception:EX-n>; deps=<none|TASK-ID,…|DEP-ID,…>; writeset=<no-overlap|序列化于 TASK-ID>; release=<independent|family child|family release point|no-release>; split-from=<TASK-ID|N/A>; exception=<EX-ID[,EX-ID…]|N/A>`

## 测试矩阵

本表登记「本计划最终必须被覆盖到什么」，不是「每张卡都要跑完整张表」。执行阶段按 ER-13 只跑与本卡相关的行；整张表由收口阶段的全量门统一覆盖。

| 类别 | 必须覆盖 | Target / command |
|---|---|---|
| 单元 | `<纯逻辑>` | `<cargo test --lib ...>` |
| 集成 | `<真实 CLI/HTTP 工作流>` | `<cargo test --test ...>` |
| 打包 | `<PKGBUILD 构建与安装>` | `<makepkg -si / namcap>` |
| 安装脚本 | `<install.sh 安全校验与 systemd 生成>` | `<script/test_installer*.sh>` |
| 隔离安装 | `<qlean VM 冒烟>` | `<cargo test --features qlean-ci --test qlean_installer -- --ignored>` |
| 真实环境 | `<Omarchy 笔记本手工验证>` | `<手工命令 + sanitized 截图/日志>` |

## 追溯表

| 任务 | 来源/证据 | 落点 | 文档动作 | 指定测试 |
|---|---|---|---|---|
| `<ID>` | `<file:line / issue / repo@sha>` | `<src/tests/docs/packaging>` | `<README, deploy/README, docs/...>` | `<target::test_fn>` |

## 里程碑验收与回滚

| 里程碑 | 完成条件 | 发布/证据 | 回滚或前滚 |
|---|---|---|---|
| M0 | `<基线冻结>` | `<commit/test/doc>` | `<N/A>` |
| M1 | `<首个可发布切片>` | `<version/test/review>` | `<rollback/forward fix>` |

### 故障恢复矩阵

| 故障点 | 可接受残留 | 恢复动作 | 禁止结果 |
|---|---|---|---|
| `<安装脚本中断>` | `<部分创建的目录/用户>` | `<重跑安装脚本的幂等路径 / 手工清理指引>` | `<残留特权服务、静默成功>` |

## 风险登记

| 风险 | 影响 | 缓解 | 任务 |
|---|---|---|---|
| `<风险>` | `<高/中/低 + 影响>` | `<测试/设计/门禁>` | `<ID>` |

## 性能与容量摘要

| 操作 | 单次成本 | 累积成本 | 预算/上限 | 验证 |
|---|---|---|---|---|
| `<操作>` | `<O(...) 或耗时>` | `<O(...)>` | `<阈值>` | `<测试/手工计时>` |

## 兼容与文档收口

- [ ] `README.md` 已同步，或说明 `N/A`。
- [ ] `deploy/README.md` 已同步，或说明 `N/A`。
- [ ] 相关 `docs/*.md` 已同步，或说明 `N/A`。
- [ ] `scorpio.toml.example` 已同步，或说明 `N/A`。
- [ ] 新增打包脚本已有对应说明文档（例如 `packaging/*/README.md`），或说明 `N/A`。
- [ ] `plan-long.md` 相关状态已同步，或说明 `N/A`（本仓库尚未创建该文件时天然 `N/A`）。

## Codex review log

Result 只允许 `PASS` 或 `FAIL`。`FAIL` 必须列出 P0/P1 条目并在下一轮复审关闭；P2 可由具名责任人书面接受为 residual risk，但不改变本轮 `FAIL` 记录（ER-05）。

| Round | Scope | Result | P0/P1 | P2 处置 | Evidence |
|---|---|---|---|---|---|
| R1 | `<files/tasks>` | `<PASS / FAIL>` | `<条目与关闭状态>` | `<修复 / 具名接受人>` | `<test commands / 复审轮次>` |

## 非目标与延后项

| ID | 延后内容 | 原因 | 重启条件 | 承接位置 |
|---|---|---|---|---|
| DEFER-01 | `<内容>` | `<原因>` | `<何时重启>` | `<plan/ADR>` |

## 完成判据

计划只有在以下条件全部满足后才能标记完成：

- [ ] 所有任务卡满足粒度规则 `G-*`：无未登记的 L 例外、无 XL 卡、无碎片卡、无未登记的合并发布例外、实现写集冲突均已消解；「任务卡粒度审计表」已填齐。
- [ ] 所有非延后任务的 acceptance criteria 已满足，且 `Lifecycle=done` **且** `Acceptance=complete`（ER-04）。任何停在 `remote-pending` 的卡都必须先取得其 D 组远端后置门的绿色证据。任何仍为 `blocked` 的任务都必须先解除阻塞或按 `DEFER-*` 正式延后。
- [ ] 所有任务的 Verification 命令已运行并记录结果。
- [ ] **计划完成门 / 全量收口门（ER-13）**：`cargo fmt --all -- --check`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --verbose --all-features`、`cargo test --doc` 全绿；若本计划改动 `install.sh`/打包/systemd 表面，`script/test_installer.sh`、（有权限时）`script/test_installer_systemd.sh` 与至少一次 `qlean-ci` 或真实 Omarchy 笔记本安装冒烟已通过并记录证据。
- [ ] **全量收口门暴露的 Bug 已全部处理（ER-13）**：定位到本计划的失败已前滚修复，并已重跑全量至全绿；判定为既有失败的用例已附「计划基线 commit 上同样失败」的复现证据并登记 `FIX-*` 或 `DEFER-*`。
- [ ] 必要的文档更新已完成（见「兼容与文档收口」）。
- [ ] 必要的迁移、回滚、故障恢复验证已完成；每张卡的 `Rollback mode` 都已被实际验证或记录为不可验证的原因。
- [ ] Review 最终结论为 `PASS`，P0/P1 全部关闭；仅 P2 residual risk 允许保留，且有具名接受人。
- [ ] 如有发布要求：本卡已 `patch + 1`（或声明的 minor/major）、`Cargo.lock` 仅由工具链刷新、构建/安装/提交/推送并推送 `v<version>` tag 完成（该 tag push 即触发 `release.yml`，其 `release` job 自动创建 GitHub Release，不需要也不应再手动调用 `gh release create`），并取得 GitHub Release 证据（ER-08）。
- [ ] 「修订历史」已记录成稿后的全部规范性变更（G-09）。
- [ ] `plan-long.md` 相关状态已同步，或明确 `N/A`。
