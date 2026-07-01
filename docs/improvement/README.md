# ScorpioFS 改进计划

## 目录用途

本目录汇总 ScorpioFS v0.2.2 在**部署、配置、易用性**三个维度的改进方向,基于源码静态审查得出。每项改进标注优先级(`P0`–`P3`),不含源码行号引用(避免随代码漂移),不预估工时。

本轮复核结论:原计划方向总体合理,但 P0 范围偏大,且缺少跨文档依赖、安全边界、兼容策略和验收口径。计划应先稳定配置与接口契约,再做部署产物,最后做体验增强,避免在配置模型、CLI、API、容器和 systemd 多条线同时改动时引入破坏性变更。

## 子文档

| 文档 | 聚焦领域 |
|---|---|
| [deployment.md](./deployment.md) | 打包、容器化、systemd、CI release、系统依赖 |
| [configuration.md](./configuration.md) | 配置类型化、文档对齐、env 覆盖、校验、双配置文件 |
| [usability.md](./usability.md) | CLI 统一、API 合并、日志、健康检查、doctest、仓库卫生 |

## 优先级总表

| 优先级 | 改进项 | 类别 |
|---|---|---|
| P0 | 修文档与实现不一致([antares] 表 / base_url vs mega_url / api.md JSON) | 配置 |
| P0 | 强类型 config 兼容层 + env 覆盖 + 启动期校验 | 配置 |
| P0 | Dockerfile + docker-compose 快速体验路径 | 部署 |
| P0 | systemd unit 模板 + 最小安装脚本 | 部署 |
| P0 | 统一 CLI 子命令框架,保留旧入口兼容 | 易用性 |
| P0 | 统一日志(tracing + EnvFilter) | 易用性 |
| P1 | CI release workflow(二进制产物优先,crates.io 发布需人工门禁) | 部署 |
| P1 | 移除 set_defaults 静默改写用户配置 | 配置 |
| P1 | 修 update_config 空操作 + 硬编码 config.toml | 配置 |
| P1 | 废弃/合并老 API + 文档统一 | 易用性 |
| P1 | shell completion + 主端口 /health | 易用性 |
| P1 | 修 lib.rs doctest(crate 名) | 易用性 |
| P2 | capabilities/setuid 指引文档 | 部署 |
| P2 | 改造 init.bash | 部署 |
| P2 | scorpio config validate / init 子命令 | 配置 |
| P2 | 校验增强(URL/路径可写/数值范围) | 配置 |
| P1 | 统一状态文件写入路径 + 消除 `to_toml` 静默失败 | 配置 |
| P2 | 替换生产路径 unwrap | 易用性 |
| P2 | scorpio doctor 子命令 | 易用性 |
| P2 | config.toml 出库 + example 模板 | 易用性 |
| P3 | HTTP 鉴权评估(localhost 默认 / token) | 部署/易用性 |
| P3 | perf 脚本集成 / banner 清理 | 易用性 |

## 实施状态(已全部落地)

本计划的 P0–P3 改进项均已实现并通过逐项 code review。功能与代码映射如下:

| 改进项 | 实现 |
|---|---|
| 强类型 config 兼容层 + env 覆盖 + 启动期校验 | `src/util/config.rs`(`ScorpioConfig` 强类型、`RawResolver` 优先级 `CLI>env>file>default`、`SCORPIO_*`、URL/数值/枚举校验、移除 `set_defaults` 回写) |
| 统一状态文件路径 + 消除 `to_toml` 静默失败 | `src/manager/mod.rs` 原子写、全部用 `config::config_file()`、错误传播 + 回滚 |
| 统一日志(tracing + EnvFilter) | `src/util/logging.rs`、`main.rs`/`antares.rs`/`server`/`fetch` 全量改 `tracing`,`log_level` 配置项 |
| 统一 CLI 子命令框架 | `src/cli.rs` + `src/main.rs`(`serve/mount/umount/list/http-mount/config/doctor/completions`,旧 flag 兼容,`antares` 别名,稳定退出码,主二进制改名 `scorpio`) |
| 废弃/合并老 API + `/health` + status 统一 | `src/daemon/mod.rs`(根级 `GET /health`、`Deprecation` 头 + 日志、`POST /api/config` status 统一) |
| shell completion + lib.rs doctest | `scorpio completions`(clap_complete)、`src/lib.rs` 示例改 crate 名 + `no_run` |
| 生产路径 unwrap/panic 清理 + banner | unmount/mount/daemon/`from_toml`/signal 全部优雅化;banner 移除 |
| `scorpio config init/validate/show` | `src/cli.rs` + `config::validate_file`(收集全部问题) |
| `scorpio doctor` + config.toml 出库 + init.bash 改造 | `src/doctor.rs`、`.gitignore`/`.libraignore`、`scorpio.toml.example`、`script/mktestdirs.sh` |
| 部署:Dockerfile + compose + systemd + install.sh | `Dockerfile`、`docker-compose.yml`、`deploy/systemd/*.service`、`install.sh`、`deploy/README.md` |
| CI release + 供应链 | `.github/workflows/release.yml`(tar + SHA256 + 受保护环境 crates.io 门禁)、`LICENSE-MIT`/`LICENSE-APACHE`、`.github/dependabot.yml` |
| perf 脚本集成 | `script/run.rs` → `examples/fs_read_perf.rs`(cargo example)、`script/README.md` |
| HTTP 鉴权(P3 评估) | 已在 `deploy/README.md` 文档化风险,systemd/compose 默认 loopback;token/mTLS 留作后续 |

> 历史问题描述(下文各 Phase / 子文档)保留为设计背景;实际状态以本表为准。

## 实施路线建议

- **Phase 0(立即修正)**:只改文档与示例,修复 `docs/antares.md`、`docs/api.md`、`README.md` 与当前实现不一致的问题。该阶段不改变运行时行为,风险最低。**进度:** `docs/antares.md`、`docs/api.md`、仓库根 `README.md` 和 `docs/perf_test.md` 的主要文档漂移已完成同步(扁平键对齐、URL 前缀章节、`/mounts/{id}/ready`、删除幽灵 git 结构、补 `select` 端点、`mega_url`/`mount_path` 映射表、README 指向 `scorpio.toml`/`workspace`/`--http-addr`、清理开发者本机路径)。剩余 Phase 0 风险主要是后续代码变更再次引入文档漂移。
- **Phase 1(P0 基础契约)**:配置加载链路、env 覆盖、启动期校验、统一日志、CLI 子命令框架。该阶段先保留旧扁平配置、旧 CLI/API 入口和 `antares` 二进制,避免一次性破坏用户脚本。
- **Phase 2(P0/P1 部署闭环)**:在配置 env 覆盖可用后再交付 Dockerfile、docker-compose、systemd unit、最小 install.sh;随后做 CI 二进制 release。crates.io 自动发布放在人工审批门禁后。
- **Phase 3(P1 兼容收敛)**:明确 `/api/fs/*` 与 `POST /api/config` 的兼容/废弃策略,补 `/health`、completion、doctest,并修复 `config_file` 写错路径。
- **Phase 4(P2-P3 持续治理)**:unwrap 清理、doctor、config 出库、perf 集成、banner 清理、安全加固和发行版打包。

## 多维度综合分析

以下结论基于 `src/`、`docs/`、`.github/`、根配置文件的二次源码核对,并作为各子文档约束的单一事实来源。

| 维度 | 评分 | 结论摘要 | 主要缺口 / 风险 |
|---|---|---|---|
| 合理性 | 高 | 配置契约漂移、部署产物缺失、CLI/API 割裂是 v0.2.2 的真实阻断项;改进主线与代码现状吻合。 | P0 若并行推进配置 schema、CLI 重构与 Docker 交付,会在契约未稳定时固化错误假设。 |
| 可行性 | 中高 | 文档对齐、env 覆盖、tracing、Dockerfile、systemd 模板均可分阶段落地;强类型迁移与跨架构 release 成本较高。 | `HashMap<String,String>` 全局 `OnceLock` 与库侧早读路径使配置重构必须保留兼容访问器。 |
| 完整性 | 中 | 三大子文档已覆盖主要问题,但仍缺 HTTP 鉴权、semver 细则、状态写路径全景、库消费者契约。 | 无鉴权 API 暴露在 `0.0.0.0` 时等同开放挂载控制面;git 路由决策未 closure。 |
| 安全性 | 中低 | FUSE 特权、`CAP_SYS_ADMIN`、`curl \| sh`、配置热更新、无鉴权 HTTP 均为实质风险。 | 默认绑定 `0.0.0.0:2725`;`POST /api/config` 即使空操作也返回伪造成功;敏感路径可能经 health/日志泄露。 |
| 功能正确性与接口兼容性 | 中 | 核心 FUSE/挂载链路可用;原"文档契约与实现多处不一致"已在 Phase 0 修复(`docs/antares.md`/`docs/api.md` 已对齐),剩余为**代码侧**契约名漂移。 | 文档侧已修复:`[antares]` 表/扁平键、端点前缀 `/mounts`÷`/antares/mounts`、幽灵 git 接口、缺失 select 端点。代码侧遗留:`ConfigRequest` 仍用 `mega_url`/`mount_path`,与配置键 `base_url`/`workspace` 不一致。 |
| 数据流与控制流 | 中低 | 主配置加载链尚可追踪,但状态持久化路径不统一、错误被静默丢弃、配置优先级未实现。 | 硬编码写路径 2 处;`fetch.rs` 3 处静默丢弃 `to_toml` 结果;配置合并顺序在子文档间曾不一致(已统一为 CLI > env > file > defaults)。 |
| 性能与效率 | 高 | 启动热路径无阻塞性设计缺陷;Dicfuse TTL/并发已有调优项。 | 启动期深度校验或 health 探远端会拖慢冷启动;Docker 多阶段构建需 registry cache。 |
| 可靠性与容错性 | 中低 | 优雅退出框架已存在,但生产路径仍有 unwrap/panic 与状态写失败无反馈。 | `DEFAULT_CONFIG` 懒初始化路径含 `panic!`;`TcpListener::bind`/`axum::serve` unwrap;卸载双重 unwrap。 |
| 兼容性与互操作性 | 中 | 依赖 libfuse + OpenSSL + Linux FUSE 模块;`rfuse3` 已启用 `unprivileged` feature。 | libfuse2/3、glibc/musl、容器 FUSE 可用性差异大;多实例共享 `antares_state_file` 未文档化。 |
| 可扩展性与可维护性 | 中 | 模块边界清晰(daemon/manager/antares/dicfuse),但弱类型配置与分散文档增加漂移成本。 | 无配置 schema 测试、无 API 契约测试、无 CLI help 快照测试。 |
| 合规性与标准符合性 | 中 | MIT/Apache-2.0 双许可;部分 12-factor 诉求合理。 | 缺 semver 废弃策略成文、FHS 路径约定、SBOM/checksum 发布规范、systemd hardening 与 FUSE 冲突说明。 |

### 跨文档依赖图

```text
Phase 0 文档对齐(无行为变更)
  └─> Phase 1 配置契约(env/校验/强类型兼容层) + 统一日志 + CLI 框架
        ├─> Phase 2 部署闭环(Docker/systemd/install.sh)  [硬依赖 env 覆盖]
        │     └─> Phase 2b CI release [依赖 install.sh URL 与产物命名]
        └─> Phase 3 API/health/completion 收敛 [依赖 CLI 框架与配置 show/validate]
              └─> Phase 4 治理(doctor/unwrap/perf/安全加固)
```

**硬依赖说明:**

- 无 env 覆盖前不应把端口/路径写进 Docker 镜像默认值。
- 无 `/health` 前 systemd `Type=notify` 或编排健康检查缺乏稳定探针。
- 无 `scorpio config validate` 前 `install.sh` 无法在安装后做离线门禁。
- 强类型 schema 落地前不应删除扁平键兼容层。

### 风险登记册

| ID | 风险 | 影响 | 缓解措施(已纳入计划) |
|---|---|---|---|
| R1 | 配置重构破坏库消费者(orion 等)早读 `config::*` 路径 | 集成方静默使用错误 `base_url` | 保留双 `OnceLock` 语义;`init_config` 始终优先;兼容访问器不改签名 |
| R2 | 并行改动 CLI + API + 配置字段名 | 用户脚本与 CI 大面积失效 | 旧入口/endpoint/扁平键保留 ≥1 minor;deprecation 日志 |
| R3 | Docker 示例暗示任意环境可跑 FUSE | 用户误判产品可移植性 | 文档明确内核模块、`/dev/fuse`、capability 前置条件 |
| R4 | `install.sh` / `curl \| sh` 供应链攻击 | 远程代码执行 | 提供 checksum 校验路径;Release 受保护环境 |
| R5 | HTTP API 无鉴权且默认 `0.0.0.0` | 未授权挂载/卸载 | P2 文档警示;P3 评估绑定 localhost 默认或 token 中间件 |
| R6 | 状态文件写失败被静默忽略 | 重启后挂载状态丢失 | 统一 `config_file()`;`to_toml` 错误必须传播或记日志 |
| R7 | `set_defaults` 回写 `scorpio.toml` | 只读 ConfigMap/注释丢失 | P1 移除回写;改 `config init` |
| R8 | systemd `TimeoutStopSec` 过短 | SIGKILL 导致 FUSE 残留 | `TimeoutStopSec` ≥ 40s(代码内 20s join + 15s Antares 清理 + 余量) |

### 待决策项(实施前需 closure)

| 决策 | 选项 | 建议默认 |
|---|---|---|
| 推荐 HTTP API 前缀 | 维持 `/antares/mounts` / 新增 `/api/v1/mounts` | 短期维持 `/antares/*`,文档明确 nest 前缀;v0.4 再评估 `/api/v1` |
| `POST /api/config` | 实现热更新 / 标记 deprecated | **deprecated**(无鉴权与一致性模型) |
| git 路由 | 启用 `daemon::git::router()` / 从 api.md 删除 | **已采纳:** 幽灵接口已从 api.md 删除;启用 `daemon::git::router()` 单列 P3+ 里程碑 |
| 配置 schema 格式 | 扁平键 / `[antares]` 子表 | 子表为目标格式,扁平键只读兼容一个 minor |
| HTTP 默认绑定地址 | `0.0.0.0` / `127.0.0.1` | 保持 `0.0.0.0` 但文档强调防火墙;评估 v0.3 改 localhost 默认 |

## 源码二次核对补充(修正与新增发现)

对前述计划做了一轮源码逐条核对(`src/`、`docs/`、`.github/`、根配置文件)。原计划识别的核心问题基本属实,但以下若干处需要修正或补充,已同步落入各子文档:

**对原结论的修正(避免据错误前提施工):**

- **主端口并非"无健康检查"**:Antares 路由被 nest 到主端口后,`GET /antares/health` 与按挂载粒度的 `GET /antares/mounts/{id}/ready` 已可用。准确表述应为"缺少根级、稳定、统一的 `/health` 入口"。新增 `/health` 应复用既有 Antares 健康逻辑。
- **`--http-addr` 已存在**:主二进制确实支持 `--http-addr`(默认 `0.0.0.0:2725`),且 `README.md` 已补充该参数;antares 也有 `--bind`。端口"硬编码"应修正为"默认值写死且缺少 env/配置文件覆盖链"。
- **`mega_url`/`mount_path` 不是单纯文档错误**:代码中的 `ConfigRequest` 结构体本身就用 `mega_url`/`mount_path`,与配置键 `base_url`/`workspace` 不一致;且 `POST /api/config` 是空操作,字段根本不落地。修复需对齐 API 契约层与配置层,不能只改文档。
- **优雅退出已部分实现**:`src/main.rs`/`src/daemon/mod.rs` 已有 SIGTERM/SIGINT 优雅关闭、先停 HTTP 再卸载、Antares 清理 15s 超时、daemon join 20s 超时。unwrap 清理与 systemd `TimeoutStopSec` 配置应在此框架内协同推进。
- **开发者本机路径已清理**:实际 `scorpio.toml`、`README.md` 与 `docs/perf_test.md` 均已使用 `/tmp/scorpio-megadir/...` 通用路径;后续若新增示例应避免 `/home/<developer>/...`。

**原计划遗漏、需补入的问题(部分文档项已在 Phase 0 修复,逐条标注):**

- **【已修复】** `GET /api/fs/select/{request_id}` 路由原"已注册但完全无文档",现已在 `docs/api.md` §2 完整文档化。
- **【已修复】** git 路由(status/commit/push/reset/add 等)在 `src/daemon/mod.rs` 已被注释禁用(移至 `daemon::git::router()`);`docs/api.md` 原先仍保留其数据结构(描述了一组不存在的接口),现已删除这些幽灵结构并改为说明"默认未启用、不予文档化"。
- **【代码侧待办】** 状态文件持久化:**两处硬编码** `"config.toml"`(`src/manager/mod.rs` 的 `remove_workspace`、`src/daemon/mod.rs` 的临时挂载路径);**三处**通过 `config::config_file()` 取得路径但用 `let _ =` 静默丢弃写失败(`src/manager/fetch.rs` ×3)。读路径(`main.rs` → `config_file()`)与写路径不一致是隐蔽缺陷。
- **【已修复】** `README.md`(仓库根)"How to Use" 已改为让用户编辑 `scorpio.toml` 的 `base_url`/`workspace`/`store_path`,并说明 `config.toml` 是运行时状态文件。
- **【已修复】** antares 端点前缀在独立 `serve`(`/mounts`)与主进程 nest(`/antares/mounts`)下的不一致已在 `docs/antares.md` 新增"访问方式与 URL 前缀"章节澄清(双写两种前缀);原漏写的 `GET /mounts/{mount_id}/ready` 已补(§4.2);`[antares]` 子表格式问题已改为扁平键并标注;`--mount-root`/`--upper-root`/`--cl-root`/`--state-file` CLI 覆盖在 `src/bin/antares.rs` **已实现**且已与文档一致。
- **【代码侧待办】** banner 在 `Args::parse()` 之前打印,污染 `--help`/`--version` 输出。

## 全局验收门禁

- 每个阶段必须有可回滚路径:旧配置、旧 endpoint、旧二进制入口至少保留一个 minor 版本。
- 配置 schema、CLI help、OpenAPI/API 文档和 README 示例必须能被测试或脚本校验,避免再次漂移。
- 发布产物必须包含版本号、目标平台、SHA256 校验和;自动发布到 crates.io 必须使用受保护环境或人工审批。
- `install.sh` 不应默认静默修改 `/etc/fuse.conf` 或授予高权限;涉及系统权限的操作必须提示并支持 dry-run。
- health endpoint 默认只做轻量本地检查;远端 mega 连通性、FUSE 权限、目录可写性放入 readiness 或 `scorpio doctor`。

## 分阶段验证矩阵

| 阶段 | 验证项 | 通过标准 |
|---|---|---|
| Phase 0 | 文档与实现一致性 | `docs/antares.md` 配置章节与 `scorpio.toml` 可读;`docs/api.md` 无幽灵 git 接口;README 指向 `scorpio.toml`/`workspace` |
| Phase 1 | 配置加载 | `SCORPIO_BASE_URL=... scorpio` 覆盖生效;非法类型启动即失败;`scorpio.toml` 启动后字节不变 |
| Phase 1 | 日志 | 全路径 `tracing`;`RUST_LOG=debug scorpio --help` 无 banner 污染 |
| Phase 1 | CLI 框架 | `scorpio serve` 等价旧 `scorpio -c ...`;`antares` 二进制仍可用 |
| Phase 2 | Docker | 干净环境 `docker compose up` 后 `curl /health` 200;最小挂载读写成功 |
| Phase 2 | systemd | `TimeoutStopSec≥40`;SIGTERM 后无残留 mountpoint;`Restart=on-failure` 生效 |
| Phase 2 | install.sh | `--dry-run` 可预览;checksum 失败拒绝安装 |
| Phase 3 | API 兼容 | 旧 `/api/fs/*` 仍可用且带 deprecation;新 `/health` 与 `/antares/health` 语义一致 |
| Phase 3 | 状态文件 | 自定义 `config_file` 路径时挂载/卸载状态写入正确;写失败返回错误 |
| Phase 4 | 可靠性 | 端口占用/状态损坏/卸载非法 inode 不 panic;`scorpio doctor` 覆盖 fuse/权限/mega |

## Semver 与废弃策略

- **PATCH**: 文档修正、日志、非破坏性 bug 修复。
- **MINOR**: 新子命令、env 覆盖、新 health 端点;旧 CLI/endpoint/扁平键保留并打 deprecation 警告。
- **MAJOR**: 删除旧 `/api/fs/*`、删除 `antares` 独立二进制、删除扁平配置键、更改默认绑定地址。
- 废弃周期:至少 **1 个 minor 版本**;废弃信息同时出现在日志、CLI stderr 与 API 响应头(如 `Deprecation: true`)。
