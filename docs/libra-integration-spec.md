# ScorpioFS × Libra 集成方案 SPEC

- **版本**：v0.5.0（核心实现完成；`chain` 模式仍后续）
- **日期**：2026-09-20
- **状态**：ADR-6、`materialize` fork、VCS pointer、`worktree add --backend scorpiofs`、Libra 顶层 `fork` 与 `sync` **已实现并通过真实 mega2 + FUSE E2E**；`chain` 模式仍是后续优化。
- **范围**：ScorpioFS 作为 Libra 的一种 worktree backend 选项；围绕 ScorpioFS 重构两边与 git 相关的操作；新增 `fork`（复制工作区）与 `sync`（= add + commit + push）。

### 实施进度（v0.5.0）

| 项 | 状态 | 证据 |
|---|---|---|
| 三条不变量 + `mode=chain/materialize`（ADR-1/ADR-2） | 设计定稿 | 评审确认 |
| **ADR-6 upper/CL 白障格式改 OCI** | ✅ **已实现** | libfuse-fs 新增 `new_antares_passthroughfs_layer_with`（rk8s 分支 `feat/libfuse-fs-antares-whiteout-format`，基于 `upstream/main`，已核实与 crates.io 0.2.0 字节一致）；ScorpioFS `ANTARES_WHITEOUT_FORMAT = OciWhiteout` + `/health` 增 `whiteout.oci.v1`；`cargo check` 干净 |
| ADR-6 回归测试 | ✅ **5/5 通过** | 含 `antares_passthrough_layer_reports_oci_whiteout`（层确实通过 `Layer` trait 报出 OCI 格式）与 `oci_whiteout_name_is_what_the_change_scanner_expects` |
| **ADR-6 端到端验证（真实 FUSE 挂载）** | ✅ **全部通过** | mega2 起在 `127.0.0.1:19000`、repo A 已 seed（tip `a950a30d`，含 `src/alpha.txt`/`src/beta.txt`/`docs/gamma.md`/`.gitkeep`）；`POST /antares/mounts {path:/project}` 挂载成功并读出 lower 全部文件。然后实测：① 删除**只存在于 lower** 的 `src/beta.txt` → **unlink 成功**（无需 `CAP_MKNOD`）② upper 里出现 `src/.wh.beta.txt`，且 `stat` 确认是**普通空文件、非字符设备**（`regular empty file mode=0`）③ `GET /worktree` 报 `{kind:deleted, path:src/beta.txt}` ④ 修改 `src/alpha.txt` → 报 `{kind:modified}`。**即「删除现在可被观测」，`sync` 能正确删远端文件** |
| E2E 环境 | ✅ 就绪 | WSL `/dev/fuse` + `user_allow_other` 就位；daemon 需 sudo 跑（ScorpioFS 无条件 `allow_other(true)`，其容器部署同样给 `CAP_SYS_ADMIN`） |
| §5.5 指针写入 / §6.1 `fork` 端点 | ✅ **`fork` 已实现并端到端验证**（`materialize`） | `src/daemon/upper_fork.rs`（含 8 条单测）+ `POST /mounts/{id}/fork` + `/health` 增 `fork.v1`。真实 FUSE 挂载实测（父挂载上改一个文件 + 删一个文件后 fork）：① 子继承**修改**（`parent-edit-1`）② 子继承**删除**（whiteout 被复制，`gamma.md` 在子视图中消失）③ **I2**：fork 之后父再写入，子仍是 `parent-edit-1`（快照成立，硬链接禁令有效）④ 子写不影响父、父写不影响子 ⑤ **父仍可写**（未变只读）⑥ 两挂载各自报告状态。**全部通过** |
| **`fork` 期间发现并修复的一个真实缺陷** | ✅ 已修 | 第一版把 delta 复制到**已挂载**的子 upper 上 → FUSE 层只在挂载时 `import()`，所以运行中的子挂载**看不到**复制的文件（读 `alpha` 而非 `parent-edit-1`，且写入报 `EEXIST`），而 `GET /worktree` 因为直接扫目录却报告正确 —— **API 与文件系统视图互相矛盾**。修法：新增**内部**（`skip_deserializing`，不进 HTTP 契约）字段 `CreateMountRequest::upper_dir`，fork 先复制到 staging upper，再由 `create_mount` 采用它，从而保证 delta 在 FUSE 会话启动前就已落盘 |
| §5.5 指针写入 | ✅ **已实现并端到端验证** | `POST .../worktree/base` 的 `vcs_pointer{commondir,worktree_id}`（可选字段，**向后兼容**）。实测：① 两文件按 `值\n` 逐字节写入（34 = 33+1 校验通过）② **`dirty=false`、`changes=[]`** —— 指针不污染变更视图（关键：否则 worktree 永远"脏"）③ 重复同内容绑定幂等 ④ **不同**指针被 `400` 拒绝，且原 `worktree_id` 未被覆写 ⑤ 空/相对 `commondir` 被拒 ⑥ 省略指针的旧客户端行为不变 |
| §7 Libra 侧（backend 配置 / `worktree add --backend` / `libra fork` / `libra sync`） | ✅ **全部已实现并实测** | `worktree add --backend scorpiofs` 正式 attach；`libra fork <path>` 通过 ScorpioFS `/fork` 创建独立 materialize child；`libra sync` add+commit+push。正式 fork E2E 验证了 child 继承父修改/删除、父子 upper 隔离；mega2 + FUSE 全链路通过 |
| **ScorpioFS 自定义 mountpoint API** | ✅ 已实现 | `CreateMountRequest.mountpoint` 可选字段：省略时保留 UUID mount root 行为；提供时直接挂到空目标目录，供 Libra backend 使用。已有 118 条 ScorpioFS lib tests 全绿 |
| `libra sync` CLI | ✅ **已实现并实测** | `src/command/sync.rs`：`add -A → commit → push`；要求 attached branch；默认推当前仓库 `origin` 的 trunk `main`（Mega2 只接受 `refs/heads/main`），默认 commit message 可自动生成。正式 backend worktree 中修改/删除文件后，sync 将 mega2 `main` 推进到 `c82496bd3cb589597be3422f842cf214fe8b0138` |
| **E2E 全链路（挂载 → 编辑 → `libra sync` → push 回 A）** | ✅ **真实 mega2 + FUSE 全部通过** | 已覆盖手工 attach 与正式 `worktree add --backend scorpiofs` 两条路径：Mega2 repo `/project`、ScorpioFS FUSE lower/upper、Libra linked worktree pointer/index、修改/删除、add+commit+push；期间修正 detached HEAD、Mega2 trunk-only ref 和 `HEAD` refspec 不兼容三个真实问题 |

### v0.2.0 → v0.3.0 变更（**两处 my-spec 纠错，都是"只读到局部就下结论"**）

| # | 变更 | 触发 |
|---|---|---|
| 1 | **Q8 纠错**：挂载点非空校验**已存在**（`src/server/mod.rs:118-126`），且 Antares 路径走得到（`src/antares/fuse.rs:110`）。v0.2.0 说"缺失"是**错的**；真正的缺口只是**时机**（校验在 `mount()` 才跑，fork 需要更早） | 读 `docker-compose.yml` 注释时发现矛盾，回头核实 |
| 2 | **Q7 纠错**：Antares API 的有效前缀**就是 `/antares/mounts`**（`src/daemon/mod.rs:226` 有 `app.nest("/antares", ...)`）。v0.2.0 说"文档写错了、应改成 `/mounts`"是**错的** —— 我只读了 `AntaresDaemon::router()` 内部的根相对路由，漏看 nest。**文档不需要改** | 核 `http-mount` 默认 endpoint 时发现 |
| 3 | 新增事实：**mega2 已提供 `/api/v1/tree`、`/api/v1/blob`、`/api/v1/tree/content-hash`** —— 即 N1 一直声称"Mega 尚未提供"的**版本可寻址读取**。N1 的阻塞可能比原判断小得多，需重新评估 | E2E 环境实测 `/api/openapi.json` |
| 4 | E2E 环境事实：宿主机 **127.0.0.1:9000 无法绑定**（`ss`/`iptables`/`Get-NetTCPConnection`/容器列表均无持有者），导致 Docker 端口绑定失败并**使容器彻底没有网络**，表现为误导性的 `Temporary failure in name resolution`。E2E 改用 **19000** | 实测 |
| 5 | `libfuse-fs` 开发期通过 `[patch.crates-io]` 指向本地检出（**不得提交**）；该路径在 scorpiofs 目录之外，会**打断容器化构建** → E2E 形态定为「mega2 容器 + scorpiofs/libra 原生跑在 WSL」 | 评审确认 |

### v0.1.0 → v0.2.0 变更

| # | 变更 | 触发 |
|---|---|---|
| 1 | **修正 v0.1.0 的错误**：`materialize` 原计划用**硬链接**复制父层 —— 这会因 upper 是原地写而静默损坏数据。改为 **reflink / 真实复制**，并新增「三条不变量 I1–I3」与**硬链接禁令**（ADR-1） | 评审意见：「fork 功能你要确保被物化的层没有被修改」 |
| 2 | `chain` 模式重新设计为**封存-接链**：父会话的 upper 被改名封存为共享只读层，同时给父会话换新空 upper。**父会话 fork 后可继续写**（v0.1.0 的「父会话变只读」约束被消解） | 同上 + 使 O(1) fork 与 I2 不变量同时成立 |
| 3 | **§6.2 的"最高风险项"根因定位**：不是缺失功能，而是 `whiteout_format` 配置未覆盖（Linux 默认 `CharDev` → `mknod` → 需 `CAP_MKNOD`，而 ScorpioFS 只给 `CAP_SYS_ADMIN`）。新增 **ADR-6** 决定改用 OCI(`.wh.`) 白障，**零特权** | 深入 libfuse-fs 依赖源码核实 |
| 4 | `mode` 取值 `flatten`/`cow` → `chain`/`materialize`；capability `upper-wh.v1` → `whiteout.oci.v1` + `fork-chain.v1` | 随 #1/#2/#3 |
| 5 | Q5（对象格式）按评审结论**定为默认开启 sha256、允许显式关闭** | 评审意见：「这个默认还是开 可以关闭吧」 |
| 6 | 新增验收项 **A10（I2 不变量回归）**、**A11（layer 顺序不变量）**、**A12（一致性协议）**、**B8（降级必须回报）**；实施顺序改为先 `materialize` 后 `chain` | 覆盖 #1/#2 引入的新风险面 |

---

## 0. 如何验证本文

本文所有代码锚点都写成 `文件:行号`，可直接核对。建议按顺序执行以下命令确认基线成立——**如果任何一条对不上，本文的结论就需要重写，请先反馈而不是直接开工**。

```bash
# 1. ScorpioFS 侧：确认没有任何 git 子进程 / git2 依赖
cd D:/--------code----------/mega-scorpiofs/scorpiofs
grep -rn "Command::new(\"git\")\|git2" src/ ; echo "exit=$? (expect 1 = 零命中)"

# 2. 确认 scorpiofs 只剩 git-internal 的零散引用（提交对象 / 对象存储）
grep -rn "git_internal" src/

# 3. 确认 git_author / git_email 在运行时无读取点（只有测试引用）
grep -rn "git_author()\|git_email()" src/

# 4. 确认 overlay 已支持多层 lower（fork 的关键前提）
sed -n '44,72p' src/antares/fuse.rs

# 5. 确认 upper 白障格式未被覆盖（ADR-6 的根因；注意这里是 libfuse-fs 源码，不是本仓库）
grep -n "new_antares_passthroughfs_layer" src/antares/fuse.rs    # 应见 2 处调用
ls ~/.cargo/registry/src/*/libfuse-fs-0.2.0/                     # 定位依赖源码
grep -n -A 8 "pub async fn new_antares_passthroughfs_layer" \
  ~/.cargo/registry/src/*/libfuse-fs-0.2.0/src/passthrough/mod.rs      # 应见 ..Default::default()，未设 whiteout_format
grep -n -A 12 "impl Default for WhiteoutFormat" \
  ~/.cargo/registry/src/*/libfuse-fs-0.2.0/src/util/whiteout.rs        # 应为 Linux=>CharDev / macOS=>OciWhiteout
grep -n "whiteout_format: WhiteoutFormat::default()" \
  ~/.cargo/registry/src/*/libfuse-fs-0.2.0/src/passthrough/config.rs   # Config 默认值
```

```bash
# 6. Libra 侧：确认 worktree/backend/fork/sync 已存在
cd D:/--------code----------/libra
grep -n "backend.*scorpiofs\|Commands::Fork\|Commands::Sync" src/cli.rs src/command/worktree.rs src/command/fork.rs src/command/sync.rs
```

---

## 1. 事实基线

### 1.1 ScorpioFS

| 事实 | 证据 |
|---|---|
| 无任何 git 子进程、无 `git2`；唯一进程派生是 fusermount/umount/mount/findmnt | `src/util/fuse_platform.rs`；全仓 grep 零命中 |
| `git-internal` 只剩两处用途：构建 Commit 对象、按 git 风格落 blob | `src/manager/fetch.rs:834-850`（`Signature::new` / `Commit::new`）；`src/manager/store.rs:133-210`（`objects/<sha[0:2]>/<sha[2:]>`） |
| `CommitStore` 把 Commit 当 JSON 存进 sled，**不是 git 的 loose-object zlib 格式** | `src/manager/store.rs:17,74-91`（`COMMIT_KEY="commit:v2"`） |
| `git_author` / `git_email` 是**死配置**：只在测试里被读 | `src/util/config.rs:1196-1202` 定义；唯一调用点 `:1356-1357` 是测试。`:574-575` 只做非空校验 |
| push / integrate / add 已被删除，源码留注释标记 | `src/manager/mod.rs:82-84,108-109,147` |
| CLI 无 git/worktree 子命令，worktree **仅走 HTTP** | `src/main.rs:44-89`；`src/daemon/antares.rs:84-92` |
| overlay 已是 **Vec\<lower\>**：CL 层压在 Dicfuse 之上 | `src/antares/fuse.rs:44-72`（`lower_layers.push(cl)` 后 `push(dic)`）→ **多层 lower 已被支持** |
| upper 层语义：passthrough 可写，CL 层可选且同样可写 | `src/antares/fuse.rs:47-72`；注释见 `:60-62` |
| **upper 白障格式在 Linux 上默认是 `CharDev`（需 `CAP_MKNOD`），且 ScorpioFS 未覆盖该默认值** | `libfuse-fs-0.2.0/src/util/whiteout.rs`（`impl Default`：Linux→`CharDev`）、`:191,244`（`Config.whiteout_format` 取默认）、`src/passthrough/mod.rs:117-134`（`new_antares_passthroughfs_layer` 用 `..Default::default()`）；ScorpioFS 侧调用点 `src/antares/fuse.rs:51,59`。详见 **ADR-6** |
| 删除的白障在 upper 里有**两种**编码，扫描侧**都**识别（无条件，无 cfg 门控） | `src/daemon/antares.rs:818-843`（`.wh.` 前缀 `:826-830`；字符设备 `:832-834`） |
| `oci_whiteout_path`（OCI 白障**创建**助手）只在 `test` 与 `macos` 下编译 | `src/daemon/antares.rs:807`（`#[cfg(any(test, target_os = "macos"))]`）→ 暗示 Linux 白障路径未被测试覆盖 |
| 一个 worktree = 一个 Antares mount = 私有 upper + 共享 dicfuse | `src/antares/mod.rs:213`（`mount_job_at`）；`docs/libra_worktree_task.md:16-24` |
| 层目录布局：upper / cl / dicfuse 三路 | `src/daemon/antares.rs:511-517`（`MountLayers`） |
| 变更扫描**只扫私有 upper**（+CL），且**已跳过顶层 `.libra`** | `src/daemon/antares.rs:847-906`（`scan_layer_changes`，`:880-884` 跳过 `.libra`）；`:907-937`（`scan_mount_changes`） |
| 只有 `ChangedPath{kind,path,source_path}` 三个字段，`kind ∈ {Modified, Deleted}` | `src/daemon/antares.rs:615-630` |
| 挂载点假定为空目录，未检查非空 | `src/antares/fuse.rs:32,83`（直接 `metadata` / `create_dir_all`） |
| 已有 worktree 控制面契约：base 绑定 / refresh-plan / ready / changes | `docs/worktree-api.md`；`docs/worktree-state-transitions.md`；`src/daemon/antares.rs:84-92` |
| `refresh-plan` 是**非变更**的守卫；lower 切换尚未实现 | `docs/worktree-state-transitions.md:118-129`；`src/daemon/antares.rs:605-611` |
| 所有权契约已写明：Libra 拥有 HEAD/index/refs/对象/transport，ScorpioFS 拥有 mount/lower/upper | `docs/worktree-api.md:22-30` |
| **Antares API 的有效路由前缀是 `/antares/mounts`**（`AntaresDaemon::router()` 内部的 `router()` 写的是根相对的 `/mounts`，但 `scorpio serve` 把它 nest 到 `/antares`；`scorpio http-mount` 的默认 endpoint 也正是 `http://127.0.0.1:2725/antares`） | `src/daemon/antares.rs:72-93`（router 定义）+ **`src/daemon/mod.rs:225-226`（`app.nest("/antares", antares_router)`）**；`src/cli.rs:277`（`{endpoint}/mounts`）；`src/main.rs`（`http-mount` 默认 endpoint）。→ **`docs/worktree-api.md:39-47` 是对的**（v0.3.0 修正） |

### 1.2 Libra

| 事实 | 证据 |
|---|---|
| **全仓零命中 "scorpio"** —— 集成尚未开始 | 全仓 grep |
| worktree 已实现，10 个子命令 | `src/cli.rs:384`；`src/command/worktree.rs:74-163`（Add/List/Lock/Unlock/Move/Prune/Remove/Umount/Repair） |
| 已有 **`worktree-fuse` Cargo feature**，走 `libfuse_fs::overlayfs` | `Cargo.toml`；`src/command/worktree-fuse.rs:16,574` |
| **无 `fork` / `sync` 顶层命令** | 全仓 grep；`fork` 只在 `docs/development/gap/*.md:749` 作为**被否决的想法**出现 |
| worktree 注册表是 **JSON**（`<storage>/.libra/worktrees.json`），**不是** git 的 `.git/worktrees/` | `src/command/worktree.rs:819`；`:176-232`（`WorktreeEntry` / v2 schema）；`docs/commands/worktree.md:495` |
| 每个 linked worktree 是**真实目录**（非 symlink），含**自己的 `.libra/`**，里面**恰好两个文件**：`commondir`（→ 共享的 db/objects/hooks）+ `worktree_id`（私有 HEAD/index 作用域） | `src/command/worktree.rs:2157-2170`（`create_worktree_gitdir`）；`src/utils/util.rs:414-420`（`try_get_worktree_gitdir`）；`src/internal/worktree_scope.rs` |
| linked worktree 的 `commondir` 是**规范化绝对路径**，且会被**校验**：必须 canonicalize 到本仓库自己的 storage，re-attach 时还会读 HEAD 验证链路通 | `src/command/worktree.rs:3282`（写入）；`:1743-1770`（校验）；`:3449-3477`（migrate 后验证） |
| refs/HEAD 存在 **SQLite**，不是文件 | `src/internal/db/migration.rs:866`（`reference` 表带可空 `worktree_id`） |
| 对象存储 trait `Storage` 抽象的是 **git 对象**，不是工作区文件来源 | `src/utils/storage/mod.rs:19`；实现在 `local.rs:693` / `remote.rs:99` / `tiered.rs:201` |
| 真正决定「worktree 文件从哪来」的接缝是 `WorkspaceStrategy` 枚举 | `src/internal/ai/agent_run/event.rs:153`（`Worktree \| Sparse \| FullCopy \| Blocked`，`#[non_exhaustive]`） |
| 仓库配置在 SQLite `config_kv`，**不是 TOML** | `src/internal/config.rs`；`src/command/config.rs` |
| 存储后端由 **env** 选择（`LIBRA_STORAGE_TYPE=local/s3/r2`），非配置文件 | `src/utils/client_storage.rs:894-1144` |
| 已有 FUSE 挂载工具与 task worktree 前缀 | `src/utils/fuse.rs`（`libra-task-worktree-fuse-`，`:206`） |
| **对象格式默认 sha1**，但已支持 sha256（`core.objectformat`） | `CLAUDE.md`「Global hash-kind preflight」；`git_internal::hash::set_hash_kind` |

---

## 2. 目标与非目标

### 2.1 目标

1. **G1** — ScorpioFS 可作为 Libra 的一种 worktree backend 选项，与 Libra 现有 worktree 实现**可切换**并存。
2. **G2** — `fork`：从当前工作区**毫秒级**派生一个独立工作区目录。
3. **G3** — `sync`：一条命令完成 add + commit + push，且**只由 Libra 执行**。
4. **G4** — ScorpioFS 侧的 git 语义收敛到零（除 `fork` 所需的层复制）；`.libra` 组成重新定义为「不存 git 对象」。
5. **G5** — 两种形态都支持：**HTTP daemon 为主路径**，libra 内嵌 crate 为可选路径，两者语义一致。

### 2.2 非目标

- **N1** — 不实现 lower 的**版本切换**（即 refresh-plan 之后真正的 Dicfuse remount）。仍受 §1.1 最后一条与 `worktree-state-transitions.md:118-129` 的约束：`base_revision` 仍是控制面溯源信息。本文的 `fork` 是**目录派生**，不是版本回滚。
- **N2** — 不做三路合并 / rebase / stash 的冲突模型（`worktree-state-transitions.md:131-146` 已登记为未来工作）。
- **N3** — 不做跨机器 worktree 迁移。
- **N4** — 不在本文范围内解决 `docs/worktree-api.md` 与代码的路由前缀不一致（登记为 Q7，可顺手修文档）。
- **N5** — 不引入 ScorpioFS ↔ Libra 的**编译期循环依赖**（见 ADR-4 的边界）。

---

## 3. 决策记录

### ADR-1：`fork` 的三条不变量、硬链接禁令，与两种模式

> **原稿 v0.1.0 的错误已在此修正**：原稿让 `materialize` 模式用**硬链接**复制父层。**那是错的**，会造成静默数据损坏。见下方「为什么禁止硬链接」。

#### 三条不变量（任何 fork 实现都必须满足）

- **I1 — 被引用的层不可写。** 任何被别的会话当作 lower 引用的层，必须没有任何会话把它当 upper。否则子会话的视图会随父会话的写入而漂移。
- **I2 — fork 是快照。** fork 之后父会话的任何写入，**绝不可**出现在子会话的视图里。
- **I3 — fork 不得撕裂。** 不能把「正在被写入一半」的文件捕获进子会话。

> **为什么禁止硬链接**：Antares 的 upper 是 **passthrough 目录**，overlay 对 upper 内文件的写入是**原地写**（open/write/truncate 打到同一个 inode），不是文件级写时复制。所以若让 B_up 的文件是 A_up 文件的硬链接，两个路径指向**同一 inode**：A 一写，B 立刻看到 —— **违反 I2，且是静默的**（没有任何报错，只有错误的内容）。同理也不能用硬链接去"省空间"地共享只读层：只要该 inode 还可能被某个 upper 写，就不安全。**因此 `materialize` 只能用 reflink 或真实复制。**

#### 模式 `chain`（默认）—— 封存-接链

```text
fork 前：
  A = { upper: A_up, lower: [ dicfuse ] }

fork A -> B 后：
  A_up 原子改名为 A_frozen，并被"封存"（此后没有任何会话以它作 upper）
  A = { upper: A_up2(新建空), lower: [ A_frozen, dicfuse ] }   # A 可见内容不变，且可继续写
  B = { upper: B_up(新建空),  lower: [ A_frozen, dicfuse ] }
```

**关键点**：不是把 `A_up` 直接留给 B 当 lower（那会让 A 的后续写入泄漏进 B，违反 I2），而是把 `A_up` **从「可写 upper」的角色上摘下来**，同时给 A 换一个**新的空 upper**。于是：

- `A_frozen` 成为**只读层**，被 A 与 B 共享 → 满足 I1；
- A 的可见内容**完全不变**（`A_frozen` 成了它自己的 lower）；
- **A 可以继续写**（写进 `A_up2`）→ **这解决了原稿 C2「父会话 fork 后变只读」的问题**，也是 `chain` 优于朴素写时复制的根本原因；
- B 看到的是 fork 那一刻的 A（含 A 当时的全部 dirty 改动），且此后不再变化 → 满足 I2；
- 全程**零文件复制**，只有两次目录改名/新建 → O(1)，毫秒级 → 满足 I3（没有任何文件被读取，自然不存在撕裂）。

#### 模式 `materialize` —— 复制快照

把 `A_up` 复制进 `B_up`（优先 **reflink**，回退**真实复制**；**绝不硬链接**），lower 保持 `[dicfuse]`。A 不动、可继续写。

- 层链深度**恒定**，不需要链感知的扫描与卸载顺序（ADR-2 里 chain 的配套改动全部不需要）；
- 代价是 O(A 的 dirty 文件数) 而非 O(1)，并且**必须实现一致性协议**（ADR-2 §materialize）才满足 I3。

#### 选择建议

| | `chain` | `materialize` |
|---|---|---|
| fork 成本 | O(1) | O(dirty 文件数) |
| 父会话 fork 后 | **可继续写** | **可继续写** |
| 磁盘增量 | 0 | 0（reflink）/ O(bytes)（复制） |
| 层链深度 | 每次 fork +1 | 恒为 1 |
| 需要链感知扫描 | 是 | 否 |
| 需要持久化 schema 升级 | 是 | 否 |
| 卸载顺序约束 | 是 | 否 |
| deep fork 性能 | 线性衰减（受深度上限约束） | 恒定 |

**建议**：`chain` 作默认（O(1) 且父可继续写，最贴合「随时 fork」），并设**层链深度上限**（建议 8），到上限后 `fork` 自动降级为 `materialize` 而不是报错。若你更看重「不引入 DAG / 不动持久化 schema」的实现简单性，则把默认改成 `materialize` —— 此时唯一的硬性前置是 **reflink 可用性探测**（Linux 上 btrfs / XFS(reflink=1) 支持 `FICLONE`；不支持则回退真实复制，代价是 O(bytes)）。

### ADR-2：两种模式各自的配套改动

#### `chain` 模式的配套改动

- **C1 — 变更扫描必须链感知。** `scan_mount_changes(mount_id, upper_dir, cl_dir)`（`src/daemon/antares.rs:907`）要泛化为接受**层链**并按 **nearest-wins** 合并：
  - 更近的层覆盖更远的层；
  - 白障必须**抵消**更远层里的同名条目（`layer1` 有 `.wh.foo` + `layer0` 有 `foo` → 合并结果 `foo` 为 Deleted，且 `.wh.foo` 本身**不得**作为路径上报）；
  - 这套语义与 libfuse-fs 自身的 `lookup` 一致（`overlayfs/mod.rs:234-236` 按 `layer.whiteout_format()` 判白障）。
  - 现状里 `cl_dir` 已经是"多扫一层"的雏形（`:913-916`），把它泛化成 `&[Path]` 即可。
- **C2 — 封存是不可逆操作，失败必须可回滚。** 改名 + 新建 upper 必须**顺序化且可回滚**：先建 `A_up2` → 再改名 `A_up → A_frozen` → 再更新 A 与 B 的层链 → 最后删除失败残留。任一步失败要恢复到 fork 前状态，**不能出现「A 的 upper 被改名了但层链没更新」的中间态**（那等于 A 丢掉全部 dirty 改动）。
- **C3 — 层链深度上限。** 深度 = 一次 lookup 要穿透的层数。设上限（建议 8），超限时 `fork` 降级为 `materialize`（此时 `materialize` 需要先"压平"链：把整条链物化成单层，否则复制出来的只是一层）。
- **C4 — FUSE unmount 必须逆序。** 先卸引用者、再卸被引用层，否则会出现悬空 lower。需要显式的引用计数（一个 frozen 层可能被 N 个会话引用，必须最后一个引用消失才能删除该目录）。
- **C5 — 持久化与恢复。** `PersistedMountState`（`:946-960`）新增 `lower_chain: Vec<String>` 与一个 **schema 版本字段**（当前无版本字段 → 属破坏性变更，需要迁移策略）；`MountLayers`（`:511-517`）也要能表达链而非单一 `dicfuse`。
- **C6 — layer 顺序是核心不变量。** `build_overlay`（`src/antares/fuse.rs:44-72`）里**先 push 的 lower 优先级更高**（`cl_layer` 先 push、`dicfuse` 后 push，因为 CL 要覆盖 dicfuse，见 `:48-55` 与其注释）。因此链必须按 **nearest-first** 顺序 push：`[A_frozen, dicfuse]`。**顺序写反会静默地把优先级倒过来**，必须专门写一个测试钉死这一点。

#### `materialize` 模式的一致性协议（满足 I3）

对每个要复制的文件：

1. `fstat` 记录 `(ino, size, mtime_ns, ctime_ns)`；
2. 复制（reflink / read+write）；
3. **重新 `fstat` 同一 inode 并比对**。若任一字段变化，或 `ino` 已不同（文件被替换），则该文件视为撕裂 → **重试该文件**（建议上限 3 次）；
4. 重试耗尽 → **整个 fork 失败**，返回 `409 source_busy`，提示调用方先静默父会话的写入者。**不得**留下一个部分复制的子会话。

整批复制完成后再做一次**整体校验**：重新扫描父 upper，与复制前记录的变更集（path 集合 + `generation`，`src/daemon/antares.rs:919-930`）比对；若路径集合变了，说明父会话在 fork 期间仍在增删文件 → 按同样策略重试或失败。

**白障与特殊节点**（两种模式都要处理）：
- OCI 白障 `.wh.<name>`：按**普通文件原样复制**（见 ADR-6 的 whiteout format 决策后，Linux 上不再有字符设备白障，因此**不需要 `CAP_MKNOD`**）；
- 不透明目录标记 `.wh..wh..opq`：同样原样复制；
- 符号链接、目录权限位、xattr：按 `lstat` 结果保真复制；
- 复制过程中**跳过顶层 `.libra`**（与扫描一致，`src/daemon/antares.rs:880-884`）。

### ADR-3：`sync` 由 **Libra** 执行

**决策**：ScorpioFS **不算 hash、不写对象、不更新 index、不碰 refs、不 push**。ScorpioFS 只负责「报告哪些路径变了」。

**理由**：
- Libra 侧已有完整实现（`src/command/add.rs` 62KB、`commit.rs`、`push.rs`）；ScorpioFS 侧相关代码**已被删除**（`src/manager/mod.rs:82-84,108-109,147`）。
- 与既有契约一致：`docs/worktree-api.md:22-30`、`docs/worktree-state-transitions.md:5-17` 已经把对象/refs/transport 判给 Libra。
- 避免双 VCS 实现导致的行为漂移（mtime 精度、ignore 规则、大小写、行尾）。

**分工**：

| 步骤 | ScorpioFS | Libra |
|---|---|---|
| 探测变更路径 | ✅ `GET /mounts/{id}/worktree` 返回 `ChangedPath[]` | — |
| 读取内容 / 算 hash | 仅提供文件内容（普通 FUSE 读） | ✅ 从挂载点读文件并 hash |
| 写对象 / index / refs | ❌ | ✅ |
| commit / push | ❌ | ✅ |
| 变更后的 upper 清理 | ✅ 待定（见 Q4） | 先行 refs 更新 |

**后果**：`sync` **不是 ScorpioFS 的命令**，而是 **Libra 的命令**。ScorpioFS 只新增 HTTP 端点（§5.3）。

### ADR-4：双形态接入，HTTP 优先

**决策**：主路径 = 独立 `scorpio serve` 进程 + 现有 HTTP 控制面；可选路径 = libra 内嵌 `scorpiofs` crate，用 Cargo feature 门控。

**理由**：HTTP 路径沿用已有的 capability 协商（`mount.v1` / `ready.v1` / `changes.v1` / `worktree-base.v1` / `refresh-plan.v1`，见 `docs/worktree-api.md:7-20`），进程级解耦、可独立升级、跨机器可用；内嵌路径省 IPC 但版本强耦合。

**边界**：两条路径必须共享**同一份类型定义**。做法：ScorpioFS 把控制面类型放进一个**不依赖 FUSE** 的子模块（例如 `scorpiofs::worktree_contract`），Libra 只依赖该模块的**数据类型**，**不得**依赖其 FUSE 实现——以此避免 N5 的循环依赖。若 Libra 内嵌需要挂载能力，再由 `worktree-fuse` 之外的独立 feature 引入。

**代价**：测试矩阵翻倍（HTTP / 内嵌 / 二者语义一致）。建议内嵌路径的测试只覆盖契约等价性，功能测试集中在 HTTP 路径。

### ADR-5：`.libra` 组成重新定义

**决策**（针对你提出的「scorpiofs 不经过 clone，所以不保存 git 对象」）：

不再把 `.libra` 当成一个整体。按**三层**职责拆开：

| 层 | 位置 | 内容 | 谁写 |
|---|---|---|---|
| **完整元数据** | **main worktree**（local store） | `libra.db`(config/HEAD/主 refs)、`index`、`objects/`、`vault.db` | Libra |
| **linked worktree 私有** | 各 worktree 的 `.libra/` | **恰好两个文件**：`commondir`（规范化绝对路径 → main store）、`worktree_id`。私有 HEAD/index 行是 **SQLite 行**（带 `worktree_id`），**不在** `.libra/` 里落文件；**无 `objects/`、无 `vault.db`** | Libra（`create_worktree_gitdir`） |
| **挂载内投影** | ScorpioFS mount 内 | 同样是 `commondir` + `worktree_id`，即**可重建指针**。`base_revision` 建议**不落文件**（避免双真相，见 Q9） | ScorpioFS（内容由 Libra 传入） |

**理由**：
- Libra **已经**是「linked worktree 有自己的 `.libra/`，里面只有 `commondir` + `worktree_id`」（`src/command/worktree.rs:2157-2170`），所以这**不是新机制**，而是把「objects 不复制」这条明确下来。
- 因此「挂载内可重建指针」不是抽象概念，而是**可以直接照抄 `create_worktree_gitdir` 的两个文件**：

  ```text
  <mount>/.libra/commondir      # 一行：规范化绝对路径 → main store
  <mount>/.libra/worktree_id    # 一行：稳定 worktree id
  <mount>/.libra/base_revision  # 可选：溯源用（权威仍在 mount 绑定上，见 Q9）
  ```

- ⚠️ **写这两个文件必须严格对齐 Libra 的校验**（`src/command/worktree.rs:1743-1770`）：`commondir` 会被 canonicalize 并与「本仓库自己的 storage」比对，不符即拒绝 re-attach。所以：
  - 必须写**规范化后的绝对路径**，不能写相对路径、不能带尾随未规范化片段；
  - ScorpioFS 侧**不应自行拼这个路径**——应由 Libra 在 `POST …/worktree/base` 时把指针内容**随请求传入**，ScorpioFS 只负责原样落盘。这样路径规范化、repo 身份、`worktree_id` 的权威都在 Libra，ScorpioFS 保持 VCS 无关。
- ScorpioFS 侧**已经**跳过挂载内顶层 `.libra`（`src/daemon/antares.rs:880-884`），与「挂载内只有可重建指针」互相印证——这个跳过逻辑**必须保留并在注释里写明原因**，否则未来有人会当成无用分支删掉。「不保存 git 对象」的准确含义：**linked worktree 不持有独立对象库**，对象由 main store 或 Mega 远端提供。ScorpioFS 侧那个 sled 对象缓存（`src/manager/store.rs`）是**投影缓存**，不是权威对象库，应标注为可丢弃。

**对象格式（Q5 已定）**：**默认开启 sha256**（`core.objectformat=sha256`），**允许显式关闭**改为 sha1（`libra config set core.objectformat sha1`）。两个后果要一并接受：(a) 从 Mega 拉对象可做**完整性校验**（sha256 内容寻址，与 ScorpioFS 的 MST/2 快照一致，`src/snapshot/durable.rs:31`）；(b) **Libra 兼容性矩阵**中受影响的 Git 互操作项要按 `partial` 登记（见 §7.4）。因为默认开启，**升级首个版本必须提供「从 sha1 仓库切到 sha256」的明确路径或明确的拒绝路径**——这属于 Libra 侧既有 preflight 的职责（`cli.rs` 的 `core.objectformat` 读取 + `set_hash_kind`），不在本文范围内，但需要 Libra 侧确认；若切换不可行，则「默认开」应改为「新仓库默认开、旧仓库保持原样」。

### ADR-6：upper 白障格式改用 **OCI（`.wh.`）**，不用字符设备

**这是 §6.2 那个"最高风险项"的真实根因**，且它不是一个"缺失的功能"，而是一处**配置未覆盖**。

**已核实的事实链**：

1. libfuse-fs 的 `WhiteoutFormat` 有两个变体：`CharDev`（字符设备 0:0，`mknod` 创建）与 `OciWhiteout`（空普通文件 `.wh.<base>`）。`Default` = **Linux 取 `CharDev`**、macOS 取 `OciWhiteout`。证据：`libfuse-fs-0.2.0/src/util/whiteout.rs`（`enum WhiteoutFormat` 与 `impl Default`，含 `default_per_platform` 测试）。
2. `PassthroughFs::Config.whiteout_format` 默认取 `WhiteoutFormat::default()`。证据：`libfuse-fs-0.2.0/src/passthrough/config.rs:191,244`。
3. ScorpioFS 的 upper / CL 层都由 `new_antares_passthroughfs_layer` 创建（`src/antares/fuse.rs:51,59`），而它用 `..Default::default()` 构造 Config，**没有覆盖 `whiteout_format`**。证据：`libfuse-fs-0.2.0/src/passthrough/mod.rs:117-134`。
4. → Linux 上 Antares upper 的白障走 `CharDev` 分支，即 `mknod(S_IFCHR, makedev(0,0))`。证据：`libfuse-fs-0.2.0/src/overlayfs/layer.rs:52-79`（`create_whiteout` 的 `CharDev` 分支）。
5. → 而 `mknod` 需要 **`CAP_MKNOD`**（`util/whiteout.rs` 的模块文档明确写了 "Requires `CAP_MKNOD` on Linux"）。ScorpioFS 的部署只授予 `CAP_SYS_ADMIN`（`deploy/systemd/scorpiofs.service`；`plan-20260831.md` 的事实基线行）。
6. → 因此**删除一个只存在于 Dicfuse lower 的文件**时，白障创建会 `EPERM` 失败。而且 `src/daemon/antares.rs:807` 的 `oci_whiteout_path` 只在 `#[cfg(any(test, target_os = "macos"))]` 下编译 → **这条路径在 Linux 上很可能从未被测试过**。

**决策**：给 upper 与 CL 的 passthrough Config **显式设置 `whiteout_format: WhiteoutFormat::OciWhiteout`**。

**收益**：
- **不需要任何特权**（OCI 白障是普通空文件）；
- 白障形态变成 `.wh.<name>`，而 `classify_layer_entry`（`src/daemon/antares.rs:818-843`）**本来就无条件识别 `.wh.` 前缀**（`:826-830`，无 cfg 门控）→ **扫描侧零改动**；
- `fork` 的 `materialize` 只需复制普通文件，**不需要 `mknod` 重建白障**（ADR-2 因此简化）；
- 与 libfuse-fs 自身 overlay 的 lookup 一致（`overlayfs/mod.rs:234-236` 按 `layer.whiteout_format()` 判白障）。

**代价与限制**（必须写进用户文档）：
- `is_user_creatable_name`（`libfuse-fs-0.2.0/src/util/whiteout.rs`）在 OCI 格式下会**拒绝**用户创建任何以 `.wh.` 开头的名字 → 仓库里若真有文件叫 `.wh.*`，在挂载内**无法创建**；`.wh..wh..opq` 被保留为不透明目录标记。这是**真实但罕见**的限制（同理 `CharDev` 格式下不受此限制）。
- 需要确认 **libfuse-fs 的 overlay 读取侧**在 OCI 格式下对**跨层**白障的抵消行为与 `scan_mount_changes` 的链感知合并（ADR-2 C1）语义一致——两者必须用同一套 `.wh.` 判定。

**备选（否决）**：给 systemd unit / 容器加 `CAP_MKNOD`，保留 `CharDev`。否决理由：授予更多特权；且 `fork` 的 `materialize` 还必须 `mknod` 重建白障，进一步放大特权面与失败面；OCI 方案零特权且扫描侧已有现成支持。

**可观测性**：`/health` 增 `whiteout.oci.v1`（§5.1），Libra 在缺失时**拒绝 `sync`**——因为缺失意味着可能出现**漏删**，这是最危险的失败模式。

---

## 4. 架构总览

```text
┌──────────────────────────────────────────────────────────┐
│ Libra（VCS 权威）                                         │
│   objects / index / HEAD / refs / commit / push / vault   │
│   worktree.backend = local | worktree-fuse | scorpiofs    │
│   新增: libra fork / libra sync                           │
└───────────────────────────┬──────────────────────────────┘
                            │ 路径 1（默认）HTTP 127.0.0.1:2725
                            │ 路径 2（可选）in-process crate
┌───────────────────────────▼──────────────────────────────┐
│ ScorpioFS（投影层）                                        │
│   Antares mount = upper(rw,私有) + CL?(opt) + lower(ro)   │
│   [chain 模式]  mount = upper(rw) + lower 链(ro)           │
│   新增: POST /mounts/{id}/fork  (mode=chain|materialize)   │
│   修复: upper 白障格式 → OCI（ADR-6，去 CAP_MKNOD 依赖）    │
│   移除: git_author/git_email 死配置                        │
└───────────────────────────┬──────────────────────────────┘
                            │ FUSE
┌───────────────────────────▼──────────────────────────────┐
│ 工作区目录（POSIX 语义给 Agent / 编译器 / 测试工具）        │
└──────────────────────────────────────────────────────────┘
                            │ HTTP
                     ┌──────▼──────┐
                     │ Mega（对象源）│
                     └─────────────┘
```

**典型闭环**：

```bash
libra worktree add --backend scorpiofs /w/a            # A 挂载，bound base_revision
# ... Agent 在 /w/a 里改代码 ...
libra fork /w/b                                        # B = A 的派生（毫秒级）
# ... 在 /w/b 上做隔离实验，失败就丢弃 ...
libra sync                                             # ChangedPath -> add+commit+push
```

---

## 5. 接口契约

### 5.1 能力协商扩展

`GET /health`（`src/daemon/antares.rs:73`）在现有 capability 之外**追加**：

```text
fork.v1            # 支持 POST /mounts/{id}/fork
fork-chain.v1      # 支持 mode=chain（封存-接链，ADR-1）
whiteout.oci.v1    # upper 白障为 OCI(.wh.) 形态 —— 删除可被 scan 观测（ADR-6）
```

**理由**：`docs/worktree-api.md:7-20` 已确立「客户端应协商能力，而不是假定每个 daemon 都实现 worktree API」的约定。`sync` 的正确性依赖删除可见性，因此把「删除可观测」也做成能力，让 Libra 在不支持的 daemon 上**拒绝**而不是**静默漏删**。这是本设计里最重要的一条防御。

### 5.2 `POST /mounts/{mount_id}/fork`

**请求**：

```json
{
  "path": "/w/b",
  "job_id": "b-fork-001",
  "mode": "chain",
  "inherit_cl": false
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `path` | 是 | 衍生工作区的挂载点。**必须不存在或为空目录** |
| `job_id` | 否 | 不传则服务端生成 |
| `mode` | 否 | `chain`（默认，ADR-1 封存-接链，O(1)，父会话可继续写）\| `materialize`（ADR-1 复制快照，层链深度恒定）。默认值可由服务端配置覆盖；`chain` 需 capability `fork-chain.v1` |
| `inherit_cl` | 否 | 默认 `false`。CL 层是**构建基线**，不是工作区编辑（`worktree-state-transitions.md:19-20`），因此默认不继承。设为 `true` 时新会话的链里包含源 CL 层 |
| `max_chain_depth` | 否 | 仅 `mode=chain`。不传则用服务端默认（建议 8）。**超过此值时服务端自动降级为 `materialize` 并在响应里报告**，而不是报错 |

**响应** `201 Created`：

```json
{
  "mount_id": "…",
  "job_id": "b-fork-001",
  "path": "/w/b",
  "source_mount_id": "…",
  "mode": "chain",
  "mode_downgraded_from": null,
  "lower_chain": ["<frozen-id>", "<dicfuse-id>"],
  "base_revision": null,
  "mount_state": "provisioning",
  "source_frozen_layer": "<frozen-id>",
  "copy_stats": null
}
```

- `lower_chain` **按 nearest-first 顺序**（`[0]` 优先级最高）——与 ADR-2 C6 的不变量一致，Libra 不得重排。
- `mode_downgraded_from`：若因深度上限自动降级，此处填原请求的 `mode`，否则 `null`。**Libra 必须**把它透传给用户（否则用户以为拿到了 O(1) fork）。
- `source_frozen_layer`：`mode=chain` 时给出被封存的层 id，便于诊断与引用计数；`materialize` 时为 `null`。
- `copy_stats`：`mode=materialize` 时给出 `{files, bytes, reflink_used, retries}`；`chain` 时为 `null`。

**错误**：

| 状态 | 条件 |
|---|---|
| `400` | `path` 已存在且非空；`mode` 取值未知 |
| `404` | 源 `mount_id` 不存在 |
| `409` | 源 mount 状态非 `Ready`（如 `Quiescing`）；`mode=materialize` 时一致性协议重试耗尽（`source_busy`，告警含需静默的提示） |
| `501` | 未编译 `fork.v1`（`mode=chain` 另需 `fork-chain.v1`，缺失时降级为 `materialize` 而非报错） |

**新 mount 的初值**：`base_revision = null`、`mount_state = Provisioning`。**Libra 必须**在新 mount 可写之前调用 `POST …/worktree/base` 绑定 revision——与既有 `docs/worktree-api.md:31-48` 的 attach 流程完全一致，`fork` 不改变这个前置条件。

### 5.3 `sync` 所需的变更报告（**不需要新端点**）

`GET /mounts/{id}/worktree`（已存在，`src/daemon/antares.rs:84`）当前已返回：

```json
{ "mount_id": "…", "path": "…", "base_revision": "…", "mount_state": "ready",
  "dirty": true,
  "changes": { "mount_id": "…", "generation": 123456,
               "changes": [ { "kind": "modified", "path": "src/lib.rs" },
                            { "kind": "deleted",  "path": "src/old.rs" } ] } }
```

`generation` 是变更集的稳定指纹（`src/daemon/antares.rs:907-937`，FNV-1a over sorted path+kind）。**Libra 用 `(mount_id, base_revision, generation)` 做幂等缓存**，避免重复 hash 未变的路径——这正是 `docs/worktree-api.md:78-82` 设想的用法。

**唯一缺口**：`ChangeKind` 需要**新增一个 `Renamed`** 吗？**结论：不需要。** `scan_layer_changes` 输出的是 (path, kind) 集合，rename 自然表现为「旧路径 Deleted + 新路径 Modified」，Libra 侧由 add 的 rename 探测（相似度）自行还原。**保持契约不动**是更好的选择。

### 5.4 契约一致性要求（双形态）

HTTP 类型与内嵌 crate 类型必须是**同一份定义**（§ADR-4）。具体：把 §5.2/§5.3 的类型移入 `scorpiofs::worktree_contract`（**不依赖 `antares` / `fuse` / `libfuse-fs` 任何模块**），HTTP handler 与内嵌 API 都引用它。

**验收方式**：写一个契约等价性测试——同一组输入经 HTTP 与内嵌两条路径序列化出的 JSON **逐字节相等**。

### 5.5 挂载内指针的写入契约（配合 ADR-5）

`POST /mounts/{mount_id}/worktree/base`（已存在，`src/daemon/antares.rs:87`）的请求体**扩展为可选字段**：

```json
{
  "base_revision": "<resolved-commit-oid>",
  "vcs_pointer": {
    "commondir": "/host/abs/canonical/path/to/main/.libra",
    "worktree_id": "wt-…"
  }
}
```

**语义**：
- `vcs_pointer` **可选**，省略时行为与现状完全一致（**必须保持向后兼容**——Libra 未升级的客户端继续可用）。
- 提供时，ScorpioFS 在 mount 内创建 `<mount>/.libra/` 并**原样写入** `commondir`、`worktree_id` 两个文件（各自一行，行尾 `\n`，与 `create_worktree_gitdir` 一致）。
- ScorpioFS **不解释**这两个值，也**不做路径规范化**——规范化责任在 Libra（§ADR-5 的 ⚠️）。
- 幂等：重复提交**相同**内容为 no-op；提交**不同**内容返回 `409`（避免静默改写 worktree 身份）。
- 该目录被 `scan_layer_changes` 跳过（`src/daemon/antares.rs:880-884`），因此不会污染 `changes`。
- 绑定 `base_revision` 与写入指针应当**原子**：先落指针、再置 `base_revision`；任一步失败则指针回滚，`base_revision` 保持未绑定。

**为什么放在 `base` 而不是 `fork`**：`fork` 产出的新 mount 也必须绑定 base（§7.3 步骤 5），而新建 mount 与 fork 产出的 mount 都需要写指针。把指针写在 `base` 上，**两条路径共用同一个落点**，不必在 `fork` 里复制一遍逻辑。

---

## 6. ScorpioFS 侧改造

### 6.1 新增 `fork` 能力

| 改动 | 位置 | 说明 |
|---|---|---|
| **upper/CL 白障格式改为 OCI** | `src/antares/fuse.rs:51,59` | **最高优先级**，见 ADR-6 与 §6.2。改为显式 Config（不依赖 `..Default::default()`），两处都要改 |
| 新增请求/响应类型 | `src/daemon/antares.rs`（紧邻 `:558-611` 的既有 worktree 类型） | 按 §5.2 |
| 新增路由 | `src/daemon/antares.rs:72-92`（`router()`） | `POST /mounts/{mount_id}/fork` |
| service trait 新增方法 | 同文件 trait 定义处（默认实现返回 `UNSUPPORTED`） | 与 `:398-422` 的既有默认实现模式一致 |
| 真实实现 — `chain` | 同文件 `AntaresServiceImpl` 内 | 封存-接链（ADR-1）：新建空 upper → 原子改名旧 upper 为 frozen → 更新两边的链 → 失败回滚。**不是**复制 |
| 真实实现 — `materialize` | 同上 | 复制 A_up → B_up：**优先 reflink(`FICLONE`)，回退真实复制，绝不 `hard_link`**；走后 ADR-2 的一致性协议 |
| **链感知扫描** | `scan_mount_changes` `:907`、`scan_layer_changes` `:847` | 仅 `chain` 需要：接受层链，nearest-wins 合并 + 白障抵消（ADR-2 C1）。现状 `cl_dir` 已是雏形（`:913-916`） |
| **layer 顺序不变量** | `build_overlay` `src/antares/fuse.rs:44-72` | nearest-first push（ADR-2 C6）。**必须有测试钉死**，顺序写反会静默颠倒优先级 |
| 层链持久化 | `PersistedMountState` `:946-960`、`MountLayers` `:511-517` | 仅 `chain` 需要：新增 `lower_chain: Vec<String>` + schema 版本字段 |
| 引用计数 | 同上 | 仅 `chain` 需要：frozen 层可能被 N 个会话引用，最后一个引用消失才能删目录（ADR-2 C4） |
| capability | `/health` handler `:73` 对应处 | 追加 `fork.v1` / `fork-chain.v1` / `whiteout.oci.v1` |
| `base` 支持指针写入 | `bind_worktree_base`（`src/daemon/antares.rs:2236`） | 按 §5.5 扩展可选字段 `vcs_pointer`，保持向后兼容 |
| 卸载逆序 | `src/antares/fuse.rs:137`（`unmount`）及 manager 卸载路径 | 仅 `chain` 需要 |

**目标路径校验（已修正）**：v0.1.0/v0.2.0 曾说「挂载点非空检查缺失」——**那是错的**。检查是存在的，在 `mount_filesystem_with_antares_cache`（`src/server/mod.rs:81`）里：

```rust
// src/server/mod.rs:118-126
let has_entries = std::fs::read_dir(path)
    .map(|mut it| it.next().is_some())
    .unwrap_or(true);            // 不可读 = 视为非空
if has_entries {
    return Err(Error::other(format!(
        "mountpoint is not empty or is inaccessible: {}",
        path.display()
    )));
}
```

而 Antares 挂载路径**确实会走到它**：`src/antares/fuse.rs:110` 调用 `mount_filesystem_with_antares_cache(logfs, self.mountpoint, false)`。`docker-compose.yml` 的注释也印证了这一点（「ScorpioFS refuses a non-empty mountpoint」）。

**但 `fork` 仍需要自己提前校验一次**，原因是**时机**：这个检查发生在 **`mount()` 里**，而 `fork` handler 在那之前就已经建目录、复制 delta、落指针了。若目标是非空目录，fork 会先做完全部副作用再在 mount 时失败，直接违反验收项 A3。→ **在 `fork` handler 入口调用同一个校验逻辑**（可把 `src/server/mod.rs:98-126` 抽成一个可复用的 `validate_mountpoint_empty(path)`），失败即返回 `400` 且不留任何痕迹。

### 6.2 ⚠️ upper 层删除痕迹（**根因已定位，见 ADR-6**）

**结论**：这**不是**缺失的功能，而是一处**配置未覆盖**，根因与完整证据链见 **ADR-6**（v0.1.0 的"待验证"已收敛为确定结论）。摘要：

- Linux 上 `WhiteoutFormat::default()` = `CharDev`，而 ScorpioFS 的 `new_antares_passthroughfs_layer` 用 `..Default::default()` 建 upper，**没覆盖该字段**；
- `CharDev` 白障靠 `mknod` 创建，**需要 `CAP_MKNOD`**，而 ScorpioFS 部署只给 `CAP_SYS_ADMIN`；
- → 删除一个**只存在于 Dicfuse lower 的文件**会在创建白障时 `EPERM` 失败；
- 且 `src/daemon/antares.rs:807` 的 `oci_whiteout_path` 只在 `test|macos` 下编译，**这条路径在 Linux 上很可能从未被测过**。

**若不做这个修复，后果是**（两个都是静默的，所以危险）：
1. `sync` **漏掉删除** → 远端仓库的旧文件永远删不掉；
2. `fork` **把已删除的文件继承回来**。

**动作**（属 §6.1 表格的一行，此处单独强调）：

| 改动 | 位置 | 说明 |
|---|---|---|
| upper / CL 层设 `whiteout_format = OciWhiteout` | `src/antares/fuse.rs:51,59` 两处 `new_antares_passthroughfs_layer` 调用点 | 改为传入显式 Config 而非依赖 `..Default::default()`。**不要**只改 upper 而漏掉 CL |

**为什么这不是"规范落地缺口"**：MST2 spec 12 §3 已把 whiteout 写成 lookup 的**规范语义**——

> lookup先查 **upper/whiteout**，再查lower固定元数据；lower negative仅表示当前snapshot该目录下没有此name，**不能屏蔽upper新文件**。

所以「upper 里必须有可被 lookup 观测到的白障」是既有规范。ADR-6 的改动是**让实现符合该规范**。读取侧本身是完备的（`classify_layer_entry`，`src/daemon/antares.rs:818-843`，无条件识别 `.wh.` 与字符设备两种编码），缺的只是**写入侧选错了格式**。

**开工第一步必须先实测确认**（即使证据链已很硬，也要在真实环境跑一次，因为 `CAP_MKNOD` 的实际授予取决于部署方式）：

```bash
# 手动挂一个 antares mount，删掉一个只存在于 lower 的文件，然后看 upper
ls -la <upper_dir>/<被删文件所在目录>
# 期望（修复前）：什么都没看到，且 unlink 报 EPERM  -> 确认根因
# 期望（修复后）：看到 .wh.<name> 空文件
```

### 6.3 移除 git 残留

| 项 | 位置 | 动作 |
|---|---|---|
| `git_author` / `git_email` | `src/util/config.rs:21-22`（字段）、`:150-151`（默认）、`:381-382`（解析）、`:574-575`（校验）、`:760-793`（收集）、`:984-985`、`:1196-1202`（访问器）、`:1325-1326`、`:1356-1357`（测试） | **删除**。理由：运行时零读取点（§1.1）。若为兼容保留，必须在字段上加注释标明「已废弃，无运行时语义」，避免下一个人以为它影响 commit 身份 |
| 配置模板里的 `git_author`/`git_email` | `src/cli.rs:409-410`、`scorpio.toml`、`scorpio.toml.example` | 同步删除 |
| 构建 Commit 对象 | `src/manager/fetch.rs:834-850` | **保留但重新归类**：它的唯一用途是把 `parent_commit` 落成 `work_path/commit` 文件（`:754-756`）作为**溯源信息**，不是 VCS 权威对象。建议在 `:754` 加注释写明「溯源 mark，非可提交对象；权威对象归 Libra」，并考虑改名 `commit_ref` 以免误读 |
| 硬编码 `Signature` | `src/manager/fetch.rs:834-850` 中 `String::new()` 作 email | 已知问题（email 恒为空）。既然不再是 VCS 权威路径，**记录为已知限制**即可，不必修 |
| sled 对象缓存 | `src/manager/store.rs` 整体 | **保留**，但注释标明「投影缓存，可丢弃，非权威对象库」，与 ADR-5 一致 |

### 6.4 CLI

`fork` **不新增 CLI 子命令**。理由：`fork` 必须知道源 mount 的 `mount_id`/`job_id`，这是**会话**概念，属于调用方（Libra）。ScorpioFS 只暴露 HTTP；本机调试用 `curl` 或 `scorpio list` 取 id 后用 HTTP 调用。

`sync` **不在 ScorpioFS**（ADR-3），ScorpioFS 侧无 CLI 改动。

→ **ScorpioFS 的 CLI 表面零变更**（`src/main.rs:44-89` 不变）。

---

## 7. Libra 侧改造

### 7.1 backend 选择

**冲突点**：Libra **已有** `worktree-fuse` ——一个基于 `libfuse_fs::overlayfs` 的内置 overlay worktree（`src/command/worktree-fuse.rs:16,574`）。ScorpioFS 的 Antares 也是 overlay。

**结论**：ScorpioFS **不是**替代 `worktree-fuse`，而是它的**另一个 backend 实现**——差别在于 lower 来自 **Mega/Dicfuse（按需拉取、共享、部分克隆）** 而不是本地对象库。这恰好是 ScorpioFS 存在的理由（`README.md:33-45` 的 partial clone / prefetch / sparse checkout）。

**做法（当前实现）**：`worktree add` 新增 `--backend scorpiofs`，与默认 backend 兼容。当前连接参数使用边界 env（避免把 daemon 地址硬编码到 repo config）：

```text
LIBRA_SCORPIOFS_ENDPOINT=http://127.0.0.1:2725/antares
LIBRA_SCORPIOFS_REPO_PATH=/project
```

`worktree.backend`/`worktree.scorpiofs.*` 的 SQLite 配置键仍可作为后续配置层（当前实现先保证命令和 HTTP 契约可运行）。**不要**复用 `LIBRA_STORAGE_TYPE`：那套抽象的是对象存储（`src/utils/storage/mod.rs:19`），不是工作区来源。

### 7.2 `worktree add --backend` 与 `--fuse` 的关系

`worktree add` 现有 `-f/--fuse` 标志（`src/command/worktree-fuse.rs:68`）。当前实现保留 `--fuse` 原行为，并新增 `--backend scorpiofs`。ScorpioFS backend 的顺序是：

1. Libra 先执行现有 worktree 注册、HEAD scope 和 private index seed，但**跳过 working-tree restore**；
2. 将 host-side `.libra` 暂存，保证目标目录为空；
3. `POST /antares/mounts`，同时传 `mountpoint=<目标路径>`；
4. 轮询 `GET …/ready`；
5. `POST …/worktree/base`，写入 `base_revision` 与 `vcs_pointer`；
6. 将 private index 通过挂载写回 `.libra/index`，删除 host-side staging。

这保证 Libra 的注册表/SQLite scope 与 ScorpioFS 的实际 POSIX 工作区保持同一路径，而不需要 host bind mount 或先物化整棵树。

> ### ⚠️ 关键约束：Libra 需要「**不物化目录就登记 worktree**」的能力
>
> 这是实施阶段实测出来的、原稿**没有预见到**的一点，也是 Libra 侧改动的主要难点。
>
> **两者的前置条件互相冲突**：
>
> | | 对目标目录的要求 |
> |---|---|
> | Libra `worktree add` | 会**写入**该目录（checkout），且拒绝非空目录 |
> | ScorpioFS `mount` | 要求目录**必须为空**（`src/server/mod.rs` 的 `prepare_mountpoint`），然后由挂载**填充**它 |
>
> 所以**不能**用「先 `worktree add` 再挂载」的顺序，也不能用「先挂载再 `worktree add`」（目录已非空，libra 会拒）。
>
> **必须的顺序**是：
> 1. Libra **登记** linked worktree：写 `worktrees.json` 条目、准备 host 侧 index/HEAD 行，**但不 checkout 任何文件**（目标目录保持为空）；
> 2. Libra 请求 ScorpioFS 在该目录 `POST /mounts` → 等 `ready` → `POST .../worktree/base`（带上 §5.5 的 `vcs_pointer`，`commondir` 指向主仓库 storage、`worktree_id` 用第 1 步登记的 id）；
> 3. 此后挂载内的目录树就是 libra 的工作树，`libra` 进程在挂载内能通过 `.libra/commondir` + `worktree_id` 完成发现。
>
> 即：**Libra 侧需要把 `add_worktree` 的「登记」与「物化」拆开**，并给 `--backend scorpiofs` 提供只做「登记」的路径。这是本方案在 Libra 侧最核心、也最容易低估的一处改动。
>
> **顺带一个已实测的坑**：upper 层只在 **FUSE 会话启动时**被 `import()`。所以任何"挂载之后再往 upper 里塞文件"的做法都**不可见**（`fork` 的实现就踩到了，见 §5.5 上方的进度表条目）。Libra 侧任何"挂载后补文件"的设想都不可行，必须保证挂载前目录就已是期望状态。

### 7.3 `libra fork` 与 `libra sync`

**已实现：`libra fork <path>`**。Libra 从当前 worktree 的 `.libra/scorpiofs_mount_id` 找到源 mount；先登记子 linked worktree（不向 host 目录 restore），调用 ScorpioFS materialize fork 到目标 mountpoint，继承 base revision，绑定新的 VCS pointer，再把 private index 和新 mount id 写进子挂载。真实 E2E 验证了子继承父修改/删除、父后续写入不影响子。

**已实现：`libra sync [--message <msg>]`**。依次执行 `add -A`、commit、push；要求 attached branch；默认推当前仓库的 `origin` 到 `refs/heads/main`（Mega2 trunk 约束）。正式 backend E2E 验证删除和修改都进入 commit 并推回 Mega2。

**仍未实现：`chain` 模式**。ScorpioFS `/fork` 当前将 chain 请求降级为 materialize，并通过响应报告实际 mode。封存层、引用计数、链感知扫描仍是后续优化。

**这里的 `sync` 与 ScorpioFS `src/snapshot/` 的 "sync"（`examples/mst2_sync.rs`，快照注水）无关**，命名重叠只在搜索时需要区分。

### 7.4 兼容性矩阵与文档

- `COMPATIBILITY.md`：`fork` / `sync` 都是 Libra-only workflow extensions，已登记为 `intentionally-different`；`fork` 当前只支持 ScorpioFS materialize backend
- 每个已实现新命令必须有对应 examples/help/docs 守卫；`sync` 已有 `SYNC_EXAMPLES` 与 `docs/commands/sync.md`

---

## 8. 待确认问题

| ID | 问题 | 影响 | 建议 |
|---|---|---|---|
| **Q1** | fork 默认模式取 `chain` 还是 `materialize`？ | 当前已实现并验证 `materialize`；`chain` 仍是后续优化，需要 frozen-layer schema/引用计数/链感知扫描 | **当前默认 `materialize`**（父可继续写、层链恒定、正确性已验证）；`chain` 后续再做，不能在未实现时对外宣称支持 |
| **Q2** | 是否接受 OCI 白障带来的限制：**用户不能在挂载内创建任何 `.wh.*` 名字的文件**（ADR-6，`is_user_creatable_name`）？ | 若有人仓库里真有名为 `.wh.*` 的文件，在 OCI 格式下无法创建；这是把白障从字符设备换成普通文件后**必然**付的代价 | 建议**接受**（罕见），并在用户文档写明。若不可接受 → 只能回到 `CharDev` + 给 unit 加 `CAP_MKNOD`，代价是更多特权 + `fork` 也要 `mknod` |
| **Q3** | ~~Linux 上 upper 删除白障会在真实部署里成功吗？~~ **已验证** | 真实 WSL FUSE + 仅 `CAP_SYS_ADMIN` 场景下，OCI whiteout unlink 成功，upper 是普通 `.wh.*` 文件，`GET /worktree` 正确报告 deleted | ADR-6 已关闭该问题；后续部署只需保持 `whiteout.oci.v1` capability |
| **Q4** | `sync` 成功后，upper 里已提交的路径如何处置？ | `worktree-state-transitions.md:85-89` 要求「原子切换到新 lower 并只移除已提交的 upper 条目」，但该切换**尚未实现**（N1）。在切换缺失时，upper 会永久累积已提交文件，`scan` 会持续把它们报为 Modified | 短期：**不清 upper**，把 `base_revision` 推进到新 commit，靠 `generation` 去重；并在文档里明确「已知：已提交路径仍留在 upper」。长期：实现 lower 切换 |
| **Q5** | ~~是否强制 `core.objectformat=sha256`？~~ **已定：默认开启，允许关闭**（见 ADR-5 末段） | 内容完整性校验能力 vs Libra git 兼容性降级 | 默认 sha256（`core.objectformat=sha256`），可用 `libra config set core.objectformat sha1` 显式关闭。**遗留子问题**：从既有 sha1 仓库切到 sha256 的升级路径是否存在？若不存在，「默认开」需收窄为「新仓库默认开、旧仓库保持原样」 |
| **Q6** | 内嵌 crate 路径要现在做吗？ | 测试矩阵翻倍，但需先做 §5.4 的「不依赖 FUSE 的契约子模块」重构 | 建议**分两阶段**：先只做 HTTP，把契约子模块建好；内嵌留到需要离线/降耗时场景时再补 |
| **Q7** | ~~`docs/worktree-api.md` 写 `/antares/mounts`，代码是 `/mounts`。哪个对？~~ **已定**（v0.3.0 修正）：**文档是对的**，有效前缀就是 `/antares/mounts` | 我先前只读了 `AntaresDaemon::router()`（其中路由写成根相对的 `/mounts`），漏看了 `src/daemon/mod.rs:226` 的 `app.nest("/antares", antares_router)`。`scorpio http-mount` 的默认 endpoint 也印证了这一点 | **不需要修文档**，也不需要改代码。Libra 侧集成必须用 `/antares/mounts`（这一点很关键：接错前缀会 404） |
| **Q8** | ~~`fork` 的目标路径为空目录校验放在哪？~~ **已定**（v0.3.0 修正）：校验**已存在**于 `mount_filesystem_with_antares_cache`（`src/server/mod.rs:118-126`），Antares 路径也走到它（`src/antares/fuse.rs:110`） | 原判断有误。真正的缺口只是**时机**：该检查在 `mount()` 时才跑，而 fork handler 在那之前已产生副作用 | 把 `src/server/mod.rs:98-126` 抽成 `validate_mountpoint_empty(path)`，在 **fork handler 入口**调用一次（早失败、零副作用），`mount()` 继续沿用它。不影响既有 `mount` 语义 |
| **Q9** | `.libra/base_revision` 要不要落成文件？ | 权威已在 mount 绑定上（`BindWorktreeBaseRequest`，`src/daemon/antares.rs:558`，`worktree-state-transitions.md:24` 不变量 1）。再落一份文件 = **双真相**，二者不一致时无法判断谁对 | **建议不落**。只保留 `commondir` + `worktree_id`（与 `create_worktree_gitdir` 完全一致），需要 `base_revision` 时问 daemon。若确实需要给「不挂载时也能读」的场景留一份，则必须写明它是**缓存**且挂载时可被覆盖 |
| **Q10** | ~~`fork` 的目标路径若已被 Libra 注册为另一个 worktree，如何处理？~~ **已通过实现约束解决** | Libra 先登记新 target，ScorpioFS 使用同一个 target 作为 mountpoint；目标已有 registry entry 时 `worktree add` 的现有 duplicate/reattach 保护先失败，避免 daemon 与 Libra 两层状态分叉 | 当前实现保留 fail-closed；失败后的残留修复沿用 `worktree repair` |
| **Q11** | `chain` 模式下，层链深度超上限时是否允许**自动降级**为 `materialize`（而不是报错）？ | 自动降级对用户友好、不中断 `fork`，但会让「fork 的代价」不可预测 | 建议**允许自动降级但必须回报**：`/fork` 响应用 `mode_downgraded_from` 说明（§5.2），Libra 必须把它透传给用户。若你希望代价可预测优先，则改为**报错**并要求调用方显式传 `mode=materialize` |

---

## 9. 验收标准

### 9.1 ScorpioFS

- **A1** — `POST /mounts/{id}/fork` 对干净 mount 成功，返回 `201`；新 mount 的 `/worktree` 显示 `dirty=false`
- **A2** — fork 后写 B 的文件，`GET /mounts/{B}/worktree` **只**返回 B 的 delta（不含 A 的变更，不含 fork 前的基线）
- **A3** — 对非空目标路径返回 `400`，且**不产生**任何挂载或目录副作用
- **A4** — `fork` 耗时：`chain` 模式为 O(1) 且有明确上界并记录；`materialize` 模式对 ≥10k 已变更文件有明确上界并记录（并报告 `copy_stats.reflink_used`）
- **A5** — **白障格式与删除可见性**（ADR-6 的核心回归）：(a) 修复后上层白障是 `.wh.<name>` **普通文件**，`upper_dir` 里**不出现字符设备**；(b) 删除一个**只存在于 lower** 的文件后，`GET /worktree` 的 `changes` **必须**含 `kind=deleted`；(c) 全过程**不需要 `CAP_MKNOD`**（在只给 `CAP_SYS_ADMIN` 的进程里跑通）；(d) `/health` 报告 `whiteout.oci.v1`。**这是本 SPEC 的核心回归测试**
- **A6** — `git_author`/`git_email` 删除后 `cargo test` 全绿；配置文件含旧键时**不报错**（向后兼容，只是忽略）
- **A7** — 契约等价性测试：HTTP 与内嵌两条路径对同一输入的 JSON 逐字节相等（若做 Q6）
- **A8** — `cargo clippy --all-targets --all-features -- -D warnings` 与 `cargo fmt --check` 通过
- **A9** — 指针写入契约（§5.5）：省略 `vcs_pointer` 时行为与升级前**逐字节一致**（向后兼容回归）；提供时两个文件内容与 `create_worktree_gitdir`（`src/command/worktree.rs:2157-2170`）产出的完全一致；重复相同内容为 no-op，不同内容返回 `409`；且这些文件**不出现**在 `GET /worktree` 的 `changes` 里
- **A10** — **I2 不变量回归（最高价值的 fork 测试）**：`fork A→B` 成功**之后**，在 A 里修改一个**两侧都存在的文件**，然后读 B 的该文件 —— **必须仍是 fork 时刻的内容**，B 的 `/worktree` **不得**出现该路径。这条测试直接钉死「硬链接禁令」（ADR-1）与封存-接链的正确性；用 `materialize` 模式再跑一遍
- **A11** — **layer 顺序不变量**（ADR-2 C6）：构造 `chain` 后 A_frozen 与 dicfuse 都有同名文件，验证**更近的层胜出**；再把链顺序反转，验证测试**失败**（证明测试确实能抓到顺序错误）
- **A12** — **一致性协议**（ADR-2）：在 `materialize` 复制期间持续写父层的目标文件，验证要么复制结果与某一时刻的完整内容一致，要么 `fork` 以 `409 source_busy` 失败；**不得**留下部分复制的子会话

### 9.2 Libra

- **B1** — `libra worktree add --backend scorpiofs <path>` 完成 attach（mount → ready → bind base），且 `--fuse` 与 `--backend` 同时给出时报错
- **B2** — `libra fork <path>` 派生成功，注册表与私有 `.libra/`（含 `commondir`）齐备，`base_revision` 与源一致
- **B3** — `libra sync` 完成 add+commit+push；重复执行第二次报「无变更」（靠 `generation` 幂等）
- **B4** — 在**不支持** `whiteout.oci.v1` 的 daemon 上，`libra sync` **拒绝执行**并给出可执行提示（不得静默成功）
- **B5** — `libra sync` 能正确删除远端文件（依赖 A5）
- **B6** — `COMPATIBILITY.md`、`docs/commands/fork.md`、`docs/commands/sync.md` 齐备，三个 help/examples 守卫通过
- **B7** — 质量三件套（`cargo +nightly fmt --all --check`、`cargo clippy --all-targets --all-features -- -D warnings`、`source .env.test && cargo test --all`）全过
- **B8** — `libra fork` 把 `mode_downgraded_from` **透传给用户**（发生自动降级时必须可见，不得静默）

---

## 10. 分阶段实施

| 阶段 | 内容 | 退出条件 | 风险 |
|---|---|---|---|
| **P0 白障实测 + 修复** | 实测 Q3（§6.2 命令）；按 **ADR-6** 把 upper/CL 的 `whiteout_format` 设为 `OciWhiteout`（`src/antares/fuse.rs:51,59`）；`/health` 增 `whiteout.oci.v1` | A5 全绿；Q2 拍板 | **阻塞后续全部**。但根因已定位（ADR-6），不再是"待排查"，而是"待确认+改配置" |
| **P1 ScorpioFS 契约** | §5.4 抽 `worktree_contract` 子模块；修 `docs/worktree-api.md` 路由前缀（Q7） | A7（若做）+ A8 | 低 |
| **P2 ScorpioFS fork** | §6.1 端点 + 空目录校验（Q8）；按 Q1 实现 mode（建议先只做 `materialize`——它不需要链感知扫描/schema 升级/引用计数） | A1–A4、A12 | 中 |
| **P3 ScorpioFS `chain`（可选）** | ADR-2 的 C1–C6：链感知扫描、封存回滚、深度上限、引用计数、`PersistedMountState` schema 升级 | A4（chain）、A10、A11 | **高**（唯一需要动持久化 schema 与扫描语义的一段） |
| **P4 ScorpioFS 清理** | §6.3 删死配置、加注释 | A6 + A8 | 低 |
| **P5 Libra backend** | §7.1 配置键 + §7.2 `--backend scorpiofs` attach | B1 + B7 | 中 |
| **P6 Libra 命令** | §7.3 `fork` / `sync` + help/docs/COMPATIBILITY | B2–B4、B6–B8 | 中 |
| **P7 端到端** | 闭环冒烟：add → 改 → fork → 改 → sync → 校验远端 | B5 + 全部 A/B | 中 |

**建议的最小可验证切片**：**P0 → P5 → P6 的 `sync`**。即「先只做 sync（不含 fork）、且先只做 HTTP 路径」。理由是 `sync` 是价值最高也最容易出**静默错误**（漏删）的一段，先把它的正确性和 capability 防线立起来。

**建议的 fork 推进顺序**：先只做 `materialize`（P2），把 I2/I3 两条不变量和全部错误路径测厚；`chain`（P3）作为**纯优化**后置——它唯一多的收益是 O(1) 与省复制，但代价是 DAG、扫描语义变更、持久化 schema 升级与引用计数。**如果时间紧，`chain` 可以完全不做**，`materialize` + reflink 在多数场景已接近 O(1)。

---

## 11. 与既有文档的关系

| 文档 | 关系 |
|---|---|
| `docs/worktree-api.md` | **本文的上游契约**。本文 §5 是其扩展；其第 121-129 行「lower 未实现切换」是本文 **N1**，并在 **Q4** 给出降级约定。其路由前缀 `/antares/mounts` **是正确的**（Q7 已修正，不需要改它）；其「Ownership contract」中「挂载内只可有可重建 `.libra` 指针」一条被本文 ADR-5 具体化为 `commondir` + `worktree_id` 两个文件 |
| `docs/worktree-state-transitions.md` | 不变量 1–6 与 §3 的所有权划分**被本文完整继承**。ADR-6（白障格式）是不变量 4（dirty 阻塞切换）与 `sync` 正确性的**共同前提**：没有可观测的白障，删除既进不了 `changes`，也无法阻塞切换。Q4 是其第 85-89 行「commit 后切换」在切换未实现时的降级约定 |
| `docs/libra_worktree_task.md` | 实习任务书。其 §3 伪代码「挂载后在该 mount 内 checkout 分支」与本文 ADR-5「挂载内不得持久化 `.libra` 状态」**冲突**——`checkout_branch_at(&path, branch)` 会往挂载里写 refs。**建议修订该任务书**，改为「Libra 解析 revision 后调用 `/worktree/base` 绑定」。另：该任务书只规划 `add/list/remove/lock`，**未涉及 `fork`/`sync`**，本文可视为其后续 |
| `docs/plan/plan-20260831.md` | 打包计划，非目标重合（其「非目标」明确「不改功能代码」）。本文的产出会成为其输入：(a) 新增 capability 需同步进版本/产物基线；(b) **ADR-6 的去特权方向与该计划的 `AmbientCapabilities=CAP_SYS_ADMIN` 权限模型一致**——它明确反对扩大特权面 |
| `docs/improvement/configuration.md` | §6.3 删除 `git_author`/`git_email` 属于其「配置类型化/文档对齐」范畴，应同步更新该文档 |
| `Mega_ScorpioFS_MST2_Specs_0.2.1` spec 12（`12-scorpio-fuse-and-workspace-interfaces.md`） | **规范性接口文档**。已核实：其 §3 把 **upper/whiteout 写进 lookup 规范语义**（`lookup先查upper/whiteout，再查lower`），这是 **ADR-6 / §6.2 的规范依据**——白障可观测是规范要求，而当前 Linux 默认配置不满足它。其 §2 的 mount `generation` 概念与本文 §5.3 的 `generation` 幂等一致。**但该 spec 全文未提及 fork / worktree 集成**（grep `fork`、`worktree` 零命中），因此**不构成**对本文 §5 / ADR-1 / ADR-2 的上位约束，也无冲突。若后续版本新增 fork 语义，应以 spec 12 为准并回来修订本文 |
