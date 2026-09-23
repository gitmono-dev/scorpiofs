# ScorpioFS Tree-Pack 同步（commit+tree 一次下载）SPEC

- **版本**：v1.0.0（设计稿，未实现）
- **日期**：2026-09-22
- **状态**：待评审。本文只做设计与实测论证；实现分两步（§5 mega2 侧、§6 ScorpioFS 侧）。
- **关联**：`docs/scorpiofs-libra-complete-spec-v1.md`（Worktree v2 协议）、`docs/scorpiofs-libra-protocol-v2.md`

---

## 1. 背景与实测

ScorpioFS 当前的 lower 投影是**逐目录惰性拉取**：`Dicfuse` 按需对每个目录调用
`GET /api/v1/tree`（`fetch_dir`），一次 HTTP 往返换取一个目录的条目。挂载/refresh
之后第一次全树浏览要付出 N 次往返（N = 目录数）。

实测（本机 WSL2 + docker mega2，monorepo `/project` = mega + rk8s 源码；
2783 个 tree / ~5140 个 blob / 5177 个文件 / 2743 个目录）：

| 场景 | 耗时 | 传输 |
|---|---|---|
| `git clone`（全对象，一次 pack） | **4.9s** | 8.7 MB pack |
| `git clone --filter=blob:none`（mega2 忽略 filter，退化为全量） | 2.8s | 8.6 MB pack，**5126 个 blob 仍被下发** |
| ScorpioFS 冷全树 metadata 遍历（2743 目录逐个 fetch_dir） | **22.2s** | 2743 次 HTTP，每次仅一个目录 |
| ScorpioFS 热遍历（store 已加载） | 2.5s | 0 |
| ScorpioFS 单目录 fetch 延迟 | ~8ms | 本地 docker；广域部署会显著放大 |

结论：
1. **一次 pack 传输完胜逐目录往返**——git 全量（含全部 blob）都比 ScorpioFS 的纯
   metadata 惰性遍历快 4.5 倍。
2. **逐目录方案的成本随目录数线性增长**，且对网络延迟敏感（本机 8ms，远程 50ms+
   时 22s 会变成分钟级）。
3. mega2 目前**不支持** git partial clone 的 object filter（实测 `--filter=blob:none`
   被忽略、仍下发全部 blob；协议调查见 §5.1）。

## 2. 目标 / 非目标

**目标**
- G1：挂载或 refresh 后，**全树的 commit+tree 元数据通过一次（或极少次）pack 传输
  到位**；之后任意路径的 readdir/lookup 零网络。
- G2：**异步**——pack 下载与导入不阻塞挂载 Ready，也不阻塞 commit-finalize；
  导入完成前既有惰性 `fetch_dir` 路径照常兜底，两层并存、去重。
- G3：blob 继续**按需**逐个拉取（`fetch_file_content` 语义不变）；pack 里不含 blob。
- G4：mega2 侧实现 **git 标准的 partial clone filter**（`blob:none` / `blob:limit`），
  让标准 git 客户端同样受益，而非私有端点。
- G5：优雅降级——对端 mega2 不支持 filter 时，ScorpioFS 行为退化为现状（惰性 +
  可选的低深度预热），不报错。

**非目标**
- 不做 blob 的批量预取/缓存预热（blob:limit 可作为后续增强）。
- 不改变 Worktree v2 的挂载/finalize/refresh 语义与 generation 乐观锁。
- 不改变 upper/CL/frozen 层次与白障语义（pack 只影响 lower 的加载速度）。

## 3. git partial clone 原理（协议对照）

1. **能力广告**：upload-pack 在 capability 列表中广告 `filter`；客户端未见 `filter`
   即退化为全量 clone（实测正是如此：blobless clone 拿到 5126 个 blob）。
2. **请求**：`git clone --filter=blob:none` 的客户端在 want 行后发送 `filter blob:none`。
3. **服务端 pack 生成**：遍历 want 对象图时按 filter 排除对象——`blob:none` 时
   traverse 到 blob 直接跳过（tree 条目引用 blob OID 但不递归、不下发）。
4. **filter 语法族**：`blob:none`（只要 commit/tag/tree）、`blob:limit=<n>`（小 blob
   照发）、`tree:<depth>`（连 tree 都省，tree:0 只发 commit）、`object:type=<t>`、
   `combine:<f1>+<f2>`。
5. **客户端 promisor 机制**：clone 产物为 partial repo（`extensions.partialclone`），
   checkout/log/diff 缺失 blob 时向 promisor remote 发批量 fetch 并缓存；对上层透明。

ScorpioFS 的数据模型（tree 全量、blob 按需）与 `blob:none` 语义天然对齐。

## 4. 总体方案

```
attach / refresh
   │
   ├─(同步，现状不变)── wait_for_ready(root) → FUSE Ready
   │                                    │
   │                            用户请求 → 惰性 fetch_dir 兜底（现状）
   │
   └─(异步，新增)──── tree-pack sync worker
                          │  ① GET pack（commit+tree only）
                          │  ② 解析 pack → 对象
                          │  ③ 构建 per-revision store 的路径↔inode↔条目
                          │  ④ 完成标记（store 元数据），后续 readdir 零网络
                          ▼
                    blob 仍按需 fetch_file_content
```

- pack 导入**不改变** store 的对外语义：导入前后 `fetch_dir` / `get_inode` 返回
  同样的内容；导入只是把"将来要惰性拉的目录"提前批量填好。
- 并发安全：导入走 store 现有的 `dir_locks` / `ensure_dir_loaded` 通道（与惰性
  fetch 同一把锁），或以"先到先得、后到跳过"的条目合并规则写入（§6.3）。

## 5. mega2 侧：实现 git 标准 object filter

### 5.1 现状（协议调查结论）

- Smart HTTP 入口：`mono/src/git_protocol/http.rs`（`git_info_refs` / `git_upload_pack`
  / `git_receive_pack`），路由分发 `mono/src/server/http_server.rs:445`。
- 能力广告仅 `multi_ack_detailed no-done include-tag side-band-64k ofs-delta
  agent=mega/0.1.0`（`ceres/src/transport/protocol/smart.rs:34-41, 79-83`）。
- upload-pack 命令循环只处理 want/have/done（`smart.rs:120-135`）。
- **object filter：不存在**——Capability 枚举无 `filter`（`ceres/src/transport/
  protocol/mod.rs:86-117`），不广告、不解析。
- **shallow clone：不存在**（`full_pack` 遍历全部父提交，`pack/monorepo.rs:236,
  262-279`）。
- pack 生成：`ceres/src/transport/pack/`——trait `RepoHandler`（`mod.rs`，共享递归
  `traverse`:332）、`monorepo.rs::full_pack:236 / incremental_pack:240`；编码用
  git-internal 0.8.6 的 `PackEncoder::encode_async`（`mod.rs:320`）。
- 树快照/批量 tree 导出 API：不存在（仅逐级 `GET /api/v1/tree*` 与原始单 tree 的
  `/api/v1/file/tree`）。

### 5.2 改动清单（标准 filter，非私有端点）

| # | 改动 | 位置 |
|---|---|---|
| M1 | Capability 枚举增加 `Filter`；info/refs 广告 `filter` | `ceres/src/transport/protocol/mod.rs:86-117`、`smart.rs:34-41,79-83` |
| M2 | upload-pack 命令循环解析 `filter <spec>` 行（session 状态携带 filter） | `smart.rs:120-135` |
| M3 | `full_pack` / `traverse` 接受 filter：`blob:none` 跳过 blob（不递归、不编码）；`blob:limit=<n>` 按 size 判断 | `ceres/src/transport/pack/monorepo.rs:236`、`mod.rs:332` |
| M4 | （可选，本期不做）shallow/deepen | — |

兼容性约束：
- 仅当客户端**显式发送** filter 才启用；不带 filter 的请求行为完全不变。
- `blob:none` 的 pack 不含 blob → git 客户端标 promisor，checkout 时按需回取
  （走 mega2 现有 blob 端点，标准协议）。
- LFS：>1MB 文件在 mega2 中本就以 LFS 指针形式存在，tree 条目引用的 blob 即指针
  内容——filter 不影响指针下发（它也是 blob，只是小）。**ScorpioFS 不消费这些
  指针 blob**（仍走按需），故语义无冲突。

### 5.3 验收

- `git clone --filter=blob:none $M2/project` 后 `git cat-file --batch-all-objects |
  grep -c blob` 为 **0**（或 `blob:limit` 场景下符合 limit）。
- pack 大小：全树 tree-only pack ≤ ~2MB（对照全量 8.7MB）。
- 全量 clone（无 filter）回归不变。

## 6. ScorpioFS 侧：异步 tree-pack 导入器

### 6.1 触发点

- `create_mount` / `attach_worktree`：Ready 返回**之后**后台启动。
- `commit_finalize` Phase B 与 `refresh_lower`：remount 完成后，对新 pinned store
  后台启动（同一 store 最多一个在跑，幂等去重）。
- 手动触发（运维/测试）：`POST /antares/worktrees/{id}/sync-tree-pack`（可选，
  返回当前导入状态）。

### 6.2 下载与解析

- 下载：**复用 git 标准 Smart HTTP**——ScorpioFS 内嵌一个最小 upload-pack 客户端
  （want `<trunk-tip 的 mega 内部 commit>` + `filter blob:none` + done），或直接
  起子进程 `git fetch --filter=blob:none`（二选一，见 §7 决策点 D1）。
- 解析：git-internal 已在依赖树内（`PackDecoder` / 对象遍历）。
- 导入目标：per-revision `DictionaryStore`（`-rev-` 目录）。tree 对象按
  「tree-hash → 目录条目」重建：与 `fetch_dir` 相同的 StorageItem 结构
  （inode/parent/name/children/hash），路径↔inode 映射沿用 radix_trie + 持久层。

### 6.3 并发与一致性

- 导入以 store 为单位持锁（`global_import_semaphore` 限并发，8 worker 解析）。
- **先到先得合并**：某目录已被惰性 `fetch_dir` 加载 → 跳过（不覆盖 inode 分配，
  避免已打开 fd 的 inode 漂移）；未加载 → 批量填入。
- tree 之外的对象（commit/tag）仅用于确定根与父子关系，不做 FUSE 投影。
- 失败重试：指数退避（1s/4s/16s），三次后放弃并保留惰性路径（G5 降级）。

### 6.4 状态与可观测

- `GET /antares/worktrees/{id}/state` 增加可选字段：
  `tree_pack: { state: idle|syncing|done|failed|unsupported, trees_imported, elapsed_ms }`
  （`unsupported` = 对端无 filter 能力，已降级）。
- daemon 日志：开始/完成/失败各一条 INFO/WARN，含 trees/blobs 计数。

### 6.5 与既有修复的关系

- 本方案**不回退**"pinned store 跳过深预热"的修复：该修复禁用的是同步整树
  `load_dir_depth`（会阻塞 Ready 且风暴式压垮远端）；tree-pack 是**异步单请求**
  传输，远端压力为一次顺序读，二者不冲突。
- 保留现有惰性 fetch 与 `fetch_file_size` 重试：pack 导入完成前的兜底不变。

## 7. 决策点（评审时定）

| # | 问题 | 候选 | 倾向 |
|---|---|---|---|
| D1 | ScorpioFS 侧 pack 客户端形态 | (a) 内嵌最小 upload-pack 客户端；(b) 子进程 `git fetch --filter=blob:none` + 解析本地对象 | (a)：避免外部 git 依赖，协议面窄（want+filter+done） |
| D2 | filter 能力探测 | 从 info/refs 的 capability 广告判断；无 `filter` → 降级 | 按标准协议探测 |
| D3 | pack 内容是否持久化复用 | per-revision store 已按 `-rev-` 目录持久化，导入结果随 store 复用 | 随 store 持久化，不额外存 pack |
| D4 | `blob:limit` 增强 | 后续版本 | 本期只做 `blob:none` |

## 8. 预期收益（基于 §1 实测外推）

| 指标 | 现状（惰性） | tree-pack 后 |
|---|---|---|
| 冷全树 metadata 就绪 | 22.2s（串行）/ 广域分钟级 | **≈1s**（传输 ~1-2MB + 解析），且后台异步 |
| 冷 readdir/lookup 网络往返 | 每目录 1 次 | 0（导入完成后） |
| blob 首次读 | 按需 1 次 HTTP | 不变 |
| mega2 远端负载 | 每目录 1 请求 × N 客户端 | 每客户端 1 请求 |

## 9. 实施切分

1. **P1（mega2）**：M1-M3 + 验收（§5.3）。独立可发布，标准 git 客户端即可受益。
2. **P2（ScorpioFS）**：下载/解析/导入（§6.2-6.3）+ 状态字段（§6.4）+ 降级（§2 G5）。
3. **P3（可选）**：`blob:limit`、手动触发端点、`blob` 批量预热。

每步独立可回归：P1 落地后用标准 git 验证；P2 落地前后各跑一轮
`bench-scorpio-vs-git.sh` 对比。
