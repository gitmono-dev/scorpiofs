# ScorpioFS × Libra 完整集成与传输协议 SPEC

- **版本**：v1.1.0（v2 协议已实现并通过 E2E）
- **日期**：2026-09-21
- **状态**：§6 的 Worktree Control Protocol v2（effective diff / commit-finalize / refresh）已实现并通过真实 mega2 + FUSE E2E；Libra `sync` 已接入 finalize。`.libra` 移出 mount 的 chain 模式已实现并 E2E；host-side metadata 已实现并 E2E。

### 实施进度（v1.1.0 增量）

| 项 | 状态 | 证据 |
|---|---|---|
| Mega `refs` 语义实测 | ✅ 已确认 | `tree/blob/content-hash/latest-commit` 均接受 `refs=`；**仅接受 mega 内部 monorepo commit OID**（`latest-commit` 返回值），git tip OID 不可解析（空结果）、分支名 500。content-hash OID = 标准 git blob OID（`sha1("blob <len>\0"+content)`，用 mega 自身回报值验证）。因此 lower switch 的 revision 键 = 内部 OID |
| Dicfuse revision pinning | ✅ 已实现 | `DictionaryStore.pinned_refs`（`store.rs`）→ `fetch_dir` 携带 `&refs=`（6 处调用点）；`Dicfuse::new_with_base_path_store_path_and_refs`；`DicfuseManager::for_base_path_and_refs`（缓存键含 refs，store 目录带 `-rev-<前缀>` 后缀，防跨 revision 复用持久化树库） |
| effective diff | ✅ 已实现并 E2E | `daemon/worktree_v2.rs::effective_changes`：扫描 upper（跳过 `.libra`、OCI whiteout 语义）→ 每路径计算 git blob OID 与 lower 对比 → Added/Modified/Deleted；upper==base 报 clean（消除假 dirty）；`generation_of` 路径排序后 FNV，顺序无关（单测抓出并修复过顺序敏感 bug） |
| `GET /worktrees/{id}/state` | ✅ 已实现并 E2E | 返回 `lower_revision`/`base_revision`（两个命名空间分开）/generation/dirty/changes |
| `POST /worktrees`（v2 attach） | ✅ **已实现并 E2E** | 单调用 attach：**挂载即钉定 lower**（client 传内部 OID 或留空由 daemon 解析 latest-commit），无 v1 的 tip 漂移窗口；mountpoint 可指定（空目录预校验，失败零副作用）；`base_revision` 原样记录（client 命名空间）；**daemon 不写任何 VCS metadata 进 mount**。Libra `worktree add --backend scorpiofs` 已迁移到该端点，指针文件由 Libra 经挂载自写（`commondir`/`worktree_id`/`index`/`scorpiofs_mount_id`） |
| **修复的真实缺陷：MountEntry 丢 pin** | ✅ 已修 | `create_mount` 构造 `MountEntry` 时未写入 `request.pinned_refs` → mount 用 pinned 实例但 entry 记录 None，finalize/state 会对比**错误的 lower**。E2E 的 attach 断言（lower_revision 非空）抓出 |
| **host-side `.libra` metadata（§3.2）** | ✅ **已实现并 E2E** | 实现形态：mount 内 `.libra` = **一个符号链接** → `<main>/.libra/worktrees/<id>/`（host 侧 per-worktree gitdir，含 commondir/worktree_id/index/scorpiofs_mount_id）。内核在 FUSE 之前解析 symlink → Libra 命令直接读写 host 侧状态，**discovery 代码零改动**；upper 只含这一个指针 inode。配套：Libra layout 检测（`is_legacy_symlink_worktree` + `detect_entry_layout`）学会区分「指向隔离 host gitdir 的 symlink（含 worktree_id 文件）」与「legacy 主库 symlink」——否则 mutation 门会 fail-closed 误拒 |
| **`POST /worktrees/{id}/commit-finalize`** | ✅ 已实现并 E2E | 事务：A 段（generation 乐观锁、base 校验、构建新 pinned Dicfuse、**逐路径哈希验证 committed 内容**）→ B 段（unmount → 精确删除 committed upper 条目含 whiteout + 空父目录剪枝 → remount → 换 entry）→ 失败回滚旧 lower + 幂等重试；`GENERATION_CHANGED`/`BASE_MISMATCH`/`TREE_MISMATCH`/`SWITCH_FAILED` 错误码 |
| `POST /worktrees/{id}/refresh` | ✅ 已实现 | require_clean → BlockedDirty；lower 切换到目标/最新内部 revision；状态机 Quiescing→Ready |
| 能力协商 | ✅ | `/health` 增 `worktree.state.v2`、`worktree.commit-finalize.v2`、`worktree.refresh.v2` |
| 重启恢复 | ✅ | `PersistedMountState.pinned_refs`；恢复时 pinned mount 用 pinned 实例重挂（防静默回退到移动 tip） |
| **E2E：v2 finalize 全链路** | ✅ **全部通过** | 挂载→（改/增/删三 kinds）→ state 报正确三种变更+正确 OID → git clone 复刻提交并 push → finalize → `lower pinned`、cleaned=3、upper 清空、mount 从新 lower 供给内容、dirty=false；过期 generation 正确拒绝 |
| **E2E：libra sync 接入 finalize** | ✅ **全部通过** | `worktree add --backend scorpiofs`（job_id 加随机后缀——修复确定性 job_id 幂等命中旧 mount 导致的 base 冲突）→ mount 内编辑 → `libra sync`（state→add→commit→push→finalize）→ 远端 tip 前进、dirty=false、lower 钉到新内部 revision |
| 修复的真实缺陷：确定性 job_id | ✅ 已修 | 同路径重复注册会幂等命中旧 mount（其 base/lower 与新 HEAD 不符）→ add 400。改为每次注册唯一 job_id；崩溃残留由 repair/GC 清理（spec Q10 的实现注记） |
| **`POST /worktrees`（v2 attach）** | ✅ 已实现并 E2E | 单调用 attach：**挂载即钉定 lower**（client 传内部 OID 或留空由 daemon 解析 latest-commit），无 v1 的 tip 漂移窗口；daemon 不写任何 VCS metadata 进 mount。Libra attach 已迁移至该端点 |
| **host-side `.libra` metadata（§3.2）** | ✅ **已实现并 E2E** | mount 内 `.libra` = 一个符号链接 → `<main>/.libra/worktrees/<id>/`（host 侧 gitdir）。内核在 FUSE 前解析 symlink，Libra 命令直接读写 host 侧，discovery 零改动；upper 只含这一个指针 inode。配套：Libra layout 检测识别「symlink 目标含 worktree_id = 隔离布局」，否则 mutation 门误拒 |
| **chain 模式（ADR-1 封存-接链）** | ✅ **已实现并 E2E** | `fork mode=chain`：源 upper 原子改名封存为共享只读层 + 父会话换新空 upper（**零拷贝、父可继续写**）；子 lower = [sealed] + 源链；`CreateMountRequest.sealed_chain`（内部字段）+ `AntaresFuse::with_frozen_layers` 多层 lower；链感知 effective diff（最近优先 + 白障穿透）；finalize/refresh 将链**摊平进 upper** 再切 lower（陈旧链不再遮挡新投影），摊平后链清空；delete_mount 回收无引用 sealed 层；源未钉定时安全降级 materialize 并回报。E2E：子继承父编辑/新增、双向不泄漏、父保持可写、child sync 推回远端 |
| 修复的真实缺陷：链泄漏父指针 | ✅ 已修 | sealed 层携带父 `.libra` symlink → 子经 overlay 解析到**父的** gitdir（错 index/身份）。修复：封存时摘除指针、父新 upper 重建同目标 symlink、子视图天然无指针（E2E EEXIST 抓出） |

---

## 0. 一句话结论

```text
Libra 管 VCS 状态和 Git objects/pack
ScorpioFS 管 POSIX 文件视图和 upper
Mega 管远端 refs、Git pack、revision tree/blob

每个 ScorpioFS mount 内不放 .libra
每个 linked worktree 的 .libra 放在 host-side metadata store
```

工作区同步的完成条件：

```text
commit 成功
+ push 成功
+ ScorpioFS lower 切换到新 commit
+ 已提交 upper 安全清理
= sync 成功
```

`push` 成功但 lower 没切换，不能报告 sync 成功。

---

# 1. 问题与设计原则

## 1.1 当前冲突

原始方案把 `.libra` 放入 ScorpioFS mount 内：

```text
/worktree/.libra/
/worktree/src/
```

这会冲突：

1. ScorpioFS mount 要求目标目录为空；
2. Libra worktree 初始化会先写 `.libra`、index、HEAD scope；
3. mount 之后 host-side `.libra` 会被 FUSE 覆盖；
4. `.libra` 会被误认为普通 upper 文件；
5. 按需加载 lower 不应该负责加载 Libra 的 index/objects/refs；
6. fork、remount、恢复时会出现 host metadata 与 FUSE view 双真相。

## 1.2 设计原则

1. **VCS metadata 与 POSIX projection 分离**。
2. **Git pack 只在 Libra ↔ Mega 之间传输**。
3. **ScorpioFS 不实现 Git object/index/commit/push**。
4. **每个 worktree 不复制 objects，只共享主 Libra store**。
5. **upper 是相对于 base revision 的临时覆盖层，不是永久 commit 存储**。
6. **effective diff 由 upper 与 base tree 比较产生，不能把所有 upper entry 都当成修改**。
7. **commit finalize 必须是幂等事务**。
8. **materialize 是默认 fork 模式；chain 不是 v1 必需能力**。

---

# 2. 三层架构

```text
┌────────────────────────────────────────────────────────────┐
│ Libra                                                      │
│                                                            │
│ VCS 控制面                                                 │
│ HEAD / refs / index / objects / pack / commit / push       │
│ worktree registry / private worktree scope                 │
│ worktree add / fork / sync                                 │
└───────────────┬────────────────────────────────────────────┘
                │ Worktree Control Protocol v2
                │ HTTP/JSON
                ▼
┌────────────────────────────────────────────────────────────┐
│ ScorpioFS                                                  │
│                                                            │
│ POSIX/FUSE 数据面                                           │
│ upper(rw) + optional CL + revision-addressed lower(ro)    │
│ whiteout / effective diff / lower switch / fork materialize │
└───────────────┬────────────────────────────────────────────┘
                │ Revision Projection API
                ▼
┌────────────────────────────────────────────────────────────┐
│ Mega                                                        │
│                                                            │
│ Git Smart HTTP / refs / upload-pack / receive-pack         │
│ revision tree / blob / content-hash                         │
└────────────────────────────────────────────────────────────┘
```

---

# 3. Libra metadata布局

## 3.1 主 Libra store

```text
<repo>/.libra/
├── libra.db
├── objects/
│   ├── <loose objects>
│   └── pack/
├── vault.db
├── hooks/
└── worktrees.json
```

主 store 保存：

```text
objects / pack
refs / HEAD 的 SQLite rows
config
vault
worktree registry
```

## 3.2 linked worktree metadata

```text
<repo>/.libra/worktrees/<worktree_id>/
├── commondir
├── worktree_id
└── index
```

说明：

- `commondir`：指向主 `.libra` store；
- `worktree_id`：稳定 linked worktree identity；
- `index`：当前 worktree 私有 index；
- 不放 `objects/`；
- 不放 `pack/`；
- 不放 `vault.db`；
- 不放在 ScorpioFS mount 内。

## 3.3 ScorpioFS mount 内

```text
<mountpoint>/
├── src/
├── docs/
└── ...
```

默认不出现：

```text
.libra/
.git/
objects/
pack/
refs/
```

ScorpioFS mount 只提供代码文件和 POSIX 语义。

## 3.4 当前 worktree 的发现

Libra 不再依赖：

```text
cwd/.libra
```

而是统一使用：

```rust
resolve_current_worktree() -> WorktreeContext
```

解析顺序：

```text
1. canonicalize 当前 cwd
2. 读取 main .libra/worktrees.json
3. 找到最长匹配的 worktree path
4. 得到 worktree_id
5. 打开 .libra/worktrees/<id>/index
6. 读取 SQLite 中该 worktree scope 的 HEAD/refs
7. 读取 ScorpioFS backend/mount_id metadata
```

因此，`libra status`、`add`、`commit`、`push`、`fork`、`sync` 必须全部使用同一个 `WorktreeContext` resolver。

---

# 4. Git objects 与 pack 协议

## 4.1 Pack 协议边界

```text
Libra ───── Git Smart HTTP ───── Mega
```

ScorpioFS 不参与：

```text
upload-pack
receive-pack
pack encode/decode
pack index
ref negotiation
```

## 4.2 Fetch

```text
libra fetch origin
```

流程：

```text
1. Libra 读取 remote URL
2. 请求 info/refs
3. 请求 upload-pack
4. 接收 pack stream
5. 解码 commit/tree/blob
6. 写入主 .libra/objects 或 pack
7. 更新 refs/remotes/origin/*
```

之后所有 linked worktree 共享这些对象。

## 4.3 Push

```text
1. Libra 从当前 index/HEAD 构造 tree
2. 创建 commit object
3. 计算 commit OID
4. 找到远端缺失对象
5. 生成 pack
6. 调用 receive-pack
7. 更新远端 ref
```

commit OID 由 Libra 本地确定性计算：

```text
commit = hash(
    "commit " + canonical_commit_payload_length + "\0" + payload
)
```

其中 payload 包含：

```text
tree <tree_oid>
parent <parent_oid>
author <author>
committer <committer>

<message>
```

hash 算法由：

```text
core.objectformat = sha1 | sha256
```

决定。

## 4.4 “最新 commit”不是 ScorpioFS 计算的

需要区分：

```text
remote latest commit  = Mega refs/heads/<branch>
worktree base         = ScorpioFS 当前 base_revision
new local commit      = Libra 根据 index/tree/parent 创建的 commit
```

获取远端 branch 最新 commit：

```text
libra fetch origin
读取 refs/remotes/origin/main
```

获取当前 worktree base：

```text
GET /worktrees/{id}/state
读取 base_revision
```

计算本次新 commit：

```text
ScorpioFS effective diff
        ↓
Libra add 更新 private index
        ↓
index 构造完整 tree T
        ↓
parent B + tree T + metadata
        ↓
Libra 计算 commit C
```

因此：

```text
ScorpioFS 不决定 latest commit
ScorpioFS 不生成 commit OID
Libra 不从文件 mtime 推断 commit
```

---

# 5. Mega Revision Projection API

Git pack 解决 VCS 对象传输；ScorpioFS 需要的是按 revision 读取文件树。

Mega 提供或适配以下能力：

```http
GET /api/v1/tree?path=<path>&revision=<commit>
GET /api/v1/tree/content-hash?path=<path>&revision=<commit>
GET /api/v1/blob/<blob-oid>
```

## 5.1 ScorpioFS lower 读取流程

```text
open("src/main.rs")
    ↓
ScorpioFS 根据 base_revision 查询 tree entry
    ↓
得到 blob_oid / size / mode / content_hash
    ↓
按需读取 blob
    ↓
校验 content_hash
    ↓
返回 POSIX read
```

## 5.2 Libra 读取对象的优先级

Libra 自己需要 commit/tree/blob 时：

```text
1. 本地 .libra/objects
2. 本地 pack/index
3. 本地 alternates（如果启用）
4. fetch/upload-pack
5. remote revision API（只作为受控读取适配器，不替代对象存储）
```

ScorpioFS 读取工作区文件时：

```text
1. upper
2. lower cache
3. Mega revision tree/blob API
```

两个 resolver 不混合。

---

# 6. Worktree Control Protocol v2

有效基础路径：

```text
/antares/worktrees
```

旧的 `/antares/mounts` 保留为兼容别名。

## 6.1 Capability

```http
GET /health
```

最低能力：

```json
{
  "protocol": "worktree.v2",
  "capabilities": [
    "worktree.attach.v2",
    "worktree.state.v2",
    "worktree.commit.v2",
    "worktree.refresh.v2",
    "worktree.fork.materialize.v2",
    "lower.switch.v2",
    "whiteout.oci.v1"
  ]
}
```

## 6.2 Attach

```http
POST /antares/worktrees
```

```json
{
  "worktree_id": "wt-123",
  "repo_id": "repo-a",
  "repo_path": "/project",
  "mountpoint": "/workspace/feature-a",
  "base_revision": "<commit-oid>"
}
```

ScorpioFS 必须：

1. 检查 mountpoint 不存在或为空；
2. 创建 upper；
3. 将 lower 固定到 `base_revision`；
4. mount FUSE；
5. 保存 worktree_id、repo_id、base_revision、generation；
6. 不写 `.libra` 到 mountpoint。

响应：

```json
{
  "worktree_id": "wt-123",
  "mount_id": "mount-456",
  "base_revision": "<commit-oid>",
  "generation": 1,
  "state": "ready"
}
```

## 6.3 State / effective diff

```http
GET /antares/worktrees/{worktree_id}/state
```

```json
{
  "worktree_id": "wt-123",
  "mount_id": "mount-456",
  "base_revision": "B",
  "generation": 42,
  "state": "dirty",
  "changes": [
    {
      "path": "src/main.rs",
      "kind": "modified",
      "content_hash": "sha256:new",
      "base_hash": "sha256:old"
    },
    {
      "path": "src/old.rs",
      "kind": "deleted",
      "content_hash": null,
      "base_hash": "sha256:old"
    },
    {
      "path": "src/new.rs",
      "kind": "added",
      "content_hash": "sha256:new",
      "base_hash": null
    }
  ]
}
```

规则：

```text
upper file == base content -> clean
upper file != base content  -> modified
upper file + no base        -> added
whiteout + base exists      -> deleted
whiteout + no base         -> no-op
```

`generation` 只在 effective diff 改变时递增。

## 6.4 Commit finalize

```http
POST /antares/worktrees/{worktree_id}/commit-finalize
```

```json
{
  "expected_base_revision": "B",
  "new_base_revision": "C",
  "new_tree": "T",
  "expected_generation": 42,
  "committed_paths": [
    "src/main.rs",
    "src/old.rs",
    "src/new.rs"
  ],
  "committed_hashes": {
    "src/main.rs": "sha256:new",
    "src/old.rs": null,
    "src/new.rs": "sha256:new"
  }
}
```

ScorpioFS 必须按顺序：

```text
1. 校验 base_revision
2. 校验 generation
3. 验证 new_tree / committed_hashes
4. 进入 switching
5. 切换 lower 到 C
6. 删除 committed_paths 对应的 upper/whiteout
7. 更新 base_revision=C
8. 返回 ready/clean
```

失败时：

```text
不删除 upper
不推进 base_revision
不报告 clean
进入 recovery/conflict
```

该接口必须幂等：重复提交同一个 `C` 可以继续完成上次未完成的 lower switch/upper cleanup。

## 6.5 Refresh

```http
POST /antares/worktrees/{worktree_id}/refresh
```

```json
{
  "expected_base_revision": "B",
  "target_revision": "T",
  "require_clean": true
}
```

- clean：切换 lower 到 T；
- dirty：返回 `blocked_dirty`；
- base 不匹配：返回 `base_mismatch`；
- 切换失败：保留 upper，进入 recovery。

## 6.6 Fork

```http
POST /antares/worktrees/{worktree_id}/fork
```

```json
{
  "mountpoint": "/workspace/child",
  "mode": "materialize"
}
```

处理：

1. 读取 source effective upper delta；
2. 在 child FUSE session 启动前复制 delta；
3. child 继承 source base_revision；
4. child 创建独立 upper；
5. 返回 child mount_id。

v1 只实现 `materialize`。`chain` 不作为必要协议能力。

---

# 7. Libra 命令映射

## 7.1 `worktree add --backend scorpiofs`

```text
Libra 注册 worktree scope
        ↓
不 restore host tree
        ↓
POST worktrees attach
        ↓
ScorpioFS 直接挂到目标路径
        ↓
Libra 写 host-side private index
```

mount 内没有 `.libra`。

## 7.2 `libra fork`

```text
Libra registry resolver 找到当前 worktree
        ↓
读取 mount_id
        ↓
POST worktrees/{id}/fork
        ↓
注册 child metadata
        ↓
写 child private index
```

## 7.3 `libra sync`

```text
1. resolve_current_worktree()
2. GET state/effective diff
3. libra add -A
4. libra commit -> C/tree(C)
5. libra push -> Mega
6. POST commit-finalize(C)
7. ScorpioFS lower switch + upper cleanup
```

只有第 6 步成功，sync 才返回成功。

---

# 8. Failure and recovery

## 8.1 Push 成功，finalize 失败

```text
remote/main = C
local base = B
upper 保留
state = recovery
```

允许重试：

```http
POST commit-finalize(C)
```

## 8.2 finalize 期间文件再次变化

```text
expected_generation != current_generation
```

返回：

```json
{
  "state": "conflict",
  "code": "GENERATION_CHANGED"
}
```

不得清理新 upper 内容。

## 8.3 upper 与 base 同内容

upper 可以存在，但 effective diff 必须为空：

```text
upper/a == base/a
=> no change
```

可以在下一次 successful finalize 或后台 compaction 时安全删除该冗余 upper。

---

# 9. 迁移旧协议

| v1 接口 | v2 语义 |
|---|---|
| `POST /mounts` | `POST /worktrees` attach |
| `GET /mounts/{id}/worktree` | `GET /worktrees/{id}/state` |
| `POST /worktree/base` | attach 的一部分 |
| `POST /worktree/refresh-plan` | refresh 的 preflight/执行合并 |
| `POST /fork` | materialize fork |
| 新增 | `POST /worktrees/{id}/commit-finalize` |

迁移期间：

```text
v1 client 可以继续读写
v2 client 才能获得 lower switch + upper cleanup 的完整一致性保证
```

---

# 10. 最终简化边界

```text
Git pack:
  Libra ↔ Mega

Revision tree/blob:
  ScorpioFS ↔ Mega

Worktree control:
  Libra ↔ ScorpioFS

VCS metadata:
  host-side Libra .libra

POSIX workspace:
  ScorpioFS mount，不包含 .libra
```

这套设计避免了：

- `.libra` 被 FUSE mount 隐藏；
- 按需加载把 VCS metadata 当成代码文件；
- upper 文件与 lower 相同时产生假 dirty；
- commit 后旧 upper 永久遮挡新 lower；
- ScorpioFS 重复实现 Git pack；
- push 成功但本地 worktree 仍停留在旧 revision。
