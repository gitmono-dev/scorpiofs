# ScorpioFS × Libra 简化架构与传输协议 v2

- **版本**：v2.0.0-draft
- **状态**：设计稿，作为现有 `docs/worktree-api.md`、MST/2 workspace 接口和当前实现的收敛版。
- **目标**：减少控制面接口数量，明确 Git pack 与 POSIX 文件传输的边界，并解决 upper 提交后继续遮挡 lower 的一致性问题。

---

## 1. 核心结论

### 1.1 Libra 是 VCS 权威

Libra 唯一拥有：

```text
HEAD
index
refs
objects / pack
commit / tree / blob
fetch / push
merge / rebase / conflict stages
```

### 1.2 ScorpioFS 是工作区数据面

ScorpioFS 只拥有：

```text
FUSE/POSIX mount
Dicfuse lower projection
Antares upper layer
whiteout
changed-path/effective-diff scan
lower revision switch
```

ScorpioFS 不拥有：

```text
Git objects
Git pack
refs
HEAD
index
credentials
commit/push protocol
```

### 1.3 worktree 不复制 Git objects

主 Libra store：

```text
main/.libra/
├── libra.db
├── objects/
├── pack/
├── vault.db
└── worktrees.json
```

linked/ScorpioFS worktree：

```text
worktree/.libra/
├── commondir
├── worktree_id
├── index
└── scorpiofs_mount_id
```

**每个 worktree 不保存 `objects/` 或 pack。** 所有 worktree 共享主 Libra store 的对象。

---

## 2. 三条完全分离的数据链

```text
┌─────────────────────────────────────────────────────────┐
│ Git object/pack plane                                   │
│ Libra  ───── Git Smart HTTP upload-pack/receive-pack ─▶ Mega │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ POSIX file plane                                        │
│ ScorpioFS ───── tree/blob/content-hash ───────────────▶ Mega │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│ Worktree control plane                                  │
│ Libra  ─────────── HTTP/JSON ─────────────────────────▶ ScorpioFS │
└─────────────────────────────────────────────────────────┘
```

### 2.1 Git pack 不经过 ScorpioFS

`fetch`：

```text
Libra -> upload-pack -> Mega
     -> 解码 pack
     -> 写入主 .libra/objects/pack
     -> 更新 refs
```

`push`：

```text
Libra -> 构造 commit/tree/blob
     -> 生成 pack
     -> receive-pack -> Mega
```

ScorpioFS 只从 Mega 读取指定 revision 的 tree/blob 内容，用于提供文件视图。它不理解 pack。

### 2.2 两条 Mega API 可以并存

| 用途 | 协议 |
|---|---|
| Libra 对象同步 | Git Smart HTTP / pack |
| ScorpioFS 文件投影 | `/api/v1/tree`、`/api/v1/blob`、`content-hash` |

这是有意的双协议，不是重复实现：

```text
pack = VCS 对象传输
blob/tree = 文件系统按需读取
```

---

## 3. 单一 Worktree 控制协议

有效前缀：

```text
/antares/worktrees
```

现有 `/antares/mounts` 可以保留为兼容别名，但新客户端使用 `/worktrees` 语义。

### 3.1 Capability

```http
GET /health
```

v2 服务至少声明：

```json
{
  "protocol": "worktree.v2",
  "capabilities": [
    "worktree.attach.v2",
    "worktree.state.v2",
    "worktree.commit.v2",
    "worktree.fork.materialize.v2",
    "whiteout.oci.v1",
    "lower.switch.v2"
  ]
}
```

如果缺少 `lower.switch.v2`，客户端必须拒绝把 commit 宣布为完整同步成功。

---

## 4. Worktree 生命周期

```text
Absent
  │ attach
  ▼
Ready ── local write/delete ──▶ Dirty
  │                              │
  │ commit finalize              │ commit finalize
  ▼                              ▼
Switching ───────────────────▶ Ready
  │
  └── failure ──▶ Conflict/Recovery
```

核心不变量：

1. 一个 mount 只有一个 `base_revision`。
2. `base_revision` 对应当前 lower 的真实 revision，不只是备注字段。
3. upper 中与 base 内容相同的文件不算 dirty。
4. commit finalize 成功前，upper 不删除任何内容。
5. lower 切换失败时，HEAD/index/upper 都保持不变。
6. upper 里的已提交内容不能无限期遮挡新的 lower。

---

## 5. v2 接口

### 5.1 Attach

```http
POST /antares/worktrees
```

```json
{
  "worktree_id": "wt-123",
  "repo_path": "/project",
  "mountpoint": "/workspace/feature",
  "base_revision": "<commit-oid>",
  "commondir": "/repo/main/.libra",
  "mode": "materialize"
}
```

语义：

1. `mountpoint` 必须不存在或为空目录；
2. ScorpioFS 创建 Antares mount；
3. lower 固定到 `base_revision`；
4. 写入 `.libra/commondir` 和 `.libra/worktree_id`；
5. 返回 `mount_id`、`base_revision`、`generation`。

```json
{
  "worktree_id": "wt-123",
  "mount_id": "...",
  "base_revision": "<commit-oid>",
  "generation": 1,
  "state": "ready"
}
```

### 5.2 State / effective diff

```http
GET /antares/worktrees/{worktree_id}/state
```

```json
{
  "worktree_id": "wt-123",
  "mount_id": "...",
  "base_revision": "<commit-oid>",
  "generation": 42,
  "state": "dirty",
  "changes": [
    {
      "path": "src/main.rs",
      "kind": "modified",
      "content_hash": "sha256:...",
      "base_hash": "sha256:..."
    },
    {
      "path": "src/old.rs",
      "kind": "deleted",
      "content_hash": null,
      "base_hash": "sha256:..."
    },
    {
      "path": "src/new.rs",
      "kind": "added",
      "content_hash": "sha256:...",
      "base_hash": null
    }
  ]
}
```

#### 关键变化：返回 effective diff，而不是 upper entry 列表

当前 upper 目录里有文件，不代表它一定相对于 lower 有变化：

```text
upper/a = X
lower/a = X
```

v2 必须返回：

```text
没有 change
```

而不是简单地把所有 upper 文件都标记为 modified。

effective diff 的判定：

| upper/lower 情况 | 结果 |
|---|---|
| upper 文件存在，hash 等于 base | clean/no change |
| upper 文件存在，hash 不等于 base | modified |
| upper 文件存在，base 不存在 | added |
| OCI whiteout，base 存在 | deleted |
| OCI whiteout，base 不存在 | no-op，可清理 |

`generation` 只在 effective diff 改变时递增。

### 5.3 Commit finalize

```http
POST /antares/worktrees/{worktree_id}/commit
```

请求：

```json
{
  "expected_base_revision": "<old-commit>",
  "new_base_revision": "<new-commit>",
  "commit_tree": "<tree-oid>",
  "expected_generation": 42,
  "committed_paths": [
    "src/main.rs",
    "src/old.rs",
    "src/new.rs"
  ]
}
```

这是 v2 最重要的接口。

服务端必须按以下顺序处理：

```text
1. 校验 expected_base_revision == 当前 base_revision
2. 校验 expected_generation == 当前 generation
3. 读取 new_base_revision 对应的 tree/content hashes
4. 确认 committed_paths 在新 tree 中的内容一致
5. 进入 Switching
6. 切换 Dicfuse lower 到 new_base_revision
7. 只删除 committed_paths 对应的 upper entries/whiteouts
8. 更新 base_revision = new_base_revision
9. generation += 1
10. 返回 Ready/Clean
```

成功响应：

```json
{
  "state": "ready",
  "base_revision": "<new-commit>",
  "generation": 43,
  "cleaned_paths": [
    "src/main.rs",
    "src/old.rs",
    "src/new.rs"
  ]
}
```

失败响应：

```json
{
  "state": "conflict",
  "code": "BASE_CHANGED|GENERATION_CHANGED|TREE_MISMATCH|SWITCH_FAILED",
  "base_revision": "<old-commit>",
  "generation": 42
}
```

失败时：

```text
不删除 upper
不推进 base_revision
不伪造 clean 状态
```

### 5.4 Refresh / pull

```http
POST /antares/worktrees/{worktree_id}/refresh
```

```json
{
  "expected_base_revision": "<old-commit>",
  "target_revision": "<new-commit>",
  "require_clean": true
}
```

处理规则：

```text
clean -> 切换 lower 到 target_revision
 dirty -> blocked_dirty
 base 不匹配 -> base_mismatch
```

`refresh` 与 `commit` 都使用同一个 lower switch transaction，不再维护独立的“只做计划但不执行”的半协议。

### 5.5 Fork

```http
POST /antares/worktrees/{worktree_id}/fork
```

```json
{
  "mountpoint": "/workspace/child",
  "mode": "materialize"
}
```

当前唯一支持：

```text
materialize
```

处理：

1. 读取父 effective upper delta；
2. 在 child mount 启动前复制到 child upper；
3. child 继承相同 base_revision；
4. child 使用独立 upper；
5. 父后续写入不影响 child。

`chain` 暂不属于 v2 必需能力，避免为性能优化引入另一套持久化和恢复模型。

---

## 6. 简化后的 Libra 命令

### 6.1 `worktree add --backend scorpiofs`

```text
Libra 登记 linked worktree
        ↓
POST worktrees attach
        ↓
目标目录被 ScorpioFS 挂载
        ↓
写 pointer + private index
```

### 6.2 `fork`

```text
Libra 读取当前 worktree mount_id
        ↓
POST worktrees/{id}/fork
        ↓
登记 child linked worktree
        ↓
写 child pointer + index
```

### 6.3 `sync`

```text
1. GET state/effective diff
2. Libra add -A
3. Libra commit，得到 C/tree(C)
4. Libra push C 到 Mega
5. POST worktrees/{id}/commit
6. ScorpioFS 切 lower + 清 committed upper
```

注意：

```text
push 成功 != worktree 同步完成
```

只有 `POST .../commit` 成功，才算本地工作区完成同步。

如果 push 成功但 lower switch 失败：

```text
远端已有 commit
本地 worktree 进入 Conflict/Recovery
upper 保留
不能继续伪装成 clean
```

---

## 7. Upper 文件的正确语义

upper 不是 Git index，也不是永久提交记录。

它表示：

```text
当前 base_revision 之上的未确认工作区覆盖层
```

因此每个 upper path 逻辑上应携带：

```text
base_revision_at_copyup
current_content_hash
kind
```

如果无法在首次 copy-up 时保存 base hash，至少在 `GET state` 时使用：

```text
Mega tree/content-hash(base_revision, path)
```

比较 upper 与 base。

禁止以下简化：

```text
upper 有文件 => 一定 modified
sync 成功 => 直接清 upper
push 成功 => 直接推进 base_revision
```

这三条都会制造一致性错误。

---

## 8. 最小实现范围

v2 只要求：

```text
attach
state/effective diff
commit finalize
refresh
materialize fork
```

明确不要求：

```text
chain frozen layers
ScorpioFS 自己实现 Git pack
ScorpioFS 自己实现 commit/push
透明 FUSE 读时 Git object hydration
```

这样系统只有三条清晰边界：

```text
Git pack        Libra ↔ Mega
文件 projection ScorpioFS ↔ Mega
worktree state  Libra ↔ ScorpioFS
```

---

## 9. 迁移现有协议

现有接口保留兼容：

| 旧接口 | v2 映射 |
|---|---|
| `POST /mounts` | `POST /worktrees` |
| `GET /worktree` | `GET /worktrees/{id}/state` |
| `POST /worktree/base` | attach 的一部分 |
| `POST /worktree/refresh-plan` | `POST /worktrees/{id}/refresh` 的 preflight/执行合并 |
| `POST /fork` | 保留，增加 `materialize` 明确语义 |
| 新增 `POST /worktrees/{id}/commit` | commit 后 lower switch + upper cleanup |

迁移期间：

```text
v1 client -> 继续使用旧 API，但不能声称 commit 后 lower 已同步
v2 client -> 使用 commit finalize，得到完整一致性保证
```

---

## 10. 结论

简化后的正确模型是：

```text
Libra 管 VCS 对象和 pack
ScorpioFS 管 POSIX 文件和 upper
Mega 管远端仓库与按 revision 读取

commit 的完成条件不是 push 成功，
而是 push 成功 + lower switch 成功 + committed upper 清理成功。
```

这样可以同时解决：

- upper 文件与 lower 内容相同导致的假 dirty；
- commit 后 upper 永久残留；
- 旧 upper 遮挡远端新 lower；
- push 成功但本地工作区仍显示旧内容；
- ScorpioFS 和 Libra 各自重复实现 Git pack 的问题。
