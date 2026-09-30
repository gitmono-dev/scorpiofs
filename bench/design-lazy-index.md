# ScorpioFS 工作区的惰性索引 — 设计与待确认问题

状态：**待用户确认**（Q1–Q4 需拍板）。确认后再动代码。

关联：本轮 L 档实测暴露的问题；`bench/ACK-REMOTE-WORKTREE-2026-09-29.md`。

---

## 1. 问题（实测）

L 档（500k 文件、服务端跨节点）下 `libra clone` **61.2 秒**，且**中途失败**：

```
libra-clone-ms=61228  rc=128
LBR-NET-001: failed to fetch objects from 'http://mega2:8000/project/'
ABORT: libra clone (main) failed
```

服务端同时在 `git-internal-0.9.0/src/internal/pack/encode/mod.rs:181` **panic**：

```
called `Result::unwrap()` on an `Err` value: SendError
```

即客户端断开 → 服务端 `SendError` → **未捕获 panic → mega2 进程退出**。

**但真正的问题是设计层面的**：ScorpioFS 模式下，文件内容本来就由 daemon 按需从 mega2 投影，**`libra clone` 却把 500k 个 blob 全下载了一遍**——这些 blob 在挂载工作区里根本用不到。

| 层 | 取数方式 | 是否按需 |
|---|---|---|
| ScorpioFS daemon（FUSE） | 按目录/按读请求 | ✅ 按需 |
| `libra clone`（挂载前的前置步骤） | 全量 500k 对象 | ❌ **全量** |

**为什么现在必然是全量**（代码为证）：

- `libra/src/command/clone.rs:319-321`：`--filter is ignored: Libra has no partial-clone support`
- mega2 的 git 端点只广播 `multi_ack_detailed no-done shallow side-band-64k ofs-delta`，**不含 `filter`**

所以这不是配置问题，**是缺少一条"I only need metadata"的路径**。

---

## 2. 关键洞察：需要的东西比现在拿的少得多

挂载一个 ScorpioFS 工作区，**只需要**：

| 需要 | 用途 |
|---|---|
| 一个 commit OID | 钉住 daemon 的 lower revision |
| 每个条目的 `(path, mode, oid)` | 建私有 index（status/diff/commit 的基线） |

**完全不需要 blob 内容**——读取由 FUSE 走 daemon。

而 `restore::execute_checked`（`worktree.rs:1655`）之所以要本地对象库，是为了从 HEAD 树**枚举条目**——**枚举只要树对象，不要 blob**。现在的实现却因为"整个 repo 是完整 clone 的"这个前提，顺手把 blob 也下了。

---

## 3. 现成的通道（无需新建协议）

mega2 已经有 daemon 正在用的按目录元数据接口：

```
GET /api/v1/tree/content-hash?path=<dir>&refs=<rev>
  -> Vec<TreeHashItem>   目录: dir hash   文件: content hash (git blob OID)
```

（`mega/mono/src/api/router/preview_router.rs:238-260`）

**daemon 已经在用它**（`store.rs` 的 `fetch_dir`）来投影每个目录。所以惰性索引不需要新协议，只需要让 libra 也走这条路。

---

## 4. 设计

### 4.1 目标

ScorpioFS 模式下的"拿到可用工作区"，从
`完整 clone（500k 对象）→ worktree add`
变为
`只取元数据 → attach 时按需投影`。

预期：**61 s → 秒级**（只取树元数据，不取 blob）。

### 4.2 形态

新增一条模式（名称待定，见 Q1）：

```
libra worktree add --backend scorpiofs --lazy-index -b <branch> <path>
```

行为：

1. **不要求**主仓库有完整对象库；只要求能解析出一个 commit OID。
2. 私有 index 通过 **mega2 的 `/tree/content-hash` 递归枚举**构建，而不是 `restore --staged --source HEAD`。
   - 递归由 libra 发起，**按目录**取（与 daemon 同一条通道、同样的单位）。
   - 每个条目写入 `(path, mode, oid, size=?)`。
   - **size 未知**：见 Q2。
3. attach 时把该 commit OID 作为 `base_revision` 传给 daemon（现有流程不变）。
4. 之后一切照旧：`status` 走 upper 层快路径，`sync` 走 push + finalize。

### 4.3 需要解决的三个具体问题

**(a) 主仓库对象库不完整时，libra 别的命令还能用吗？**

这是最需要小心的地方。`status`/`add`/`commit`/`sync` 会：
- 读 index（有）
- 与 HEAD 的树比较（需要**树对象**，不需要 blob）—— `commit` 的 `validate_index_objects` 现在会**校验每个 oid 的存在性**（B#2 改成批量类型探测），在惰性索引下**这些 blob 本地根本不存在** → 会失败。

这是真正的工作量所在。选项见 Q3。

**(b) index 条目的 `size` 字段**

`IndexEntry` 有 size，`/tree/content-hash` 不返回 size。选项见 Q2。

**(c) 与现有完整 clone 路径的关系**

保留两条路（向后兼容）还是一刀切？见 Q4。

---

## 5. 待确认问题

**Q1（入口形态）**
- **A（推荐）**：`libra worktree add --backend scorpiofs --lazy-index`（显式开关，老的完整 clone 路径不动）
- B：`libra clone --scorpiofs` 的一个新变体（把 clone 与 attach 合成一步）
- C：ScorpioFS 模式下自动启用（无需开关）——最省事但行为变化隐蔽

我倾向 **A**：它是纯增量，且把"要不要惰性"这个决定留给调用方；等验证稳定后，再考虑 C 作为默认。

**Q2（size 字段）**
- **A（推荐）**：写 0，并在 index 里标记"size 未知"。风险：某些按 size 的逻辑（如 `ls-files -s` 展示、LFS 阈值判断）会不准。
- B：attach 后**后台**补齐 size（对 500k 条目是 500k 次额外请求，可能比省下的更贵）。
- C：让 mega2 的 `/tree/content-hash` 增加 size 字段（改服务端，但一次到位——树对象里本来就有 size，返回它几乎零成本）。

**C 其实最好**，但需要动 mega2。选哪个？

**Q3（对象库不完整的影响面，工作量主要在这里）**
- **A（推荐）**：惰性索引下，**禁用本地对象校验**（`validate_index_objects` 跳过本地不存在的 blob），把"对象是否存在"的权威交给 mega2。
  - 代价：本地 `commit` 不再能发现"index 引用了不存在的对象"这类损坏；但在这个模型里对象权威本来就在服务端。
- B：attach 时**只下载树对象**（不含 blob）。树对象总量远小于 blob（500k 文件 ≈ 几千个树），但要新增"按 oid 取单对象"的批量下载，且 clone 逻辑要支持"partial tree"。
- C：保持完整 clone，只在**传输层**优化（如压缩、并行）。不解决根本问题。

**Q4（范围）**
先只做"attach 时惰性"（本轮），还是连 `commit`/`sync` 的适配一起做？

---

## 6. 验证方案

1. **正确性**：惰性索引的 `(path, mode, oid)` 三元组，必须与完整 clone 后的 index **逐字节一致**（L 档 500k 树上比对）。
2. **语义**：`status` / `add` / `commit` / `sync` 四场景（clean / 改 / 删 / 新增）在惰性索引下结果与完整路径一致。
3. **性能**：L 档（500k）上 `clone + attach` 的总时间，目标 < 5 s。
4. **回归**：不启用惰性时，现有完整路径行为不变。
5. **服务端稳定性**：确认不再触发 `SendError` panic（因为不再有大 pack 传输）。

---

## 7. 附带发现（与本设计无关但必须记下）

**mega2 的一个真实健壮性缺陷**：`git-internal 0.9.0` 的 `pack/encode/mod.rs:181` 用 `.unwrap()` 处理通道发送结果——**任何客户端中途断开都会 panic 并使 mega2 进程退出**。这不是本设计引入的，但本设计会让它**不再被触发**（不再有大传输）。建议单独提上游：
`Err(SendError)` 应当作为"客户端已离开"处理并优雅终止该请求，而不是 panic。

---

## 8. 不在本次范围

- mega2 侧 `SendError` 的健壮性修复（建议独立上游 PR）。
- 1M+ 规模测试。
