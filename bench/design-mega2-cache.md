# mega2 服务端缓存与增量更新设计（spec）

**日期**：2026-09-30　**状态**：待确认（确认后再动代码）
**目标**：消除 `git-upload-pack` 每次请求从头重算的问题；commit 更新时**迭代式**更新，
而不是全量重算。

---

## 一、实测事实（全部在 ACK 集群测得，非推断）

### 1.1 症状

| 测量 | 数值 |
|---|---|
| `GET /project/info/refs` 首字节 | **2.5 ms** |
| `POST /project/git-upload-pack` 首字节 | **81.28 s** |
| 同上，总耗时 | 82.87 s |
| 同上，响应体 | 54.4 MB |
| `git clone` 全程 | 84.5–85.3 s |

81 秒里**一个字节都不发**。libra 的 HTTP 客户端固定 60 s idle 读超时
（`libra/src/internal/protocol/https_client.rs:38`）→ 客户端先撤 → 服务端 pack 编码器
拿到 `SendError` → `git-internal 0.9.0` `pack/encode/mod.rs:181` 的 `.unwrap()` panic
掉一个 tokio worker。git 客户端无此上限，所以 git 能过、libra 不能。

### 1.2 完全没有缓存

```
full clone r1 : 83,115 ms
full clone r2 : 86,679 ms        ← 连续两次，无任何加速
```

### 1.3 81 秒花在打包历史

| 场景 | 耗时 | blob 数 | tree 数 | 传输/落盘 |
|---|---|---|---|---|
| 全量 clone | **83–98 s** | **550,001** | 10,466 | 75.4 MB |
| `--depth 1` clone | **8.2–9.6 s** | **50,000** | 823 | 9.4 MB |

**10 倍差距全部来自那 50 万祖先历史对象**（tip 只需要 5 万）。父提交
`ac72fd39e3` 是 500k 文件的种子，tip 后来被收缩到 50k 子树，但历史仍在。

> 注：代码阅读曾推断 `--depth 1` **不会**有帮助（`shallow_pack` 只记录
> `shallow_commits`、似不截断树遍历）。**实测推翻了这个推断** —— 这是必须实测而非
> 读码的典型案例。

### 1.4 已经达成的指标（作为基线）

```
git  全量 clone      : 85308 ms
mono clone+attach    : 14882 ms  (clone 9583【--depth 1】+ attach 5299)
比值 = 0.174          D3 目标 ≤ 0.2  ✅
```

**即：用 `--depth 1` 已经达标。** 本 spec 要解决的是**为什么需要 `--depth 1` 这个
绕道** —— 服务端本不该在每次 clone 时重算 55 万个对象。

---

## 二、根因（代码位置）

### R1　上传路径**完全不读缓存**

monoengine 里已经有一套 Redis 缓存 `GitObjectCache`
（`ceres/api_service/cache.rs`，rkyv 序列化，`SET EX 86400`，1 天 TTL），
**已经接进 `ProtocolApiState`**（`ceres/protocol/mod.rs:282`，
`Monorepo` 可通过 `self.git_object_cache` 拿到，`monorepo.rs:66`）——
**但 pack 路径从不调用它**。

走缓存的是 history / diff / mui / gpg 这些 API 路径；
`Monorepo` 与 `MonoApiService::get_tree_by_hash`（`mono_api_service.rs:1584`）
**直接落到 `mono_storage()`**。

日志里那句 `git_internal::internal::pack::cache: Caches clear` 是 **receive-pack
的解码缓存**（`decode.rs:1917`），每次 `unpack_stream` 新建、用完就清 —— 跨请求什么
都不留。

### R2　N+1：一次 50k clone ≈ 15,000 次 PG 往返 + 50,000 次 S3 GET

`incremental_pack`（`ceres/pack/monorepo.rs:504`）的流程：

```
① get_commits_by_hashes       1 SQL
② 父提交 BFS                  每父提交 1 SQL
③ get_trees_by_hashes         1 SQL
④ traverse_for_count  ──┐     每目录 1 SQL（串行递归）
⑤ PackEncoder::new(window=0)  
⑥ traverse            ──┘     每目录：get_blobs_by_hashes(S3) + get_blob_metadata_by_hashes(1 SQL) + get_trees_by_hashes(1 SQL)
```

**两个致命点**：

- **树遍历做了两遍**（④ 计数 + ⑥ 发送），每遍每目录 1 次 SQL，且是**串行深度优先**
- **`get_trees_by_hashes` 在递归里逐层调用、从未上提**，`get_many` 虽然批量了 key
  但**每个 key 发一次 `get_stream`**

另外 `pack/mod.rs:459` 取 `pack_id`/`pack_offset`/`is_delta`，
而 `parallel_encode` 只推 `entry.inner` —— **每目录一次 SQL 取的数据从未被使用**。

### R3　响应必须等整个 pack 建完才开始

`contract/git_protocol/http.rs:247` 把 `pack_protocol.git_upload_pack(...)` **await
到底**，之后才构造响应体。NAK/ACK 也是在里面（`protocol/smart.rs:271`）才产出，
到 `http.rs:250` 才吐。

**所以那 81 秒的静默是"按构造"的，不是协议要求。**

### R4　没有 side-band-2 进度

`build_side_band_format`（`smart.rs:698-711`）**只发 channel 1（packfile）**，
从不在 upload-pack 上发 channel 2（进度）。服务端协商了 `side-band-64k` 却不用它
传进度，客户端除了干等别无选择。

### R5　无 delta 压缩 + 编码不能与取对象重叠

- `PackEncoder::new_with_hash_kind(.., 0, ..)`（`monorepo.rs:592`）：**`window_size = 0`
  ⇒ 不做 delta**，每个对象独立 zlib。这解释了 54 MB 这个偏大的体积。
- `parallel_encode` 要先**填满** `max(1000, cap/10)` = **100,000** 条才压
  （`encode/parallel.rs:45-70`），所以 ~5 万对象的 zlib 突发**只会在 fetch 全部结束
  后**才开始 —— 取对象与压缩**零重叠**。

### R6　`SendError` panic

`git-internal-0.9.0/src/internal/pack/encode/mod.rs:181`：

```rust
pub async fn send_data(&mut self, data: Vec<u8>) {
    if let Some(sender) = &self.pack_sender {
        sender.send(data).await.unwrap();   // 客户端一断就 panic
    }
}
```

`parallel.rs:43`、`:121` 同样。而 `encode_async`（`monorepo.rs:598`）**丢掉了
`JoinHandle`** → 这个 panic 杀死一个游离的 tokio 任务，**永远不会冒到 HTTP 层**。

### R7　陈旧 oid：不是缓存 bug，是 ref 行不一致

API `latest-commit` 报 `ce2861cd…`，git 广播 `01cb3c85…`。原因：
`mega_refs` 有**两行** —— API 走 `get_main_ref("/")`（`mono_api_service.rs:1040`），
git 走 `materialize_path_refs(&storage, "/project")`（`monorepo.rs:144-157`）。
**两行不同的 tip**。与缓存无关。

---

## 三、设计

### 核心洞察：**内容寻址让增量更新几乎免费**

tree 与 commit 的 oid 是**内容地址**。新 commit 只产生新的**根树**，
**所有未变的子树按 oid 原样复用**。

因此：

- **子树 / blob 元数据缓存：键 = `tree_oid`，不需要任何失效逻辑。** 内容变了 oid 就变，
  天然不会命中旧值。
- **pack 产物缓存：键含 `commit_oid`。** 新 commit = 新键，旧条目**自动成为死条目**，
  按 TTL / 数量回收即可，不会返回错误数据。

这正是你要的"迭代式更新"：**不是**"commit 来了就重算"，而是
**"commit 来了只补算新 oid 覆盖的部分，其余按 oid 直接命中"**。

### L1　树/commit 元数据走已有的 Redis 缓存（最低风险，先做）

| 项 | 内容 |
|---|---|
| 缓存什么 | tree 对象、commit 对象（内容寻址，不可变） |
| 键 | `tree_oid` / `commit_oid`（`GitObjectCache` 现有设计即如此，`cache.rs:30-146`） |
| 存哪 | **已存在的 Redis `GitObjectCache`** —— 组件已实现、已测试、已接线 |
| 挂在哪 | `Monorepo::get_trees_by_hashes`（`monorepo.rs:627`）与 `pack/mod.rs` 的递归，改用**批量 `MGET`** 而非每目录一次 SQL |
| 预期 | 消除 ~15,000 次往返中的大部分 —— 数十秒级 |
| 失效 | **无需**（内容寻址） |
| 风险 | 最低 —— 复用现成组件 |

### L2　pack 产物持久化缓存（收益最大）

| 项 | 内容 |
|---|---|
| 缓存什么 | 生成好的 **pack 字节** |
| 键 | `(namespace/path, commit_oid, "full")`；非空 `have` 的情况再加 `want`/`have` 指纹 |
| 存哪 | 对象存储（rustfs/S3），流式 `put_object` |
| 挂在哪 | `incremental_pack` / `full_pack`（`monorepo.rs:504` / `:255`）—— miss 时把编码器的 `stream_rx` **tee** 一份存入；hit 时 `get_stream` 直接接 `ReceiverStream` |
| 预期 | 主导场景（`git clone`）**81 s → 一次 S3 GET** |
| 失效 | commit oid 在键里；每 path 保留最近 N 个 commit + TTL |
| 范围 | **先只做 `have.is_empty()`（即 clone）、非 shallow、非 filtered 的情况**，风险可控后再扩 |

### L3　消除重复遍历与无用查询（无缓存语义，纯收益）

- **两遍树遍历合并为一遍**（④ 计数 + ⑥ 发送）
- 每层**上提** `get_trees_by_hashes`，用 `buffer_unordered` 替代串行递归
- **删掉** `pack/mod.rs:459` 的 `get_blob_metadata_by_hashes`（取的数据从未使用）

预期：每目录 SQL 减半，并去掉整趟遍历。让 L1/L2 未命中时的代价也降下来。

### S1　让响应立即开始 + 用 side-band-2 报进度（**修掉 81 s 的"静默"本身**）

把 NAK/ACK 从 `git_upload_pack` 里挪出来，**先开始响应**，并像标准服务端那样用
**channel 2** 发进度。

**这一条单独就能让当前客户端成功** —— libra 的 idle 超时只在**静默**时触发，有字节流动
就不会触发。它不减少计算量，但消除了"静默"这个**致命的呈现方式**，
也让 `--depth 1` 不再是必须的绕道。

### S2　边取边编码

`parallel_encode` 不要等填满 100,000 条才压；让编码与取对象**重叠**。

### S3　`SendError` 不 panic

```rust
// 客户端断开 = 工作结束，不是崩溃
if sender.send(data).await.is_err() {
    return Err(GitError::IoError(..));   // 让调用方干净地丢弃任务
}
```

三处：`encode/mod.rs:181`、`parallel.rs:43`、`parallel.rs:121`。
另修 `encode_async` 丢弃 `JoinHandle` 的遮蔽问题（`monorepo.rs:598`），
让失败至少能被观察到。

### S4　`window_size = 0` → 启用 delta

`monorepo.rs:592`。这不会缩短 81 s（压缩不是瓶颈），但能显著缩小 pack 体积。

---

## 四、优先级

按 **预期收益 ÷ 实现风险** 排序：

| 顺序 | 项 | 收益 | 风险 | 说明 |
|---|---|---|---|---|
| **1** | **L1** 元数据走 Redis | 数十秒 | 最低 | 组件已存在、已接线，只是没被调用 |
| **2** | **S1** 立即响应 + 进度 | 消除 81 s **静默** | 低 | 单独就能让当前客户端不需绕道 |
| **3** | **L3** 去掉重复遍历 | 减半 SQL | 低 | 纯清理，无缓存语义 |
| **4** | **S3** 不 panic | 移除真实缺陷 | 低 | 三行改动 |
| **5** | **L2** pack 产物缓存 | 81 s → 一次 GET | 中 | 收益最大，但范围要先收紧 |
| 6 | S2 重叠 | 中等 | 中 | 需改编码批次策略 |
| 7 | S4 delta | 体积 | 中 | 不影响延迟 |

**建议先做 1–4**：这四项都低风险，且合起来已经能去掉绝大部分成本；
L2 是"每棵树的最终形态"，但在 L1+L3+S1 之后它的紧迫性会下降。

---

## 五、验收方式

在同一集群、同一 50k 树、同一节点拓扑下：

| 指标 | 现状 | 目标 |
|---|---|---|
| `git-upload-pack` **首字节** | 81.28 s | **< 1 s**（S1） |
| 全量 `git clone` | 85.3 s | **< 20 s**（L1+L3），**< 5 s**（L2 命中） |
| 第二次 clone（缓存生效） | 86.7 s | **显著低于第一次**（当前 r1≈r2，零缓存） |
| libra 全量 clone（默认 60s 超时） | 失败 | **成功**（S1 后不需调超时） |
| 无 delta 的 pack 体积 | 54.4 MB | 下降（S4） |
| 客户端断开后的服务端 | panic | 干净退出（S3） |

**增量更新的验收**：在同一 path 上推一个新 commit（只改 10 个文件），
再次 clone —— 应观察到**只重算变化的子树**，而非重新打包全树。

---

## 六、风险与边界

- **L2 的 `have` 非空情况**（增量 fetch）：键必须包含 `want`/`have` 指纹，
  否则会返回错误的 pack。**第一版明确不支持**，只做 `have.is_empty()`。
- **shallow / filtered fetch**：同样**第一版不做**缓存，直接落原路径。
- **`--depth 1` 不是长期的正确答案**：它现在让 D3 达标，但**掩盖了服务端问题**。
  在报告里要如实说明：达标依赖 shallow，而 shallow 之所以必要是因为服务端重算历史。
  S1 修好后，全量 clone 也不再需要绕道。
- **陈旧 oid（R7）**：与本 spec 无关，是 ref 行语义问题，需要单独决策
  （API 与 git 该不该读同一行）。**本 spec 不改动它。**
