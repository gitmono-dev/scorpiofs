# MST/2 数据端点实测（2026-09-29）

## 结论：8 个端点全部可用，但 `/directory` 和 `/lookup` 有严重性能缺陷

挂载路径走 `metadata/pages` + `blob`，**不受该缺陷影响**；但 `/directory` 与 `/lookup`
在 50k scope 上要 36–71 秒，而同样内容经 `metadata/pages` 只需 1.6 秒。

## 端点形状（此前全部猜错，据 `snapshot_router.rs` 更正）

| 端点 | 方法 | 关键点 |
|---|---|---|
| `/snapshots/capabilities` | GET | 唯一免认证路由 |
| `/snapshots/resolve` | POST | body `{target:{kind:"latest"},"scope":"..."}` |
| `/snapshots/{snap}/descriptor` | GET | |
| `/snapshots/{snap}/directory` | **GET** | 路径在 **query**（`?path=&limit=`），不在 body |
| `/snapshots/{snap}/lookup` | POST | body `{"paths":[...]}`，snapshot 在 **path** |
| `/snapshots/{snap}/metadata/pages` | POST | body `{"items":[{"directory_path":"/","route":[]}]}` |
| `/snapshots/{snap}/blob` | GET | `?path=&expected_digest=` |
| `/snapshots/{snap}/objects` `/chunk-map` `/chunks` | POST/GET | 内容分片读 |

**认证（spec 04 §1）**：除 `capabilities` 外**所有** snapshot 绑定端点都要
`X-Mega-Snapshot-Lease: <lease_id>`。lease id 是 resolve 响应的**顶层**字段，
不在 `descriptor` 里 —— 知道 snapshot id 本身不构成访问凭证。

## 实测（`/project/bench50k`，50k 文件，monoengine @ ACK）

| 端点 | 耗时 | 备注 |
|---|---|---|
| `resolve` | **1.52 s** | 热态；冷态 33.7 s |
| `capabilities` | 0.25 ms | |
| `descriptor` | 0.26 ms | |
| `directory?path=/` | **38.24 s** | 返回 20 条 |
| `directory?path=/svc00` | **35.63 s** | 返回 20 条 |
| `lookup`（2 条路径） | **70.91 s** | ≈ 2× directory → **线性于路径数** |
| `metadata/pages`（同目录根页） | **1.59 s** | 二进制 MTP2 |
| `blob?path=.../f00000.rs` | **0.0037 s** | 内容读极快 |

## 根因：`proof_pages` 会递归构建整棵祖先链，包括 monorepo 根

```rust
// pages.rs:216
pub async fn proof_pages(...) -> ... {
    let mut chain = vec!["/".to_string()];      // ← 从视图绝对根开始
    if rel_path != "/" {
        for i in 1..=comps.len() {
            chain.push(format!("/{}", comps[..i].join("/")));
        }
    }
    for p in chain {
        let built = build_directory_page(handler, root_tree, &p).await?;
```

而 `build_directory_page` 是**递归**的（`pages.rs:133`）：建一个目录的 page 会**建完整棵
子树**才能算出子目录的 `page_id`。

于是 `directory?path=/svc00`（abs_path = `/project/bench50k/svc00`）的 proof 链是：

```
/                      → 递归构建整个 55 万文件 monorepo 根
/project               → 递归构建 /project 全树
/project/bench50k      → 递归构建 50k 子树
/project/bench50k/svc00→ 目标
```

**38 秒里绝大部分花在构建 monorepo 根。** 这解释了全部三个异常数字，包括
`lookup` 的 2× 线性（每条路径各跑一次 `proof_pages`，各自的祖链都要重建）。

`build_directory_page` 有 `PAGE_CACHE`（`pages.rs:70`，键 `(root_tree_id, rel_path)`），
所以同一进程内重复调用会命中 —— 但根页只有在**第一次**被请求时才付这个代价，
而 36 s 就是这个第一次。

## 对挂载路径的影响：无

**daemon 用的是 lazy mount**（`antares.rs:84` → `Mst2Fuse::from_reader_lazy`）：
一次 `metadata/pages` 取 scope 根页（1.59 s），之后每个目录按需拉自己的 page 树
（`fuse.rs:228` `ensure_dir_loaded`，BFS over branch pages）。
**不调用 `/directory`，也不调用 `/lookup`。**

所以 36–71 s 的缺陷**不在** D1/D2/D3 的关键路径上，但它是一个真实的、
值得单独修的服务端缺陷（任何一个用 `/directory` 的客户端都会撞上）。

## 仍未验证

- daemon 以 MST/2 为 lower 时挂载的读写（D1/D2）—— 需要集群里带 FUSE 的工作负载
- `metadata/pages` 在**冷**进程下的首次耗时（1.59 s 是缓存命中后的数字）
