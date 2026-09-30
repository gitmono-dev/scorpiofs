# MST/2 lower 验证：当前阻塞点（2026-09-29）

## 结论先行

**MST/2 服务端在中等规模以上不可用**，阻塞点是 `POST /api/v2/snapshots/resolve`。
因此目标 D1（MST/2 lower 可用）**未达成**，D2–D6 无法开始。

这不是配置问题，也不是我操作失误 —— 是可复现的服务端行为。

## 证据链

### 1. 服务端在跑，路由正常

| 探针 | 结果 |
|---|---|
| `GET /api/v2/snapshots/capabilities` | **200**，`resolve/directory/lookup/metadata_pages/leases` 全 `true` |
| `POST /resolve` scope=`/project/.gitkeep`（小 scope） | **409，2.2 ms 返回** ← 说明 HTTP 层与路由是活的 |
| `POST /resolve` scope=`/` | **000 超时（30–45 s），ttfb=0.000000s** |
| `POST /resolve` scope=`/project` | **000 超时（30–45 s），ttfb=0.000000s** |

`ttfb = 0` 的含义很明确：**服务端从未开始写响应**，请求卡在处理器内部。
小 scope 能秒回、大 scope 必然超时 → **卡点在"构建快照元数据"这一步，成本随树规模增长**。

### 2. 排除了三个可能的解释

| 假设 | 排除依据 |
|---|---|
| 是 `SendError` panic 的余波（worker 已死）| **重启 mega2 后仍然超时**（`restarts: 0`，全新 worker pool）|
| 是服务端配置没开 | `MEGA_MST2__ENABLED=true` + `INSTANCE_UUID` 都在；capabilities 应答 |
| 是网络/HTTP 层问题 | 同一条路由、同一次会话，小 scope **2.2 ms** 返回 |

### 3. 树规模

`/project` 当前是 **500,000 个文件**（bench 播种完成，`500001 files` 已校验）。
`/` 是更外层的范围，包含更多。两者都超时。

### 4. 日志盲区

`mega2` 日志里**没有任何** `snapshot/mst2/resolve/metadata/page` 相关行 ——
这条代码路径没有日志埋点，所以无法从日志进一步定位卡在哪一步。

## 与既有文档的冲突

`MST2-LOWER-KNOWN-ISSUES.md` 记载：

> 冷全树 metadata 遍历：**MST/2 = 2.13s** vs Dicfuse 逐目录 22.2s → **10.4×**

但那是**更早的、更小的树**上的实测（文档日期 2026-09-22，当时的树是 monorepo 源码而非 50 万合成文件）。
本轮的 500k 树上，resolve 直接超时 —— **该性能结论在 50 万文件规模下不成立**。

这不是文档写错了，而是**规模外推失效**：2.13s 的结论只在它被测量的那个规模上有效。

## 附带确认的一个服务端缺陷（与本阻塞独立，但同样真实）

`git-internal 0.9.0` 的 `src/internal/pack/encode/mod.rs:181`：

```rust
called `Result::unwrap()` on an `Err` value: SendError { .. }
```

**任何客户端在 pack 传输中途中止，都会 panic 掉一个 tokio worker。**
先前的 `libra clone` 就是这样把 mega2 的数据路径打死的（`capabilities` 仍 200，
但所有需要读数据的接口静默挂起）。修法明确：`Err(SendError)` 应作为
"客户端已离开"优雅终止该请求，而非 panic。

## 可选的下一步

### A. 把树降到中等规模，再验 MST/2（推荐）
当前 `/project` 是 500k。**换成 50k**（新 scope 或新 repo），重测 resolve。
- 若 50k 下 resolve 正常 → **可以立刻做中等规模的对比测试**（正是本轮目标）
- 若 50k 下仍超时 → 卡点与规模无关，需改 mega2 代码（下一条）

### B. 直接修 mega2 的 resolve 卡点
需要先定位它慢在哪（该路径无日志）。做法：给 `snapshot_router.rs` 的 resolve
加计时日志，重建 monoengine 镜像。**代价：约 30 分钟构建 + 部署**，且要改服务端代码。

### C. 先修 `SendError` panic（独立价值高，但不解本阻塞）
改 `git-internal`（或让 mega2 捕获该 panic），使客户端断开不再杀 worker。
这是**上游级缺陷**，值得单独提交；但即使修好，resolve 的规模问题依然存在。

## 我的建议

先做 **A**（约 10 分钟）：把规模降到 50k 立刻验证。
它成本最低，且能**直接回答**"MST/2 在中等规模下是否可用"这个决定后续一切的问题。
若 A 通过 → 进入 D1–D6 的完整测试；若 A 不通过 → 转 B，并把这个发现写进结论。
