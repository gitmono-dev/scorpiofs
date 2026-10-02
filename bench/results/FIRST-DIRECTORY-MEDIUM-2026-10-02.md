# 中等规模：首次一级目录可访问对比（2026-10-02）

## 结论与口径

**在现有服务端缓存条件、本地 WSL 同机拓扑、同一 50,000 文件子树上，mega2 + ScorpioFS 的首次一级目录可访问时间中位数为 3.370 秒，Git 为 6.090 秒：ScorpioFS 快 1.81 倍，耗时减少 44.7%。**

这是用户指定的“可访问就绪”指标，不是 Libra 完整索引构建完成，也不是全部文件下载完成。ScorpioFS 直接创建 MST/2 lazy mount，无需等待 Libra clone 或 worktree 索引种子。

**双方使用同一个可见性探测**：首次成功列出挂载/检出根目录，并确认预先指定的 `svc00` 子目录存在且能 stat 为目录。Git 可以在 clone 尚未结束时满足条件；其完整 clone 时间单独记录，不能替代首次可见时间。

结论仅适用于该目录可访问场景；不宣称 status/add/commit 全面优于 Git，也不证明 826k 文件全冷首挂已解决。

## 环境与固定内容

- 同一服务端：现有本地 mega2，`http://127.0.0.1:19000`，部署镜像 `mega2:local`。该镜像并非当前 monoengine HEAD，本轮未修改或重建服务端。
- 同一 monorepo 下的既有中等子树：`/project/bench50k`。没有另建仓库。
- Git 端点：`/project/bench50k`；MST/2 scope 同为 `/project/bench50k`。
- Git ref：`91e7e09dd762da47ad0c02b1292a5bc2325ad0df`，每轮前后核对，三份检出均为此 commit，均有 50,000 个 tracked files。
- ScorpioFS：现有 debug daemon，源码 checkout `0db2b1099a6b31c35c441774f884428db9f95050`；二进制 `/home/luxian/target-scorpiofs/debug/scorpio`，mtime 2026-09-28 10:58:47。已核对此后 checkout 的 `src/` 无新提交。
- 专用测量 daemon：37255；临时数据位于 `/home/luxian/readiness-profile`。`load_dir_depth=1`、`antares_load_dir_depth=1`（配置不接受 0）；挂载后后台预热仍启用，未从 wall time 中扣除其资源竞争。
- 服务端已有数据/验证记录；未清 PostgreSQL、Redis、页缓存或 OS 页缓存。本轮为**现有缓存条件下的新工作区**对比，不标为全冷。
- 每轮创建新的 Git 检出与新的 mount/job id；轮次顺序交替，避免只测一种固定先后顺序。
- Git 使用 `--depth 1`，不以全历史 clone 人为放大优势；ScorpioFS 提供固定快照浏览而不提供完整本地 Git 历史。

## 原始样本

| 轮次 | 顺序 | ScorpioFS 首次可见 | Git 首次可见 | Git clone 完成 |
|---|---|---:|---:|---:|
| 1 | ScorpioFS → Git | 3,369.720 ms | 8,510.828 ms | 9,227.703 ms |
| 2 | Git → ScorpioFS | 3,342.697 ms | 6,090.244 ms | 6,756.738 ms |
| 3 | ScorpioFS → Git | 3,375.339 ms | 5,886.074 ms | 6,703.682 ms |
| **中位数** | | **3,369.720 ms** | **6,090.244 ms** | **6,756.738 ms** |

`6090.244 / 3369.720 = 1.8073`。

每个样本均 `status=success`，失败样本不进入中位数。计时使用 monotonic clock；目录探测使用独立子进程和超时，避免挂死的 FUSE read 阻塞整个测量。

原始文件：[六个样本 JSONL](raw/FIRST-DIRECTORY-MEDIUM-2026-10-02.jsonl)。

## 一致性验收

另开一个独立挂载，计时结束后做只读验收（不混入就绪时间）：

- 20 个一级子目录与 Git 完整检出逐项相同。
- 每个一级目录抽样一个 tracked file，共 20 个文件，读取字节与 Git 检出完全一致，记录 SHA-256。
- 3 份 Git clone 均验证 50,000 文件、固定 commit。
- 三个计时挂载以及验收挂载均已 DELETE 清理。

证据：[只读一致性结果](raw/FIRST-DIRECTORY-MEDIUM-2026-10-02-readonly.json)。

## 仍然存在的正确性和性能边界

**截短写入缺陷，不能省略**：额外的写入验收发现：9 字节 lower 文件 copy-up 后追加为 32 字节，追加读回正确；随后以 `O_TRUNC` 写回原 9 字节，重新读取仍返回旧 32 字节（等待 2 秒仍如此）。此缺陷不影响上述只读目录可访问样本，但阻止“完整可写开发工作区已经验收通过”的结论。已单独提议依赖库调查任务，尚未声称修复。

证据：[追加/截短验收](raw/FIRST-DIRECTORY-MEDIUM-2026-10-02-content.json)。

50k 子树计时结束后，独立协议分解得到：capabilities 3.193 ms，resolve 1,275.422 ms，根 metadata page 1,251.882 ms。它是另一次请求，不与某个挂载样本相减。当前服务端目录页递归构建、快照页缓存和 Dicfuse 前置初始化仍是进一步优化方向。

826k 云端失败轮：900 秒未建立 FUSE mount，没有有效对照；不得以“秒级理论值”覆盖该失败记录。该轮 Terraform 管理的 9 个资源已销毁，状态为空，两台测试 VM 均已不存在。本地测试没有重新创建收费云资源。

## 复现入口

- `mst2-impl/first-directory-ready.py`：双方首次目录可见计时，交替顺序、新 workspace、固定 ref 检查。
- `mst2-impl/local-readiness.toml` 与 `.claude/launch.json`：独立本地 daemon 配置。
- `mst2-impl/verify-root-view.py`：根目录与抽样文件只读验收。
- `mst2-impl/profile-snapshot-readiness.py`：resolve 与根 metadata 请求分解。

实际可辩护结论：**在这个中等规模、已有服务端缓存的目录浏览/按需读取场景中，mega2 + ScorpioFS 比 depth-1 Git 新检出更早提供可访问的一级目录；其他指标与全冷大规模结果仍需独立验证。**
