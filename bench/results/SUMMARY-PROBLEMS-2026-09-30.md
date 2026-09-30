# 现状总结：问题清单（2026-09-30 本轮结束时 · 第二次更新）

## 〇、本轮最大成果：filter 端到端打通（性能提升落地）

**服务端 v0 filter（`6cf95df`）+ libra 真正发线（`768320921`）**，实测：

| 克隆方式 | 耗时 | 传输量 |
|---|---|---|
| libra 全量 | 7.7–8.2 s | 4.94 MB |
| **libra `--filter=blob:none`（新）** | **1.20 s** | **2.30 MB** |
| git 全量 | 5.7 s | 4.22 MB |
| git `--filter=blob:none` | 0.21 s | 1.38 MB |

**mono 首次 clone 快 6.4×**。旧 libra 的 `--filter` 是"接受但吞掉"（实测与全量字节相同），
现在真正发到线上。mono 侧就绪 = 1.2 s clone + ~5–7 s attach ≈ **6.5–8 s**，
与 git 全量 clone-to-worktree（~6 s）**基本持平**。

**注意**：filtered clone 不含 blob，工作区内容由 MST/2 按需加载 ——
这是 ScorpioFS 工作区的设计前提，不是巧合红利；但需要 blob 内容的操作
必须走 snapshot 后端。

## 一、本轮完成的（已验证）

| 项 | 证据 |
|---|---|
| clone 全面恢复 | 8/8 rc=0（walk-abort 修复生效） |
| v0 filter 服务端 | git-filter 0.21 s vs git-full 5.7 s（28×），pack 小 3× |
| v0 filter 客户端 | libra-filter 1.20 s vs libra-full 7.7 s（6.4×） |
| 新基线（等深全量） | git 5.8–6.0 s vs mono 7.3–7.6 s（1.27×） |
| fixture 重建 | 50,001 文件往返 OK |

## 二、当前问题清单

### P0 — 待办

1. **6 个 monoengine 提交 + 1 个 libra 提交未推送**（GitHub 不可达：代理关闭 +
   直连 443 超时）。提交安全在本地，网络恢复即推：
   - monoengine：`1f8ad3d` `8392c86` `e653e69` `e69d3e1` `6cf95df`
   - libra：`768320921`
2. **全量 `cargo test` 仍未跑完**（后台运行中，输出缓冲未吐）。两次被 docker
   build 的资源争抢挤掉。6+1 个提交的行为验证还欠着
3. **W3 未测**：status/add/commit 的 git 并排对比

### P1 — 环境（会再咬人）

4. **存储全部 `emptyDir`**（postgres、rustfs）：任何 pod 重启 = 数据全丢。
   本轮已因此丢过一次；vault key 与空库不匹配还会启动死锁（解法已记录）
5. **ACR 公网→VPC 传播延迟分钟级**：推完立即部署必 NotFound，等 ~5 分钟自愈
6. namespace wipe 后要手工重建 `bench-script` ConfigMap 和 tool pod

### P2 — 代码遗留

7. git-internal `send_data` 的 unwrap panic（上游 0.9.0/0.10.2 都有）；
   mega2 侧已缓解，根治要上游 PR
8. 旧 fixture 的 57 s drain 瓶颈**未定案**（rustfs 饱和假说的验证因测量工具
   错误无效）；新 fixture 对象少 11×，大概率不再是主导，但无数据

### P3 — 方法论

9. 本轮累计 **6 次假设落空、3 次测量工具坏、1 次自引回归** —— 全部写入记忆。
   核心规则：先测量再优化；结果太好先验证；"回滚无效"先查数据；不降低失败宽容度

## 三、结论状态（对照 GOAL-MONOREPO-WIN）

| 判据 | 状态 |
|---|---|
| W1 全量不慢于 git ×1.5 | ✅ 1.27×；**filter 口径下就绪时间与 git 持平（6.5–8 s vs ~6 s）** |
| W2 多工作区磁盘 ≤0.2 | ✅ 约 800×（真实落盘），呈现口径 0.16 |
| W3 日常操作 ≤3× | ⬜ 未测 |
| W4 不依赖 shallow | ✅ 全量 rc=0；filter 口径已实测 |

**当前可辩护的结论**：
- **多工作区场景：数量级磁盘优势（约 800×）**
- **单工作区就绪：filter 口径下与 git 持平**（此前是慢 2.2×）
- 日常操作对比待补（W3）

## 四、下一步

1. 网络恢复 → 推送 6+1 个提交
2. cargo test 跑完 → 修复任何暴露的问题
3. 补 W3 并排测量
4. （可选）旧形状 fixture（550k 历史）复测，确认大历史下的 filter 收益
