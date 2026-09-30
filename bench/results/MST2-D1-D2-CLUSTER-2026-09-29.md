# D1/D2 集群实测：MST/2 lower 挂载读写（2026-09-29）

## 结论

**D1 全部通过。D2 8/9 通过**，唯一失败是**文档已记载的已知边界**（append），且
失败形态与文档不同（ENOSYS 而非 ENOENT），说明镜像里的 libfuse-fs 是**第三个版本**。

| 判据 | 结果 |
|---|---|
| **D1** MST/2 lower 可用 | ✅ `serving MST/2 snapshot view as the lower layer` / 挂载 50,000 文件 / 深层读正确 |
| **D2** 写路径 | ⚠️ **8/9** —— 仅 append 失败（已知边界） |
| **D3** 就绪时间比 | ❌ **不达标**：mono 107.4 s vs git 84.5 s → **1.27×**（要求 ≤ 0.2） |
| **D4** 多工作区磁盘 | ⬜ 未测 |
| **D5** 日常操作 | ✅ 1.7 s（`status` 在 50k 上） |

**D3 不达标是当前的核心问题**，原因明确：mono 侧的就绪时间 = `libra clone`(100.4 s)
+ `attach`(6.9 s)，而 `libra clone` 的 100 s 里 **81 s 是服务端生成 pack 的静默**。
两侧下载的是同一份对象，所以这不是 MST/2 的问题，是 **git 传输路径的服务端成本**。

## 实测数据

```
daemon_health              = 200
clone_rc                   = 0        clone_ms      = 100429
attach_rc                  = 0        attach_ms     = 6929
mono 就绪（clone+attach）  = 107358 ms
git  clone                 =  84537 ms     ← 同树、同网络
就绪比 = 0.787（要求 ≤ 0.2）
mount_files                = 50000      ← 完整
```

### D1 读路径

| 项 | 结果 |
|---|---|
| daemon 日志含 MST/2 lower 消息 | ✅ `scope="/project"` `snapshot=sha256:dc740285…` |
| 挂载文件数 | 50000 |
| 深层文件 `bench50k/svc00/pkg000/mod00/f00000.rs` | ✅ `// s50 0` |
| 目录遍历 | ✅ |

### D2 写路径

| 操作 | 结果 |
|---|---|
| 新建文件 | ✅ |
| `mkdir -p` | ✅ |
| lower 目录内 mkdir（copy-up） | ✅ |
| 覆盖 lower 文件（truncate） | ✅ |
| **append 到 lower 文件** | ❌ **`Function not implemented` (ENOSYS)** |
| append 后 `tail` 读取 | ✅ `// s50 1`（**无 EBADF**） |
| 删除 lower 文件 | ✅ |
| rename | ✅ |
| 3 MB 写 + stat | ✅ |

`libra status` 正确识别：`modified:` 覆盖的文件、`deleted:` 删除的文件、未跟踪的新文件。

## 关于 append 失败：这是预期内的已知边界

`MST2-LOWER-KNOWN-ISSUES.md` 记录了 O_APPEND 的失败及其修复，关键事实：
**修复写在本机 libfuse-fs checkout（36a90171f），从未提交**。

而 ScorpioFS 的依赖是 **crates.io 的 `libfuse-fs 0.3.0`**：

```
Cargo.toml:56   libfuse-fs = "0.3.0"
Cargo.lock      version = "0.3.0"  source = registry+...  checksum = 6e1f5135…
```

所以镜像里没有那个修复。

**但失败形态变了**，这本身是信息：

| 版本 | append 的错误 |
|---|---|
| 文档记录的修复前 | `No such file or directory` (ENOENT) |
| 文档记录的修复后 | 成功 |
| **镜像实测** | **`Function not implemented` (ENOSYS)** |

ENOSYS 意味着路径走到了一个**未实现 `write` 的层**。结合 `tail` 不再 EBADF，可以判断
镜像是**另一个不同的状态**，不是文档描述的两端任何一个。要彻底定位需要在集群里对
`scorpio` 做 `strace`，并以日志级别控制输出量。

**影响范围**（与文档一致）：读、浏览、目录操作、新建、覆盖、删除、rename 都正常；
只有**追加写**到 lower 已有文件失败（编辑器保存、`>>`、部分构建产物）——这在
"工作区就绪"和"多工作区"两个关键指标上**不构成阻塞**，但会影响日常编辑体验。

## 顺带发现：scope 必须是仓库根

探测中间有一次把 `SCOPE` 设成 `/project/bench50k`，结果：
**挂载根就是 scope**，而 libra 索引路径是**仓库相对**的（`bench50k/svc00/…`）→
挂载里看不到任何索引路径 → `status` 把 5 万文件**全部报成删除**。

这是**会误导使用者的静默错误**。建议：daemon 应在 `mst2_scope` 与仓库根不一致时给出
显式告警（规范 15 §3 的"显式模式、无静默回退"精神）。当前 `/project` 恰好就是 50k 树，
所以用 `/project` 是正确的。

## 复现方式

```bash
# 探针 Job（privileged + /dev/fuse + podAntiAffinity 到 mega2）
kubectl apply -f scorpiofs/bench/infra/k8s/mst2-probe.yaml
kubectl -n gitmono logs job/mst2-probe
```

探针脚本：`scorpiofs/bench/infra/k8s/mst2-probe.sh`
（含 3 个前置校验：daemon 健康、clone rc、lower 文件存在性——每个都对应一次踩坑）

## 未完成

- **D3 就绪比**：需要先解决服务端 81 s pack 生成（见缓存规划）
- **D4 多工作区磁盘**：`fork` 路径尚未在集群里测
- **D5 完整**：只测了 `status`（1.7 s）；`add`/`commit` 未测
- append 的 ENOSYS 需要在集群内 `strace` 定位，才能判断是哪个版本、修的是什么
