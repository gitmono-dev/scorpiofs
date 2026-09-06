# ScorpioFS 的版本化只读层设计

这份文档是设计入口。ScorpioFS 的职责可以概括为：

> 从 Mega 取得一个完整 monorepo 版本 ID，并让一个 workspace 在整个生命周期内始终读取这个版本。

ScorpioFS 不决定各 Git 仓库发布哪个 commit，也不重建 Mega 的历史挂载关系。Libra 继续负责 Git refs、index、commit 和 push；ScorpioFS 负责文件投影、惰性读取、缓存和 writable upper。

## 当前问题

现有 Dicfuse readonly 层按路径访问 Mega，但路径背后的 import 可能已经更新：

~~~text
build 开始：
  /project/app       = 主仓 M1
  /third-party/lib   = import I1

运行期间 Mega 更新 lib 到 I2

如果 readonly 层读取 latest：
  已缓存文件仍来自 I1
  第一次访问的文件可能来自 I2
~~~

这个 workspace 已经不是任何真实发布过的 monorepo 状态。

## 挂载时固定一个 Mega 版本

~~~text
workspace
└── Mega 版本 V100
    ├── 主仓 root M1
    ├── /third-party/lib → I1
    └── /toolchains/rust → R7
~~~

此后所有路径查询和对象读取都使用 V100 中固定的来源和 commit。Mega 发布 V101 后，原 workspace 仍读取 V100。

V100 和 V101 可以同时挂载：

~~~text
build-old → workspace A → V100
build-new → workspace B → V101
~~~

内容相同且授权兼容的缓存对象可以复用，但会改变路径解释的状态不能共享。

## 如何读取路径

1. 在固定版本的挂载表中判断路径属于主仓还是 import；
2. 取得该来源的固定 commit 和 root tree；
3. 从 root tree 沿相对路径遍历；
4. 向 Mega 请求精确 tree/blob；
5. 本地验证 Git 哈希后再缓存和返回。

不能查询当前 branch。Mega 返回错误时不能退回 legacy latest，也不能伪造空文件或空目录。

目录列表也固定在同一个版本。打开的 directory handle 和分页 cookie 不能跨版本使用，否则两页结果可能来自不同挂载表。

## Dicfuse、Antares 与 Libra

~~~text
Libra
  Git HEAD / index / commit / push
                │
                ▼
ScorpioFS / Antares
  workspace 生命周期、readonly lower、writable upper
                │
                ▼
Dicfuse lower
  按固定 Mega 版本惰性读取
~~~

Dicfuse lower 不需要自己实现 Git 版本管理。它只持有 Mega 版本，并保证 lookup、readdir、readlink 和 read 使用同一个版本。

Antares 创建 job mount 时固定版本。CL/upper 记录自己的 base 版本，避免未提交修改在不知情时换到另一套依赖。.libra 持久状态仍由 Libra 管理，不放进 ScorpioFS upper。

## 缓存隔离

对象缓存至少区分：访问域、对象类型、哈希算法和 OID。路径及目录元数据还要绑定版本 ID，因为同一路径在不同版本可能属于不同 source。

这样相同 blob 可以安全复用，V100 的目录和负查询不会污染 V101，私有 source 的 CAS 命中也不会造成越权。TTL 到期只触发验证或下载，不会改读新 branch。

固定远端版本不等于全部文件已下载。offline pin 只保护已经缓存且验证过的对象；完全离线需要显式导出并验证完整内容。

## workspace 更新

推荐的第一阶段语义是受控切换，最终决定仍待确认：

~~~text
运行中的任务继续使用 V100
新任务直接使用 V101
已有 workspace 更新时：
  准备 V101 lower
  暂停 workload
  检查 dirty upper、打开句柄、mmap、cwd 和进行中的请求
  持久化 PREPARED
  切换并验证 mount
  持久化 COMMITTED
  恢复 workload
~~~

如果不能证明 workload 已暂停，则返回 busy，让调用方创建新 mount 并重启任务。透明 live switch 还需定义旧 FD、mmap、inode、page cache 和新路径分别使用哪个版本，首版不能在这些语义未完成时宣称支持。

崩溃恢复只认持久化记录：只有 PREPARED 时恢复旧版本；已有 COMMITTED 时恢复新版本；日志损坏时进入可恢复失败状态，不猜测 latest。

upper 有修改时默认不切换。Libra 负责 commit 或形成 CL；ScorpioFS 只在收到可验证结果后清理仍匹配原记录的 delta。

## API 轮廓

现有 /antares 保持兼容；版本化 workspace 使用新 v2 请求，核心输入是 job ID、mount path 和 Mega view ID。相同 job ID 加相同请求是幂等重试；相同 job ID 加不同 view 返回冲突。

更新分为 plan 和 execute。plan 展示变化及 dirty/busy 检查；execute 带期望旧版本、期望 workspace 代次和幂等操作 ID，并在切换屏障内重新检查。

## 安全与失败

snapshot 功能默认关闭。只有 Mega 声明服务就绪，且 ScorpioFS 配置授权凭据、租约和允许的服务地址后才能使用。

无权限、版本过期、路径不存在、对象不可用、哈希损坏、workspace dirty/busy 和期望版本变化必须分别报告。任何情况都不能转换成 latest、空文件或空目录。凭据和 lease header 不进入日志，HTTP 不跟随到其他 origin 的重定向。

## 当前状态

已实现并测试：与 Mega 一致的版本清单编码、固定 source reader、tree/blob 哈希验证、路径/字节限制、可执行位和 symlink 语义，以及固定上下文的 HTTP 客户端。当前共有 24 个 snapshot 单元和 HTTP fixture 测试。

仍未完成，因此 PR 保持 Draft：获取真实完整版本及挂载表、接入 Dicfuse/Antares lower、按版本和访问域隔离缓存、lease/offline pin、更新日志和恢复、真实双版本 FUSE 挂载及 Libra 联调。

## 详细资料

- [完整客户端实施 Spec](spec/monorepo-versioning.md)
- [共享版本清单编码](spec/namespace-manifest-v1.md)
- [单 source 快照契约](spec/source-snapshot-v1.md)
- [系统论文路线](spec/system-paper-spec.md)
