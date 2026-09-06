# Mega 命名空间版本与 Dicfuse 不可变视图 Spec

> 第一次阅读请从 [ScorpioFS 的版本化只读层设计](../versioned-readonly-layer-design.md) 开始；本文保留实现级协议和验收细节。

状态：Draft v0.4，2026-09-06。D1（完整 native + import 原子组合视图）、D2（显式 release 目录发布后不可变）与 D4（安全启用门槛）已获用户确认；D3 待确认（§12）。文中的 MUST 是目标协议要求，不代表现有实现。命名空间协议由 [#55](https://github.com/gitmono-dev/scorpiofs/issues/55) 跟踪，本文细化 [#42](https://github.com/gitmono-dev/scorpiofs/issues/42)，约束 #43、#44、#49、#50、#51、#53。总路线见 [system-paper-spec.md](system-paper-spec.md)。Mega 侧配套实施草案位于该仓库的 `docs/spec/namespace-snapshot-spec.md`，细化 G01–G06 与 MG01–MG17；两仓共享且已验证的内容身份编码见 [namespace-manifest-v1](namespace-manifest-v1.md)，它不等于已实现实际挂载或原子发布。

当前实现进度：已增加严格 source identity、不可变 SourceReader 库层和 source-aware HTTP 客户端适配器，跨仓黄金向量、固定对象读取及本机 HTTP 测试见 [source-snapshot-v1.md](source-snapshot-v1.md)。尚未接入实际 Mega snapshot HTTP 服务、Dicfuse/Antares 挂载、lease/CAS 或工作区切换；现有挂载因此仍不具备本文承诺的版本隔离。完整 namespace 发布及所有写入者覆盖同样尚未完成。

## 1. 问题与事实基线

只读权限不提供版本隔离。Dicfuse 必须固定“某个路径由谁提供、提供哪个不可变对象”，否则一次 build 会混读不同时刻的文件。整个挂载树也不一定属于同一个 Git revision。

本次静态核对：ScorpioFS `6a2e7f2ddb7d913b497167f76ff6539bfbb983c2`；Mega `c4c79bc195541a13ac1505b94728c81a8ff3d603`。这是上游代码基线，尚未验证用户正在运行的 Mega 服务端版本、数据库或配置。

| 已验证的行为 | 源码依据 | 对设计的影响 |
| --- | --- | --- |
| import 根目录由配置指定，默认 `/third-party`；其余默认根目录有 `project/doc/release/model/toolchains` | [Mega config](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/config/config.toml#L50) | 分类使用服务端登记的归属，不能只看目录名字 |
| REST 根据 import 根和已登记仓库路径切换 Mono / Import handler | [api_handler](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/mono/src/api/mod.rs#L82) | 路由表也是快照的一部分 |
| import 仓库按路径组件寻找最长匹配；新注册检查父子仓库嵌套冲突 | [git_db_storage](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/jupiter/src/storage/git_db_storage.rs#L351) | 不把任意目录都当独立 repo，不靠字符串 starts_with 匹配 |
| 原生目录的 refs 表以 `(path, ref_name)` 标识引用；更新链逐层重建父 tree，并可产生不同 scope 的 commit | [mega_refs](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/jupiter/callisto/src/mega_refs.rs#L9)、[tree update](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/ceres/src/application/api_service/mono/logic/tree.rs#L88) | 子目录 commit 与全库 commit 的根路径不同，必须携带 scope |
| Mono `get_root_tree` 可接受 40 位 commit OID 或 tag；空 refs 取 `/` 当前主引用 | [Mono service](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/ceres/src/application/api_service/mono/service.rs#L120) | 不是所有服务端读接口都缺历史能力；缺的是统一、完整的固定版本读路径 |
| Import `get_root_tree` 忽略传入 refs，使用默认 ref | [Import service](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/ceres/src/application/api_service/import_api_service.rs#L117) | 添加 `?refs=A` 不能保证 import 返回 A |
| 二进制 tree API 只接收 path 和可选 oid；内部先查当前 path，oid 仅校验是否匹配 | [TreeQuery](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/ceres/src/model/git.rs#L55)、[tree_ops](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/ceres/src/application/api_service/tree_ops.rs#L40) | 当前 oid 参数不是历史 tree 寻址接口 |
| import receive-pack 通过创建路径/占位 `.gitkeep` 树连接总目录，同时更新 import refs；不是把完整 import 内容树直接接入原生树 | [import attach](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/ceres/src/application/code_edit/post_receive/import.rs#L78) | 主仓 root OID 不足以重建完整 import 内容及其历史挂接 |
| 原生与 import 元数据共享数据库事务入口 | [Storage](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/jupiter/src/storage/mod.rs#L327) | 组合发布可加入同一事务；对象存储可读性仍须单独保证 |
| import 网页编辑独立更新默认 ref；receive-pack 准备阶段可先登记新仓库 | [web edit](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/ceres/src/application/api_service/import_api_service.rs#L374)、[registration](https://github.com/gitmono-dev/mega/blob/c4c79bc195541a13ac1505b94728c81a8ff3d603/ceres/src/transport/protocol/mod.rs#L168) | publisher 不仅覆盖 push；登记表不等于成功发布的 binding |
| Dicfuse metadata fetch 只传 path，实例按 store/path 复用，内容按 inode 持久化 | [store](https://github.com/gitmono-dev/scorpiofs/blob/6a2e7f2ddb7d913b497167f76ff6539bfbb983c2/src/dicfuse/store.rs#L493)、[manager](https://github.com/gitmono-dev/scorpiofs/blob/6a2e7f2ddb7d913b497167f76ff6539bfbb983c2/src/dicfuse/manager.rs#L47)、[content store](https://github.com/gitmono-dev/scorpiofs/blob/6a2e7f2ddb7d913b497167f76ff6539bfbb983c2/src/dicfuse/content_store.rs#L28) | 需要同时改变读接口、命名空间和缓存身份 |

结论：不能把方案简化成“每个目录有一个版本号”，也不能把一个主仓 commit 直接当作所有 import 仓库的 commit。

## 2. 目录分类与版本来源

| 类型 | 示例（示意路径） | 应固定的身份 | 更新规则 |
| --- | --- | --- | --- |
| 原生普通目录 | `/project/a/src`、`/doc`、`/toolchains` | 选定原生 root tree 下的 subtree | 随新的原生视图发布；普通目录无需独立 revision |
| 可独立 clone 的原生 scope | `/project/a` | 原生 source + `scope_path` + commit + scope tree | 独立 checkout 使用 scope commit；全库视图默认从同一个全库 root 派生 |
| import 独立仓库 | `/third-party/vendor/lib` | 稳定 repo ID + commit + tree + hash algorithm | branch/tag 只在 resolve 时解析一次；后续按对象读 |
| import 中间/聚合目录 | `/third-party`、`/third-party/rust/crates` | 原生目录树 + 固定挂接目录索引 | 子仓新增、删除、换路径、版本变化必须生成新 namespace view |
| 版本号命名的 import | `/third-party/rust/crates/…/1.3.0` | 同上，并记录发布策略 | 版本字符串不等于内容哈希；是否允许替换由 D2 决定 |
| CL / 候选变更 | `/project/a` 的 `refs/cl/X` | 基础 view + CL scope + base/head OID + delta digest | CL 移动生成新候选视图；未合并 CL 不推进默认全库视图 |

`release/model/toolchains` 是名字，不天然代表第三种对象存储。只有服务端显式注册为新 source 类型时才启用新 adapter。Git submodule、LFS 指针和外部 artifact 也不能凭目录名称自动展开：M1 返回原始指针/声明 unsupported；将来展开时，materialization policy 及对象 digest 必须进入身份。

## 3. 拟议身份模型

使用三个层次，避免把 commit、内容身份和工作区生命周期混成同一个 `generation`：

```text
SourceSnapshot       一个版本域里的 commit/tree，以及它对应的路径 scope
NamespaceView        原生 snapshot + 不可变挂接索引 + 显式覆写规则
WorkspaceGeneration  工作区使用的 view、delta 序号与切换事务
```

### 3.1 SourceSnapshot

```rust
// 协议草图；ObjectId/SourceId 均为有验证器的类型，不接受任意字符串。
struct SourceSnapshot {
    source_id: SourceId,       // persistent UUID mapped to instance/backend/repo
    scope_path: RepoPath,      // 已验证的投影根对应的命名空间位置
    commit_oid: ObjectId,
    root_tree_oid: ObjectId,   // commit 的 tree；scope 映射经服务端验证
    object_format: ObjectFormat,
}
```

`/project/a` 的 scope commit，其 tree 根已经是 `a/` 的内容；读取 `src/lib.rs` 时不能再向它附加 `project/a/`。全库 root commit 的同一路径则要从 `/` 遍历。服务端 MUST 返回并验证 scope，不能仅凭一个存在的 commit OID 猜测。

若服务端从已证明的 native source 派生子目录 descriptor，commit_oid 保留 base commit provenance，root_tree_oid 是从固定 base tree 验证得到的 subtree，不要求再次等于 commit.tree。它与直接 scope commit 的证明类型不同；两者即使投影内容相同，provenance identity 也可能不同。

Mega 当前 `mega_commit` 没有 scope 字段，因此需要持久化 `(source_id, scope_path, commit_oid) → root_tree_oid + proof`；同一 commit 可有多个有效 scope，不设唯一反向映射。子 scope ref 被清理不能丢失历史证明；存量 commit 没有证明时返回 `SOURCE_SCOPE_UNVERIFIED`，不能默认它属于 `/`。clone 派生 scope commit/证明本身不代表全库可见树变化。

revision selector 使用带类型联合：`published_view`、`source_commit`、`source_ref`。branch/tag 解析结果带完整 ref 名和最终 commit；裸 tree 请求必须显式声明 `tree` 类型，不能伪装成 commit。当前 Mega 的 SHA-1 限制作为 capability 返回；协议保留 SHA-256 类型但不得提前宣称支持。

单 source 的 v1 校验器和 canonical 编码现已落地，详见 [source-snapshot-v1.md](source-snapshot-v1.md)。该基础实现不等于 namespace 发布、leases 或 FUSE 集成已完成；view/index 的编码仍在各自实施闸门内。

### 3.2 NamespaceView

```rust
struct NamespaceView {
    schema_version: u32,
    instance_id: InstanceId,
    native: SourceSnapshot,
    bindings_root: Digest,          // 持久化目录 trie/Merkle index 的根
    overrides_root: Option<Digest>, // 显式候选 scope 的替换记录
    materialization_policy: Digest,
}
struct Binding {
    mount_path: RepoPath,
    source: SourceSnapshot,
    source_subpath: RelativePath,
    policy: BindingPolicy,         // 固定目标 + 发布约束
}
```

`view_id = sha256(canonical_descriptor_bytes)`。序列化规范固定 schema、字段顺序/编码、路径字节顺序、可选项表达和 object type；不纳入时间戳、租约、可移动 ref 标签。实现先给 golden vectors，再选择一种确定性编码；JSON 示例只作可读表示，不能直接 hash 任意 JSON 文本。

为保证两次不同 commit 但相同 tree 能共享数据，另设 `projection_key`：由实际可见内容图、路由、scope、object format、materialization policy 和有效访问域决定；commit provenance 独立保存在 view。两者不可互相替代。inode 分配、stat 的合成属性策略也须稳定，或进入 projection key。

发布记录另存 `{publication_seq, view_id, parent_view_id, reason, created_at}`。seq 只比较同一实例的发布顺序；view_id 用于重放，workspace generation 用于条件更新，delta_seq 用于修改增量查询。

### 3.3 大规模目录索引

不能为每次 mount 枚举全量 import refs。挂接索引是不可变、可分页、按前缀查询的持久化树；改变一个 import 只重写该索引的祖先节点。挂载拉取 view 根及所需路由分支，未访问对象保持惰性。

首次导入既有目录表可做一次受控 O(R) 建索引（R 为登记仓库数）。此后更新成本按变动绑定数和索引深度增长。测试必须包含 large-R/small-working-set，不能只用 64 个小 repo 宣称可扩展。

节点 fanout/大小也必须有界；若根节点内嵌百万 child，即使“只写祖先节点”仍会 O(R) 重写。Mega 草案采用受限分支的持久化 byte-radix trie，支持 prefix seek 与分页，并计量节点/字节读写放大。canonical descriptor/index 编码及 golden vectors 在 G01 冻结后才能宣布 v1 capability。

## 4. 读取规则与边界

1. mount 先 resolve 一个 view 并获取保留租约，成功后才公开可访问路径。
2. lookup 在该 view 的固定索引内定位路径归属，按路径组件匹配；不得再次查询实时 `git_repo` 表来改变归属。
3. 普通目录从选定 native tree 下行；遇到 import 边界切换到 Binding 中固定的 source tree。边界视图替换占位内容，不能把占位 `.gitkeep` 与真实 repo 根随意合并。
4. parent readdir 合并原生名字和 view 索引的边界名字；同名冲突必须是声明过的挂接替换，否则 resolve 拒绝。历史 view 不出现之后新登记的仓库。
5. directory handle 绑定 view/projection，readdir cookie 仅对该 handle 有效。tree/blob 读取只使用 source + immutable OID；临时错误不得回退到 latest。
6. 完整性验证区分 Git blob OID 和裸内容 digest。Git 对象哈希包含对象类型与长度头；CAS 不通过“内容看起来像 Git 头”自动剥离真实文件前缀。
7. Git tree 不携带 blob 长度。size 从同 OID 的 metadata/size index 或对象头取得；缺少 length 不能把非空文件报为零。权限 mode、symlink target、可执行位进入 oracle。
8. 路径以无歧义的组件编码表示，v1 使用 UTF-8 组件，不做大小写折叠/Unicode 归一化，拒绝 NUL、`.`、`..`、重复分隔和越界路径；非 UTF-8 名字在 capability 中明确拒绝。未来 byte-path 支持单独版本化。symlink 解析不能借路径路由突破 workspace 根。
9. 在不可变 view 内 TTL 只影响缓存回收/重试，不触发读新 branch。`mutable/latest` 兼容模式使用独立 namespace，且不宣称快照一致。

同一个 `base_path` 的 view A/B MUST 允许同时存在。一个 workspace 的更新不得修改其他 workspace 正在使用的 `Arc<Dicfuse>`。

## 5. 服务端发布与历史保留

### 5.1 推荐：Mega 发布全库组合视图（D1 待确认）

一次发布包含原生主仓 root 和固定 import 绑定，必要时还包含候选 scope 的显式覆写。发布流程：

```text
保存并校验新 objects
  → 基于 expected published view 构建新 native tree / bindings index
  → 验证对象可读并建立保留根
  → 数据库事务 CAS 更新 published_view 指针及关联 ref/映射
  → 提交后发送更新事件
```

published_view 指针是全库读取的线性化点。序列号可以有空洞，不能重复/倒退。对象准备失败不发布；事务失败留下的孤立对象稍后 GC。事件采用事务 outbox 或等价机制，丢失通知只影响及时性；客户端仍通过期望 seq 拉取恢复。

原生 merge、import 同步、登记/删除/重命名、默认分支变化、工具链修改等每条可见写路径都必须参与同一 publication contract。当前部分操作已有事务或 CAS 不等于全库协议已完成。跨数据库/异步对象存储场景不能用“顺序读取几个 HEAD”冒充原子快照；先完成对象可达性，再发布唯一指针。

当前 Mega 原生/import 元数据可用同一个应用 DB transaction；应在该事务内更新 refs、bindings、view/head、scope proof、operation receipt 与 outbox。除 published head CAS 外，每个被修改 ref 都必须验证 expected-old；现有 `update_ref_in_txn` 没有该参数，不能仅依赖原生 root CAS。网页编辑固定一个 base 生成 tree 与 parent，避免先读旧 tree 后读新默认 ref。响应丢失通过 operation 查询确定结果，事件失败不反向宣称内容事务回滚。

新 import 的提前登记是 staged，不是默认视图中的公开 binding；失败 unpack/无有效默认 commit 不发布空目录。当前 transport 已拒绝删除默认分支，需保留并在事务内对所有入口重验。当前分支 report-status 位于 finalize 之后，publisher 必须保持在此成功边界内；tag 提前持久化的行为单独回归，不擅自扩展为整个 push 的原子承诺。

默认 import branch 的哪个 tip 对外发布应有固定策略。向非默认 branch 推送不会自动替换默认全库内容。source ref 变更可与正式发布分离，但必须显示 `staged/unpublished`；只有发布成功的 view 可被 `latest` 返回。写事务如果宣称同时更新主仓与依赖，则二者必须在同一个新 view 内生效。

这里“不替换内容”不等于现有实现绝不产生新 native commit：当前 import attach 可能额外创建 root commit；如果保留该行为，provenance 变化仍需发布新 view，即使 projection 可复用。未来消除这种额外 root 写入作为独立优化。selected ref 根据整个成功批次之后的状态选择，不取第一条 push command。

### 5.2 存量兼容与能力分级

| 能力 | 可以承诺什么 | 不能承诺什么 |
| --- | --- | --- |
| `source-snapshot.v1` | 单一 source/scope 的历史 tree/blob 一致 | 一个全库 root 自动覆盖 imports |
| `view-lock.v1` | 持久化的显式组合可重放 | 各 ref 恰好来自同一全局时刻 |
| `namespace-snapshot.v1` | 经服务端发布的完整 namespace view | 未登记在 view 中的外部资源也被固定 |

迁移初期先获得当前数据库的一致读快照，冻结 native root、repo 路由和已选择的 import commits，保存为初始 view。对旧历史主仓 commit，如果没有当时的挂接索引，MUST 返回 `HISTORICAL_BINDINGS_UNAVAILABLE`；不能拿今天的 import heads 补齐后声称是历史全库版本。

首版回填推荐受控维护窗口：暂停相关元数据 writer，验证并建立初始索引、原子 cutover，再放行全部已接入 publisher 的 writer。普通 DB begin/分页扫描不自动等于一致读快照。旧二进制/直接改表脚本未被隔离前不宣布 namespace capability；更大规模在线回填另做 changelog/catch-up 方案。回退保留已分配 view 的读服务与租约，不能将这些 view_id 重新解释为 legacy latest。

若只能先做客户端 lock，返回 `consistency=explicit_composition`，记录各 source 的解析结果。它是可复现组合，不是服务端原子发布；仍需固定 routing，禁止混用实时 registry。此降级作为独立模式而非静默 fallback。

### 5.3 保留/GC

view manifest 永久存在不等于历史内容永久可读。Mega 必须按 source snapshot 保留 tree/blob 的可达闭包（以及显式开启时的 LFS/artifact 对象），允许租约续期，并给出历史保留期。pin 不要求预下载全仓库。

GC 与 lease 创建/续期须协调，防止有效续租对象被并发删除；按 pack 存储时还要保护包含有效对象的 pack 及 delta 解码依赖，或安全 repack。已存在的 artifact GC 不是 Git snapshot GC 的证明。lease 只保留对象，不绕过当前鉴权；撤权可失败，不能改读另一个版本。

ScorpioFS 持有 active workspace、open handle、refresh prepare 的引用；服务端依据 view lease/历史保留策略阻止对应对象回收。租约失效后禁止承诺未缓存数据可用；离线完整重建只在可达闭包已导出并验证时支持。

本地 CAS 的活动 read lease 防止 eviction 竞争；显式 offline pin 保护已缓存对象。已固定远端快照但尚未下载的对象不计为本地驻留空间；预算不足明确返回 `CACHE_PIN_BUDGET_EXCEEDED`，不无界增长。

## 6. 拟议 API（尚未实现）

### 6.1 Mega：发现、固定与读取

| API | 请求关键字段 | 响应/约束 |
| --- | --- | --- |
| `GET /api/v1/snapshots/capabilities` | 服务端发现 | instance/schema/算法/路径编码、source/namespace readiness、retention 限制 |
| `POST /api/v1/snapshots/resolve` | selector、scope、expected publication（可选） | view/source descriptor、resolved commit/tree、consistency、lease；symbolic ref 只解析一次 |
| `GET /api/v1/snapshots/{view_id}` | 固定 view_id | 同一 ID 内容永远一致 |
| `GET /api/v1/snapshots/{view_id}/bindings` | prefix、cursor | 固定索引分页；cursor 绑定 view/prefix，不能混页 |
| `GET /api/v1/snapshots/{view_id}/tree` | path、cursor | 依据固定路由的 entries，含 source、tree OID；区分不存在与空目录 |
| `GET /api/v1/sources/{source_id}/trees/{oid}` | 同 descriptor 的 source 和 tree OID | 原始 tree 或有验证关系的结构化 entries；不解析当前 refs |
| `GET /api/v1/sources/{source_id}/blobs/{oid}` | object format、必要的授权上下文 | 精确字节与 size；不因其他 repo 命中缓存而跳过授权 |
| `POST /api/v1/snapshots/{view_id}/leases` | client token、期限 | 创建/续期 pin 的不透明租约 |
| `DELETE /api/v1/snapshot-leases/{lease_id}` | lease_id | 幂等释放；不删除仍被其他引用保留的 view |
| `GET /api/v1/snapshot-operations/{operation_id}` | actor-domain 内的 operation_id | 查询已提交结果，恢复未知响应；不允许跨用户枚举 |

source ID 是稳定、不透明、可 URL 编码的标识，不是供客户端传任意 URL 的位置。resolve 内部需要校验 source/commit/scope 对应关系。按对象读取也必须约束到有权访问的 source/view。

可用固定 tree walk 生成的 object ticket 或等价可验证上下文证明 scope/root 到 OID 的可达性，初始 root 由 resolver 验证；不能以客户端给出任意裸 OID 或全局 CAS 命中代替授权。证明机制应惰性展开，不要求 mount 时全树遍历；ticket 不授予超越当前 ACL 的访问权。

错误统一 `{code,message,retryable,details}`。400 参数/路径错误；403 未授权（隐藏存在性政策可统一 404）；404 不存在；409 `EXPECTED_VIEW_MISMATCH` / `REF_MOVED` / `SOURCE_SCOPE_MISMATCH` / `SOURCE_SCOPE_UNVERIFIED` / `BINDING_CONFLICT` / `IMMUTABLE_BINDING` / `DEFAULT_REF_REQUIRED`；410 `SNAPSHOT_EXPIRED`；422 `HISTORICAL_BINDINGS_UNAVAILABLE`；501 capability 不支持；503 `OBJECT_UNAVAILABLE` / `PUBLICATION_NOT_READY`。旧接口的“空数组/空 body”不能用于表示这些失败。

### 6.2 ScorpioFS：版本化工作区

新接口拟放在 `/antares/v2` 下，保留当前 `/antares` v1 的字段/语义。

创建 mount 请求（值为示意占位符）：

```json
{
  "job_id": "build-a-001",
  "mount_path": "/",
  "base": {"kind": "published_view", "view_id": "sha256:VIEW_A"},
  "mode": "immutable",
  "state_owner": "scorpiofs"
}
```

成功响应包含 `workspace_id, view_id, projection_key, generation, delta_seq, resolved_sources, mountpoint, readiness, state_owner`。大规模 sources 返回分页引用，不在每个 status 重复完整列表。同 job_id + 相同规范化请求幂等；不同请求返回 409。

`readiness` 区分 `identity_resolved`、`mount_accessible`、`prefetch_complete`。lazy mount 对外可访问只要求身份和根已验证；完全预取不应成为固定版本挂载的必要条件。延迟对比必须使用同一种 readiness。

更新计划包含 `expected_generation, expected_view, target_view, expected_delta_seq`；返回 changes summary、dirty/busy/owner checks、expiry。plan 本身不是锁，执行时必须在屏障内重验。

真正切换请求另有 `operation_id`（幂等键）、owner token、期望状态及 plan digest。相同 operation_id 不同 payload 返回 409。长期操作返回 202 和 operation URL；GET operation 可恢复未知响应结果。API MUST 同时返回 committed state 和是否已恢复 workload，不混淆两者。

## 7. 更新语义

### 7.1 日常版本前进

```text
Mega 发布 V1 → 新工作区 W1 绑定 V1
Mega 发布 V2 → 新工作区 W2 绑定 V2；W1 继续使用 V1
W1 显式请求 update → 检查/暂停 → 新 generation 使用 V2
```

原生子目录发生变化时只需新 native tree 和祖先节点；未变 subtree/OID 继续复用。import branch 更新改变其 Binding 和索引根；没有改动的 source snapshot 与 blobs 继续复用。新增/移除 import 也是 view 变更；旧 view 保留旧名字及来源。

全库 view 中一般从同一个 native root 派生所有原生子目录。若指定 `/project/a@A` 和 `/project/b@B` 混搭，必须是显式 `overrides_root` 描述的候选组合，不能伪称一个原生 root commit。路径覆写不能穿过另一个 source 边界，冲突在 resolve 时拒绝。

下面用符号 OID 展示更新结果，不表示实际部署内容：

| 已发布视图 | native root | `/project/a` | `/third-party/vendor/lib` | 使用者 |
| --- | --- | --- | --- | --- |
| V100 | M0 | M0 派生的 tree A0 | repo R 的 commit I0 / tree T0 | 旧 build 固定在此 |
| V101（只改 a） | M1 | M1 派生的 tree A1 | 仍是 I0 / T0 | 新 build 可选择 V101 |
| V102（只更新 import） | M1 | 仍是 A1 | I1 / T1 | 显式 update 的任务选择 V102 |

V100 即使第一次访问 import 发生在 V102 发布后，也必须下载 I0/T0 的对象。若新增 `/third-party/newlib`，只有包含新 binding 的视图能列出它。A1/I0 和 A1/I1 都是有效组合，但只有发布记录决定哪一份是该时刻的默认视图；不靠访问先后顺序决定。

### 7.2 第一阶段 refresh：暂停后切换（D3 待确认）

1. 验证 target view、取得租约并准备一个独立 target lower。读旧 workspace 仍可继续。
2. 由 state owner 取得工作区 mutation lock 和 workload 停止/暂停屏障；检查普通 FD、directory FD、mmap、cwd/root 引用、活跃写入与 FUSE 请求。无法证明受控访问时返回 `WORKSPACE_BUSY`，采用创建新 mount + 重新启动任务的方式。
3. 在屏障内重验 expected generation/view/delta_seq。普通 checkout 要求 clean；dirty 返回 `DIRTY_WORKSPACE`。`.libra` 控制数据排除，但不能因此忽略源码和生成文件的修改。
4. 持久化 PREPARED 日志，包含旧/新 view、operation_id、目标 mount 配方和保留的 upper 路径。fsync 文件及父目录后继续。
5. 在外部不可访问期间完成 mount 替换/重建并运行树身份 probe。缓存失效必须包括负 dentry、attr、page cache；不能仅替换一个用户态指针就宣布完成。
6. 验证成功后 fsync COMMITTED 记录；它是恢复选择新 generation 的唯一依据。对调用者恢复访问前，控制面与实际挂载 identity 必须一致。
7. 解除屏障并返回新状态，异步释放旧 lower/租约。失败保留 upper 和可解释的 recovery 状态。

只有 PREPARED：重启时按旧 view 重建；有有效 COMMITTED：按新 view 重建。介于 mount 变更与 durable commit 的窗口对 workload 必须不可访问。日志损坏无法判断时进入 `FailedRecoverable`，不猜测 latest。进程 crash 和整机断电的测试分开，普通未 fsync 应用写入不额外承诺持久性。

若选择 kernel OverlayFS，不得改变仍在使用的 lowerdir 内容；不得把旧 upper/workdir 同时挂到两个 overlay。默认 clean refresh 建立新 upper/workdir（控制元数据另管），旧目录保留到事务结束。dirty commit 清理路径需 §7.3 和 delta manifest 配合，不能盲目复用 origin/index/redirect 元数据。

透明 live-refresh 延后为单独 capability：旧 handle/mmap 绑定旧 generation，新 path resolution 绑定新 generation，还需处理 cwd、共享 writable upper 与 kernel caches。不能因实验的普通 open/read 通过就宣称所有 POSIX 访问语义成立。

### 7.3 Commit、CL 与 delta 清理

Libra 负责 refs/HEAD/index、commit、merge/conflict。Mega 负责验证 source/scope 和发布 namespace view。ScorpioFS 负责按已解析 identity 提供文件树。

commit 成功不等于全库 main 已更新：未合并 CL 或尚未发布的 source commit 使用候选 descriptor；其父 view、scope、base/head OID 都固定。CL overlay 的 read-only 层必须记录相同 base view，不能在 CL 编辑中悄悄更换基础依赖。

清理已提交 delta 必须以 `{path, entry_seq, committed_oid, mode}` 条件匹配。先 prepare candidate base，再验证 commit 对象存在/可提供，执行受控切换，持久化 commit receipt，最后只清理仍匹配的条目；期间新增修改全部保留。删除的 tombstone 同样带版本。Libra 的 HEAD/index 与 ScorpioFS 的 mount 不是一个数据库事务：使用唯一 state owner 的持久化 saga/恢复记录，直到双方可验证一致才允许新 workload。

跨不同 source 的 rename/hardlink v1 返回 `EXDEV`；跨原生普通目录的语义由同一 native scope 的 POSIX 实现负责。原生 scope 与 import 之间的跨域原子 commit 不在第一阶段范围。

## 8. Delta、存储和请求调度的联动

每个 delta entry 至少保存 `path, kind, base_view, source_id, base_oid, source_path, entry_seq`。`content_oid` 在 write 完成/稳定检查点后才可确定，不能在首次 copy-up 时冒充最终内容。rename-directory/opaque whiteout/hardlink 必须有专项语义测试。

更新 manifest 与 upper 不是天然原子：先写 mutation intent，再做文件操作/必要 fsync，最后提交 manifest；崩溃后只 reconcile 未完成 intent 涉及的路径，严重不一致才全扫描。通过 FUSE 之外修改 upper 的场景不得宣称 event index 完整；检测到不可信状态就阻止 clean 判定并修复。

存储分开：

```text
objects/<access-domain>/<type>/<algorithm>/<oid>  verified bytes
views/<view_id>                                immutable descriptor
bindings/<digest>                             immutable routing index
metadata/<projection_key>/                    immutable nodes
workspaces/<id>/state + journal + delta        mutable control state
workspaces/<id>/upper                          private files
```

对象 CAS 原子发布使用临时文件→流式校验→fsync→同文件系统 rename→目录 fsync，配额包含 in-flight 临时字节。原有 inode-keyed db 与新 schema 隔离重建，不原地解释为新对象缓存。

FetchKey 至少包含访问域、source/对象存储身份、类型、算法、OID。physical CAS 在已授权域内可对相同已验证内容去重；scheduler 不能只以裸 OID 绕过 source 的访问检查。共享 future/文件句柄返回结果，单个 waiter 取消不取消其他 waiter；refresh 取消无消费者预取，保留仍被旧工作区需求读取的请求。

## 9. 架构 ADR：先验证，再选择共享挂载方案

当前 Antares 是 `libfuse_fs::unionfs::OverlayFs` 用户态实现，不能把它的 hook 能力直接套到 Linux kernel OverlayFS 上。

| 方案 | 预期优势 | 必须先证明的门槛 |
| --- | --- | --- |
| A：共享不可变 Dicfuse mount + kernel OverlayFS | 有机会复用 lower 内核缓存 | FUSE lower 兼容性、权限、upper 写事件/manifest 完整性、受控换代与恢复 |
| B：单 FUSE supermount 路由 workspace | 容易集中维护 generation 与 observer | inode/handle 路由隔离、无 sibling 可见性、connection 瓶颈；单 connection 不自动证明 page-cache 共享 |
| C：现有 per-workspace FUSE | 最短路径验证版本正确性；作为 baseline | 用户态共享 lower/CAS，单独计量每 workspace session 成本 |

优先在 C 上完成 snapshot 读链路和双版本 oracle，同时限时研究 A/B。A 的实现须提供可靠 delta 观测方案；仅用可能丢事件的 watcher 不能满足 #49。主线 ADR 暂不选生产胜出方案。

Linux 文档规定挂载中的 overlay 不允许底层内容变化，部分特性还限制离线更换 lower。来源：[OverlayFS — Changes to underlying filesystems](https://docs.kernel.org/filesystems/overlayfs.html#changes-to-underlying-filesystems)（2026-09-06 查阅）。因此 A 的 refresh 设计必须采用独立 generation 和受控重新挂载。

## 10. 验证矩阵与交付门槛

以下为拟新增测试/实验 ID，当前仓库尚未具备相应命令。

| ID | 场景 | 必须满足的断言 |
| --- | --- | --- |
| V01 | 原生 A/B，branch 前进 | A mount 始终返回 A；新 mount 返回 B |
| V02 | 同一路径 scope commit / 全库 commit | 正确处理 scope；不重复附加前缀；不接受错误 scope |
| V03 | import 显式 A，默认 branch 前进到 B | tree、blob、size、readdir 都来自 A；不忽略 refs |
| V04 | 老总目录 view + 新 import 登记 | 老 view 名字集合不变；新 view 可见新 repo |
| V05 | import 原地更新/删除/改默认分支 | 旧 view 仍可寻址旧对象；新 view 依发布策略变化 |
| V06 | 并发原生 merge 与 import 发布 | 只读到一个有效发布组合；无半旧半新路由 |
| V07 | `/rust` vs `/rust_v1`、嵌套 repo | 组件匹配正确；拒绝冲突；固定分页不串 view |
| V08 | 双 commit 同 tree、双 scope 同 blob | provenance 不丢；允许 metadata/CAS 复用；inode 不串内容 |
| V09 | tree API 旧 oid、空目录、缺失目录 | 明确区分 immutable hit / empty / not found / unsupported |
| V10 | refresh 遇 dirty、FD、mmap、cwd | v1 明确拒绝或使用受控新 workspace；未静默切换 |
| V11 | 每个 refresh phase 注入 crash | 恢复旧或新 generation；COMMITTED 决定恢复选择 |
| V12 | commit 后同 path 再编辑 | selective cleanup 保留新 entry_seq 和新内容 |
| V13 | CL 更新与 main merge | 未合并 CL 不改变默认 view；候选 base/head 可重放 |
| V14 | CAS 损坏、GC/read 竞争、lease 过期 | 无错字节；有效 pin 不回收；过期明确错误 |
| V15 | million bindings，更新一个 import | mount 不枚举全 registry；写放大受索引深度约束 |
| V16 | 模式/链接/目录替换、opaque whiteout | oracle 比较 path/type/mode/content；与基线语义一致 |
| V17 | 旧 commit 无历史 catalog | 拒绝伪历史 snapshot；显式 lock 模式准确标注 |
| V18 | 32/64 workspace 与两种 source 版本 | 修改隔离、共享请求可计量、取消与回收不影响邻居 |

oracle 用公开 fixture 的 Git object tree + 独立实现的 binding composition 物化期望树；不能调用被测 Dicfuse resolver 自证。每轮冻结 source commits、namespace descriptor、seed、配置、内核、架构模式和实际失败率。

## 11. 实施顺序和跨仓依赖

| 工作包 | 仓库/现有 issue | 交付物 | 前置与退出条件 |
| --- | --- | --- | --- |
| V0 | ScorpioFS / #55，#42 前置协议追踪 | 目录分类、capabilities、descriptor、公开 fixture | D1 确认；V02/V07/V17 的预期结果冻结 |
| V1 | Mega G01/G02（基础已实现，读服务未接通） | Import 按 commit 读；scope proof、scope-aware tree/blob、共享 fixture | source-snapshot.v1；V01–V03/V09、MG01–MG04/MG13 通过 |
| V2 | Mega G03–G05（拟议） | bounded bindings index、全部 writer 的 publication transaction、leases/迁移 | D1/D2 确认；V04–V07/V14/V15、MG05–MG17 相关门槛通过 |
| V3 | ScorpioFS / #42、#43 | ViewResolver、immutable Dicfuse、CAS/schema 隔离 | 可先接 fake backend；V01–V09 通过后接真实 Mega |
| V4 | ScorpioFS / #44、#49 | refresh journal、upper manifest、受控切换 | D3 确认；V10–V12/V16 通过 |
| V5 | ScorpioFS / #50、#51 | FetchCoordinator，A/B/C bakeoff | correctness 不退化，V18 与资源计量通过 |
| V6 | Libra/Orion 协议对接 / #53、#54 | state-owner saga、candidate view、E2E artifact | V13/V18；源码变更需相应跨仓实施任务 |

M0 观测/benchmark 与 V0/V1 并行。不要等待全部观测实现才能开始正确性 fixture；也不要在 V0–V3 之前锁定共享 lower 的生产架构。

## 12. 待确认决策与明确假设

持续审阅：[Mega Draft PR #2181](https://github.com/gitmono-dev/mega/pull/2181)、[ScorpioFS Draft PR #56](https://github.com/gitmono-dev/scorpiofs/pull/56)。两者仍是基础实现检查点，不表示下列完整目标已经交付。

| 决策 | 方案及确认状态 | 另一选择及成本 |
| --- | --- | --- |
| D1 全库版本边界 | **已确认（2026-09-06）**：Mega 原子发布 native root + 固定 import bindings 的组合 view，ScorpioFS 固定该 view 读取 | 未选：首期只做 source snapshot 并延期全库原子一致性；单 source 仅作中间工作包 |
| D2 版本号路径策略 | **已确认（2026-09-06）**：显式标记为 release 的目录首次发布后不可变，新内容使用新版本路径；不靠数字目录名推断；普通 import 开发 branch 仍可演进 | 未选：release 原地替换；所有写入口必须拒绝内容变更与 policy 降级绕过 |
| D3 工作区更新体验 | 运行任务固定旧 view；新任务用新 view；现有 mount 暂停/检查后显式切换 | 要求透明运行中切换，必须扩展 handle/mmap/cwd/upper generation 协议 |

**D4 已于 2026-09-06 获用户确认**：Mega snapshot 读 API 默认关闭，仅在显式配置 source/scope 读授权与对象保留策略后启用。配置缺失/无效或后端门槛未满足时，ScorpioFS 不得静默转用 legacy latest。Mega 当前开发期通用 guard 不能替代 source/path 授权；当前基础代码尚未开放新 HTTP 路由。

其他暂定项：M1 支持现有 Mega SHA-1 并显式拒绝不支持的格式；不自动 hydrate LFS/submodule；GC 保留期、部署内核和 import 实际拓扑需在实施前填入环境 manifest。本文不把 A/B 架构选择当成已获确认的决策。
