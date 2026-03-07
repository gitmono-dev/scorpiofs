# 任务：基于 Antares 实现 Libra Worktree 功能

## 任务分值
**60分**

---

## 背景描述

Libra 是一个用 Rust 开发的 Git 客户端的部分实现，目标是打造一个 AI agent 原生的版本控制系统。Libra 已经支持大部分 Git 命令，但目前还缺少 `worktree` 功能。

Git worktree 允许同一个仓库创建多个工作树，每个工作树可以检出不同的分支，共享同一个 `.git` 目录。这在需要同时处理多个分支、运行并行构建或测试时非常有用。

Scorpiofs 是一个基于 FUSE 的虚拟文件系统，其中的 **Antares** 子系统提供了三层联合文件系统（upper/CL/Dicfuse），非常适合实现 worktree 的隔离需求：

```text
┌─────────────────┐
│   upper (rw)    │  ← 工作树特定的修改
├─────────────────┤
│    CL (rw)      │  ← 可选的变更列表层
├─────────────────┤
│  Dicfuse (ro)   │  ← 共享的只读基础层（Git 对象存储）
└─────────────────┘
```

---

## 需求描述

### 功能要求

1. **实现 `libra worktree add` 命令**
   - 在指定路径创建新的工作树
   - 支持检出指定的分支或创建新分支
   - 使用 Antares 的 `mount_job_at` 为每个 worktree 创建独立的 FUSE 挂载点
   - 每个 worktree 应该有独立的 upper 层（隔离文件修改）
   - 所有 worktree 共享 Dicfuse 层（共享 Git 对象存储）

2. **实现 `libra worktree list` 命令**
   - 列出所有活动的工作树
   - 显示每个工作树的路径、当前分支和挂载状态
   - 集成 Antares 的 `list_mounts()` 方法

3. **实现 `libra worktree remove` 命令**
   - 移除指定的工作树
   - 正确卸载 Antares FUSE 挂载点
   - 清理相关的 upper 层目录和状态

4. **实现 `libra worktree lock/unlock` 命令**
   - 锁定工作树防止被意外删除
   - 解锁工作树允许后续删除

### 技术要求

1. **集成 scorpiofs crate**
   - 在 `libra` 项目的 `Cargo.toml` 中添加 `scorpiofs` 依赖
   - 使用 `scorpiofs::prelude::*` 导入 Antares 相关类型

2. **Antares 管理器初始化**
   - 在 libra 初始化时创建 `AntaresManager` 实例
   - 配置合适的路径结构（upper_root, cl_root, mount_root, state_file）
   - 可参考 [tests/antares_test.rs](https://github.com/web3infra-foundation/scorpiofs/blob/master/tests/antares_test.rs)

3. **Worktree 与 Antares 的映射**
   ```rust
   // 伪代码示例
   pub async fn add_worktree(
       &self,
       path: PathBuf,      // worktree 路径
       branch: &str,       // 要检出的分支
   ) -> Result<()> {
       // 1. 生成唯一的 job_id
       let job_id = format!("worktree-{}", uuid::Uuid::new_v4());
       
       // 2. 使用 Antares 挂载
       let config = self.antares_manager
           .mount_job_at(&job_id, path.clone(), None)
           .await?;
       
       // 3. 在挂载点检出指定分支
       self.checkout_branch_at(&path, branch).await?;
       
       // 4. 记录 worktree 元数据
       self.save_worktree_metadata(&path, branch, &job_id).await?;
       
       Ok(())
   }
   ```

4. **错误处理**
   - 正确处理 FUSE 挂载/卸载失败
   - 处理路径冲突（worktree 路径已存在）
   - 处理分支不存在的情况
   - 提供清晰的错误信息

5. **测试覆盖**
   - 单元测试：测试 worktree 元数据管理
   - 集成测试：测试完整的 add/list/remove 流程
   - FUSE 测试：验证多个 worktree 可以同时挂载和访问
   - 并发测试：验证多个 worktree 并行操作的正确性

### 参考资料

- [Scorpiofs Antares 文档](https://github.com/web3infra-foundation/scorpiofs/blob/master/src/antares/mod.rs)
- [Scorpiofs 测试示例](https://github.com/web3infra-foundation/scorpiofs/blob/master/tests/antares_test.rs)
- [Git worktree 官方文档](https://git-scm.com/docs/git-worktree)
- [Libra 仓库](https://github.com/web3infra-foundation/libra)

---

## 代码标准

1. 所有 **PR** 提交必须签署 `Signed-off-by` 和使用 `GPG` 签名，即提交代码时（使用 `git commit` 命令时）至少使用 `-s -S` 两个参数，参考 [Contributing Guide](https://github.com/web3infra-foundation/libra/blob/main/docs/contributing.md)；
2. 所有 **PR** 提交必须通过 `GitHub Actions` 自动化测试，提交 **PR** 后请关注 `GitHub Actions` 结果；
3. 代码注释均需要使用英文；
4. 代码风格遵循 Rust 官方规范，提交前运行 `cargo fmt` 和 `cargo clippy`；
5. 所有公开 API 必须包含文档注释（`///`），并提供使用示例；

---

## PR 提交地址

提交到 [libra](https://github.com/web3infra-foundation/libra) 仓库的 `main` 分支 `src/commands/worktree` 目录；

---

## 开发指导

1. **认领任务**：认领任务参考 [r2cn 开源实习计划 - 任务认领与确认](https://r2cn.dev/docs/student/assign);

2. **开发流程**：
   - Fork libra 仓库并 clone 到本地
   - 创建功能分支：`git checkout -b feat/worktree-antares`
   - 在 `Cargo.toml` 添加 scorpiofs 依赖
   - 在 `src/commands/` 创建 `worktree` 模块
   - 实现核心功能并编写测试
   - 运行测试确保通过：`cargo test`
   - 提交代码：`git commit -s -S -m "feat: implement worktree with Antares FUSE"`
   - 推送并创建 PR

3. **测试要求**：
   ```bash
   # 运行所有测试
   cargo test
   
   # 运行 worktree 相关测试
   cargo test worktree
   
   # 运行 FUSE 集成测试（需要 root 权限）
   sudo -E cargo test --test worktree_integration -- --ignored
   
   # 检查代码质量
   cargo clippy --all-targets --all-features -- -D warnings
   cargo fmt --check
   ```

4. **调试技巧**：
   - 使用 `RUST_LOG=debug` 查看详细日志
   - 使用 `fusermount -u <path>` 手动卸载挂载点
   - 查看 Antares state 文件了解当前挂载状态
   - 使用 `mount | grep fuse` 查看所有 FUSE 挂载

---

## 导师及邮箱

请申请此题目的同学使用邮件联系导师，或加入到 [R2CN Discord](https://discord.gg/WRp4TKv6rh) 后在 `#p-mega` 频道和导师交流。

1. Quanyi Ma <genedna@qq.com>
2. Tianxing Ye <yetianxing2014@gmail.com>

---

## 备注

1. **认领实习任务的同学，必须完成测试任务和注册流程，请参考：**
   - [r2cn 开源实习计划 - 测试任务](https://r2cn.dev/docs/student/pre-task)
   - [r2cn 开源实习计划 - 学生注册与审核](https://r2cn.dev/docs/student/signup)

2. **技术细节**：
   - Antares FUSE 挂载需要 `libfuse3` 和相关系统权限
   - 建议在 Linux 环境开发和测试
   - 可以参考 scorpiofs 的测试用例了解 Antares 的使用方式

3. **预期时间投入**：40-60 小时
   - 熟悉 Antares API：8-10 小时
   - 实现核心功能：20-30 小时
   - 编写测试和文档：8-12 小时
   - 代码审查和修改：4-8 小时

4. **评分标准**：
   - 功能完整性（40%）：所有命令正确实现
   - 代码质量（30%）：规范、可读、可维护
   - 测试覆盖（20%）：完善的单元和集成测试
   - 文档质量（10%）：清晰的 API 文档和使用示例

---

## 附加资源

- [Antares 使用示例](https://github.com/web3infra-foundation/scorpiofs/blob/master/src/lib.rs#L35-L90)
- [FUSE 开发指南](https://www.kernel.org/doc/html/latest/filesystems/fuse.html)
- [Rust 异步编程](https://rust-lang.github.io/async-book/)
