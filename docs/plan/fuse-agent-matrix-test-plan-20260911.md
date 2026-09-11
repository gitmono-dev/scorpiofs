# ScorpioFS 多 Agent FUSE 使用特性测试方案（定版）

日期：2026-09-11
状态：可执行 pilot；账号登录和模型可用性检查待运行前完成

本方案固定测试以下五组 `harness + model`。这里的比较单位是一个 pair，
因此结果可以回答“这组 agent 组合如何使用 ScorpioFS”，但不能仅凭这五组
数据把 harness 效应和模型效应完全拆开。后续如需做因果归因，再固定同一
模型横向切换多个 harness。

## 1. 固定测试矩阵

| Pair ID | Harness | 固定模型 | WSL 启动方式 | 运行形态 |
|---|---|---|---|---|
| `kimi-k3` | Kimi Code CLI | `kimi-k3`（Kimi K3） | `kimi --model volcengine/kimi-k3` | 终端 CLI |
| `dsh-v41-flash` | DeepSeek Harness | `deepseek-flash`（当前路由为 DeepSeek-V4.1-Flash） | `dsh web --no-open` | 本地 Web UI |
| `codex-gpt56-sol` | Codex CLI | `gpt-5.6-sol` | `codex --model gpt-5.6-sol` | 终端 CLI |
| `claude-minimax-m3` | Claude Code | `minimax-m3` | `claude -p ...` | headless CLI |
| `zcode-glm53` | ZCode CLI | `glm-5.3` | `zcode --mode edit --prompt ...` | headless CLI / 终端 TUI |

模型 ID 必须按表中值使用。若某个账号或 endpoint 不支持目标模型，标记为
`blocked`，不能静默切换到相近模型，否则会破坏矩阵含义。

### 1.1 当前 WSL 安装状态

目标发行版：`Ubuntu-24.04`，WSLg 已可用（`DISPLAY=:0`）。已安装并自检：

| 工具 | 安装版本 | 安装位置/方式 |
|---|---:|---|
| Kimi Code CLI | `0.42.0` | `~/.kimi-code/bin/kimi`，官方 Linux 安装脚本 |
| DeepSeek Harness | `0.1.5-rc.1` | npm 全局包 `@deepseek-ai/dsh` |
| Codex CLI | `0.154.0` | npm 全局包 `@openai/codex` |
| Claude Code | `2.1.268` | npm 全局包 `@anthropic-ai/claude-code` |
| ZCode CLI | `3.11.2-22`（runtime `0.16.5`） | npm 全局包 `zcode-app-cli`，Node 22 bin 目录 |

新开一个 WSL 交互 shell 后，应能得到以下命令路径：

```bash
command -v kimi
command -v dsh
command -v codex
command -v claude
command -v zcode
```

如果使用非交互 shell，先加载 Node 22 和 Kimi 路径：

```bash
export PATH="$HOME/.kimi-code/bin:$HOME/.nvm/versions/node/v22.21.0/bin:$HOME/.local/bin:$PATH"
```

当前 WSL 已为以下 pair 写入本机凭据和模型配置；配置文件权限为 `0600`：

- Kimi Code：`~/.kimi-code/config.toml`，默认 `volcengine/kimi-k3`；
- DeepSeek Harness：`~/.dsh/cordis.patch.yml` 与 `~/.dsh/.credentials.yaml`，默认路由
  使用 `deepseek-flash`；
- Codex：执行 `codex login` 或使用组织提供的 `OPENAI_API_KEY`；
- Claude + MiniMax：`~/.claude/settings.json`，固定 `minimax-m3`；
- ZCode CLI：`~/.zcode/cli/config.json`，固定
  `volcengine-code-plan/glm-5.3`。

凭据已配置不代表 endpoint/model 一定有账号权限。正式矩阵运行前仍需各做一次
最小 smoke request；若返回鉴权、模型不存在或套餐限制错误，按 `blocked` 记录。

## 2. 可比性原则

每个 run 都使用同一个 ScorpioFS commit、同一个被测仓库 commit、同一份
pristine working tree 和同一份 prompt。每个 run 使用独立目录：

```text
/tmp/fuse-agent-runs/<run-id>/
  config.toml
  workspace-seed/
  store/
  mount/
  profile.tsv
  agent.log
  metadata.json
```

被测仓库不能直接使用当前有未提交修改的 `/home/luxian/scorpiofs`。先制作
固定 seed，再为每个 run 生成 disposable workspace。mutating workload 完成
后不得复用该 workspace。

### 2.1 Cache 状态

- `cold`：从同一个 seed 新建 daemon、store 和 mount；不执行预热命令。
- `warm`：仍然从同一个 seed 新建 runtime，先执行固定的 cache-priming
  命令；priming 区间不计入 agent window。
- 两种状态都不能通过复用上一个任务的工作树来产生，否则修改和 build
  artifacts 会成为混杂变量。

固定记录：machine、WSL kernel、CPU/memory limit、ScorpioFS commit、仓库
commit、agent 版本、model ID、prompt、cache state、profile 配置、开始结束
时间、退出码和任务结果。

### 2.2 Profile 时间窗口

ScorpioFS daemon 的 profile context 会同时覆盖主 workspace FUSE mount 和
通过 Antares HTTP API 创建的 overlay mount。因此：

1. daemon 启动、mount readiness 和 overlay 初始化单独记录；
2. agent 真正开始执行 prompt 时记录 `agent_start_ns`；
3. agent 完成并验证结果后记录 `agent_end_ns`；
4. agent 指标只统计 profile TSV 中落在该窗口的事件。

不能把 daemon 启动阶段的 lookup/getattr/readdir 归因给 agent。

## 3. Workload 定义

下面六个 task 对五组 pair 使用完全相同的自然语言 prompt。每个 task 都要
保存 prompt 原文，不允许在某个 agent 上临时改写措辞。

| Task ID | 固定 prompt 的目标 | 主要 FUSE 信号 |
|---|---|---|
| `discovery` | 只读分析 mount、FUSE 路由和关键实现，不修改文件 | lookup/getattr/opendir/readdirplus、小块 read |
| `search` | 搜索指定符号的所有引用并总结，不修改文件 | 并发小 read、重复 metadata |
| `small-edit` | 修改预先指定的一个小问题，运行最窄相关测试，不提交 | lookup、read、write、flush/fsync/release |
| `build` | 执行 `cargo check`，报告结果，不主动修改源码 | 大量 read、构建目录写入、并发请求 |
| `generate` | 创建 20 个受控 Markdown fixture 文件并报告数量 | mkdir/create/write/close |
| `cleanup` | 删除上一阶段生成的 fixture 并恢复工作树 | unlink/rmdir/forget/batch_forget |

推荐的 prompt 模板如下；其中 `<SYMBOL>`、`<BUG_FILE>`、`<FIXTURE_DIR>`
由 run metadata 固定替换，不由 agent 自己选择：

```text
discovery:
  只读检查当前仓库，定位 FUSE mount、MegaFuse、Antares overlay 和 profile
  相关实现。解释请求从内核到具体 filesystem 的调用路径。不要修改文件，
  不要创建文件，不要运行会改变工作树的命令。

search:
  搜索当前仓库中所有 <SYMBOL> 的定义和引用，按文件归纳调用关系。只读，
  不要修改文件，也不要生成索引或临时文件。

small-edit:
  修复 <BUG_FILE> 中预先描述的小问题：<BUG_DESCRIPTION>。只修改必要文件，
  运行 <TEST_COMMAND>，不要提交 commit，并报告修改文件和测试结果。

build:
  在当前仓库执行 cargo check，不修改源码，不执行 git clean，不提交 commit，
  返回命令退出码和关键失败信息。

generate:
  在 <FIXTURE_DIR> 下创建恰好 20 个 Markdown 文件，每个文件写入固定的短
  内容，完成后列出文件数量。不要修改其他路径。

cleanup:
  删除 <FIXTURE_DIR> 下本轮生成的 20 个 fixture，确认目录已删除，并确认
  git status 没有由本任务产生的未预期修改。不要删除其他文件。
```

`generate` 和 `cleanup` 必须成对使用同一 fixture 规格，但不复用 agent 的
普通工作树；cleanup 使用独立的、已经生成好 fixture 的 seed，避免把
generate 的思考过程差异带进 cleanup 的测量。

## 4. 每组 Agent 的实际运行方式

### 4.1 Kimi Code + K3

```bash
cd "$SCORPIO_MOUNT"
kimi --model k3
```

在新会话中粘贴固定 prompt。Kimi Code CLI 的模型 ID 是 `k3`，不是
`kimi-k3`；建议固定 thinking effort 为 `high`，并在 metadata 中记录实际
effort。K3 的上下文上限较大，但本测试不以最大上下文为变量，不能因为某次
任务更长临时切换模型或 effort。

### 4.2 DeepSeek Harness + DeepSeek-V4.1-Flash

```bash
cd "$SCORPIO_MOUNT"
dsh web --no-open
```

浏览器访问本地 Web UI，选择当前 workspace，在 Settings → Models 中配置
DeepSeek，并选择 `deepseek-flash`。当前官方路由将该 ID 指向
DeepSeek-V4.1-Flash；metadata 同时保存 UI 中显示的 resolved model。固定为
Standard mode，不能在某次 run 改用 Minimal/Code mode。

DSH 是 developer preview。每次正式实验前先运行一次 smoke task，确认模型
配置、workspace、工具权限和本地 URL 都正常；preview 升级后重新记录版本。

### 4.3 Codex + GPT-5.6 Sol

```bash
cd "$SCORPIO_MOUNT"
codex --full-auto --model gpt-5.6-sol
```

正式测试使用 disposable mount 和受控权限；`--full-auto` 只在隔离 runtime
中使用。需要固定 reasoning effort（建议 `high`），并在 metadata 中记录。
如果账号只暴露 `gpt-5.6` alias，先确认它解析到 `gpt-5.6-sol`，不能改用
其他 GPT-5.x 模型。模型解析失败的 run 标记 `blocked`。

### 4.4 Claude Code + MiniMax M3

Claude Code 使用 headless 方式，便于记录 stdout、stderr、退出码和时间：

```bash
export ANTHROPIC_BASE_URL="https://api.minimax.io/anthropic"
export ANTHROPIC_AUTH_TOKEN="$MINIMAX_API_KEY"
export ANTHROPIC_MODEL="MiniMax-M3"
cd "$SCORPIO_MOUNT"
claude -p "$PROMPT" --permission-mode acceptEdits --output-format json
```

这里的 `MiniMax-M3` 是目标模型，不允许自动 fallback。当前 MiniMax 的不同
公开 endpoint/文档版本对 Anthropic-compatible 模型目录可能不同；run 前必须
用实际 key 查询 endpoint 或完成一次 smoke request，确认 M3 被接受。若
endpoint 仅返回 M2.x，记录 `blocked`，不要把 M2.x 结果写入本 pair。

`acceptEdits` 允许必要的文件编辑，但 shell 命令仍应通过明确的权限规则或
交互确认执行。测试环境禁止使用 root；不使用 `--dangerously-skip-permissions`
除非 runtime 是专门隔离的 container/VM。

### 4.5 ZCode + GLM-5.3

```bash
zcode --mode edit --prompt "$PROMPT"
```

ZCode 使用 WSL 内的命令行客户端 `zcode-app-cli`，不依赖 WSLg。工作目录设为
当前 `SCORPIO_MOUNT`，CLI 配置中的 `model.main` 必须保持
`volcengine-code-plan/glm-5.3`。批量实验优先使用 `--prompt` 无头调用，人工
pilot 可直接运行 `zcode` 进入终端 TUI。固定 `--mode edit`，不要改为 `yolo`。

`agent_start_ns` 在启动命令前记录；`agent_end_ns` 在命令退出且输出持久化后
记录。保存 CLI exit code、session ID、stderr 和最终响应，避免把首次安装或
交互式 setup 向导耗时混入 agent service-time 分析。

## 5. ScorpioFS 启动与采集

每个 run 使用独立配置和 profile 文件。示例：

```bash
RUN_ID=codex-gpt56-sol__discovery__cold__r01
RUN_ROOT="/tmp/fuse-agent-runs/$RUN_ID"
mkdir -p "$RUN_ROOT" "$RUN_ROOT/store" "$RUN_ROOT/mount"

scorpio \
  --config-path "$RUN_ROOT/config.toml" \
  --fuse-profile \
  --fuse-profile-path "$RUN_ROOT/profile.tsv" \
  --fuse-profile-agent codex-gpt56-sol \
  --fuse-profile-task discovery-cold-r01 \
  --fuse-profile-capacity 262144 \
  --fuse-profile-flush-interval-ms 10 \
  serve
```

配置中的 `workspace`、`store_path`、`mount_root`、`state_file` 也必须指向
本 run 目录。启动后按以下顺序操作：

1. 等待 daemon `/health` 返回成功；
2. 从 WSL 对 mount 做最小 filesystem probe；
3. 记录 `ready_ns`；
4. 启动对应 agent；
5. 记录 `agent_start_ns`，发送固定 prompt；
6. 验证任务结果，记录 `agent_end_ns`；
7. 正常停止 ScorpioFS，让 profile writer 写出 footer；
8. 检查 `events_written`、`dropped_events` 和任务日志。

profile 事件按 `agent_start_ns..agent_end_ns` 截取。主 workspace 和 Antares
overlay 共用 profile 文件时，不能按文件名或 mount header 区分事件；当前
phase 1 以时间窗口为准。

## 6. 实验规模和执行顺序

### 6.1 Pilot

第一轮为：

```text
5 pairs × 6 tasks × 2 cache states × 1 repetition = 60 agent runs
```

每个 pair 内随机化 task 顺序；cold/warm 不要总是固定先后。一次只运行一
个 agent，避免模型服务、CPU、磁盘和 FUSE 请求相互干扰。

### 6.2 Overhead control

profiling overhead 不用 agent 本身测，而使用同一 mount 上的确定性 shell
workload 做配对对照：

```text
profile disabled vs enabled
同一 seed、同一命令、同一 cache state
每种状态至少 5 次，丢弃首次编译/首次启动 warm-up
```

命令 workload 至少覆盖 directory walk、metadata scan、sequential read、
small write、rename/unlink 和 mixed workload。比较 wall time、CPU、read/write
bytes、FUSE request count、p95/p99 service time；不能用 agent token 数替代
文件系统 workload。

### 6.3 Formal runs

Pilot 通过后，选择 `discovery`、`small-edit`、`build` 三个代表任务，执行：

```text
5 pairs × 3 tasks × 2 cache states × 3 repetitions = 90 agent runs
```

正式报告使用配对 median、range 和 bootstrap 95% confidence interval。若某
pair 的任务成功率、模型可用性或 profile 数据质量不满足门槛，单独列为
blocked/invalid，不用其他 pair 的结果补齐。

## 7. 指标与分析口径

### 7.1 必须报告

- 每个 `op` 的次数、占比和每 wall-clock second 的次数；
- `lookup/getattr/access/statfs` metadata amplification；
- read/write 总字节、请求尺寸、返回/接受字节、offset 分布；
- 按 `(pid, inode, fh, timestamp_ns, sequence)` 排序后的 sequential/random
  fraction；
- 每个 op 以及 read/write 的 p50/p90/p95/p99/max `duration_ns`；
- 一秒窗口内 ops/s、read/write bytes/s、active inode 数和 error 数；
- error rate、正数 `errno` 分布、profile drop rate；
- task wall time 与 FUSE service time，二者分开报告；
- 任务成功率、测试命令退出码、修改文件数和生成文件数。

### 7.2 数据质量门槛

一个 run 只有同时满足以下条件才进入主统计：

- profile header/schema version 正确；
- 每个事件恰好 17 列；
- footer 存在且 `events_written`、`dropped_events` 可解析；
- `dropped_events == 0`；如果非零，进入数据质量附录并报告 drop rate；
- 对完整 profile，`max(sequence) == events_written + dropped_events`；
- agent window 非空且 `agent_end_ns > agent_start_ns`；
- 模型 ID、harness mode、workspace 和权限设置与矩阵一致；
- cold run 没有复用 warm runtime 或旧 build artifacts；
- 任务结果和 agent 日志可以互相核对。

### 7.3 解释限制

profile 只描述到达 ScorpioFS 的 FUSE 请求，不等于应用发出的全部 syscall；
kernel/FUSE cache 可能隐藏请求。并发请求的 service time 不能简单求和来推导
应用总延迟。当前 `readdir/readdirplus` 是 reply-ready latency，不包含 stream
消费完成时间和目录 entry 数量。

## 8. 交付物

每个 run 交付：

```text
metadata.json
profile.tsv
agent.log
task-result.json
```

最终交付：

1. run inventory 和 blocked/invalid 清单；
2. 五组 pair 的 operation distribution 对比；
3. read/write size、offset 和 sequential/random 对比；
4. latency p50/p95/p99 表；
5. cold/warm 差异；
6. profile overhead 对照；
7. 至少一个有证据支撑的行为差异；
8. 数据质量、模型 endpoint、FUSE cache 和 layer attribution 限制。

## 9. 参考安装与模型文档

- Kimi Code CLI：<https://www.kimi.com/code/docs/en/kimi-code-cli/guides/getting-started>
- Kimi 模型配置：<https://www.kimi.com/code/docs/en/kimi-code/models.html>
- DeepSeek Harness：<https://www.deepseek.com/harness/en/>
- DeepSeek Harness 配置模型：<https://deepseek-harness.github.io/deepseek-harness/en/guide/providers>
- Codex CLI：<https://help.openai.com/en/articles/11096431>
- GPT-5.6 Sol：<https://developers.openai.com/api/docs/models/gpt-5.6-sol>
- Claude Code CLI：<https://code.claude.com/docs/en/cli-usage>
- ZCode CLI：<https://github.com/kingsword09/zcode-cli>
