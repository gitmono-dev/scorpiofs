# FUSE Profile Multi-Agent Test Plan

Date: 2026-09-10
Owner: ScorpioFS
Status: Ready for pilot

## 1. Goal

Measure and compare how several coding agents use ScorpioFS when they perform
the same repository tasks. The results should identify:

1. operation distribution by FUSE interface;
2. read/write byte volume and request-size distribution;
3. metadata amplification and lookup/getattr pressure;
4. sequential versus random I/O;
5. service-time latency by operation;
6. short-lived bursts and long-running I/O phases;
7. differences between discovery, editing, build, and cleanup workloads.

The first implementation records server-side FUSE service time. It does not
claim to measure kernel queueing or total application syscall latency.

## 2. Tested Agents

Use one isolated ScorpioFS daemon and mount per agent so the header label is a
reliable attribution. Do not share one profile file across agents in phase 1.

| Agent ID | Description | Why it is useful |
|---|---|---|
| `codex` | Codex CLI coding agent | Terminal-oriented code discovery, edits, and test execution |
| `claude-code` | Claude Code agent | Compare another terminal agent's tool and process pattern |
| `cursor` | Cursor agent/runtime if available | GUI/editor-oriented access and background indexing behavior |
| `aider` | Aider or another minimal edit agent | Narrower edit loop with fewer auxiliary tools |

If an agent is unavailable, substitute another coding runtime and keep the agent
ID stable. The important comparison is between agent behavior classes, not
specific vendor marketing names.

## 3. Workload Matrix

Run each agent against the same repository commit, prompt, and pristine working
tree. Every task gets a disposable runtime directory so edits, build artifacts,
and generated files cannot leak into another task.

| Task ID | Prompt / action | Expected profile signal |
|---|---|---|
| `discovery` | "Locate the implementation of FUSE mounting, explain the call flow, but do not modify files." | High `lookup`, `getattr`, `opendir`, `readdirplus`, and broad small reads |
| `small-edit` | "Fix a small, predefined issue in one source file and run the narrowest relevant test." | Metadata discovery followed by small writes, `flush`, `fsync`, and `release` |
| `build` | "Run `cargo check` and report failures; do not edit files." | Bulk sequential reads, high read bytes, build-artifact writes |
| `search` | "Search for all uses of `MegaFuse` and summarize the call sites." | Concurrent small reads and repeated metadata lookups |
| `generate` | "Generate 20 Markdown notes under a new directory using repository context." | Directory creation and clustered writes |
| `cleanup` | "Remove the generated notes directory and restore the worktree." | `unlink`, `rmdir`, `forget`, and `batch_forget` bursts |

Pilot scope:

- 4 agents;
- 6 tasks;
- cold and warm cache states;
- 1 repetition;
- one no-profile control for each representative workload used for overhead
  measurement.

Formal scope after pilot:

- keep all cold runs;
- repeat the three most representative tasks (`discovery`, `small-edit`,
  `build`) three times;
- pair profile and no-profile runs on the same machine and repository snapshot;
- report median, range, and a bootstrap confidence interval, not a single run.

## 4. Profile Output

The implementation writes a fixed-column TSV file. The header identifies the
mount, agent, task, schema version, start time, and columns.

```text
# fuse-profile	1
# started_unix_ns	...
# mount_id	...
# agent	codex
# task	discovery
# columns	timestamp_ns	...	errno
# completed_unix_ns	...
# events_written	...
# dropped_events	0
```

Event columns:

| Column | Meaning |
|---|---|
| `timestamp_ns` | Event completion timestamp |
| `sequence` | Profile-local monotonic sequence |
| `op` | FUSE interface name |
| `request_id` | FUSE `unique` request ID |
| `pid` | Client process ID |
| `uid` | Client user ID |
| `gid` | Client group ID |
| `inode` | Primary inode |
| `fh` | File handle where applicable |
| `parent` | Parent inode where applicable |
| `offset` | Read/write/seek offset where applicable |
| `requested_size` | Requested bytes, flags, count, or operation-specific value |
| `bytes` | Returned read bytes, accepted write bytes, or xattr size |
| `entries` | Entry count for `batch_forget`; directory stream count is future work |
| `status` | `ok` or `error` |
| `duration_ns` | Server-side service time |
| `errno` | Positive Linux errno for failed requests, or `-` |

The FUSE worker only fills an in-memory event and pushes it to a bounded
crossbeam queue. A separate writer task formats TSV and performs buffered disk
I/O. Queue-full events are dropped and counted; they never block the FUSE path.

The final comment records `events_written` and `dropped_events`, so the report
can calculate data loss without scraping daemon logs. The profile writer opens
the sink and writes the header before mounting; invalid paths or permissions
fail the run immediately.

The log does not contain file contents, paths, filenames, or xattr values.

## 5. Run Procedure

### 5.1 Prepare

For every run, record:

- machine and OS;
- CPU and memory limit;
- repository URL and commit;
- ScorpioFS commit and profile settings;
- agent runtime and version;
- exact prompt;
- whether the mount was cold or warm;
- wall-clock start and end;
- success/failure and final agent answer summary.

Use a separate runtime directory per run:

```text
/tmp/fuse-profile-runs/<run-id>/
  config.toml
  store/
  mount/
  profile.tsv
  task.log
  metadata.json
```

### 5.2 Start ScorpioFS

Use separate `workspace`, `store_path`, and `config_file` values for each run.
Example:

```bash
scorpio \
  --config-path /tmp/fuse-profile-runs/<run-id>/config.toml \
  --fuse-profile \
  --fuse-profile-path /tmp/fuse-profile-runs/<run-id>/profile.tsv \
  --fuse-profile-agent codex \
  --fuse-profile-task discovery \
  --fuse-profile-capacity 262144 \
  --fuse-profile-flush-interval-ms 10 \
  serve
```

Wait for both the daemon health endpoint and a filesystem probe from the mounted
workspace to succeed before launching the agent. Record readiness as a separate
timestamp. Stop ScorpioFS normally after the task completes so the profile
writer flushes and shutdown metadata is written.

### 5.3 Run Agent

Launch the agent with its working directory set to the ScorpioFS mount. Capture
stdout/stderr and process exit status. Do not run unrelated applications inside
the mount during the measurement.

When the agent uses an Antares mount created through the daemon API, the same
profile file also contains the daemon workspace mount's startup and readiness
requests. Record `agent_start_ns` and `agent_end_ns` in `metadata.json`, and
restrict agent-comparison metrics to that interval. Report mount setup and
readiness separately; do not attribute them to the agent.

For a cold run, create a fresh daemon, store, and mount from the pristine
snapshot. For a warm run, create a fresh runtime from the same snapshot, execute
the standardized cache-priming command, exclude that priming interval from the
agent window, and then launch the identical prompt. This makes cache state a
controlled factor without carrying task edits or build artifacts across runs.

For mutating tasks, the agent window ends only after the expected output is
verified. Cleanup runs use a fresh generated fixture and are analyzed
separately from the task that created it.

## 6. Required Metrics

### 6.1 Operation Distribution

For each `(agent, task, cache_state)`:

- count and percentage of every `op`;
- operations per wall-clock second;
- metadata operation count:
  `lookup + getattr + setattr + access + statfs`;
- namespace operation count:
  `create + mkdir + unlink + rmdir + rename + rename2 + link + symlink`;
- directory operation count:
  `opendir + readdir + readdirplus + releasedir + fsyncdir`;
- data operation count: `read + write`;
- close/sync operation count: `flush + fsync + release`;
- error count and error rate by operation.

### 6.2 Data Characteristics

For `read`:

- total bytes;
- minimum, maximum, mean, and mode of `bytes`;
- requested-versus-returned bytes;
- distinct `(inode, fh, offset, bytes)` triples;
- repeated-read ratio;
- read bytes per second;
- fraction of reads at offset 0;
- p50/p95/p99 read size.

For `write`:

- total bytes;
- minimum, maximum, mean, and mode of `requested_size`;
- accepted bytes versus requested bytes;
- write bytes per second;
- maximum single-write size;
- p50/p95/p99 write size;
- distinct written inodes.

For both:

- sequential-run fraction: after sorting data events by `(pid, inode, fh,
  timestamp_ns, sequence)`, next offset equals previous offset plus previous
  size for the same `(pid, inode, fh)`;
- random-access fraction;
- distinct inodes touched;
- reads per written inode;
- read-to-write byte ratio.

### 6.3 Latency

For every operation and for `read`/`write` separately:

- p50, p90, p95, p99, and maximum `duration_ns`;
- total service time;
- mean service time;
- operations with duration above 100 ms and above 1 s;
- latency distribution by request-size bucket:
  `1-4 KiB`, `4-64 KiB`, `64-128 KiB`, and `>128 KiB`.

Report service latency separately from task wall time. Do not add FUSE service
times to infer application latency because operations may run concurrently.

### 6.4 Temporal Behavior

Bucket events into one-second windows and compute:

- operations per second;
- read bytes per second;
- write bytes per second;
- active distinct inodes per second;
- error count per second.

Identify:

- time to first `lookup`;
- time to first read;
- time to first write;
- peak read second;
- peak write second;
- idle periods longer than five seconds;
- whether build activity is a distinct high-read phase.

### 6.5 Derived Comparison Metrics

For every agent/task:

- metadata-to-data operation ratio;
- `lookup` calls per read call;
- `getattr` calls per read call;
- read bytes per edited file;
- read bytes per output token or final answer, if token count is available;
- write bytes per generated file;
- distinct inodes read;
- distinct inodes written;
- repeated read percentage;
- sequential access percentage;
- error rate;
- profile drop count.

## 7. Expected Behavioral Hypotheses

These are hypotheses to test, not assumptions.

### `codex`

- Strong metadata-heavy discovery before edits.
- Broad source reads with many small requests.
- Small, clustered edits and occasional test/build reads.
- `lookup` and `getattr` may dominate operation count even when read bytes are
  large.

### `claude-code`

- Likely similar to Codex for repository discovery.
- Tool implementation may produce more subprocesses and concurrent reads.
- Editing may generate more temporary or backup-style writes if configured.

### `cursor`

- Possible background indexing or workspace scanning.
- May show broad directory traversal and repeated small reads.
- GUI workflows may create longer idle gaps between discovery and editing.

### `aider`

- Narrower edit loop with less broad repository traversal.
- Higher fraction of reads concentrated in explicitly referenced files.
- Lower metadata amplification if repository map generation is cached.

### Build Workload

- `cargo check` should produce the highest read-byte volume.
- Read sizes should skew toward FUSE maximum transfer size.
- Build artifact writes may be large and bursty.
- Service-time p95 may increase when many reads are concurrent.

### Cleanup Workload

- `unlink`, `rmdir`, and `forget`/`batch_forget` should dominate.
- Write volume should be near zero.
- `forget` bursts may occur after file close and unmount.

## 8. Analysis Deliverables

Produce a Markdown report with:

1. executive summary;
2. run inventory and data-quality checks;
3. agent comparison table;
4. task-by-task operation distribution;
5. read/write characteristics;
6. latency table;
7. one-second temporal plots or summaries;
8. notable behavioral differences;
9. limitations and follow-up recommendations.

Suggested comparison table:

| Agent | Task | Cache | Ops | Read MiB | Write MiB | Read/Write | Metadata/Data | p95 read | p95 write | Errors |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|

Data-quality checks:

- profile header exists and schema version is 1;
- every event has the expected 17 fields;
- sequence numbers are monotonic after sorting;
- `max(sequence) = events_written + dropped_events` for a complete profile;
- dropped events are zero, or drop rate is reported;
- wall-clock duration is greater than zero;
- each profile file contains exactly one agent and task label;
- cold runs do not accidentally reuse a warm mount.

## 9. Python Analysis Script Requirements

A future script should consume one or more profile files and output Markdown
and CSV without external Python dependencies.

Required behavior:

- parse `#` header metadata;
- validate the column list and row width;
- skip and count malformed rows;
- aggregate by input file, agent, task, op, and status;
- compute byte and latency statistics;
- reconstruct sequential access runs by `(pid, inode, fh)`;
- bucket time into one-second windows;
- merge multiple files while preserving file-level agent/task labels;
- emit a data-quality summary including dropped-event count when exposed;
- never infer filenames or contents from the profile.

The current schema includes `fh` for file and directory handle operations where
the FUSE protocol provides it.

## 10. Success Criteria

- All pilot runs complete without ScorpioFS mount errors.
- Profile files contain valid headers and event rows.
- No profile events are dropped during pilot workloads; if drops occur, report
  drop rate and increase queue capacity.
- Profiling-disabled behavior remains unchanged.
- Profiling-enabled overhead is measured against a no-profile baseline.
- The final report clearly separates cold and warm cache runs.
- The report identifies at least one meaningful difference in metadata
  amplification, read breadth, write clustering, or operation latency between
  agents.

## 11. Known Limitations

- FUSE and kernel caching can hide calls from ScorpioFS.
- Service time excludes kernel queueing and client scheduling.
- Current directory profiles record reply-ready time, not stream-consumption
  time or entry count.
- The phase-1 schema has no path or filename.
- Agent attribution is mount-scoped; a shared mount cannot reliably distinguish
  agents without a future PID registry.
- Concurrent operations make it invalid to sum service times and call the total
  application latency.
