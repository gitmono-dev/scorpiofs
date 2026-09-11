# FUSE I/O Profiling Plan

Date: 2026-09-10
Owner: ScorpioFS
Status: Phase 1 implementation complete; pilot validation pending

## 1. Objective

Build a profiling mode that captures how different agents use ScorpioFS while executing coding, search, build, and cleanup tasks. It should answer:

1. Which FUSE interfaces are called, how often, and in what order?
2. What is each interface's latency distribution?
3. What read/write sizes, offsets, and byte totals characterize each workload?
4. How do patterns differ by agent, task, mount, and cache state?
5. Which underlying layer serves each request? (Phase 2 attribution)

Profiling is disabled by default. It must not change filesystem semantics, must not turn normal diagnostics into a high-volume operation log, and must have a measured overhead budget.

## 2. Current State

The main entry point is `MegaFuse` in `src/fuse/async_io.rs`. It implements the FUSE `Filesystem` trait and routes calls to `Dicfuse`, Antares overlay, or the readonly layer.

The phase-1 implementation is now present as `src/fuse/logfuse.rs` and
`src/fuse/profile.rs`. It provides event-mode TSV output with a bounded
crossbeam queue and an asynchronous writer. It is disabled by default and is
selected independently from `log_level`.

Current limitations:

- There is no summary-mode recorder or `scorpio profile report` command yet.
- `readdir` and `readdirplus` record reply-ready latency, but do not wrap the
  returned stream, so `entries` is not populated for directory streams.
- One profile context is shared by the daemon workspace and Antares overlays;
  the file header identifies the daemon session, not each individual mount.
  Agent metrics must therefore use the recorded wall-clock task window.
- Direct `scorpio mount`/`umount` manager operations are not wired to the
  daemon-owned profile writer; use `scorpio serve` plus the Antares HTTP API
  for the pilot.

The asyncfuse `Request` provides `unique`, `uid`, `gid`, and `pid`, which support request correlation and client-process attribution.

## 3. Architecture: Independent `LogFuse`

Add a decorator rather than instrumenting every implementation inside `MegaFuse`:

```text
FUSE Session
    |
    +--> LogFuse<MegaFuse>
              +--> Dicfuse / readonly layer
    |
    +--> LogFuse<OverlayFs> for Antares mounts
```

`LogFuse` implements the same `Filesystem` trait and delegates every operation unchanged. It is responsible only for request metadata, timing, operation-specific counts and byte sizes, agent/task attribution, event recording, and aggregate updates.

This keeps `MegaFuse` focused on routing and semantics, gives one place to cover all FUSE interfaces, and allows unit tests with a mock `Filesystem`.

```rust
pub struct LogFuse<F> {
    inner: F,
    context: Arc<FuseProfileContext>,
}
```

Mount setup should choose the wrapper from configuration:

```rust
let fs = MegaFuse::new(...);
let fs = if profile_enabled {
    LogFuse::new(fs, profile_context)
} else {
    fs
};
```

### 3.1 Shared Observer

Every trait method should call one shared helper rather than duplicating logging logic. The
mount-selection branch keeps this helper completely out of the disabled path:

1. capture operation name as `&'static str`;
2. start the timer at `LogFuse` entry;
3. dispatch to the inner filesystem;
4. inspect success/error and extract reply metadata;
5. emit one event or update one aggregate;
6. return the original result unchanged.

FUSE return types include streams and complex replies, so the observer may need small wrappers for plain results, `Result<T>`, and streaming directory replies. Do not distort normal return types to force a single generic signature.

### 3.2 Timing Semantics

`duration_ns` measures from entry into `LogFuse` through construction of the final reply. It does not include kernel queueing, scheduling, or time before the request reaches the FUSE server.

Phase 1 records reply-ready latency for streaming `readdir` and `readdirplus`
replies. It does not consume or wrap the returned stream, so `duration_ns`
must not be interpreted as time to encode all directory entries and `entries`
is not populated for directory streams. Stream wrapping and entry counts are a
follow-up milestone because consuming the stream in the decorator could change
backpressure and kernel-visible behavior.

Follow-up milestone: add a stream wrapper that records both
`reply_ready_ns` (reply object creation) and end-to-end `duration_ns` (observable
stream completion), plus the emitted directory-entry count. This must be
benchmarked separately because consuming or wrapping the stream can change
backpressure and kernel-visible behavior.

### 3.3 Layer Attribution

`LogFuse` does not add a per-event layer field in phase 1. The file header is
session-scoped and the same context can cover the base workspace plus Antares
overlays. Per-mount/layer attribution can be added later only if it avoids
extending every inner trait or introducing a hidden global context.

## 4. Event Schema

Emit one structured event per completed request using target
`scorpio::fuse::profile`. The profile output uses fixed-column TSV so it can be
streamed, merged, and parsed by Python without a database or JSON dependency.

| Field | Purpose |
|---|---|
| `timestamp_ns` | Event completion time |
| `sequence` | Profile-local sequence; gaps indicate dropped events |
| `request_id` | `Request::unique` |
| `pid`, `uid`, `gid` | Client identity |
| `op` | FUSE interface name |
| `inode` | Primary inode |
| `fh` | File handle where applicable |
| `parent` | Parent inode where applicable |
| `offset`, `requested_size` | Operation parameters |
| `bytes` | Bytes returned by `read` or submitted by `write` |
| `entries` | Batch-forget count when available |
| `status` | `ok` or `error` |
| `duration_ns` | Server-side service time |
| `errno` | Positive Linux errno for failed requests, or `-` |

`mount_id`, `agent`, and `task` are stored in the file header. The writer adds
`completed_unix_ns`, `events_written`, and `dropped_events` as trailing comment
metadata.

Do not log file contents, xattr values, filenames, or paths by default. Add
explicit opt-in switches for these sensitive fields.

## 5. Agent and Task Attribution

### 5.1 Mount-Scoped Session

Create one daemon/profile file per agent task and assign labels when creating
the run:

```text
agent=claude
task=repo-search-001
mount_id=...
```

Every request inherits the same file-header labels. The daemon-owned profile
context is shared by the base workspace and Antares overlay mounts, so the
agent's metrics must be cut to the externally recorded task window.

### 5.2 Optional Shared-Mount Registry

When one mount is shared, use `Request::pid`:

1. A task launcher calls a local control endpoint before starting the agent.
2. The registry maps PID, process start time, and process scope to
   `(agent, task)`.
3. `LogFuse` resolves the PID while constructing an event.
4. Unknown PIDs receive `agent=unknown`, `task=unknown`.

The registry is optional. It should capture process start time to handle PID
reuse and register process groups or process trees for child processes.

## 6. Configuration

Use a dedicated configuration section. The currently implemented subset is:

```toml
[fuse_profile]
enabled = false
path = "/tmp/scorpiofs-fuse-profile.tsv"
agent = "unlabeled"
task = ""
capacity = 262144
flush_interval_ms = 10
```

`mode`, sampling, operation filters, summary counters, and sensitive-field
switches remain planned fields; do not present them as available until they
are implemented.

Precedence should follow existing conventions: CLI > environment variable >
configuration file > built-in default.

Current CLI:

```bash
scorpio serve \
  --fuse-profile \
  --fuse-profile-path=/tmp/agent.tsv \
  --fuse-profile-agent=claude \
  --fuse-profile-task=build-001
```

Existing `log_level` remains a diagnostic filter. Enabling the
`scorpio::fuse::profile` tracing target alone must not silently enable profile
collection when `fuse_profile.enabled` is false.

## 7. Recorder Modes

### Summary Mode (planned)

Keep concurrent aggregates by operation, agent, task, and status. Track:

- call count and operations per second;
- error count and error rate;
- bytes read and written;
- bytes per read/write call;
- directory entry count;
- minimum, maximum, total, p50, p95, and p99 latency;
- active operation count.

This is a future mode; phase 1 has no in-memory summary output.

### Events Mode (implemented)

Emit one TSV record per request through a bounded non-blocking queue and a
dedicated writer task. On a full queue, drop the event and increment
`dropped_events`; never block a FUSE worker. The count is written in the file
footer. Operation filters and deterministic sampling are future work.

### Combined Mode (planned)

`all` updates aggregates and emits events. Use it only for short benchmarks that
can tolerate the additional overhead.

## 8. Operation Coverage

`LogFuse` should implement every method already implemented by `MegaFuse`:

- Lifecycle: `init`, `destroy`, `interrupt`
- Metadata: `lookup`, `getattr`, `setattr`, `readlink`, `statfs`, `access`
- Namespace: `mknod`, `mkdir`, `symlink`, `unlink`, `rmdir`, `rename`,
  `rename2`, `link`, `create`
- File data: `open`, `read`, `write`, `flush`, `fsync`, `release`, `fallocate`,
  `lseek`
- Directory data: `opendir`, `readdir`, `readdirplus`, `releasedir`, `fsyncdir`
- Extended attributes: `setxattr`, `getxattr`, `listxattr`, `removexattr`
- Reference and cache: `forget`, `batch_forget`
- Locking: `getlk`, `setlk`
- Other implemented operation: `bmap`

Operation-specific fields:

- `read`: `requested_size`, returned `bytes`;
- `write`: submitted `bytes`;
- `readdir` and `readdirplus`: reply-ready latency; entry counts are planned;
- `getxattr` and `listxattr`: returned byte size;
- `forget`: `nlookup`;
- `batch_forget`: count;
- `open` and `create`: flags where useful.

`batch_forget` must emit one parent event with a count. It must not also emit
child `forget` events merely because it delegates to the inner implementation.

## 9. Reporting

Add `scorpio profile report` to:

- read one or more TSV files;
- aggregate by agent, task, operation, status, and time window;
- output JSON, Markdown, and CSV;
- calculate p50/p95/p99 latency, error rate, byte totals, bytes per call, and
  operations per second;
- identify first read, first write, peak I/O period, and metadata amplification.

For long-running mounts, optionally expose a local endpoint or signal that
returns the in-memory summary and atomically resets counters.

## 10. Experiment Matrix

Run each task across relevant mount types and cache states:

| Task | Purpose |
|---|---|
| `ls -R` / `find` | Directory traversal and metadata amplification |
| `rg` | Concurrent lookup, open, read, close |
| `git status` | Metadata-heavy access |
| `git diff` | Selective file reads |
| `cargo check` / `cargo build` | Dependency traversal and bulk reads |
| Agent edit loop | Small random reads/writes and frequent metadata |
| Large-file generation | Sequential writes and flush behavior |
| Cleanup task | Unlink, rmdir, and forget behavior |

Compare different agents, the same agent with different prompts, cold versus
warm cache, readonly `Dicfuse` versus Antares overlay, and summary versus events
mode overhead.

Record the repository commit, mount type and ID, cache state, task command,
wall-clock start/end, profile configuration hash, ScorpioFS version, and machine
details for every run.

## 11. Cache Caveat

FUSE and kernel caching can hide calls from ScorpioFS. The profile describes
requests that reach ScorpioFS, not every `stat`, `open`, or `read` executed by
the application. Keep cache state explicit and record application wall time
alongside FUSE service time.

## 12. Milestones

### M1: Foundation (complete)

- Add `LogFuse`, `FuseProfileContext`, event schema, and shared observer.
- Wrap `MegaFuse` at mount setup when enabled.
- Instrument lifecycle, metadata, open, read, write, release, and directory
  operations.
- Unit-test with a mock filesystem.

### M2: Coverage and Semantics (wrapper complete; stream counts pending)

- Audit xattrs, locks, rename variants, forget/batch forget, fallocate, lseek,
  bmap, and all remaining methods against the actual `Filesystem` implementation.
- Verify one event per request and no duplicate parent/child events.
- Add the stream wrapper and directory-entry counts only after a separate
  backpressure/overhead benchmark.

### M3: Sinks and Configuration

- Add `[fuse_profile]`, CLI, and environment configuration.
- Add bounded non-blocking TSV output, startup sink validation, footer drop
  counters, and graceful shutdown flush.
- Keep diagnostics and profile records in separate streams.

### M4: Summary and Reports

- Add concurrent histograms and aggregate counters.
- Add `scorpio profile report`.
- Add JSON, Markdown, and CSV outputs.

### M5: Agent Attribution

- Add mount-scoped agent/task labels.
- Add the optional shared-mount PID registry.
- Emit a task manifest linking agent, task, mount ID, and profile file.

### M6: Benchmark and Hardening

- Run the experiment matrix.
- Measure overhead with profiling disabled, summary mode, and events mode.
- Stress-test concurrent requests, queue drops, unmount, and shutdown.

## 13. Performance Requirements

- Disabled: the daemon uses the original wrapper, with no profile event
  allocation, timer acquisition, or writer wake-up on the FUSE path.
- Summary: bounded memory and no per-operation I/O.
- Events: bounded queue, non-blocking writer, startup validation, and explicit
  footer drop counters.
- Use structured values rather than preformatted strings.
- Shard aggregation state by operation or use a concurrent map to avoid
  serializing hot FUSE workers.
- Flush on normal unmount; tolerate a crashed producer without corrupting
  already-written records.

## 14. Privacy and Security

- Disabled by default.
- Do not capture file contents.
- Do not log xattr values, filenames, or paths by default.
- Treat profile files as sensitive because timing, inode values, names, and
  operation patterns can reveal project activity.
- Restrict label registration and summary endpoints to a local, authenticated
  listener.

## 15. Out of Scope for Phase 1

- OpenTelemetry export.
- Distributed tracing integration.
- Full path reconstruction for every event.
- Kernel-side queueing attribution.
- Remote metrics aggregation.
- Persistent profile storage.

These can be layered on after the local wrapper, event schema, and report
format are stable.
