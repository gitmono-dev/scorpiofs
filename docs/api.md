# ScorpioFS Legacy HTTP API (`/api/*`)

## Overview

The main `scorpio` binary exposes a legacy HTTP API on port **2725** (default `0.0.0.0:2725`, overridable via `--http-addr`). These endpoints manage workspace mounts and expose read-only configuration.

> **Recommended API:** For build-system overlay mounts, use the Antares API documented in [antares.md](./antares.md). When running the main `scorpio` process, Antares routes are nested under the `/antares` prefix (e.g. `GET /antares/health`, `POST /antares/mounts`).

> **Deprecation:** The legacy `/api/fs/*` and `/api/config` endpoints remain available for at least one minor release but are **deprecated**. Every response from these routes carries a `Deprecation: true` header (RFC 8594) and the server logs a warning on each call. Migrate to the Antares API (`/antares/*`).

> **Git routes:** Git-related HTTP routes (`/api/git/*`) exist in `src/daemon/git.rs` but are **not enabled** in the default server. They are not documented here.

**Base URL (default):** `http://localhost:2725`

---

## Health (liveness)

### `GET /health`

A lightweight, **non-deprecated**, root-level liveness probe. It checks only that
the process is up and the router responds; it does **not** contact the remote
mega server, perform deep FUSE checks, or leak absolute workspace/store paths.
Use it for container/systemd liveness checks. For per-mount readiness use
`GET /antares/mounts/{mount_id}/ready`.

**Response (200):**

```json
{
  "status": "ok",
  "version": "0.2.2",
  "uptime_secs": 42,
  "mount_count": 0
}
```

`mount_count` is `null` if the mount manager is momentarily busy (the probe uses
a non-blocking lock so liveness never stalls behind an in-flight mount).

---

## Endpoints

### 1. Mount Directory

**URL:** `POST /api/fs/mount`  
**Description:** Mounts a monorepo path. The handler runs synchronously but returns a `request_id` for status tracking. Use `GET /api/fs/select/{request_id}` to retrieve mount details after completion.

**Request Body (JSON):**

```json
{
  "path": "third-party/mega/scorpio",
  "cl": "optional-cl-id"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `path` | string | yes | Monorepo path to mount |
| `cl` | string | no | Optional changelist identifier (buck2 temp mount) |

**Response (JSON) — success:**

```json
{
  "status": "Success",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Mount task completed"
}
```

**Response (JSON) — failure:**

```json
{
  "status": "Fail",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Mount failed: <reason>"
}
```

---

### 2. Query Mount Task Status

**URL:** `GET /api/fs/select/{request_id}`  
**Description:** Returns the status of a mount operation identified by `request_id` from `POST /api/fs/mount`. Completed tasks (`finished` or `error`) are removed from memory after this call.

**Response (JSON) — task found:**

```json
{
  "status": "Success",
  "task_status": "finished",
  "mount": {
    "hash": "abc123",
    "path": "third-party/mega/scorpio",
    "inode": 42
  },
  "message": "Task found"
}
```

| `task_status` | Meaning |
|---|---|
| `fetching` | Mount in progress |
| `finished` | Mount succeeded; `mount` is populated |
| `error` | Mount failed |
| `not_found` | No task with this `request_id` |

---

### 3. List Mounted Directories

**URL:** `GET /api/fs/mpoint`  
**Description:** Lists all workspaces currently recorded in the runtime state file (`config_file` in `scorpio.toml`, default `config.toml`).

**Response (JSON):**

```json
{
  "status": "Success",
  "mounts": [
    {
      "hash": "hash1",
      "path": "third-party/mega/scorpio",
      "inode": 12345
    }
  ]
}
```

---

### 4. Unmount Directory

**URL:** `POST /api/fs/unmount`  
**Description:** Unmounts a workspace by `path` or `inode`. At least one must be provided.

**Request Body (JSON):**

```json
{
  "path": "third-party/mega/scorpio",
  "inode": 12345
}
```

**Response (JSON) — success:**

```json
{
  "status": "Success",
  "message": "Directory unmounted successfully"
}
```

**Response (JSON) — failure:**

```json
{
  "status": "Fail",
  "message": "Umount process error :<reason>."
}
```

---

### 5. Get Configuration

**URL:** `GET /api/config`  
**Description:** Returns a snapshot of the currently loaded configuration. JSON field names differ from `scorpio.toml` keys (see mapping table below).

**Response (JSON):**

```json
{
  "status": "Success",
  "config": {
    "mega_url": "http://localhost:8000",
    "mount_path": "/tmp/scorpio-megadir/mount",
    "store_path": "/tmp/scorpio-megadir/store"
  }
}
```

| API field | `scorpio.toml` key | Description |
|---|---|---|
| `mega_url` | `base_url` | Mega/monorepo service base URL |
| `mount_path` | `workspace` | FUSE workspace mount point |
| `store_path` | `store_path` | Local store directory |

---

### 6. Update Configuration (deprecated, non-functional)

**URL:** `POST /api/config`  
**Description:** **Deprecated and does not persist or apply configuration changes.** The handler only echoes the request body fields in the response and returns a `Deprecation: true` header. Configuration changes require editing `scorpio.toml` and restarting the process. The request still uses the legacy field names `mega_url`/`mount_path` (mapping to `base_url`/`workspace`); since the endpoint is deprecated they are kept as-is rather than renamed.

**Request Body (JSON):**

```json
{
  "mega_url": "http://example.com",
  "mount_path": "/new/mount",
  "store_path": "/new/store"
}
```

**Response (JSON):**

```json
{
  "status": "Success",
  "config": {
    "mega_url": "http://example.com",
    "mount_path": "/new/mount",
    "store_path": "/new/store"
  }
}
```

> Note: `POST` and `GET /api/config` now both return `"Success"` (the prior lowercase `"success"` mismatch has been unified). Responses also carry `Deprecation: true`.

---

## Data Structures

### MountRequest

```rust
struct MountRequest {
    path: String,
    cl: Option<String>,
}
```

### MountResponse

```rust
struct MountResponse {
    status: String,      // "Success" or "Fail"
    request_id: String,
    message: String,
}
```

### MountInfo

```rust
struct MountInfo {
    hash: String,
    path: String,
    inode: u64,
}
```

### SelectResponse

```rust
struct SelectResponse {
    status: String,           // "Success" or "Fail"
    task_status: String,      // "fetching", "finished", "error", "not_found"
    mount: Option<MountInfo>,
    message: String,
}
```

### UmountRequest

```rust
struct UmountRequest {
    path: Option<String>,
    inode: Option<u64>,
}
```

### ConfigRequest / ConfigInfo

```rust
struct ConfigRequest {
    mega_url: Option<String>,    // maps to base_url in scorpio.toml
    mount_path: Option<String>,  // maps to workspace in scorpio.toml
    store_path: Option<String>,
}
```

---

## Examples

```bash
# Mount (legacy API)
curl -X POST http://localhost:2725/api/fs/mount \
  -H "Content-Type: application/json" \
  -d '{"path": "third-party/mega/scorpio"}'

# Query mount status (use request_id from mount response)
curl http://localhost:2725/api/fs/select/<request_id>

# List mounts
curl http://localhost:2725/api/fs/mpoint

# Unmount
curl -X POST http://localhost:2725/api/fs/unmount \
  -H "Content-Type: application/json" \
  -d '{"path": "third-party/mega/scorpio"}'

# Read config
curl http://localhost:2725/api/config

# Antares API on the same port (recommended for build mounts)
curl http://localhost:2725/antares/health
curl -X POST http://localhost:2725/antares/mounts \
  -H "Content-Type: application/json" \
  -d '{"job_id":"job-1","path":"/third-party/mega"}'
```

See [antares.md](./antares.md) for full Antares endpoint documentation.