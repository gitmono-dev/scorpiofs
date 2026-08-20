# Antares worktree control API

This API is the ScorpioFS half of a Libra-managed worktree. It exposes mount
lifecycle, writable-upper state, and safe base-switch preflight. It does not
implement Git refs, commits, index updates, merge, rebase, or credentials.

## Capability discovery

Check `/health` before using worktree operations. A compatible daemon reports:

```text
mount.v1
ready.v1
changes.v1
worktree-base.v1
refresh-plan.v1
```

Clients should negotiate capabilities instead of assuming every ScorpioFS
daemon implements the worktree API.

## Ownership contract

Libra owns HEAD, index, refs, object storage, commit construction, remote
transport, and conflict stages. ScorpioFS owns the mount, the Dicfuse lower
projection, and the writable Antares upper layer.

The mount must contain only a reconstructable `.libra` pointer. Do not create
or persist `.libra` refs or index files inside an Antares upper directory.

## Attach a Libra worktree

1. Libra resolves the commit/tree it intends to expose.
2. Create an Antares mount without a `cl` field.
3. Wait for the mount to become ready.
4. Bind the resolved revision before allowing a process to write to the mount.

```bash
curl -X POST http://127.0.0.1:2725/antares/mounts \
  -H 'Content-Type: application/json' \
  -d '{"job_id":"libra-dev-42","path":"/project/aardvark-dns"}'

curl http://127.0.0.1:2725/antares/mounts/<mount-id>/ready

curl -X POST http://127.0.0.1:2725/antares/mounts/<mount-id>/worktree/base \
  -H 'Content-Type: application/json' \
  -d '{"base_revision":"<resolved-commit-oid>"}'
```

Binding is rejected for a mount with a CL layer, a dirty upper layer, or an
existing different base binding. Repeating the same binding is idempotent.

## Read worktree state

```bash
curl http://127.0.0.1:2725/antares/mounts/<mount-id>/worktree
```

Example response:

```json
{
  "mount_id": "...",
  "path": "/project/aardvark-dns",
  "base_revision": "abc123",
  "mount_state": "ready",
  "dirty": true,
  "changes": {
    "mount_id": "...",
    "generation": 123456,
    "changes": [
      { "kind": "modified", "path": "src/lib.rs" }
    ]
  }
}
```

`changes` contains private upper-layer edits only. A CL layer is a build
baseline, not an unstaged Git worktree edit. Libra may use `generation` to
avoid reprocessing an unchanged path set, but it remains responsible for blob
hashing when updating its index.

## Plan a refresh

`refresh-plan` is a guard, not a mutation. Libra resolves target refs and calls
it before a later checkout, fast-forward, merge, or rebase operation.

```bash
curl -X POST \
  http://127.0.0.1:2725/antares/mounts/<mount-id>/worktree/refresh-plan \
  -H 'Content-Type: application/json' \
  -d '{
    "expected_base_revision":"abc123",
    "target_revision":"def456",
    "require_clean":true
  }'
```

The disposition is one of:

| Value | Meaning | Libra action |
| --- | --- | --- |
| `ready` | Bound base matches and the requested cleanliness rule passes | Proceed to a future transactional base switch |
| `already_at_target` | Target already matches the bound base | No worktree action |
| `unbound` | No Libra base has been registered | Bind a base first |
| `base_mismatch` | Caller has stale worktree metadata | Re-read state and resolve refs again |
| `blocked_dirty` | Upper layer has local edits | Reject, stash, merge, or rebase in Libra |

## Git operation mapping

| Libra operation | ScorpioFS action |
| --- | --- |
| `libra fetch origin` | None; only remote refs change |
| `libra status` | Read `/worktree`, then compare candidate paths with Libra index |
| `libra add` | None; Libra updates its host-local index |
| clean `pull --ff-only` | Call `refresh-plan`, then a future base switch/remount |
| dirty pull or checkout | `blocked_dirty`; Libra owns stash/merge/rebase |
| `libra commit` | Future transaction: switch lower to committed tree, then clear committed upper paths |
| `libra push` | None; pushing does not mutate the active mount |

## Current lower-snapshot boundary

`base_revision` records the revision selected by Libra. An actual Dicfuse lower
switch is deliberately not implemented until Mega exposes revision-addressable
tree, hash, and blob APIs. A successful refresh plan therefore proves only
that the worktree is safe to switch; it does not claim that the FUSE lower tree
has already moved to the target revision.
