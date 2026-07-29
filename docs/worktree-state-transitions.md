# ScorpioFS worktree state transitions

## Scope and ownership

ScorpioFS is a projection and FUSE ownership layer. Libra owns Git-compatible
objects, refs, HEAD, index, commits, transport, and credentials. ScorpioFS
must never persist `.libra` state in an Antares upper layer.

The mapping is deliberately narrow:

| Version-control concept | Owner | ScorpioFS representation |
| --- | --- | --- |
| `HEAD` commit/tree | Libra | `base_revision` bound to an Antares mount |
| index | Libra | not materialized by ScorpioFS |
| working tree | both | immutable Dicfuse lower plus Antares upper |
| unstaged changes | ScorpioFS | changed paths in the private upper layer |
| remote-tracking refs | Libra | not a filesystem update |

An optional Antares CL layer is a build-view input. It is not a Libra index and
cannot be used with a Libra worktree-base binding.

## Invariants

1. A mounted worktree has at most one immutable `base_revision`.
2. `libra fetch` only updates remote refs and never changes Dicfuse lower files.
3. ScorpioFS never changes a mounted lower tree merely because a remote branch moved.
4. A dirty upper layer blocks a safe fast-forward base switch by default.
5. A base switch is coordinated by Libra and must be an explicit remount/generation
   change; the current `refresh-plan` API is intentionally non-mutating.
6. Open handles continue to observe their existing generation until the caller
   quiesces the worktree and performs the switch.

## Mount states

```text
Provisioning -> Mounted -> Ready
                         -> Quiescing -> Refreshing -> Ready
                         -> Quiescing -> Conflict
                         -> Unmounting -> Unmounted
```

`Refreshing` and `Conflict` are target states for the future revision-aware
Dicfuse switch. Existing implementations use `Ready` plus the non-mutating
refresh plan and do not claim to have switched the lower tree.

## Git-compatible transitions

### Attach

1. Libra resolves the exact commit/tree to check out.
2. Libra creates the Antares mount.
3. Before a client writes to the mount, Libra calls
   `POST /mounts/{id}/worktree/base` with that immutable revision.
4. ScorpioFS rejects the binding if the mount has a CL layer or an upper delta.

### Fetch

`libra fetch origin` updates `refs/remotes/origin/*` only. It must not invoke a
Dicfuse refresh, invalidate an active worktree, or change `base_revision`.
The status command can report ahead/behind using Libra refs while the visible
filesystem remains pinned.

### Status

Libra computes staged changes from `HEAD -> index`. It obtains unstaged and
untracked candidates from `GET /mounts/{id}/worktree`, then compares only those
paths against its index. This avoids recursively walking a monorepo mount.

### Clean fast-forward

1. Libra resolves target commit `T`.
2. Libra calls `POST /mounts/{id}/worktree/refresh-plan` with the currently
   bound revision and `T`.
3. A `ready` plan permits a future transactional base switch.
4. Libra quiesces users, switches Dicfuse to the tree for `T`, remounts, then
   updates HEAD and index atomically.

### Dirty worktree

The default plan returns `blocked_dirty`. Libra must preserve the upper layer
and either reject the operation with Git-style overwrite protection, create a
stash, or run a later three-way merge/rebase implementation. ScorpioFS must
not silently overwrite upper files.

### Commit

After Libra creates commit `C`, it must atomically switch the lower base to
`C` and remove only upper entries represented by the committed tree. If that
switch fails, HEAD/index/upper remain unchanged so no local edit is lost.

## Refresh-plan API

```http
GET  /mounts/{id}/worktree
POST /mounts/{id}/worktree/base
POST /mounts/{id}/worktree/refresh-plan
```

Base binding:

```json
{ "base_revision": "commit-or-tree-oid" }
```

Refresh preflight:

```json
{
  "expected_base_revision": "current-oid",
  "target_revision": "target-oid",
  "require_clean": true
}
```

The response is one of `ready`, `already_at_target`, `unbound`,
`base_mismatch`, or `blocked_dirty`. It is not an update command.

## Required server capability for actual switching

Mega must expose revision-addressable tree and blob reads, for example:

```text
GET /api/v1/tree?path=<path>&revision=<commit>
GET /api/v1/tree/content-hash?path=<path>&revision=<commit>
GET /api/v1/blob/<oid>
```

Until that exists, a `base_revision` is control-plane provenance only. It must
not be advertised as proof that Dicfuse is reading an immutable historical tree.

## Future conflict model

For merge/rebase, Libra owns the three-way comparison and index stages:

```text
BASE   = merge base
OURS   = HEAD/index/upper
THEIRS = target revision
```

ScorpioFS should later record the base OID at first copy-up/delete, expose it
with each changed path, and preserve upper files. Libra then creates normal
Git conflict stages and decides whether to materialize conflict files. The FUSE
layer must not write conflict markers by default because build tools may read
them as ordinary source.
