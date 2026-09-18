# MST/2 end-to-end acceptance (mega2 + ScorpioFS)

One command brings up a local mega2 with the MST/2 snapshot surface enabled,
seeds a repository, and checks that ScorpioFS can read it correctly end to end:

```bash
tests/mst2-e2e/run.sh
```

The run exits `0` only when every check passes; each check prints `PASS`/`FAIL`
so a failure names itself.

## What it builds

| Service | Source | Notes |
|---|---|---|
| `mega2` | `../monoengine` + `../mst2-codec` | `Dockerfile.mst2`: release build of the local checkout with `[mst2].enabled` and `mst2.publication_enabled`. Replaces the published image the base compose file uses. |
| `mst2-e2e` | this repository | `tests/mst2-e2e/Dockerfile`: the `mst2_mount` / `mst2_sync` example binaries plus `git`, `fuse3` and `python3`, running `acceptance.sh` once and exiting with its verdict. |
| postgres / redis / rustfs | inherited | The base `docker-compose.yml` services, unchanged. |

The stack is layered on this repository's own `docker-compose.yml`, so the
mega2/ScorpioFS wiring, ports and volumes stay defined in one place.

## What it proves

1. **Capabilities gate** — the deployment advertises exactly the MST/2 surface
   it serves (`resolve`, `directory`, `lookup`, `metadata_pages`, `raw_blob`,
   `objects`, `chunk_reads`, and an encoding).
2. **Seeding** — a push over plain Git Smart HTTP with the shapes that stress
   the reader: small files, an empty file, an executable, a symlink, a
   >256 KiB file (2 MiB + 7 B, so it ends on a short chunk) and a 129-entry
   directory (forcing an MTP2 branch page).
3. **Resolve + hydrate** — the client resolves the view, syncs it, hydrates a
   verified local store, and its own manifest is checked against a full walk.
4. **Mount vs git truth** — every file the FUSE mount serves is compared by
   SHA-256 against a plain `git clone` of the same commit, and the executable
   bit and symlink target are checked.
5. **Offline reopen** — the completed store is remounted with no server
   contact and must serve identical bytes.
6. **SYS-01** — after `main` advances, the existing mount keeps serving the
   old commit and the new file is absent from it.
7. **Incremental reuse** — a second version reports the three independent
   counters (`traversal_nodes`, `fetched_pages`, `reused_pages`) plus the
   hydrate counters, so "did not download again" and "did not traverse
   everything" are visible as separate numbers.

## Prerequisites

* Docker Engine with the Compose v2 plugin.
* The sibling checkouts `../monoengine` and `../mst2-codec` (the codec is a
  path dependency of the mega2 build).
* `/dev/fuse` on the host, and the ability to run a privileged container:
  the mount happens inside the `mst2-e2e` container. See `deploy/README.md`
  for the same prerequisite stated for the regular ScorpioFS stack.

## Notes

* Everything is local-only: mega2 runs with `push_auth=none` and the port is
  published on loopback for debugging. Nothing is exposed beyond the host.
* The acceptance runs with the lease it resolves (`M2_LEASE`, default 120s);
  the client renews in the background, so a long hydration does not lapse.
* `run.sh` always tears the project down (volumes included) on exit, so a
  rerun starts from an empty stack.
