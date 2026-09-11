# FUSE owner alignment

When `scorpio` is launched through `sudo`, the daemon remains root so it can establish
the FUSE session, but the mounted view must belong to the agent user. ScorpioFS resolves
the mounted UID/GID in this order:

1. `SCORPIO_FUSE_UID` and `SCORPIO_FUSE_GID`, when both are valid and the daemon is root;
2. `SUDO_UID` and `SUDO_GID`, when present for a sudo-launched daemon;
3. the daemon process UID/GID.

The resolved identity is applied to FUSE mount options, Dicfuse default attributes, and
new Antares upper/mount directories. This lets a non-root agent use a root-launched
daemon without turning the lower Dicfuse layer into a writable filesystem.

For a normal sudo launch, no extra setting is needed:

```bash
sudo -E scorpio --config-path scorpio.toml serve
```

For service managers, set both values explicitly:

```bash
SCORPIO_FUSE_UID=1000
SCORPIO_FUSE_GID=1000
```

The lower Dicfuse mount remains read-only. Agent writes must go through an Antares
writable overlay.
