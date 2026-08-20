## Run with elevated privileges

Add the following runner configuration to `.cargo/config.toml` if the local
FUSE setup requires root privileges:

```toml
[target.x86_64-unknown-linux-gnu]
runner = 'sudo -E'
```

## Ubuntu dependencies

```bash
sudo apt update
sudo apt install build-essential pkg-config libfuse3-dev libssl-dev
```

## Required `allow_other` support for unprivileged runs

ScorpioFS requests the `allow_other` mount option for its FUSE mounts. Before
running it as a non-root user, uncomment or add this exact line in
`/etc/fuse.conf`:

```text
user_allow_other
```

This setting permits FUSE filesystems to use the `allow_other` mount option.
The interactive installer can enable it with `--enable-user-allow-other`.
