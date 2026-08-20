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

## Optional `allow_other` support

Only when mounts must be accessible to users other than the process owner,
uncomment or add this exact line in `/etc/fuse.conf`:

```text
user_allow_other
```

This setting permits FUSE filesystems to use the `allow_other` mount option.
Leave it disabled when that access is not required.
