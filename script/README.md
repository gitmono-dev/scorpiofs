# Test & performance scripts

Helper scripts for local development, load generation, and performance
measurement. None of them are required to build or run ScorpioFS, and none use
developer-local paths (test data is created under `/tmp`).

| Script | Purpose | Run |
|---|---|---|
| `mktestdirs.sh` | Create empty scratch dirs for overlay/FUSE experiments. | `script/mktestdirs.sh` |
| `run.sh` | Generate a small deep directory tree (10 × 1 MB files) under `/tmp`. | `script/run.sh` |
| `run_1000_files.sh` | Generate a larger tree (~1000 files) under `/tmp`. | `script/run_1000_files.sh` |
| `fuse_test.py` | Python FUSE throughput/latency benchmark with plots. | `python3 script/fuse_test.py --help` |
| `log_analysis.py` | Extract abnormal/unmatched request IDs from a ScorpioFS log (writes `output.txt`). | `python3 script/log_analysis.py` |

## Read benchmark (integrated as a cargo example)

The former `script/run.rs` is now a first-class cargo target at
[`examples/fs_read_perf.rs`](../examples/fs_read_perf.rs). It walks a directory
and reports directory-load, `stat`, and read timings — run it against a native
path and against a ScorpioFS/FUSE mount to compare:

```bash
# 1. generate a test tree
script/run.sh                       # creates /tmp/test_<timestamp>/...

# 2. measure (native, then via a mount)
cargo run --release --example fs_read_perf -- /tmp/test_<timestamp>
```

The Python scripts need `matplotlib`, `seaborn`, and `numpy`
(`pip install matplotlib seaborn numpy`).
