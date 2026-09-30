# GitMono vs Git — 结果报告

| 实验 | 用例 | 指标 | git | git-off | git-on | mono | 备注 |
|---|---|---|---|---|---|---|---|
| E1 | DISK | disk_bytes | 5.5MB (p95 5.5MB) | — | — | 6.9MB (p95 6.9MB) |  |
| E1 | STATUS-SCALE | status_ms | — | 7ms ±9 (p95 17ms) | 6ms ±10 (p95 16ms) | 190ms ±118 (p95 323ms) |  |
| E1 | TTFW | ttfw | 417 ±0 (p95 446) | — | — | 341 ±0 (p95 7071) |  |

数据源: 80 行原始记录（results/raw/）
