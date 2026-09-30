# GitMono vs Git Stack — 测量结果汇总

生成时间：2026-09-27T17:37:12Z

数据源：`bench/results/raw/*.jsonl`（308 行原始记录，可回溯重算）

> 环境：WSL2 Ubuntu-22.04 / 23 GiB RAM / mega2(local docker) + ScorpioFS daemon(127.0.0.1:37251)
> libra 0.19.63 / git 系统版。

**注意**：结果按「测的是什么树」分成两代 —— `early(<2k tree)` 是 2026-09-24/25/26 
在不到 2k 文件的过滤树（synthsmoke/tokio）上测的；`current(124k)` 是 2026-09-27 起在
约 124k 文件的 `/project` monorepo 上测的。两代**不可混算**，因为树的规模差 ~60 倍。

# 数据代：current(124k)

## E1

### E1-COMMIT

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | commit_ms | 3 | **94,890** | 94,185 | 97,250 |
| mono | commit_ms | 3 | **375,393** | 366,156 | 381,753 |

### E1-DISK

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | disk_bytes | 1 | **267,679,635** | 267,679,635 | 267,679,635 |
| mono | disk_bytes | 1 | **495,901,687** | 495,901,687 | 495,901,687 |
| mono | disk_bytes_real | 1 | **268,475,704** | 268,475,704 | 268,475,704 |

### E1-READ

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | read_ms | 1 | **273** | 273 | 273 |
| mono-cold | read_ms | 1 | **9,747** | 9,747 | 9,747 |
| mono-hot | read_ms | 1 | **260** | 260 | 260 |

### E1-STATUS

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | status_ms | 3 | **128** | 125 | 229 |
| git-fsmonitor | status_ms | 3 | **155** | 141 | 164 |
| mono | status_ms | 3 | **708** | 675 | 774 |

### E1-TTFW

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | ttfw | 3 | **18,512** | 17,352 | 18,625 |
| mono | ttfw | 3 | **469** | 291 | 60,236 |

## E2

### E2-FETCH

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | fetch_ms | 2 | **180** | 177 | 183 |
| git-all | disk_bytes | 2 | **1,190,276** | 1,190,276 | 1,190,276 |
| git-all | fetch_ms | 2 | **600** | 600 | 601 |
| mono | fetch_ms | 2 | **40,688** | 39,913 | 41,462 |

### E2-PARTIAL

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | partial_ms | 2 | **186** | 174 | 199 |
| mono | partial_ms | 2 | **39,526** | 39,460 | 39,593 |

### E2-UPGRADE

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | upgrade_ms | 2 | **2,274** | 2,235 | 2,314 |
| mono | upgrade_ms | 4 | **324,997** | 262,551 | 388,687 |

## E3

### E3-INIT

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | disk_bytes | 3 | **266,049,171** | 266,049,171 | 266,049,171 |
| git | init_ms | 6 | **17,474** | 16,727 | 18,605 |
| mono | disk_bytes | 3 | **322,237,348** | 322,237,348 | 322,237,348 |
| mono | init_ms | 3 | **59,171** | 58,530 | 64,935 |

### E3-MULTI

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | disk_bytes | 1 | **1,505,781,712** | 1,505,781,712 | 1,505,781,712 |
| git | multi_ms | 1 | **28,970** | 28,970 | 28,970 |
| mono | disk_bytes | 1 | **1,461,526,828** | 1,461,526,828 | 1,461,526,828 |
| mono | disk_bytes_real | 1 | **94,490,612** | 94,490,612 | 94,490,612 |
| mono | multi_ms | 1 | **265,435** | 265,435 | 265,435 |

### E3-READ

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | read_ms | 6 | **158** | 116 | 262 |
| mono | read_ms | 6 | **300** | 260 | 25,901 |

### E3-SWITCH

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | switch_ms | 3 | **184** | 160 | 190 |
| mono | switch_ms | 3 | **37** | 33 | 212 |

# 数据代：early(<2k tree)

## E1

### E1-AD1

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| mono | agent_ms | 3 | **62,191** | 12,669 | 70,600 |
| mono | judge_pass | 3 | **1** | 1 | 1 |

### E1-AD2

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| mono | agent_ms | 1 | **11,268** | 11,268 | 11,268 |
| mono | judge_pass | 1 | **1** | 1 | 1 |

### E1-AD3

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| mono | agent_ms | 1 | **8,556** | 8,556 | 8,556 |
| mono | judge_pass | 1 | **1** | 1 | 1 |

### E1-DISK

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | disk_bytes | 1 | **5,812,947** | 5,812,947 | 5,812,947 |
| mono | disk_bytes | 1 | **7,242,948** | 7,242,948 | 7,242,948 |

### E1-STATUS-SCALE

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git-off | status_ms | 69 | **28** | 6 | 114 |
| git-on | status_ms | 69 | **27** | 5 | 94 |
| mono | status_ms | 69 | **571** | 483 | 797 |

### E1-TTFW

| side | metric | n | median | min | max |
|---|---|---|---|---|---|
| git | ttfw | 3 | **417** | 395 | 446 |
| mono | ttfw | 3 | **341** | 340 | 7,071 |

## 已归档的无效数据（保留在 raw/ 便于追溯）

- `E1-NET.INVALID-loopback-not-counted.jsonl` — 2 行，见文件内 meta 的 note 字段
- `E2-UPGRADE.INVALID-git-no-service.jsonl` — 2 行，见文件内 meta 的 note 字段
- `E3-INIT.INVALID-daemon-down.jsonl` — 2 行，见文件内 meta 的 note 字段
- `E3-INIT.INVALID-double-antares.jsonl` — 15 行，见文件内 meta 的 note 字段
- `E3-INIT.round1-partial.jsonl` — 3 行，见文件内 meta 的 note 字段
- `E3-MULTI.partial-git-only.jsonl` — 2 行，见文件内 meta 的 note 字段
- `E3-SWITCH.INVALID-mono-noop.jsonl` — 3 行，见文件内 meta 的 note 字段

