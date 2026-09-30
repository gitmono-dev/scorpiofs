#!/usr/bin/env python3
"""Consolidate bench/results/raw/*.jsonl into a readable RESULTS-SUMMARY.md.

Canonical files only (E*-*.jsonl minus the ones explicitly archived as INVALID).
Archived files are listed separately so nothing is silently dropped.
"""
import json, os, statistics as st, datetime, sys
from collections import defaultdict

RAW = sys.argv[1] if len(sys.argv) > 1 else "/mnt/d/--------code----------/mega-scorpiofs/scorpiofs/bench/results/raw"
OUT = os.path.join(os.path.dirname(RAW), "RESULTS-SUMMARY.md")

rows, skipped = [], []
for fn in sorted(os.listdir(RAW)):
    if not fn.endswith(".jsonl"):
        continue
    if "INVALID" in fn or "partial" in fn:
        skipped.append(fn)
        continue
    for line in open(os.path.join(RAW, fn)):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            pass

groups = defaultdict(list)
for r in rows:
    # The repo measured on 2026-09-24/25/26 was a <2k-file filtered tree
    # (synthsmoke/tokio); from 2026-09-27 on it is the ~124k-file monorepo.
    # Mixing them makes medians meaningless, so group by era as well.
    ts = r.get("meta", {}).get("ts", "")
    era = "current(124k)" if ts >= "2026-09-27" else "early(<2k tree)"
    groups[(era, r["exp"], r["case"], r["side"], r["metric"])].append(r["value"])

def fmt(v):
    return f"{v:,}"

L = []
L.append("# GitMono vs Git Stack — 测量结果汇总")
L.append("")
L.append(f"生成时间：{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
L.append("")
L.append(f"数据源：`bench/results/raw/*.jsonl`（{len(rows)} 行原始记录，可回溯重算）")
L.append("")
L.append("> 环境：WSL2 Ubuntu-22.04 / 23 GiB RAM / mega2(local docker) + ScorpioFS daemon(127.0.0.1:37251)")
L.append("> libra 0.19.63 / git 系统版。")
L.append("")
L.append("**注意**：结果按「测的是什么树」分成两代 —— `early(<2k tree)` 是 2026-09-24/25/26 ")
L.append("在不到 2k 文件的过滤树（synthsmoke/tokio）上测的；`current(124k)` 是 2026-09-27 起在")
L.append("约 124k 文件的 `/project` monorepo 上测的。两代**不可混算**，因为树的规模差 ~60 倍。")
L.append("")

order = {"E1": 1, "E2": 2, "E3": 3}
for era in ("current(124k)", "early(<2k tree)"):
    eras = [k for k in groups if k[0] == era]
    if not eras:
        continue
    L.append(f"# 数据代：{era}")
    L.append("")
    for exp in sorted({k[1] for k in eras}, key=lambda e: order.get(e, 9)):
        L.append(f"## {exp}")
        L.append("")
        cases = sorted({k[2] for k in groups if k[0] == era and k[1] == exp})
        for case in cases:
            L.append(f"### {exp}-{case}")
            L.append("")
            L.append("| side | metric | n | median | min | max |")
            L.append("|---|---|---|---|---|---|")
            keys = sorted([k for k in groups if k[0] == era and k[1] == exp and k[2] == case],
                          key=lambda k: (k[3], k[4]))
            for k in keys:
                v = [x for x in groups[k] if isinstance(x, (int, float)) and x >= 0]
                if not v:
                    continue
                L.append(f"| {k[3]} | {k[4]} | {len(v)} | **{fmt(round(st.median(v)))}** | "
                         f"{fmt(min(v))} | {fmt(max(v))} |")
            L.append("")

if skipped:
    L.append("## 已归档的无效数据（保留在 raw/ 便于追溯）")
    L.append("")
    for fn in skipped:
        path = os.path.join(RAW, fn)
        n = sum(1 for l in open(path) if l.strip())
        L.append(f"- `{fn}` — {n} 行，见文件内 meta 的 note 字段")
    L.append("")

open(OUT, "w").write("\n".join(L) + "\n")
print(f"wrote {OUT}")
print(f"  {len(rows)} rows from {len({(r['exp'], r['case']) for r in rows})} (exp,case) pairs")
print(f"  skipped {len(skipped)} archived file(s)")
