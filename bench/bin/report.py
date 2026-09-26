#!/usr/bin/env python3
"""bench/bin/report.py — 聚合 results/raw/*.jsonl → REPORT.md（中位数/IQR/p95 矩阵）。"""
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

RAW = Path(__file__).resolve().parent.parent / "results" / "raw"
OUT = Path(__file__).resolve().parent.parent / "results" / "REPORT.md"

PASS_HINT = {"judge_pass"}  # 0/1 指标按通过率汇总


def load():
    rows = []
    for f in sorted(RAW.glob("*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"skip malformed line in {f.name}", file=sys.stderr)
    return rows


def fmt(vals, metric):
    vals = sorted(vals)
    if metric in PASS_HINT:
        return f"{sum(vals)}/{len(vals)} pass"
    med = st.median(vals)
    p95 = vals[min(len(vals) - 1, round(0.95 * (len(vals) - 1)))]
    iqr = (vals[int(len(vals) * 0.75)] - vals[int(len(vals) * 0.25)]) if len(vals) > 3 else 0
    unit = "ms" if metric.endswith("_ms") else ("B" if metric.endswith("_bytes") else "")
    if unit == "B" and med > 1 << 20:
        return f"{med/(1<<20):.1f}MB (p95 {p95/(1<<20):.1f}MB)"
    if unit == "B" and med > 1 << 10:
        return f"{med/(1<<10):.1f}KB (p95 {p95/(1<<10):.1f}KB)"
    return f"{med:.0f}{unit} ±{iqr:.0f} (p95 {p95:.0f}{unit})"


def main():
    rows = load()
    if not rows:
        print("no data in results/raw/", file=sys.stderr)
        return 1
    groups = defaultdict(lambda: defaultdict(list))  # (exp,case,metric) -> side -> [vals]
    notes = defaultdict(list)
    for r in rows:
        key = (r["exp"], r["case"], r["metric"])
        if r["value"] == -1:
            notes[key].append(r["meta"].get("note", "failed"))
            continue
        groups[key][r["side"]].append(r["value"])
        if "note" in r["meta"]:
            notes[key].append(r["meta"]["note"])

    # 动态列：所有出现过的 side（保证 git-off/git-on/mono 多线对比可分列）
    all_sides = sorted({r["side"] for r in rows})
    lines = ["# GitMono vs Git — 结果报告", "",
             "| 实验 | 用例 | 指标 | " + " | ".join(all_sides) + " | 备注 |",
             "|---|---|---|" + "---|" * len(all_sides) + "---|"]
    for (exp, case, metric) in sorted(groups):
        sides = groups[(exp, case, metric)]
        cells = []
        for side in all_sides:
            vals = sides.get(side)
            cells.append(fmt(vals, metric) if vals else "—")
        note = "; ".join(sorted(set(notes.get((exp, case, metric), []))))[:80]
        lines.append(f"| {exp} | {case} | {metric} | " + " | ".join(cells) + f" | {note} |")
    lines += ["", f"数据源: {len(rows)} 行原始记录（results/raw/）"]
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
