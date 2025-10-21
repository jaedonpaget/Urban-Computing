#!/usr/bin/env python3
import csv, sys, statistics as stats

inp = sys.argv[1] if len(sys.argv)>1 else "gps_log.csv"
rows = []
with open(inp, newline="") as f:
    r = csv.DictReader(f)
    for row in r:
        try:
            row["timestamp_ms"] = int(row["timestamp_ms"])
            for k in ["accuracy_m","speed_mps","bearing_deg","altitude_m"]:
                row[k] = float(row[k]) if row.get(k) not in (None,"","null") else None
            rows.append(row)
        except Exception:
            continue

rows.sort(key=lambda x:x["timestamp_ms"])
n = len(rows)
span = (rows[-1]["timestamp_ms"]-rows[0]["timestamp_ms"])/1000 if n>1 else 0
gaps = []
prev = None
for r in rows:
    if prev is not None:
        dt = (r["timestamp_ms"]-prev)/1000
        if dt > 2.5: gaps.append(dt)
    prev = r["timestamp_ms"]

accs = [r["accuracy_m"] for r in rows if r["accuracy_m"] is not None]
acc_summary = (min(accs), stats.mean(accs), max(accs)) if accs else (None, None, None)

print(f"rows={n}")
print(f"span_sec={span:.1f}")
print(f"gaps_over_2.5s={len(gaps)}")
print(f"accuracy_min_mean_max={acc_summary}")
