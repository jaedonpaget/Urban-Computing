#!/usr/bin/env python3
import csv, json, subprocess, time, sys, os
from datetime import datetime, timezone

# Args: output_csv [interval_seconds]
OUT = sys.argv[1] if len(sys.argv) > 1 else "gps_log.csv"
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

SESSION = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
FIELDS = [
    "session_id","timestamp_ms","timestamp_iso",
    "latitude","longitude","accuracy_m","speed_mps",
    "bearing_deg","altitude_m","provider","raw_provider"
]

def get_loc(provider=None, max_age_ms="1000", timeout=6):
    cmd = ["termux-location","-r","once","-d",str(max_age_ms)]
    if provider:
        cmd += ["-p", provider]
    try:
        out = subprocess.check_output(cmd, timeout=timeout)
        loc = json.loads(out.decode())
        if "latitude" in loc and "longitude" in loc:
            loc["_provider_used"] = provider or loc.get("provider")
            return loc
    except Exception:
        return None
    return None

def write_header_if_needed(path, fieldnames):
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    f = open(path, "a", newline="")
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if not exists:
        w.writeheader()
    return f, w

def to_row(loc):
    ts_ms = int(loc.get("time", time.time()*1000))
    ts_iso = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).isoformat()
    return {
        "session_id": SESSION,
        "timestamp_ms": ts_ms,
        "timestamp_iso": ts_iso,
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
        "accuracy_m": loc.get("accuracy"),
        "speed_mps": loc.get("speed"),
        "bearing_deg": loc.get("bearing"),
        "altitude_m": loc.get("altitude"),
        "provider": loc.get("provider","fused"),
        "raw_provider": loc.get("_provider_used")
    }

def main():
    print(f"Logging to {OUT} every {INTERVAL}s. Ctrl+C to stop.", flush=True)
    f, w = write_header_if_needed(OUT, FIELDS)
    missed = 0
    try:
        while True:
            # Try fused (no -p), then network, then gps
            loc = get_loc(None) or get_loc("network") or get_loc("gps")
            if loc:
                w.writerow(to_row(loc))
                f.flush()
                missed = 0
            else:
                missed += 1
                # Optional: backoff print
                print(f"No fix ({missed})", file=sys.stderr)
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        f.close()

if __name__ == "__main__":
    main()
