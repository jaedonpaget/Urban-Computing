#!/usr/bin/env python3
import csv, json, subprocess, time, sys, os
from datetime import datetime, timezone

# Usage: python gps_logger.py [output_csv] [interval_seconds]
OUT = sys.argv[1] if len(sys.argv) > 1 else "gps_log.csv"
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

SESSION = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
FIELDS = [
    "session_id","timestamp_ms","timestamp_iso",
    "latitude","longitude","accuracy_m","speed_mps",
    "bearing_deg","altitude_m","provider","raw_provider","reused"
]

# Reuse last fix briefly if a call times out, to avoid gaps
LAST_LOC = None
LAST_TIME = None
REUSE_MAX_AGE_SEC = 10  # reuse last fix up to 10s old

def get_network_loc(max_age_ms=8000, timeout=6):
    """
    Get a location fix strictly from the network provider.
    - Uses: termux-location -p network -r once -d <max_age_ms>
    - max_age_ms allows using a very recent cached fix for immediate response.
    - timeout is kept short since network fixes are near-instant on your device.
    """
    cmd = ["termux-location","-p","network","-r","once","-d",str(max_age_ms)]
    out = subprocess.check_output(cmd, timeout=timeout)
    loc = json.loads(out.decode())
    if "latitude" in loc and "longitude" in loc:
        loc["_provider_used"] = "network"
        return loc
    raise RuntimeError("No lat/lon in network location JSON")

def write_header_if_needed(path, fieldnames):
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    f = open(path, "a", newline="")
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if not exists:
        w.writeheader()
    return f, w

def to_row(loc, reused=False):
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
        "provider": loc.get("provider","network"),
        "raw_provider": loc.get("_provider_used","network"),
        "reused": int(1 if reused else 0),
    }

def main():
    global LAST_LOC, LAST_TIME
    print(f"Logging NETWORK provider to {OUT} every {INTERVAL}s. Ctrl+C to stop.", flush=True)
    f, w = write_header_if_needed(OUT, FIELDS)
    missed = 0
    try:
        while True:
            loop_start = time.time()
            try:
                # Strictly network provider with quick cadence
                loc = get_network_loc(max_age_ms=8000, timeout=6)
                LAST_LOC = loc
                LAST_TIME = time.time()
                w.writerow(to_row(loc, reused=False))
                f.flush()
                missed = 0
            except Exception as e:
                # Reuse last fix briefly to avoid holes in the time series
                if LAST_LOC and LAST_TIME and (time.time() - LAST_TIME) <= REUSE_MAX_AGE_SEC:
                    w.writerow(to_row(LAST_LOC, reused=True))
                    f.flush()
                else:
                    missed += 1
                    print(f"No network fix ({missed}): {e}", file=sys.stderr)

            # Pace loop to INTERVAL
            elapsed = time.time() - loop_start
            sleep_for = INTERVAL - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        pass
    finally:
        f.close()

if __name__ == "__main__":
    main()
