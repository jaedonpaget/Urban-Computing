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
    "bearing_deg","altitude_m","provider","raw_provider",
    "source","reused"
]

LAST_LOC = None
LAST_TIME = None
REUSE_MAX_AGE_SEC = 10  # reuse last fix up to 10s old if needed

def run_cmd(args, timeout):
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        return None, "timeout"
    rc = p.returncode
    return (out if rc == 0 else None), (err.strip() if err else f"rc={rc}")

def get_network_once(timeout=8):
    # Valid flags are only -p and -r; no -d supported
    out, err = run_cmd(["termux-location","-p","network","-r","once"], timeout)
    if not out:
        raise RuntimeError(f"network once failed: {err}")
    loc = json.loads(out)
    if "latitude" in loc and "longitude" in loc:
        loc["_provider_used"] = "network"
        loc["_source"] = "once"
        return loc
    raise RuntimeError("network once: no lat/lon")

def get_last(timeout=3):
    out, err = run_cmd(["termux-location","-r","last"], timeout)
    if not out:
        raise RuntimeError(f"last failed: {err}")
    loc = json.loads(out)
    if "latitude" in loc and "longitude" in loc:
        # provider may be gps/network/fused; keep whatever is reported
        loc["_provider_used"] = loc.get("provider","network")
        loc["_source"] = "last"
        return loc
    raise RuntimeError("last: no lat/lon")

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
        "source": loc.get("_source","once"),
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
            loc = None
            # 1) Try immediate network fix (request once)
            try:
                loc = get_network_once(timeout=8)
            except Exception as e1:
                # 2) Fallback to last-known fix to avoid gaps
                try:
                    loc = get_last(timeout=3)
                except Exception as e2:
                    loc = None
                    print(f"No network fix: {e1}; no last fix: {e2}", file=sys.stderr)

            if loc:
                LAST_LOC = loc
                LAST_TIME = time.time()
                w.writerow(to_row(loc, reused=False))
                f.flush()
                missed = 0
            else:
                # 3) Reuse the last emitted fix briefly
                if LAST_LOC and LAST_TIME and (time.time() - LAST_TIME) <= REUSE_MAX_AGE_SEC:
                    w.writerow(to_row(LAST_LOC, reused=True))
                    f.flush()
                else:
                    missed += 1
                    print(f"No fix emitted ({missed})", file=sys.stderr)

            # Pace the loop to INTERVAL seconds including command latency
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
