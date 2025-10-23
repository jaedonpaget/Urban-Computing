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
    "bearing_deg","altitude_m","provider","raw_provider","reused"
]

# Keep the last good fix so we can reuse it briefly to avoid gaps
LAST_LOC = None
LAST_TIME = None
REUSE_MAX_AGE_SEC = 10  # reuse last fix up to 10s old

def get_loc(provider=None, max_age_ms=8000, timeout=12):
    """
    Call termux-location once and return a dict with lat/lon if available.
    provider: "network", "gps", or None (fused). max_age_ms allows recent cache.
    timeout: per-call timeout seconds (tune higher for GPS).
    """
    cmd = ["termux-location","-r","once","-d",str(max_age_ms)]
    if provider:
        cmd += ["-p", provider]
    out = subprocess.check_output(cmd, timeout=timeout)
    loc = json.loads(out.decode())
    if "latitude" in loc and "longitude" in loc:
        loc["_provider_used"] = provider or loc.get("provider")
        return loc
    raise RuntimeError("No lat/lon in location JSON")

def try_fix():
    """
    Strategy for your Pixel:
    1) Fast coarse fix via network (short timeout, larger allowed age).
    2) Fresher GPS with longer timeout but small allowed age to avoid stale GPS.
    3) Fallback to fused (None) if available.
    """
    # 1) Network: quick and acceptable at 1 Hz on your device
    try:
        return get_loc("network", max_age_ms=8000, timeout=6)
    except Exception:
        pass

    # 2) GPS: allow longer timeout, prefer fresh data
    try:
        return get_loc("gps", max_age_ms=2000, timeout=15)
    except Exception:
        pass

    # 3) Fused fallback
    try:
        return get_loc(None, max_age_ms=5000, timeout=8)
    except Exception:
        return None

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
        "provider": loc.get("provider","fused"),
        "raw_provider": loc.get("_provider_used"),
        "reused": int(1 if reused else 0),
    }

def main():
    global LAST_LOC, LAST_TIME
    print(f"Logging to {OUT} every {INTERVAL}s. Ctrl+C to stop.", flush=True)
    f, w = write_header_if_needed(OUT, FIELDS)
    missed = 0
    try:
        while True:
            start_loop = time.time()
            loc = try_fix()
            if loc:
                LAST_LOC = loc
                LAST_TIME = time.time()
                w.writerow(to_row(loc, reused=False))
                f.flush()
                missed = 0
            else:
                # Reuse last fix up to REUSE_MAX_AGE_SEC seconds old
                if LAST_LOC and LAST_TIME and (time.time() - LAST_TIME) <= REUSE_MAX_AGE_SEC:
                    w.writerow(to_row(LAST_LOC, reused=True))
                    f.flush()
                else:
                    missed += 1
                    print(f"No fresh fix ({missed})", file=sys.stderr)
            # Maintain approximate INTERVAL pacing (subtract time spent polling)
            elapsed = time.time() - start_loop
            sleep_for = INTERVAL - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        pass
    finally:
        f.close()

if __name__ == "__main__":
    main()
