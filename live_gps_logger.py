#!/usr/bin/env python3

import csv, json, subprocess, time, sys, os
from datetime import datetime, timezone

# Usage: python gps_logger.py [output_csv] [interval_seconds]
# Environment:
#   FIREBASE_DB_URL = https://urban-computing-ass3-default-rtdb.europe-west1.firebasedatabase.app  (or ...default-rtdb.<region>.firebasedatabase.app)
#   FIREBASE_AUTH   = <optional token>  # used as ?auth= token on REST calls

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

# -------- Firebase settings --------
import requests
FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL", "").rstrip("/")
FIREBASE_AUTH = os.environ.get("FIREBASE_AUTH")

def fb_url(path):
    if not FIREBASE_DB_URL:
        return ""
    url = f"{FIREBASE_DB_URL}/{path.lstrip('/')}.json"
    if FIREBASE_AUTH:
        sep = '&' if '?' in url else '?'
        url = f"{url}{sep}auth={FIREBASE_AUTH}"
    return url

def post_point(session_id, row):
    """
    Send a single point to Firebase Realtime Database under:
      sessions/{session_id}/points/{auto_id}
    Non-blocking best-effort: failures print a warning but do not stop logging.
    """
    if not FIREBASE_DB_URL:
        return  # disabled if URL not set
    doc = {
        "timestamp_ms": row["timestamp_ms"],
        "timestamp_iso": row["timestamp_iso"],
        "lat": row["latitude"],
        "lon": row["longitude"],
        "accuracy_m": row["accuracy_m"],
        "speed_mps": row["speed_mps"],
        "bearing_deg": row["bearing_deg"],
        "altitude_m": row["altitude_m"],
        "provider": row["provider"],
        "raw_provider": row["raw_provider"],
        "source": row["source"],
        "reused": row["reused"],
        "source_type": "device"
    }
    try:
        # POST to create an auto-id child
        url = fb_url(f"sessions/{session_id}/points")
        if not url:
            return
        requests.post(url, json=doc, timeout=5)
    except Exception as e:
        print(f"Warn: Firebase post failed: {e}", file=sys.stderr)

# -------- Termux helpers --------

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
    if not FIREBASE_DB_URL:
        print("Note: FIREBASE_DB_URL not set; cloud streaming is disabled (CSV only).", file=sys.stderr)
    f, w = write_header_if_needed(OUT, FIELDS)
    missed = 0
    try:
        while True:
            loop_start = time.time()
            loc = None
            try:
                loc = get_network_once(timeout=8)
            except Exception as e1:
                try:
                    loc = get_last(timeout=3)
                except Exception as e2:
                    loc = None
                    print(f"No network fix: {e1}; no last fix: {e2}", file=sys.stderr)

            if loc:
                LAST_LOC = loc
                LAST_TIME = time.time()
                row = to_row(loc, reused=False)
                w.writerow(row); f.flush()
                post_point(SESSION, row)
                missed = 0
            else:
                if LAST_LOC and LAST_TIME and (time.time() - LAST_TIME) <= REUSE_MAX_AGE_SEC:
                    row = to_row(LAST_LOC, reused=True)
                    w.writerow(row); f.flush()
                    post_point(SESSION, row)
                else:
                    missed += 1
                    print(f"No fix emitted ({missed})", file=sys.stderr)

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
