#!/usr/bin/env python3
import csv, json, subprocess, time, sys, os, math, threading
from datetime import datetime, timezone
import requests

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
REUSE_MAX_AGE_SEC = 10

FIREBASE_DB_URL = os.environ.get("FIREBASE_DB_URL","").rstrip("/")
FIREBASE_AUTH   = os.environ.get("FIREBASE_AUTH")
JC_KEY          = os.environ.get("JCDECAUX_API_KEY")
BIKES_POLL_SECS = float(os.environ.get("BIKES_POLL_SECS","60"))

JCD_URL = "https://api.jcdecaux.com/vls/v1/stations" 

# Shared latest bikes snapshot for nearest lookup
BIKES_LOCK = threading.Lock()
LATEST_STATIONS = []  # list of dicts with lat, lon, name, available_bikes, available_stands

def fb_url(path):
    if not FIREBASE_DB_URL:
        return ""
    url = f"{FIREBASE_DB_URL}/{path.lstrip('/')}.json"
    if FIREBASE_AUTH:
        sep = '&' if '?' in url else '?'
        url = f"{url}{sep}auth={FIREBASE_AUTH}"
    return url


def post_point(session_id, row):
    if not FIREBASE_DB_URL:
        return
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
        requests.post(fb_url(f"sessions/{session_id}/points"), json=doc, timeout=5)
    except Exception as e:
        print(f"Warn: Firebase post failed: {e}", file=sys.stderr)

def post_bike_item(item):
    if not FIREBASE_DB_URL:
        return
    try:
        requests.post(fb_url("open_data/dublin_bikes/items"), json=item, timeout=6)
    except Exception as e:
        print(f"Warn: Firebase post failed (bikes): {e}", file=sys.stderr)

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

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlmb/2)**2
    return 2*R*math.asin(math.sqrt(a))

def bikes_fetch_normalize():
    if not JC_KEY:
        return []
    r = requests.get(JCD_URL, params={"contract":"dublin","apiKey":JC_KEY}, timeout=10)
    r.raise_for_status()
    arr = r.json()
    now_ms = int(time.time()*1000)
    docs = []
    for s in arr:
        pos = s.get("position", {})
        docs.append({
            "station_id": s.get("number"),
            "name": s.get("name"),
            "lat": pos.get("lat"),
            "lon": pos.get("lng"),
            "available_bikes": s.get("available_bikes"),
            "available_stands": s.get("available_bike_stands"),
            "status": s.get("status"),
            "last_update_ms": s.get("last_update"),
            "timestamp_ms": now_ms,
            "source_type": "open_data"
        })
    return docs

def bikes_poller():
    # Background thread: poll JCDecaux and push to Firebase; keep LATEST_STATIONS for nearest lookup
    while True:
        t0 = time.time()
        try:
            docs = bikes_fetch_normalize()
            if docs:
                for d in docs:
                    post_bike_item(d)
                with BIKES_LOCK:
                    # keep the latest snapshot in memory
                    global LATEST_STATIONS
                    LATEST_STATIONS = docs
                print(f"[bikes] pushed {len(docs)} stations; snapshot updated")
        except Exception as e:
            print(f"[bikes] poll error: {e}", file=sys.stderr)
        dt = time.time() - t0
        time.sleep(max(0, BIKES_POLL_SECS - dt))

def nearest_station_to(lat, lon):
    with BIKES_LOCK:
        stations = list(LATEST_STATIONS)
    best = None
    best_d = float("inf")
    for s in stations:
        if s.get("lat") is None or s.get("lon") is None:
            continue
        d = haversine_m(lat, lon, s["lat"], s["lon"])
        if d < best_d:
            best_d = d; best = s
    return best, best_d

def main():
    global LAST_LOC, LAST_TIME
    print(f"Logging NETWORK provider to {OUT} every {INTERVAL}s; bikes poll {BIKES_POLL_SECS}s.", flush=True)
    if not FIREBASE_DB_URL:
        print("Note: FIREBASE_DB_URL not set; cloud streaming disabled.", file=sys.stderr)
    if not JC_KEY:
        print("Note: JCDECAUX_API_KEY not set; bikes polling disabled.", file=sys.stderr)

    # Start bikes poller thread
    t = threading.Thread(target=bikes_poller, daemon=True)
    t.start()

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

                # Nearest station print
                lat = row["latitude"]; lon = row["longitude"]
                if lat is not None and lon is not None:
                    best, d = nearest_station_to(lat, lon)
                    if best:
                        print(f"Nearest station: {best['name']} at {int(d)} m | bikes={best.get('available_bikes')} stands={best.get('available_stands')}")
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
