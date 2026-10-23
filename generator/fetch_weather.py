"""Fetch 2021-2025 daily weather per district from Open-Meteo (cached, resumable).

Input : data_ref/district_ref.csv
Output: data_raw/weather_daily.csv   (REAL data, source: Open-Meteo historical archive)
Re-running skips districts already cached in data_raw/weather_cache/.
"""
import csv
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REF = ROOT / "data_ref" / "district_ref.csv"
CACHE = ROOT / "data_raw" / "weather_cache"
OUT = ROOT / "data_raw" / "weather_daily.csv"

START, END = "2021-01-01", "2025-12-31"
VARS = ["temperature_2m_mean", "precipitation_sum", "relative_humidity_2m_mean"]
URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch(lat, lon, variables):
    q = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon,
        "start_date": START, "end_date": END,
        "daily": ",".join(variables),
        "timezone": "Asia/Dhaka",
    })
    with urllib.request.urlopen(f"{URL}?{q}", timeout=120) as r:
        return json.load(r)


def fetch_with_retry(lat, lon):
    variables = list(VARS)
    for attempt in range(6):
        try:
            return fetch(lat, lon, variables)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 30 * (attempt + 1)
                print(f"  rate limited, waiting {wait}s")
                time.sleep(wait)
            elif e.code == 400 and len(variables) > 2:
                variables = variables[:2]  # drop humidity if the API rejects it
                print("  dropping humidity variable")
            else:
                raise
        except (urllib.error.URLError, TimeoutError) as e:
            print(f"  network error ({e}), retrying")
            time.sleep(10)
    raise RuntimeError("giving up after 6 attempts")


def main():
    CACHE.mkdir(parents=True, exist_ok=True)
    with open(REF, encoding="utf-8") as f:
        districts = list(csv.DictReader(f))

    for i, d in enumerate(districts, 1):
        path = CACHE / f"{d['district_id']}.json"
        if path.exists():
            continue
        print(f"[{i}/{len(districts)}] {d['district']}")
        data = fetch_with_retry(d["lat"], d["lon"])
        path.write_text(json.dumps(data), encoding="utf-8")
        time.sleep(2)

    rows = 0
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["district_id", "date", "temp_mean_c", "precip_mm", "humidity_mean_pct"])
        for d in districts:
            daily = json.loads((CACHE / f"{d['district_id']}.json").read_text(encoding="utf-8"))["daily"]
            hum = daily.get("relative_humidity_2m_mean", [None] * len(daily["time"]))
            for t, temp, pr, h in zip(daily["time"], daily["temperature_2m_mean"],
                                      daily["precipitation_sum"], hum):
                w.writerow([d["district_id"], t, temp, pr, h])
                rows += 1
    print(f"wrote {rows} rows -> {OUT}")


if __name__ == "__main__":
    main()