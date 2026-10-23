"""S5: synthetic facility registry (>= 600 facilities, all 64 districts). Deterministic per seed.

Output: data_raw/synthetic/facility_registry.csv  (SYNTHETIC DATA)
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
REF = ROOT / "data_ref" / "district_ref.csv"
OUT = ROOT / "data_raw" / "synthetic" / "facility_registry.csv"


def allocate(weights, total):
    """Largest-remainder allocation of `total` items proportional to weights."""
    raw = weights / weights.sum() * total
    base = np.floor(raw).astype(int)
    rem = total - base.sum()
    order = np.argsort(-(raw - base))
    base[order[:rem]] += 1
    return base


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n", type=int, default=640, help="total facilities (>= 600)")
    args = ap.parse_args()
    assert args.n >= 600, "assignment requires at least 600 facilities"

    rng = np.random.default_rng(args.seed)
    ref = pd.read_csv(REF).sort_values("district_id").reset_index(drop=True)
    median_pop = ref.population.median()

    # every district gets one district hospital; the rest are shared by population
    extra = allocate(ref.population.to_numpy(float), args.n - len(ref))

    rows = []
    for i, d in ref.iterrows():
        size = float(np.clip((d.population / median_pop) ** 0.4, 0.7, 2.0))
        for j in range(1 + extra[i]):
            if j == 0:
                ftype = "hospital"
            else:
                ftype = rng.choice(["hospital", "upazila_health_complex", "clinic"],
                                   p=[0.08, 0.32, 0.60])
            if ftype == "hospital":
                beds = int(rng.integers(100, 301) * size)
                icu = int(round(beds * rng.uniform(0.04, 0.10)))
            elif ftype == "upazila_health_complex":
                beds = int(rng.integers(31, 51) * size)
                icu = int(rng.integers(0, 3))
            else:
                beds = int(rng.integers(5, 21))
                icu = 0
            rows.append({
                "district_id": d.district_id,
                "division_id": d.division_id,
                "facility_type": ftype,
                "beds": beds,
                "icu_beds": icu,
                "lat": round(d.lat + rng.normal(0, 0.12), 4),
                "lon": round(d.lon + rng.normal(0, 0.12), 4),
            })

    df = pd.DataFrame(rows)
    df.insert(0, "facility_id", [f"F{k:04d}" for k in range(1, len(df) + 1)])
    df.insert(1, "facility_name", df.facility_type.str.replace("_", " ").str.title()
              + " " + df.facility_id)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"wrote {len(df)} facilities in {df.district_id.nunique()} districts -> {OUT}")
    print(df.facility_type.value_counts().to_dict())
    print(f"total beds={df.beds.sum():,}  icu beds={df.icu_beds.sum():,}")


if __name__ == "__main__":
    main()