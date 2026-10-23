"""S4: deterministic synthetic encounter generator for EpiScope.

Samples encounter-level rows from synthetic daily disease counts to preserve seasonal
patterns and outbreak-shaped increases. It never reads ground-truth labels.

Examples:
  python generator/generate_encounters.py --sf 0.001 --seed 42
  python generator/generate_encounters.py --sf 1 --seed 42

Generated data stays under data_raw/ and should not be committed to Git.
"""
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
COUNTS = ROOT / "data_raw" / "synthetic" / "true_daily_counts.csv"
FACILITIES = ROOT / "data_raw" / "synthetic" / "facility_registry.csv"
OUT_DIR = ROOT / "data_raw" / "synthetic"

BASE_RECORDS = 25_000_000
CHUNK_SIZE = 250_000
DISEASES = {"dengue", "diarrhoea", "ili", "malaria"}


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic EpiScope encounters.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--sf", type=float, default=0.001,
        help="scale factor; SF=1 generates at least 25 million rows (default 0.001 for a quick test)",
    )
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)
    return parser.parse_args()


def main():
    args = parse_args()
    if args.sf <= 0:
        raise SystemExit("--sf must be greater than 0")
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be greater than 0")
    if not COUNTS.exists():
        raise SystemExit(
            f"Missing {COUNTS}. Run: python generator/generate_epi_counts.py --seed "
            f"{args.seed} --sf 1"
        )
    if not FACILITIES.exists():
        raise SystemExit(
            f"Missing {FACILITIES}. Run: python generator/generate_facilities.py "
            f"--seed {args.seed} --n 640"
        )

    # Evaluation-only ground truth is intentionally never read.
    counts = pd.read_csv(COUNTS, parse_dates=["date"])
    required_counts = {"district", "disease", "date", "cases"}
    missing = required_counts - set(counts.columns)
    if missing:
        raise SystemExit(f"Daily counts file is missing columns: {sorted(missing)}")

    counts = counts[counts["disease"].isin(DISEASES)].copy()
    counts["cases"] = pd.to_numeric(counts["cases"], errors="coerce").fillna(0).clip(lower=0)
    counts = counts[counts["cases"] > 0].reset_index(drop=True)
    if counts.empty or counts["cases"].sum() <= 0:
        raise SystemExit("No positive daily cases found; regenerate counts with --sf 1.")

    facilities = pd.read_csv(FACILITIES, dtype={"facility_id": str, "district_id": str})
    required_facilities = {"facility_id", "district_id"}
    missing = required_facilities - set(facilities.columns)
    if missing:
        raise SystemExit(f"Facility registry is missing columns: {sorted(missing)}")

    facilities_by_district = {
        str(district): group["facility_id"].to_numpy(dtype=str)
        for district, group in facilities.groupby("district_id", sort=False)
    }
    district_col = "district" if "district" in counts.columns else "district_id"
    counts[district_col] = counts[district_col].astype(str)
    districts_without_facilities = sorted(set(counts[district_col]) - set(facilities_by_district))
    if districts_without_facilities:
        raise SystemExit("No facilities for districts: " + ", ".join(districts_without_facilities))

    probabilities = counts["cases"].to_numpy(dtype=np.float64)
    probabilities /= probabilities.sum()
    total = max(1, int(np.ceil(BASE_RECORDS * args.sf)))
    rng = np.random.default_rng(args.seed)
    output = OUT_DIR / f"encounters_sf{args.sf:g}_seed{args.seed}.csv"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    written = 0
    chunk_number = 0
    while written < total:
        n = min(args.chunk_size, total - written)
        row_ids = np.arange(written, written + n, dtype=np.int64)

        # Sample from disease/district/date counts; no ground-truth file is used.
        sampled = counts.iloc[rng.choice(len(counts), size=n, replace=True, p=probabilities)]
        district_values = sampled[district_col].to_numpy(dtype=str)
        disease_values = sampled["disease"].to_numpy(dtype=str)
        encounter_dates = pd.to_datetime(sampled["date"]).to_numpy(dtype="datetime64[D]")

        facility_ids = np.empty(n, dtype=object)
        for district in np.unique(district_values):
            mask = district_values == district
            facility_ids[mask] = rng.choice(
                facilities_by_district[district], size=int(mask.sum()), replace=True
            )

        patient_numbers = rng.integers(1, 5_000_001, size=n, dtype=np.int64)
        patient_ids = np.char.add("PAT", np.char.zfill(patient_numbers.astype(str), 8))
        encounter_ids = np.char.add("ENC", np.char.zfill((row_ids + 1).astype(str), 10))

        # Approximately 2% duplicate encounter IDs.
        duplicate_positions = np.flatnonzero(rng.random(n) < 0.02)
        if len(duplicate_positions):
            targets = rng.integers(0, total - 1, size=len(duplicate_positions), dtype=np.int64)
            targets += targets >= row_ids[duplicate_positions]
            encounter_ids[duplicate_positions] = np.char.add(
                "ENC", np.char.zfill((targets + 1).astype(str), 10)
            )

        # Approximately 0.5% facility references deliberately absent from registry.
        orphan_mask = rng.random(n) < 0.005
        facility_ids[orphan_mask] = [
            f"ORPHAN{idx + 1:08d}" for idx in row_ids[orphan_mask]
        ]

        # Approximately 1% invalid ages.
        ages = rng.integers(0, 101, size=n, dtype=np.int16)
        invalid_positions = np.flatnonzero(rng.random(n) < 0.01)
        if len(invalid_positions):
            ages[invalid_positions] = np.where(
                rng.random(len(invalid_positions)) < 0.5, -1, 150
            ).astype(np.int16)

        delays = rng.integers(0, 22, size=n, dtype=np.int16)
        report_dates = encounter_dates + delays.astype("timedelta64[D]")

        chunk = pd.DataFrame({
            "encounter_id": encounter_ids,
            "patient_id": patient_ids,
            "facility_id": facility_ids,
            "district_id": district_values,
            "disease": disease_values,
            "encounter_date": pd.to_datetime(encounter_dates).strftime("%Y-%m-%d"),
            "report_date": pd.to_datetime(report_dates).strftime("%Y-%m-%d"),
            "age": ages,
        })
        chunk.to_csv(output, mode="a", header=(written == 0), index=False)
        written += n
        chunk_number += 1
        print(f"chunk {chunk_number}: wrote {written:,}/{total:,} rows")

    print(f"\nDone: {written:,} encounter rows")
    print(f"Output: {output}")
    print(f"Seed: {args.seed}; SF: {args.sf:g}; chunk size: {args.chunk_size:,}")
    print("Synthetic data is ignored by Git via data_raw/.")


if __name__ == "__main__":
    main()
