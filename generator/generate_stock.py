"""Generate a clearly labelled synthetic facility stock snapshot for S6."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FACILITIES = ROOT / "data_raw" / "synthetic" / "facility_registry.csv"
DEFAULT_OUTPUT = ROOT / "data_raw" / "synthetic" / "facility_stock_snapshot.csv"


def generate_stock_snapshot(
    facilities_path: str | Path = DEFAULT_FACILITIES,
    output_path: str | Path = DEFAULT_OUTPUT,
    snapshot_date: str | None = None,
    seed: int = 2026,
) -> pd.DataFrame:
    """Create reproducible synthetic inventory, preserving any supplied S6 file."""
    facilities_path = Path(facilities_path)
    output_path = Path(output_path)
    if not facilities_path.is_file():
        raise FileNotFoundError(f"Facility registry not found: {facilities_path}")
    if output_path.exists():
        return pd.read_csv(output_path)

    facilities = pd.read_csv(facilities_path)
    required = {"facility_id", "district_id", "beds"}
    missing = required - set(facilities.columns)
    if missing:
        raise ValueError(f"Facility registry is missing columns: {sorted(missing)}")
    beds = pd.to_numeric(facilities["beds"], errors="raise").clip(lower=0).to_numpy()
    if (beds < 0).any():
        raise ValueError("Facility beds cannot be negative.")

    if snapshot_date is None:
        counts_path = ROOT / "data_raw" / "synthetic" / "true_daily_counts.csv"
        if counts_path.is_file():
            dates = pd.read_csv(counts_path, usecols=["date"], parse_dates=["date"])["date"]
            if dates.empty or dates.isna().any():
                raise ValueError(f"Daily counts contain no valid dates: {counts_path}")
            snapshot_date = dates.max().date().isoformat()
        else:
            snapshot_date = pd.Timestamp.now(tz="UTC").date().isoformat()

    rng = np.random.default_rng(seed)
    snapshot = facilities[["facility_id", "district_id"]].copy()
    snapshot["snapshot_date"] = snapshot_date
    snapshot["beds_occupied"] = np.minimum(
        beds,
        np.rint(beds * rng.uniform(0.45, 0.85, len(facilities))).astype(int),
    )
    snapshot["iv_saline"] = np.rint(
        beds * rng.uniform(0.8, 1.8, len(facilities))
    ).astype(int)
    snapshot["ns1_kits"] = np.rint(
        beds * rng.uniform(0.4, 1.2, len(facilities))
    ).astype(int)
    snapshot["ors"] = np.rint(
        beds * rng.uniform(1.0, 2.5, len(facilities))
    ).astype(int)
    snapshot["antimalarials"] = np.rint(
        beds * rng.uniform(0.15, 0.6, len(facilities))
    ).astype(int)
    snapshot["stock_source"] = "synthetic_planning_scenario"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot.to_csv(output_path, index=False)
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--facilities", type=Path, default=DEFAULT_FACILITIES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--snapshot-date", default=None)
    parser.add_argument("--seed", type=int, default=2026)
    options = parser.parse_args()
    snapshot = generate_stock_snapshot(
        options.facilities,
        options.output,
        options.snapshot_date,
        options.seed,
    )
    print(f"stock snapshot rows={len(snapshot)} -> {options.output}")


if __name__ == "__main__":
    main()
