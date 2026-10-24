"""Create a gold weekly case mart from district disease counts."""

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "pipeline" / "gold" / "gold_weekly_cases.csv"


def build_gold_mart(source_csv=None, output_path=OUT):
    source = Path(source_csv) if source_csv else ROOT / "data_raw" / "synthetic" / "true_daily_counts.csv"
    counts = pd.read_csv(source, parse_dates=["date"])
    ref = pd.read_csv(ROOT / "data_ref" / "district_ref.csv")

    df = counts.merge(ref[["district_id", "division_id", "division", "district", "population"]], on="district_id", how="left")
    iso = df["date"].dt.isocalendar()
    df["epi_year"] = iso["year"].astype(int)
    df["epi_week"] = iso["week"].astype(int)
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.dayofweek, unit="D")

    gold = (
        df.groupby(["epi_year", "epi_week", "district_id", "division_id", "division", "district", "disease", "population", "week_start"], as_index=False)
        .agg(cases=("cases", "sum"))
    )
    gold["lab_confirmed"] = (gold["cases"] * 0.25).round().astype(int)
    gold["admitted"] = (gold["cases"] * 0.10).round().astype(int)
    gold["deaths"] = (gold["cases"] * 0.015).round().astype(int)
    gold["reports_received"] = (gold["cases"] / 10).round().clip(lower=1).astype(int)
    gold["facilities_expected"] = (gold["population"] / 1000).round().clip(lower=1).astype(int)
    gold["load_batch_id"] = "synthetic"
    gold = gold[[
        "epi_year", "epi_week", "week_start", "division", "district", "disease", "cases",
        "lab_confirmed", "admitted", "deaths", "reports_received", "facilities_expected",
        "population", "load_batch_id"
    ]]
    gold["week_start"] = pd.to_datetime(gold["week_start"]).dt.date

    output_path.parent.mkdir(parents=True, exist_ok=True)
    gold.to_csv(output_path, index=False)
    print(f"gold mart written to {output_path} ({len(gold)} rows)")
    return gold


if __name__ == "__main__":
    build_gold_mart()
