"""Forecast district-level disease demand using a simple seasonal baseline."""

from __future__ import annotations

import pandas as pd


def forecast_cases(counts: pd.DataFrame, horizon_days: int = 14) -> pd.DataFrame:
    """Return a forecast table for each district/disease combination."""
    df = counts.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["district_id", "disease", "date"]).reset_index(drop=True)

    results = []
    for (district_id, disease), group in df.groupby(["district_id", "disease"]):
        group = group.sort_values("date").copy()
        last_date = group["date"].max()
        baseline = group["cases"].tail(28).mean()
        for i in range(1, horizon_days + 1):
            forecast_date = last_date + pd.Timedelta(days=i)
            results.append({
                "district_id": district_id,
                "disease": disease,
                "forecast_date": forecast_date,
                "forecast_cases": max(0.0, round(float(baseline), 2)),
            })
    return pd.DataFrame(results)
