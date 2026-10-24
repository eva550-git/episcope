"""Build next-year disease preparedness guidance from historical monthly counts."""

from __future__ import annotations

import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose


def seasonal_calendar(counts: pd.DataFrame) -> pd.DataFrame:
    """Forecast next-year monthly cases and identify preparation lead-in months.

    `counts` must contain `date`, `division`, `disease`, and `cases`. At least two
    complete annual cycles are required for an additive monthly decomposition.
    """
    required = {"date", "division", "disease", "cases"}
    missing = required - set(counts.columns)
    if missing:
        raise ValueError(f"Calendar input is missing columns: {', '.join(sorted(missing))}.")

    data = counts.loc[:, ["date", "division", "disease", "cases"]].copy()
    data["date"] = pd.to_datetime(data["date"], errors="raise")
    data["cases"] = pd.to_numeric(data["cases"], errors="raise").clip(lower=0)
    if data[["division", "disease"]].isna().any().any():
        raise ValueError("Calendar division and disease values cannot be null.")

    rows = []
    for (division, disease), group in data.groupby(["division", "disease"], sort=True):
        monthly = group.groupby(group["date"].dt.to_period("M"))["cases"].sum()
        if len(monthly) < 24:
            continue
        full_index = pd.period_range(monthly.index.min(), monthly.index.max(), freq="M")
        series = monthly.reindex(full_index, fill_value=0.0)
        values = series.to_numpy(dtype=float)
        decomposition = seasonal_decompose(
            values,
            model="additive",
            period=12,
            extrapolate_trend="freq",
        )
        seasonal = pd.Series(decomposition.seasonal, index=full_index)
        monthly_pattern = seasonal.groupby(seasonal.index.month).mean()
        peak_month = int(monthly_pattern.idxmax())

        trend = np.asarray(decomposition.trend, dtype=float)
        x = np.arange(max(0, len(trend) - 12), len(trend), dtype=float)
        slope = float(np.polyfit(x, trend[-len(x):], deg=1)[0]) if len(x) >= 2 else 0.0
        next_year = int(series.index[-1].year + 1)
        for month in range(1, 13):
            period = pd.Period(year=next_year, month=month, freq="M")
            future_trend = max(0.0, float(trend[-1] + slope * month))
            seasonal_component = float(monthly_pattern.loc[month])
            campaign_month = (peak_month - 1 - 3) % 12 + 1
            preposition_month = (peak_month - 1 - 2) % 12 + 1
            if month == campaign_month:
                action = "Start prevention campaign"
            elif month == preposition_month:
                action = "Pre-position supplies"
            elif month == peak_month:
                action = "Peak-season readiness"
            else:
                action = "Routine monitoring"
            rows.append(
                {
                    "division": division,
                    "disease": disease,
                    "calendar_year": period.year,
                    "month": period.month,
                    "expected_cases": round(max(0.0, future_trend + seasonal_component), 2),
                    "seasonal_component": round(seasonal_component, 2),
                    "peak_month": peak_month,
                    "action": action,
                }
            )
    return pd.DataFrame(
        rows,
        columns=[
            "division",
            "disease",
            "calendar_year",
            "month",
            "expected_cases",
            "seasonal_component",
            "peak_month",
            "action",
        ],
    )
