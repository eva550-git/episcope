"""Rolling-origin evaluation for the weekly Spark forecasting model."""

from __future__ import annotations

from typing import Any

import pandas as pd

from analytics.forecast.weekly_model import (
    HORIZONS,
    MIN_TRAIN_WEEKS,
    _fit_and_forecast,
    _validated_history,
)


def _evaluate_series(pdf: pd.DataFrame, *, min_train_weeks: int) -> pd.DataFrame:
    """Generate rolling-origin scores for one Spark-grouped weekly series."""
    pdf = pdf.sort_values("week_start").reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for origin_index in range(min_train_weeks - 1, len(pdf) - 1):
        forecasts = _fit_and_forecast(
            pdf, origin_index=origin_index, min_train_weeks=min_train_weeks
        )
        for prediction in forecasts:
            horizon = int(prediction["horizon"])
            target_index = origin_index + horizon
            if target_index >= len(pdf):
                continue
            seasonal_index = target_index - 4
            seasonal_naive = (
                float(pdf.iloc[seasonal_index]["cases"])
                if seasonal_index <= origin_index
                else None
            )
            rows.append(
                {
                    "district_id": prediction["district_id"],
                    "disease": prediction["disease"],
                    "origin_week": prediction["origin_week"],
                    "horizon": horizon,
                    "actual_cases": float(pdf.iloc[target_index]["cases"]),
                    "forecast_cases": prediction["forecast_cases"],
                    "lower_80": prediction["lower_80"],
                    "upper_80": prediction["upper_80"],
                    "seasonal_naive_cases": seasonal_naive,
                }
            )
    return pd.DataFrame(rows)


def _summarize_evaluations(scored: pd.DataFrame) -> pd.DataFrame:
    """Calculate horizon metrics for a small, already-scored table."""
    metrics: list[dict[str, float | int]] = []
    for horizon in HORIZONS:
        group = scored.loc[scored["horizon"] == horizon]
        if group.empty:
            continue
        model_errors = (group["actual_cases"] - group["forecast_cases"]).abs()
        baseline_rows = group.loc[group["seasonal_naive_cases"].notna()]
        baseline_mae = (
            float(
                (
                    baseline_rows["actual_cases"]
                    - baseline_rows["seasonal_naive_cases"]
                )
                .abs()
                .mean()
            )
            if not baseline_rows.empty
            else float("nan")
        )
        covered = (
            (group["actual_cases"] >= group["lower_80"])
            & (group["actual_cases"] <= group["upper_80"])
        )
        metrics.append(
            {
                "horizon": horizon,
                "mae": float(model_errors.mean()),
                "coverage_80": float(covered.mean()),
                "seasonal_naive_mae": baseline_mae,
            }
        )
    return pd.DataFrame(
        metrics, columns=["horizon", "mae", "coverage_80", "seasonal_naive_mae"]
    )


def evaluate_weekly_forecasts(
    history: Any, *, min_train_weeks: int = MIN_TRAIN_WEEKS
) -> Any:
    """Compare rolling-origin model forecasts with a seasonal-naive baseline.

    Each origin uses only its preceding weekly history. The supplied future
    ``cases`` values are joined as evaluation outcomes after forecasts are made;
    they do not enter model fitting or its features. The returned Spark
    DataFrame contains ``horizon``, model/baseline MAE, and model empirical
    80% interval coverage for horizons 1–4. Accuracy and interval coverage are
    measured on the supplied history and are not pre-validated guarantees.

    Seasonal naive repeats the observed case count from four weeks before the
    target week; if that exact seasonal lag is unavailable it is not scored.
    """
    if min_train_weeks < 5:
        raise ValueError("min_train_weeks must be at least 5 for seasonal-naive evaluation")
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    # Allow short evaluation fixtures, while enforcing the same input contract.
    data = _validated_history(
        history, minimum_weeks=min_train_weeks + max(HORIZONS)
    )
    output_schema = StructType(
        [
            StructField("district_id", StringType(), False),
            StructField("disease", StringType(), False),
            StructField("origin_week", DateType(), False),
            StructField("horizon", IntegerType(), False),
            StructField("actual_cases", DoubleType(), False),
            StructField("forecast_cases", DoubleType(), False),
            StructField("lower_80", DoubleType(), False),
            StructField("upper_80", DoubleType(), False),
            StructField("seasonal_naive_cases", DoubleType(), True),
        ]
    )

    scored = data.groupBy("district_id", "disease").applyInPandas(
        lambda pdf: _evaluate_series(pdf, min_train_weeks=min_train_weeks),
        schema=output_schema,
    )
    metrics = scored.groupBy("horizon").agg(
        F.avg(F.abs(F.col("actual_cases") - F.col("forecast_cases"))).alias("mae"),
        F.avg(
            F.when(
                (F.col("actual_cases") >= F.col("lower_80"))
                & (F.col("actual_cases") <= F.col("upper_80")),
                F.lit(1.0),
            ).otherwise(F.lit(0.0))
        ).alias("coverage_80"),
        F.avg(
            F.abs(F.col("actual_cases") - F.col("seasonal_naive_cases"))
        ).alias("seasonal_naive_mae"),
    )
    return metrics.orderBy("horizon").select(
        "horizon", "mae", "coverage_80", "seasonal_naive_mae"
    )
