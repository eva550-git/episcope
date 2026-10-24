"""Spark-window implementation of the one-day EARS C1 detector.

The detector consumes daily counts only. Evaluation labels and reporting-artefact
intervals are deliberately handled by :mod:`ears_evaluation`, never here.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def detect_ears(
    counts: DataFrame,
    baseline_days: int = 28,
    alert_threshold: float = 3.0,
    min_baseline_days: int = 7,
) -> DataFrame:
    """Score district/disease daily counts with EARS C1 using Spark windows.

    ``counts`` must contain ``district_id``, ``disease``, ``date`` and ``cases``.
    The baseline for a date is calculated from the preceding ``baseline_days``
    calendar dates, excluding the current date. This is a date-range Spark
    window, not a Python/pandas group operation. ``c1_score`` is the number of
    baseline standard deviations above the baseline mean; a constant baseline
    uses a standard deviation of 1 to keep the score finite.

    The returned DataFrame retains every input row and adds ``baseline_mean``,
    ``baseline_stddev``, ``c1_score`` and Boolean ``alert`` columns. Dates with
    fewer than ``min_baseline_days`` observed baseline rows are not alerted.
    """
    if baseline_days < 1:
        raise ValueError("baseline_days must be positive")
    if min_baseline_days < 1:
        raise ValueError("min_baseline_days must be positive")
    if min_baseline_days > baseline_days:
        raise ValueError("min_baseline_days cannot exceed baseline_days")

    required = {"district_id", "disease", "date", "cases"}
    missing = required.difference(counts.columns)
    if missing:
        raise ValueError(f"counts is missing required columns: {sorted(missing)}")

    # rangeBetween is ordered on integral day offsets so missing calendar days
    # do not accidentally extend the lookback to older observations.
    dated = (
        counts.withColumn("date", F.to_date(F.col("date")))
        .withColumn("_cases_numeric", F.col("cases").cast("double"))
        .withColumn("_day_number", F.datediff(F.col("date"), F.lit("1970-01-01")))
    )
    baseline = (
        Window.partitionBy("district_id", "disease")
        .orderBy(F.col("_day_number"))
        .rangeBetween(-baseline_days, -1)
    )
    scored = (
        dated.withColumn("baseline_mean", F.avg("_cases_numeric").over(baseline))
        .withColumn("baseline_stddev", F.stddev_pop("_cases_numeric").over(baseline))
        .withColumn("_baseline_count", F.count("_cases_numeric").over(baseline))
        .withColumn(
            "_safe_stddev",
            F.when(F.col("baseline_stddev").isNull() | (F.col("baseline_stddev") == 0), 1.0)
            .otherwise(F.col("baseline_stddev")),
        )
        .withColumn(
            "c1_score",
            (F.col("_cases_numeric") - F.col("baseline_mean")) / F.col("_safe_stddev"),
        )
        .withColumn(
            "alert",
            (F.col("_baseline_count") >= F.lit(min_baseline_days))
            & (F.col("_cases_numeric") > F.col("baseline_mean"))
            & (F.col("c1_score") >= F.lit(float(alert_threshold))),
        )
        .drop("_cases_numeric", "_day_number", "_baseline_count", "_safe_stddev")
    )
    return scored
