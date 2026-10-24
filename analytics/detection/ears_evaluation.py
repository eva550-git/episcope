"""Explicitly supplied outbreak-label and reporting-artefact evaluation helpers.

Nothing in this module is called by the detector. Ground-truth data is only
loaded when a caller passes a DataFrame or path to an evaluation function.
"""

from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import Iterable, Union

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

FrameOrPath = Union[DataFrame, str, PathLike]


def _as_dataframe(value: FrameOrPath, spark) -> DataFrame:
    """Use an explicit DataFrame/path; never infer or search for a truth file."""
    if isinstance(value, DataFrame):
        return value
    path = Path(value)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    if suffix in {".parquet", ".pq"}:
        return spark.read.parquet(str(path))
    raise ValueError("input path must be a .csv or .parquet file")


def _require_columns(frame: DataFrame, required: set[str], label: str) -> None:
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"{label} is missing required columns: {sorted(missing)}")


def _normalise_intervals(
    intervals: DataFrame,
    start_candidates: tuple[str, ...],
    end_candidates: tuple[str, ...],
    label: str,
) -> DataFrame:
    start_name = next((name for name in start_candidates if name in intervals.columns), None)
    end_name = next((name for name in end_candidates if name in intervals.columns), None)
    if start_name is None or end_name is None:
        raise ValueError(
            f"{label} must contain an interval start/end column "
            f"(accepted starts: {start_candidates}; ends: {end_candidates})"
        )
    return intervals.withColumn("_interval_start", F.to_date(F.col(start_name))).withColumn(
        "_interval_end", F.to_date(F.col(end_name))
    )


def _alert_episodes(alerts: DataFrame) -> DataFrame:
    """Collapse consecutive alert dates into district/disease alert episodes."""
    daily = (
        alerts.filter(F.col("alert") == F.lit(True))
        .select(
            "district_id",
            "disease",
            F.to_date("date").alias("_alert_date"),
        )
        .dropDuplicates(["district_id", "disease", "_alert_date"])
    )
    by_series = Window.partitionBy("district_id", "disease").orderBy("_alert_date")
    ordered = daily.withColumn("_previous_date", F.lag("_alert_date").over(by_series))
    beginnings = ordered.withColumn(
        "_is_new_episode",
        F.when(
            F.col("_previous_date").isNull()
            | (F.datediff("_alert_date", "_previous_date") > 1),
            1,
        ).otherwise(0),
    )
    running = by_series.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        beginnings.withColumn("_episode_number", F.sum("_is_new_episode").over(running))
        .groupBy("district_id", "disease", "_episode_number")
        .agg(
            F.min("_alert_date").alias("_episode_start"),
            F.max("_alert_date").alias("_episode_end"),
        )
        .drop("_episode_number")
    )


def _metric_summary(alerts: DataFrame, intervals: DataFrame) -> dict[str, float | int | None]:
    truth = _normalise_intervals(
        intervals,
        ("start_date", "outbreak_start", "start"),
        ("end_date", "outbreak_end", "end"),
        "ground_truth_outbreaks",
    )
    _require_columns(truth, {"district_id", "disease"}, "ground_truth_outbreaks")
    truth = (
        truth.select(
            "district_id",
            "disease",
            "_interval_start",
            "_interval_end",
        )
        .filter(F.col("_interval_start").isNotNull() & F.col("_interval_end").isNotNull())
        .dropDuplicates(["district_id", "disease", "_interval_start", "_interval_end"])
    )

    episodes = _alert_episodes(alerts)
    match_condition = (
        (episodes.district_id == truth.district_id)
        & (episodes.disease == truth.disease)
        & (episodes._episode_start <= truth._interval_end)
        & (episodes._episode_end >= truth._interval_start)
    )
    matches = episodes.join(truth, match_condition, "inner")
    detected_intervals = matches.select(
        truth.district_id.alias("district_id"),
        truth.disease.alias("disease"),
        truth._interval_start.alias("_interval_start"),
        truth._interval_end.alias("_interval_end"),
        F.greatest(episodes._episode_start, truth._interval_start).alias("_detection_date"),
    ).groupBy("district_id", "disease", "_interval_start", "_interval_end").agg(
        F.min("_detection_date").alias("_detection_date")
    )

    interval_count = truth.count()
    detected_count = detected_intervals.count()
    delay_row = detected_intervals.select(
        F.datediff("_detection_date", "_interval_start").alias("_delay")
    ).agg(F.expr("percentile_approx(_delay, 0.5, 10000)").alias("median_delay")).first()

    false_episodes = episodes.join(truth, match_condition, "left_anti")
    false_count = false_episodes.count()
    district_years = (
        alerts.select(
            "district_id",
            F.year(F.to_date("date")).alias("_year"),
        )
        .dropDuplicates(["district_id", "_year"])
        .count()
    )
    return {
        "sensitivity": (detected_count / interval_count) if interval_count else None,
        "false_alerts_per_district_year": (
            false_count / district_years if district_years else 0.0
        ),
        "median_detection_delay_days": (
            float(delay_row["median_delay"]) if delay_row["median_delay"] is not None else None
        ),
        "detected_outbreaks": int(detected_count),
        "ground_truth_outbreaks": int(interval_count),
        "false_alert_episodes": int(false_count),
        "district_years": int(district_years),
    }


def evaluate_ears(
    alerts: DataFrame,
    ground_truth_outbreaks: FrameOrPath,
    thresholds: Iterable[float] = (1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0),
) -> dict[str, object]:
    """Report EARS sensitivity, false-alert rate, detection delay and trade-offs.

    ``ground_truth_outbreaks`` is mandatory and must be explicitly supplied as
    a Spark DataFrame or CSV/Parquet path. The accepted label columns are
    ``district_id``, ``disease``, ``start_date`` and ``end_date`` (the latter
    two also accept ``outbreak_start``/``outbreak_end`` or ``start``/``end``).
    Sensitivity counts outbreak intervals with at least one overlapping alert
    episode. Consecutive alert dates form one episode. The false-alert rate is
    unmatched alert episodes divided by district-years present in ``alerts``.

    The trade-off curve applies each supplied threshold to the detector's
    ``c1_score``. Thresholds therefore require output from ``detect_ears``.
    """
    _require_columns(
        alerts,
        {"district_id", "disease", "date", "alert"},
        "alerts",
    )
    if "c1_score" not in alerts.columns:
        raise ValueError("alerts must include c1_score to calculate a threshold trade-off curve")

    truth = _as_dataframe(ground_truth_outbreaks, alerts.sparkSession)
    base_alerts = alerts.withColumn("date", F.to_date("date"))
    base = _metric_summary(base_alerts, truth)
    curve = []
    for threshold in thresholds:
        threshold_alerts = base_alerts.withColumn(
            "alert",
            (F.col("c1_score") >= F.lit(float(threshold)))
            & (F.col("cases").cast("double") > F.col("baseline_mean")),
        )
        point = _metric_summary(threshold_alerts, truth)
        curve.append(
            {
                "threshold": float(threshold),
                "sensitivity": point["sensitivity"],
                "false_alerts_per_district_year": point["false_alerts_per_district_year"],
                "median_detection_delay_days": point["median_detection_delay_days"],
            }
        )
    base["threshold_tradeoff"] = curve
    return base


def evaluate_reporting_artefacts(
    alerts: DataFrame,
    artefact_intervals: FrameOrPath,
) -> dict[str, int | float]:
    """Quantify alert-days falling inside explicitly supplied artefact intervals.

    Interval start/end columns may be ``start_date``/``end_date``, the
    encounter generator's ``silent_start``/``silent_end``, or ``start``/``end``.
    If the supplied intervals include ``district_id`` and/or ``disease``, those
    keys scope the match; otherwise the interval applies globally.
    """
    _require_columns(alerts, {"district_id", "disease", "date", "alert"}, "alerts")
    intervals_input = _as_dataframe(artefact_intervals, alerts.sparkSession)
    intervals = _normalise_intervals(
        intervals_input,
        ("start_date", "silent_start", "start"),
        ("end_date", "silent_end", "end"),
        "artefact_intervals",
    ).filter(F.col("_interval_start").isNotNull() & F.col("_interval_end").isNotNull())
    alert_days = (
        alerts.filter(F.col("alert") == F.lit(True))
        .select("district_id", "disease", F.to_date("date").alias("_alert_date"))
        .dropDuplicates(["district_id", "disease", "_alert_date"])
    )
    condition = (
        (alert_days._alert_date >= intervals._interval_start)
        & (alert_days._alert_date <= intervals._interval_end)
    )
    if "district_id" in intervals.columns:
        condition = condition & (alert_days.district_id == intervals.district_id)
    if "disease" in intervals.columns:
        condition = condition & (alert_days.disease == intervals.disease)

    total_alert_days = alert_days.count()
    artefact_alert_days = alert_days.join(intervals, condition, "left_semi").count()
    return {
        "total_alert_days": int(total_alert_days),
        "artefact_alert_days": int(artefact_alert_days),
        "artefact_alert_fraction": (
            artefact_alert_days / total_alert_days if total_alert_days else 0.0
        ),
    }
