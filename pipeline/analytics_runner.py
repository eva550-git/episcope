"""Run outbreak evaluation and weather-informed weekly forecasts."""

from __future__ import annotations

import argparse
import json
from datetime import timedelta
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from analytics.detection.ears_detector import detect_ears
from analytics.detection.ears_evaluation import (
    evaluate_ears,
    evaluate_reporting_artefacts,
)
from analytics.forecast.evaluation import evaluate_weekly_forecasts
from analytics.forecast.weekly_model import HORIZONS, MIN_TRAIN_WEEKS, forecast_weekly
from pipeline.common import LAKE, ROOT, get_spark

DEFAULT_COUNTS = ROOT / "data_raw" / "synthetic" / "true_daily_counts.csv"
DEFAULT_WEATHER = ROOT / "data_raw" / "weather_daily.csv"
DEFAULT_OUTBREAKS = ROOT / "data_raw" / "ground_truth" / "ground_truth_outbreaks.csv"
DEFAULT_ARTEFACTS = ROOT / "data_raw" / "ground_truth" / "ground_truth_artefacts.csv"
DEFAULT_OUTPUT = ROOT / "pipeline" / "analytics"
DISTRICT_REFERENCE = ROOT / "data_ref" / "district_ref.csv"
EVALUATION_ORIGINS = 52


def _read_counts(
    spark,
    path: Path,
    bronze_counts_path: Path | None = None,
) -> DataFrame:
    if bronze_counts_path is not None and bronze_counts_path.exists():
        counts = spark.read.parquet(str(bronze_counts_path))
        bronze_columns = {"_batch_id", "_load_time"}
        missing_bronze_columns = bronze_columns - set(counts.columns)
        if missing_bronze_columns:
            raise ValueError(
                f"Bronze daily counts are missing lineage columns: {sorted(missing_bronze_columns)}"
            )
        latest_version = (
            Window.partitionBy("district_id", "disease", "date")
            .orderBy(F.col("_load_time").desc(), F.col("_batch_id").desc())
        )
        counts = (
            counts.withColumn("__version_rank", F.row_number().over(latest_version))
            .where(F.col("__version_rank") == 1)
            .drop("__version_rank")
        )
    else:
        if not path.is_file():
            raise FileNotFoundError(f"Daily count input not found: {path}")
        counts = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    required = {"date", "district_id", "disease", "cases"}
    missing = required - set(counts.columns)
    if missing:
        raise ValueError(f"Daily counts are missing columns: {sorted(missing)}")
    return counts.select(
        F.to_date("date").alias("date"),
        F.col("district_id").cast("string").alias("district_id"),
        F.lower(F.trim(F.col("disease"))).alias("disease"),
        F.col("cases").cast("double").alias("cases"),
    )


def _weekly_weather_history(spark, counts: DataFrame, weather_path: Path) -> DataFrame:
    if not weather_path.is_file():
        raise FileNotFoundError(f"Weather input not found: {weather_path}")
    weather = spark.read.option("header", True).option("inferSchema", True).csv(str(weather_path))
    required = {"date", "district_id", "precip_mm", "temp_mean_c"}
    missing = required - set(weather.columns)
    if missing:
        raise ValueError(f"Weather input is missing columns: {sorted(missing)}")

    weekly_cases = (
        counts.withColumn("week_start", F.date_sub(F.col("date"), F.weekday("date")))
        .groupBy("district_id", "disease", "week_start")
        .agg(F.sum("cases").alias("cases"))
    )
    weekly_weather = (
        weather.withColumn("date", F.to_date("date"))
        .withColumn("week_start", F.date_sub(F.col("date"), F.weekday("date")))
        .groupBy("district_id", "week_start")
        .agg(
            F.avg(F.col("precip_mm").cast("double")).alias("rainfall"),
            F.avg(F.col("temp_mean_c").cast("double")).alias("temperature"),
        )
    )
    history = weekly_cases.join(
        weekly_weather, ["district_id", "week_start"], "left"
    ).select(
        "district_id",
        "disease",
        "week_start",
        "cases",
        F.coalesce("rainfall", F.lit(0.0)).alias("rainfall"),
        F.coalesce("temperature", F.lit(0.0)).alias("temperature"),
    )
    return history


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, allow_nan=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run_analytics(
    counts_path: str | Path = DEFAULT_COUNTS,
    weather_path: str | Path = DEFAULT_WEATHER,
    outbreaks_path: str | Path = DEFAULT_OUTBREAKS,
    artefacts_path: str | Path = DEFAULT_ARTEFACTS,
    output_dir: str | Path = DEFAULT_OUTPUT,
    bronze_counts_path: str | Path | None = LAKE / "bronze" / "bronze_daily_counts",
    spark=None,
) -> dict[str, object]:
    """Write model outputs and evaluation metrics without exposing labels to detection."""
    counts_path = Path(counts_path)
    weather_path = Path(weather_path)
    outbreaks_path = Path(outbreaks_path)
    artefacts_path = Path(artefacts_path)
    output_dir = Path(output_dir)
    bronze_counts_path = (
        Path(bronze_counts_path) if bronze_counts_path is not None else None
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    own_session = spark is None
    session = spark or get_spark("episcope_analytics")
    cached_frames: list[DataFrame] = []
    try:
        counts = _read_counts(session, counts_path, bronze_counts_path).cache()
        cached_frames.append(counts)
        counts.count()
        invalid_counts = (
            F.col("date").isNull()
            | F.col("district_id").isNull()
            | (F.length(F.trim("district_id")) == 0)
            | F.col("disease").isNull()
            | (F.length(F.trim("disease")) == 0)
            | F.col("cases").isNull()
            | F.isnan("cases")
            | (F.col("cases") < 0)
            | (F.abs(F.col("cases")) == F.lit(float("inf")))
        )
        if counts.filter(invalid_counts).limit(1).count():
            raise ValueError("Daily counts must have valid keys, dates, and finite nonnegative cases.")
        if not counts.limit(1).count():
            raise ValueError("Daily count input is empty.")
        detected = detect_ears(counts, baseline_days=28, alert_threshold=3.0).cache()
        cached_frames.append(detected)
        detected.count()

        report: dict[str, object] = {
            "detector": "EARS C1",
            "baseline_days": 28,
            "alert_threshold": 3.0,
            "ground_truth_used_by_detector": False,
            "evaluation": None,
            "reporting_artefact_evaluation": None,
        }
        if outbreaks_path.is_file():
            report["evaluation"] = evaluate_ears(detected, outbreaks_path)
        if artefacts_path.is_file():
            report["reporting_artefact_evaluation"] = evaluate_reporting_artefacts(
                detected, artefacts_path
            )
        _write_json(output_dir / "detection_evaluation.json", report)

        districts = session.read.option("header", True).option("inferSchema", True).csv(
            str(DISTRICT_REFERENCE)
        )
        latest_date = counts.agg(F.max("date").alias("latest_date")).first()["latest_date"]
        cutoff_date = F.date_sub(F.lit(latest_date), 27)
        recent = (
            detected.where(F.col("date") >= cutoff_date)
            .groupBy("district_id", "disease")
            .agg(
                F.max("c1_score").alias("z_score"),
                F.sum("cases").alias("recent_cases"),
                F.avg("baseline_mean").alias("baseline_daily_cases"),
                F.countDistinct("date").alias("observed_days"),
            )
            .join(
                districts.select("district_id", "district", "division"),
                "district_id",
                "left",
            )
            .withColumn(
                "growth_pct",
                F.when(
                    F.col("baseline_daily_cases") > 0,
                    (
                        F.col("recent_cases") / F.col("observed_days")
                        - F.col("baseline_daily_cases")
                    )
                    / F.col("baseline_daily_cases")
                    * 100.0,
                ).otherwise(F.lit(0.0)),
            )
            .where(F.col("recent_cases") >= 5)
            .select(
                "district_id",
                "district",
                "division",
                "disease",
                F.round("recent_cases").cast("long").alias("recent_cases"),
                F.round(F.col("baseline_daily_cases") * 7, 2).alias("baseline_cases_per_week"),
                F.round(
                    F.col("recent_cases") / F.col("observed_days") * 7, 2
                ).alias("recent_cases_per_week"),
                F.round("growth_pct", 1).alias("growth_pct"),
                F.round("z_score", 2).alias("z_score"),
                F.lit((latest_date - timedelta(days=latest_date.weekday())).isoformat()).alias(
                    "latest_week"
                ),
            )
            .orderBy(F.desc("z_score"), F.desc("recent_cases"))
        )
        candidate_frame = recent.toPandas()
        candidate_frame.to_csv(output_dir / "alert_candidates.csv", index=False)

        history = _weekly_weather_history(session, counts, weather_path).cache()
        cached_frames.append(history)
        history.count()
        forecasts = forecast_weekly(history)
        forecast_frame = forecasts.orderBy(
            "district_id", "disease", "forecast_week"
        ).toPandas()
        forecast_frame.to_csv(output_dir / "weekly_forecast.csv", index=False)

        evaluation_history_weeks = (
            MIN_TRAIN_WEEKS + EVALUATION_ORIGINS + max(HORIZONS) - 1
        )
        evaluation_history = (
            history.withColumn(
                "__evaluation_rank",
                F.row_number().over(
                    Window.partitionBy("district_id", "disease").orderBy(
                        F.col("week_start").desc()
                    )
                ),
            )
            .where(F.col("__evaluation_rank") <= evaluation_history_weeks)
            .drop("__evaluation_rank")
        )
        metrics = evaluate_weekly_forecasts(evaluation_history).orderBy(
            "horizon"
        ).toPandas()
        evaluation = {
            "method": "Spark grouped ridge model with four lagged weekly case/weather observations",
            "evaluation_origins_per_series": EVALUATION_ORIGINS,
            "seasonal_naive_lag_weeks": 4,
            "metrics_by_horizon": [
                {
                    "horizon": int(row["horizon"]),
                    "mae": float(row["mae"]),
                    "coverage_80": float(row["coverage_80"]),
                    "seasonal_naive_mae": float(row["seasonal_naive_mae"]),
                }
                for row in metrics.to_dict(orient="records")
            ],
        }
        _write_json(output_dir / "forecast_evaluation.json", evaluation)
        return {
            "detection_evaluation": report,
            "forecast_evaluation": evaluation,
            "alert_candidate_count": len(candidate_frame),
            "forecast_row_count": len(forecast_frame),
        }
    finally:
        for frame in reversed(cached_frames):
            frame.unpersist()
        if own_session:
            session.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--counts", type=Path, default=DEFAULT_COUNTS)
    parser.add_argument("--weather", type=Path, default=DEFAULT_WEATHER)
    parser.add_argument("--outbreaks", type=Path, default=DEFAULT_OUTBREAKS)
    parser.add_argument("--artefacts", type=Path, default=DEFAULT_ARTEFACTS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--bronze-counts",
        type=Path,
        default=LAKE / "bronze" / "bronze_daily_counts",
        help="Bronze daily-count Parquet; latest source revision per key is used when present.",
    )
    options = parser.parse_args()
    result = run_analytics(
        options.counts,
        options.weather,
        options.outbreaks,
        options.artefacts,
        options.output_dir,
        options.bronze_counts,
    )
    print(
        f"Analytics complete: candidates={result['alert_candidate_count']}, "
        f"forecast rows={result['forecast_row_count']}"
    )


if __name__ == "__main__":
    main()
