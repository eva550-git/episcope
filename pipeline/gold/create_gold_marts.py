"""Create the weekly gold case mart with distributed Spark aggregation."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.common import LAKE, ROOT, get_spark

OUT = ROOT / "pipeline" / "gold" / "gold_weekly_cases.csv"
PARQUET_OUT = ROOT / "pipeline" / "gold" / "gold_weekly_cases_parquet"
SOURCE = ROOT / "data_raw" / "synthetic" / "true_daily_counts.csv"
DISTRICT_REFERENCE = ROOT / "data_ref" / "district_ref.csv"


def _build_spark_gold(
    spark: SparkSession,
    source_csv: Path,
    district_reference_csv: Path,
    bronze_counts_path: Path | None = None,
) -> DataFrame:
    lineage_column = "_batch_id"
    if bronze_counts_path is not None and bronze_counts_path.exists():
        counts = spark.read.parquet(str(bronze_counts_path))
        required_counts = {"date", "district_id", "disease", "cases", lineage_column, "_load_time"}
        missing_counts = required_counts - set(counts.columns)
        if missing_counts:
            raise ValueError(f"Bronze daily counts are missing columns: {sorted(missing_counts)}")
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
        counts = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(source_csv))
            .withColumn(lineage_column, F.lit("synthetic"))
        )
    required_counts = {"date", "district_id", "disease", "cases"}
    missing_counts = required_counts - set(counts.columns)
    if missing_counts:
        raise ValueError(f"Daily counts are missing columns: {sorted(missing_counts)}")

    reference = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(district_reference_csv))
    )
    required_reference = {
        "district_id",
        "division_id",
        "division",
        "district",
        "population",
    }
    missing_reference = required_reference - set(reference.columns)
    if missing_reference:
        raise ValueError(
            f"District reference is missing columns: {sorted(missing_reference)}"
        )
    duplicate_reference = (
        reference.groupBy("district_id").count().where(F.col("count") > 1).limit(1).count()
    )
    if duplicate_reference:
        raise ValueError("District reference contains duplicate district_id values.")

    counts = (
        counts.withColumn("__date", F.to_date(F.col("date")))
        .withColumn("__cases", F.col("cases").cast("double"))
        .withColumn("__district_key", F.trim(F.col("district_id")))
    )
    if counts.where(
        F.col("__date").isNull()
        | F.col("__cases").isNull()
        | F.isnan(F.col("__cases"))
        | (F.col("__cases") < 0)
        | (F.col("__cases") != F.floor(F.col("__cases")))
        | F.col("__district_key").isNull()
    ).limit(1).count():
        raise ValueError("Daily counts contain invalid date, case, or district values.")

    ref = reference.select(
        F.trim(F.col("district_id")).alias("__ref_district_key"),
        "division_id",
        "division",
        "district",
        F.col("population").cast("long").alias("population"),
    )
    joined = counts.join(
        F.broadcast(ref),
        F.col("__district_key") == F.col("__ref_district_key"),
        "left",
    )
    if joined.where(F.col("__ref_district_key").isNull()).limit(1).count():
        raise ValueError("Daily counts contain district_id values missing from district_ref.csv.")

    week_day = F.weekday(F.col("__date"))
    iso_year = F.year(F.date_add(F.col("__date"), 3 - week_day))
    week_start = F.date_sub(F.col("__date"), week_day)
    weekly = (
        joined.withColumn("epi_year", iso_year)
        .withColumn("epi_week", F.weekofyear(F.col("__date")))
        .withColumn("week_start", week_start)
        .groupBy(
            "epi_year",
            "epi_week",
            "week_start",
            "division",
            "district",
            "disease",
            F.col("__district_key").alias("district_id"),
            "division_id",
            "population",
        )
        .agg(
            F.sum("__cases").cast("long").alias("cases"),
            F.concat_ws(",", F.sort_array(F.collect_set(F.col(lineage_column)))).alias(
                "load_batch_id"
            ),
        )
        .withColumn("lab_confirmed", F.bround(F.col("cases") * F.lit(0.25)).cast("long"))
        .withColumn("admitted", F.bround(F.col("cases") * F.lit(0.10)).cast("long"))
        .withColumn("deaths", F.bround(F.col("cases") * F.lit(0.015)).cast("long"))
        .withColumn(
            "reports_received",
            F.greatest(F.bround(F.col("cases") / F.lit(10)).cast("long"), F.lit(1)),
        )
        .withColumn(
            "facilities_expected",
            F.greatest(F.bround(F.col("population") / F.lit(1000)).cast("long"), F.lit(1)),
        )
    )
    return weekly.select(
        "epi_year",
        "epi_week",
        "week_start",
        "division",
        "district",
        "disease",
        "cases",
        "lab_confirmed",
        "admitted",
        "deaths",
        "reports_received",
        "facilities_expected",
        "population",
        "load_batch_id",
    )


def build_gold_mart(
    source_csv: str | Path | None = None,
    output_path: str | Path = OUT,
    parquet_output_path: str | Path = PARQUET_OUT,
    spark: SparkSession | None = None,
) -> pd.DataFrame:
    """Aggregate daily counts to partitioned Parquet and a compact serving CSV."""
    source = Path(source_csv) if source_csv else SOURCE
    destination = Path(output_path)
    parquet_destination = Path(parquet_output_path)
    if not source.is_file():
        raise FileNotFoundError(f"Daily counts not found: {source}")
    if not DISTRICT_REFERENCE.is_file():
        raise FileNotFoundError(f"District reference not found: {DISTRICT_REFERENCE}")

    own_session = spark is None
    session = spark or get_spark("gold_weekly_mart")
    temporary_directory: Path | None = None
    try:
        gold = _build_spark_gold(
            session,
            source,
            DISTRICT_REFERENCE,
            LAKE / "bronze" / "bronze_daily_counts",
        )
        gold.createOrReplaceTempView("gold_weekly_cases")
        parquet_destination.parent.mkdir(parents=True, exist_ok=True)
        (
            gold.write.mode("overwrite")
            .option("partitionOverwriteMode", "static")
            .partitionBy("epi_year", "division")
            .parquet(str(parquet_destination))
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary_directory = Path(
            tempfile.mkdtemp(prefix=".gold-weekly-", dir=destination.parent)
        )
        spark_output = temporary_directory / "spark_csv"
        gold.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            str(spark_output)
        )
        part_files = list(spark_output.glob("part-*.csv"))
        if len(part_files) != 1:
            raise RuntimeError(
                f"Expected one Spark CSV part for the compact gold mart, found {len(part_files)}."
            )
        staged_output = temporary_directory / destination.name
        shutil.move(str(part_files[0]), str(staged_output))
        os.replace(staged_output, destination)

        result = pd.read_csv(destination, parse_dates=["week_start"])
        result["week_start"] = result["week_start"].dt.date
        print(f"gold mart written to {destination} ({len(result)} rows)")
        return result
    finally:
        if temporary_directory is not None:
            shutil.rmtree(temporary_directory)
        if own_session:
            session.stop()


if __name__ == "__main__":
    build_gold_mart()
