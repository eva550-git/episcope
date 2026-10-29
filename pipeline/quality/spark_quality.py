"""Distributed Spark quality, quarantine, and privacy processing for silver data."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.common import LAKE, get_spark
from pipeline.quality.quality_rules import (
    ALLOWED_OUTCOMES,
    ALLOWED_SEX,
    DEFAULT_DISTRICT_REFERENCE,
    DEFAULT_FACILITY_REGISTRY,
    DEFAULT_INPUT,
    DEFAULT_QUARANTINE,
    DEFAULT_REPORT,
    DEFAULT_SPARK_CLEAN,
    GENERATOR_ICD10_CODES,
    REQUIRED_COLUMNS,
    RULE_DESCRIPTIONS,
    _get_hash_salt,
)


def _read_encounters(
    spark: SparkSession,
    input_dir: Path,
    bronze_path: Path | None,
) -> DataFrame:
    source = bronze_path
    if source is not None and source.exists():
        return spark.read.parquet(str(source))

    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No encounter CSV files found in {input_dir}.")
    frames = [
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(str(path))
        for path in files
    ]
    data = frames[0]
    for frame in frames[1:]:
        data = data.unionByName(frame, allowMissingColumns=True)
    return data


def _keep_latest_encounter_versions(data: DataFrame) -> DataFrame:
    """Ignore older bronze copies while retaining duplicates within the latest load."""
    if not {"_batch_id", "_load_time"}.issubset(data.columns):
        return data

    encounter_key = F.trim(F.col("encounter_id"))
    version = F.struct(
        F.col("_load_time").cast("timestamp").alias("load_time"),
        F.col("_batch_id").cast("string").alias("batch_id"),
    )
    latest_version = F.max(version).over(Window.partitionBy(encounter_key))
    return (
        data.withColumn("__latest_encounter_version", latest_version)
        .where(
            encounter_key.isNull()
            | (F.length(encounter_key) == 0)
            | (version == F.col("__latest_encounter_version"))
        )
        .drop("__latest_encounter_version")
    )


def _read_reference(
    spark: SparkSession,
    path: Path,
    required_columns: set[str],
    description: str,
) -> DataFrame | None:
    if not path.is_file():
        return None
    reference = spark.read.option("header", "true").option("inferSchema", "false").csv(str(path))
    missing = required_columns - set(reference.columns)
    if missing:
        raise ValueError(f"{description} at {path} is missing columns: {sorted(missing)}")
    return reference


def _validate_reference_keys(reference: DataFrame, key: str, description: str) -> None:
    duplicate = reference.groupBy(key).count().where(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise ValueError(f"{description} contains duplicate {key} values.")


def _write_spark_dq_report(
    report_path: Path,
    rows_in: int,
    rows_out: int,
    rows_quarantined: int,
    counts: dict[str, int],
    enabled_rules: set[str],
) -> None:
    rows: list[dict[str, Any]] = []
    for rule_id, description in RULE_DESCRIPTIONS.items():
        enabled = rule_id in enabled_rules
        passed = counts.get(rule_id) if enabled else None
        failed = rows_in - passed if enabled and passed is not None else None
        rows.append(
            {
                "rule_code": rule_id,
                "description": description,
                "enabled": enabled,
                "pass_count": passed,
                "fail_count": failed,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "rows_quarantined": rows_quarantined,
            }
        )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
                "rows_in": rows_in,
                "rows_out": rows_out,
                "rows_quarantined": rows_quarantined,
                "rules": rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(rows).to_csv(report_path.with_suffix(".csv"), index=False)


def apply_spark_quality_and_privacy_checks(
    input_dir: Path = DEFAULT_INPUT,
    clean_path: Path = DEFAULT_SPARK_CLEAN,
    quarantine_path: Path = DEFAULT_QUARANTINE,
    report_path: Path = DEFAULT_REPORT,
    facility_registry_path: Path = DEFAULT_FACILITY_REGISTRY,
    district_reference_path: Path = DEFAULT_DISTRICT_REFERENCE,
    bronze_path: Path | None = LAKE / "bronze" / "bronze_encounters",
    spark: SparkSession | None = None,
) -> dict[str, int] | None:
    """Apply the encounter rules in Spark without collecting encounter rows to Python."""
    own_session = spark is None
    session = spark or get_spark("silver_quality")
    data: DataFrame | None = None
    cached_data: DataFrame | None = None
    try:
        data = _read_encounters(session, Path(input_dir), bronze_path)
        missing = set(REQUIRED_COLUMNS) - set(data.columns)
        if missing:
            raise ValueError(f"Input data is missing required columns: {sorted(missing)}")
        data = _keep_latest_encounter_versions(data)

        salt = _get_hash_salt().decode("utf-8")
        original_columns = data.columns
        data = data.persist(StorageLevel.DISK_ONLY)
        cached_data = data

        registry = _read_reference(
            session,
            Path(facility_registry_path),
            {"facility_id", "district_id"},
            "Facility registry",
        )
        district_ref = _read_reference(
            session,
            Path(district_reference_path),
            {"district_id", "district"},
            "District reference",
        )
        if registry is not None:
            _validate_reference_keys(registry, "facility_id", "Facility registry")
        if district_ref is not None:
            _validate_reference_keys(district_ref, "district_id", "District reference")

        rules: dict[str, F.Column | None] = {}
        required_present = F.lit(True)
        for column in REQUIRED_COLUMNS:
            required_present = required_present & F.col(column).isNotNull()
            if column in data.columns:
                required_present = required_present & (F.length(F.trim(F.col(column))) > 0)
        rules["DQ_SCHEMA_REQUIRED_COLUMNS"] = F.lit(True)
        rules["DQ_REQUIRED_VALUES_NOT_NULL"] = required_present

        age = F.col("age_years").cast("double")
        valid_age = age.isNotNull() & ~F.isnan(age) & age.between(0, 120)
        rules["DQ_AGE_IN_RANGE"] = valid_age
        sex = F.lower(F.trim(F.col("sex")))
        rules["DQ_SEX_ALLOWED"] = sex.isin(sorted(ALLOWED_SEX))

        onset = F.to_date(F.col("onset_date"))
        reported = F.to_date(F.col("report_date"))
        rules["DQ_ONSET_DATE_VALID"] = onset.isNotNull()
        rules["DQ_REPORT_DATE_VALID"] = reported.isNotNull()
        rules["DQ_DATE_ORDER"] = onset.isNotNull() & reported.isNotNull() & (onset <= reported)
        delay = F.datediff(reported, onset)
        rules["DQ_REPORT_DELAY_0_21_DAYS"] = (
            onset.isNotNull() & reported.isNotNull() & delay.between(0, 21)
        )
        rules["DQ_ICD10_ALLOWED"] = F.upper(F.trim(F.col("icd10_code"))).isin(
            sorted(GENERATOR_ICD10_CODES)
        )
        for column, rule_id in (
            ("lab_confirmed", "DQ_LAB_CONFIRMED_BINARY"),
            ("admitted", "DQ_ADMITTED_BINARY"),
        ):
            number = F.col(column).cast("double")
            rules[rule_id] = number.isNotNull() & number.isin(0.0, 1.0)
        rules["DQ_OUTCOME_ALLOWED"] = F.lower(F.trim(F.col("outcome"))).isin(
            sorted(ALLOWED_OUTCOMES)
        )

        encounter_key = F.trim(F.col("encounter_id"))
        duplicate_window = Window.partitionBy(encounter_key)
        rules["DQ_ENCOUNTER_ID_UNIQUE"] = (
            encounter_key.isNotNull()
            & (F.length(encounter_key) > 0)
            & (F.count(F.lit(1)).over(duplicate_window) == 1)
        )

        facility_key = F.trim(F.col("facility_id"))
        mapped = registry is not None
        if mapped:
            mapping_columns = registry.select(
                F.trim(F.col("facility_id")).alias("__facility_key"),
                F.trim(F.col("district_id")).alias("__mapped_district_id"),
            )
            data = data.join(
                F.broadcast(mapping_columns),
                facility_key == F.col("__facility_key"),
                "left",
            )
            rules["DQ_FACILITY_REGISTERED"] = F.col("__mapped_district_id").isNotNull()
        else:
            rules["DQ_FACILITY_REGISTERED"] = None

        geography_available = mapped or "district" in original_columns or "district_id" in original_columns
        if mapped:
            if district_ref is not None:
                district_names = district_ref.select(
                    F.trim(F.col("district_id")).alias("__ref_district_id"),
                    F.trim(F.col("district")).alias("__ref_district"),
                    *(
                        [F.trim(F.col("division")).alias("__division")]
                        if "division" in district_ref.columns
                        else []
                    ),
                    *(
                        [F.trim(F.col("division_id")).alias("__division_id")]
                        if "division_id" in district_ref.columns
                        else []
                    ),
                )
                data = data.join(
                    F.broadcast(district_names),
                    F.col("__mapped_district_id") == F.col("__ref_district_id"),
                    "left",
                )
                geography_valid = F.col("__ref_district_id").isNotNull()
            else:
                geography_valid = F.col("__mapped_district_id").isNotNull()
            if "district_id" in original_columns:
                geography_valid = geography_valid & (
                    F.trim(F.col("district_id")) == F.col("__mapped_district_id")
                )
            if "district" in original_columns and district_ref is not None:
                geography_valid = geography_valid & (
                    (F.lower(F.trim(F.col("district"))) == F.lower(F.col("__ref_district")))
                    | (
                        F.lower(F.trim(F.col("district")))
                        == F.lower(F.col("__mapped_district_id"))
                    )
                )
            rules["DQ_GEOGRAPHY_VALID"] = geography_valid
        elif geography_available:
            geography_valid = F.lit(True)
            if "district_id" in original_columns and district_ref is not None:
                valid_ids = district_ref.select(
                    F.trim(F.col("district_id")).alias("__valid_district_id")
                )
                data = data.join(
                    F.broadcast(valid_ids),
                    F.trim(F.col("district_id")) == F.col("__valid_district_id"),
                    "left",
                )
                geography_valid = geography_valid & F.col("__valid_district_id").isNotNull()
            elif "district_id" in original_columns:
                geography_valid = geography_valid & F.col("district_id").isNotNull()
            if "district" in original_columns:
                if district_ref is not None:
                    district_labels = (
                        district_ref.select(
                            F.lower(F.trim(F.col("district"))).alias("__valid_district_label")
                        )
                        .unionByName(
                            district_ref.select(
                                F.lower(F.trim(F.col("district_id"))).alias(
                                    "__valid_district_label"
                                )
                            )
                        )
                        .distinct()
                    )
                    data = data.join(
                        F.broadcast(district_labels),
                        F.lower(F.trim(F.col("district")))
                        == F.col("__valid_district_label"),
                        "left",
                    )
                    geography_valid = geography_valid & (
                        F.col("__valid_district_label").isNotNull()
                    )
                else:
                    geography_valid = geography_valid & F.col("district").isNotNull()
            rules["DQ_GEOGRAPHY_VALID"] = geography_valid
        else:
            rules["DQ_GEOGRAPHY_VALID"] = None

        reasonable = (
            valid_age
            & onset.isNotNull()
            & reported.isNotNull()
            & (onset >= F.lit("1900-01-01").cast("date"))
            & (reported >= F.lit("1900-01-01").cast("date"))
            & (onset <= F.lit(date.today() + timedelta(days=1)))
            & (reported <= F.lit(date.today() + timedelta(days=1)))
        )
        rules["DQ_REASONABLE_VALUES"] = reasonable

        enabled_rules = {rule_id for rule_id, passed in rules.items() if passed is not None}
        rule_columns = {}
        evaluated = data
        for rule_id, passed in rules.items():
            if passed is not None:
                column_name = f"__dq_{rule_id}"
                evaluated = evaluated.withColumn(
                    column_name,
                    F.coalesce(passed.cast("boolean"), F.lit(False)),
                )
                rule_columns[rule_id] = column_name
        failures = [
            F.when(~F.col(rule_columns[rule_id]), F.lit(rule_id))
            for rule_id in rule_columns
        ]
        assessed = (
            evaluated.withColumn("onset_date", onset.cast("string"))
            .withColumn("report_date", reported.cast("string"))
            .withColumn(
                "patient_id",
                F.sha2(
                    F.concat(F.lit(salt), F.coalesce(F.trim(F.col("patient_id")), F.lit(""))),
                    256,
                ),
            )
            .withColumn(
                "age_group",
                F.when(age.between(0, 5), "0-5")
                .when(age.between(5, 18), "6-18")
                .when(age.between(18, 45), "19-45")
                .when(age.between(45, 65), "46-65")
                .when(age.between(65, 120), "65+"),
            )
            .withColumn("age_years", age)
            .withColumn("lab_confirmed", F.col("lab_confirmed").cast("int"))
            .withColumn("admitted", F.col("admitted").cast("int"))
            .withColumn("dq_reason_codes", F.concat_ws("|", *failures))
            .withColumn(
                "dq_reason_code",
                F.split(F.col("dq_reason_codes"), r"\|").getItem(0),
            )
        )
        if mapped:
            assessed = (
                assessed.withColumn("district_id", F.col("__mapped_district_id"))
                .withColumn("district", F.col("__ref_district"))
            )
            if "__division" in assessed.columns:
                assessed = assessed.withColumn("division", F.col("__division"))
            if "__division_id" in assessed.columns:
                assessed = assessed.withColumn("division_id", F.col("__division_id"))

        rows_in = assessed.count()
        rule_aggregates = [
            F.sum(F.col(column_name).cast("long")).alias(rule_id)
            for rule_id, column_name in rule_columns.items()
        ]
        aggregate_counts = assessed.agg(*rule_aggregates).first().asDict()
        counts = {key: int(value or 0) for key, value in aggregate_counts.items()}
        clean = assessed.where(F.length(F.col("dq_reason_codes")) == 0).drop(
            "dq_reason_codes", "dq_reason_code", *rule_columns.values()
        )
        quarantine = assessed.where(F.length(F.col("dq_reason_codes")) > 0).drop(
            *rule_columns.values()
        )
        rows_out = rows_in - int(quarantine.count())
        rows_quarantined = rows_in - rows_out

        clean_output = clean
        if "onset_date" in clean.columns:
            clean_output = clean_output.withColumn(
                "epi_year", F.year(F.to_date("onset_date"))
            )
        if "division" not in clean_output.columns:
            clean_output = clean_output.withColumn("division", F.lit("unknown"))
        clean_target = Path(clean_path)
        clean_target.parent.mkdir(parents=True, exist_ok=True)
        clean_output.write.mode("overwrite").partitionBy("epi_year", "division").parquet(
            str(clean_target)
        )

        quarantine_target = Path(quarantine_path)
        quarantine_target.parent.mkdir(parents=True, exist_ok=True)
        quarantine.write.mode("overwrite").option("header", "true").csv(str(quarantine_target))
        _write_spark_dq_report(
            Path(report_path),
            rows_in,
            rows_out,
            rows_quarantined,
            counts,
            enabled_rules,
        )
        print(f"Spark DQ completed: clean={rows_out} quarantined={rows_quarantined}")
        print(f"clean output: {clean_target}")
        print(f"quarantine output: {quarantine_target}")
        print(f"DQ report: {report_path} and {Path(report_path).with_suffix('.csv')}")
        return {
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_quarantined": rows_quarantined,
        }
    finally:
        if cached_data is not None:
            cached_data.unpersist()
        if own_session:
            session.stop()
