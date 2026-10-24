"""Measure Spark broadcast-join and Parquet partition-pruning optimisations."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.common import ROOT
from pipeline.experiments.scale_benchmarks import _directory_bytes
from pipeline.experiments.system_info import collect_system_info

DEFAULT_OUTPUT = ROOT / "reports" / "spark_optimisations.json"


def _measure(frame: DataFrame, repeats: int) -> tuple[list[dict[str, Any]], list[float], str]:
    """Warm a query once, then time repeated actions and return its executed plan."""
    warmup = [row.asDict() for row in frame.collect()]
    elapsed = []
    result = warmup
    for _ in range(repeats):
        started = time.perf_counter()
        result = [row.asDict() for row in frame.collect()]
        elapsed.append(time.perf_counter() - started)
    plan = frame._jdf.queryExecution().executedPlan().toString()
    return result, elapsed, plan


def _summary(baseline: list[float], optimized: list[float]) -> dict[str, float]:
    baseline_median = statistics.median(baseline)
    optimized_median = statistics.median(optimized)
    return {
        "baseline_median_seconds": round(baseline_median, 6),
        "optimized_median_seconds": round(optimized_median, 6),
        "speedup": round(baseline_median / optimized_median, 3) if optimized_median else 0.0,
    }


def run_experiment(rows: int, repeats: int, output: Path) -> dict[str, Any]:
    if rows < 1 or repeats < 1:
        raise ValueError("rows and repeats must both be positive.")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("episcope-spark-optimisations")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        facts = (
            spark.range(rows)
            .select(
                F.concat(F.lit("F"), F.lpad(F.pmod(F.col("id"), F.lit(64)).cast("string"), 3, "0"))
                .alias("facility_id"),
                (F.pmod(F.col("id"), F.lit(4)) + F.lit(2022)).alias("epi_year"),
                F.pmod(F.col("id"), F.lit(51)).cast("long").alias("cases"),
            )
            .cache()
        )
        facts.count()
        registry = (
            spark.range(64)
            .select(
                F.concat(F.lit("F"), F.lpad(F.col("id").cast("string"), 3, "0"))
                .alias("facility_id"),
                F.concat(
                    F.lit("division-"),
                    F.pmod(F.col("id"), F.lit(8)).cast("string"),
                ).alias("division"),
            )
            .cache()
        )
        registry.count()

        sort_merge = facts.join(registry, "facility_id").groupBy("division").agg(
            F.sum("cases").alias("cases")
        ).orderBy("division")
        broadcast = facts.join(F.broadcast(registry), "facility_id").groupBy("division").agg(
            F.sum("cases").alias("cases")
        ).orderBy("division")
        baseline_join, baseline_join_times, sort_merge_plan = _measure(sort_merge, repeats)
        optimized_join, optimized_join_times, broadcast_plan = _measure(broadcast, repeats)
        if baseline_join != optimized_join:
            raise AssertionError("Join strategies produced different aggregates.")
        if "SortMergeJoin" not in sort_merge_plan or "BroadcastHashJoin" not in broadcast_plan:
            raise RuntimeError(
                "Spark did not use the expected join strategies; inspect the recorded plans."
            )

        with tempfile.TemporaryDirectory(prefix="episcope-spark-optimisations-") as temp_dir:
            root = Path(temp_dir)
            unpartitioned = root / "unpartitioned"
            partitioned = root / "partitioned"
            facts.write.mode("overwrite").parquet(str(unpartitioned))
            facts.write.mode("overwrite").partitionBy("epi_year").parquet(str(partitioned))
            baseline_scan = spark.read.parquet(str(unpartitioned)).where(
                F.col("epi_year") == 2024
            ).agg(F.sum("cases").alias("cases"))
            optimized_scan = spark.read.parquet(str(partitioned)).where(
                F.col("epi_year") == 2024
            ).agg(F.sum("cases").alias("cases"))
            baseline_partition, baseline_partition_times, baseline_scan_plan = _measure(
                baseline_scan, repeats
            )
            optimized_partition, optimized_partition_times, partitioned_scan_plan = _measure(
                optimized_scan, repeats
            )
            if baseline_partition != optimized_partition:
                raise AssertionError("Partitioned and unpartitioned scans produced different results.")
            if "PartitionFilters" not in partitioned_scan_plan:
                raise RuntimeError("The partitioned scan plan does not show partition pruning.")
            temporary_roots = {
                str(root),
                root.as_posix(),
                tempfile.gettempdir(),
                Path(tempfile.gettempdir()).as_posix(),
            }
            for temporary_root in temporary_roots:
                sort_merge_plan = sort_merge_plan.replace(temporary_root, "<TEMP>")
                broadcast_plan = broadcast_plan.replace(temporary_root, "<TEMP>")
                baseline_scan_plan = baseline_scan_plan.replace(temporary_root, "<TEMP>")
                partitioned_scan_plan = partitioned_scan_plan.replace(temporary_root, "<TEMP>")
            partition_bytes = {
                "unpartitioned": _directory_bytes(unpartitioned),
                "partitioned": _directory_bytes(partitioned),
            }

        report = {
            "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "host": collect_system_info(),
            "spark_version": spark.version,
            "generated_fact_rows": rows,
            "repeats_per_case_after_warmup": repeats,
            "optimisations": {
                "broadcast_registry_join": {
                    "baseline_strategy": "sort-merge join with auto broadcast disabled",
                    "optimized_strategy": "explicit broadcast hash join of the small facility registry",
                    "baseline_seconds": baseline_join_times,
                    "optimized_seconds": optimized_join_times,
                    **_summary(baseline_join_times, optimized_join_times),
                    "baseline_plan": sort_merge_plan,
                    "optimized_plan": broadcast_plan,
                },
                "parquet_partition_pruning": {
                    "baseline_strategy": "filter a full, unpartitioned Parquet scan",
                    "optimized_strategy": "filter Parquet partitioned by epi_year",
                    "baseline_seconds": baseline_partition_times,
                    "optimized_seconds": optimized_partition_times,
                    **_summary(baseline_partition_times, optimized_partition_times),
                    "baseline_plan": baseline_scan_plan,
                    "optimized_plan": partitioned_scan_plan,
                    "output_bytes": partition_bytes,
                },
            },
            "spark_ui_screenshots_captured": False,
            "limitations": [
                "Synthetic microbenchmarks demonstrate these Spark operators; they are not full-pipeline timings.",
                "Run on the assignment target hardware and capture Spark UI screenshots before claiming ENG-3 complete.",
            ],
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        return report
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    options = parser.parse_args()
    report = run_experiment(options.rows, options.repeats, options.output)
    for name, result in report["optimisations"].items():
        print(
            f"{name}: median speedup {result['speedup']:.3f}x "
            f"({result['baseline_median_seconds']:.3f}s -> "
            f"{result['optimized_median_seconds']:.3f}s)"
        )
    print(f"Evidence saved to {options.output}")


if __name__ == "__main__":
    main()
