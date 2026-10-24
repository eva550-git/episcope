"""Run the reproducible EpiScope pipeline from generation through dashboard export."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_LOG = ROOT / "pipeline" / "run_metrics.jsonl"


def load_local_environment() -> None:
    """Load simple KEY=VALUE entries from .env without overriding the process environment."""
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def prepare_reference_inputs() -> None:
    """Materialize small tracked reference assets into the ignored raw-data workspace."""
    raw = ROOT / "data_raw"
    raw.mkdir(parents=True, exist_ok=True)
    assets = (
        (ROOT / "data_ref" / "weather_daily.csv.gz", raw / "weather_daily.csv"),
        (ROOT / "data_ref" / "district_boundaries.geojson", raw / "districts.geojson"),
        (ROOT / "data_ref" / "population_by_district.csv", raw / "population_by_district.csv"),
    )
    for source, destination in assets:
        if destination.exists():
            continue
        if not source.is_file():
            raise FileNotFoundError(f"Required reference input is missing: {source}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        if source.suffix == ".gz":
            with gzip.open(source, "rb") as compressed, destination.open("wb") as output:
                shutil.copyfileobj(compressed, output)
        else:
            shutil.copy2(source, destination)


def run_stage(name: str, module: str, args: list[str], metrics: dict) -> None:
    started = time.perf_counter()
    subprocess.run([sys.executable, "-m", module, *args], cwd=ROOT, check=True)
    metrics[name] = round(time.perf_counter() - started, 3)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sf", type=float, default=0.10, help="Synthetic encounter scale factor.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--skip-generation",
        action="store_true",
        help="Ingest a new/hidden landing drop and rebuild downstream outputs.",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start the local dashboard server after exporting dashboard data.",
    )
    options = parser.parse_args()
    if options.sf <= 0:
        parser.error("--sf must be greater than zero.")

    load_local_environment()
    prepare_reference_inputs()
    metrics = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "scale_factor": options.sf,
        "seed": options.seed,
    }
    if not options.skip_generation:
        run_stage("facility_generation", "generator.generate_facilities", ["--seed", str(options.seed)], metrics)
        run_stage(
            "count_generation",
            "generator.generate_epi_counts",
            ["--sf", str(options.sf), "--seed", str(options.seed)],
            metrics,
        )
        run_stage(
            "encounter_generation",
            "generator.generate_encounters",
            ["--sf", str(options.sf), "--seed", str(options.seed)],
            metrics,
        )

    run_stage("bronze_ingestion", "pipeline.bronze.ingest_bronze", [], metrics)
    run_stage("silver_quality", "pipeline.silver.clean_silver", [], metrics)
    run_stage("gold_aggregation", "pipeline.gold.create_gold_marts", [], metrics)
    run_stage("dashboard_export", "serving.export_serving_data", [], metrics)
    metrics["completed_at"] = datetime.now(timezone.utc).isoformat()
    RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
    with RUN_LOG.open("a", encoding="utf-8") as log:
        log.write(json.dumps(metrics, sort_keys=True) + "\n")
    print(f"Pipeline stage timings recorded in {RUN_LOG}")

    if options.serve:
        subprocess.run(
            [sys.executable, "-m", "http.server", "8000", "--directory", str(ROOT)],
            cwd=ROOT,
            check=True,
        )


if __name__ == "__main__":
    main()
