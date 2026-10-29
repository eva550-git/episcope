"""Run and record the full pipeline at the assignment's required scale factors.

The generator overwrites its generated encounter files, so execution requires an
explicit acknowledgement. A 16 GB host and adequate free disk are required by
default; ``--allow-underpowered`` is an explicit opt-in, not a recommendation.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.common import ROOT
from pipeline.experiments.system_info import collect_system_info

DEFAULT_FACTORS = (0.25, 0.5, 1.0)
OUTPUT_JSON = ROOT / "reports" / "scale_benchmarks.json"
OUTPUT_CSV = ROOT / "reports" / "scale_benchmarks.csv"
RUN_LOG = ROOT / "pipeline" / "run_metrics.jsonl"


def parse_scale_factors(value: str) -> list[float]:
    """Parse and validate a comma-separated scale-factor list."""
    try:
        factors = [float(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as error:
        raise argparse.ArgumentTypeError("Scale factors must be comma-separated numbers.") from error
    if not factors or any(factor <= 0 for factor in factors):
        raise argparse.ArgumentTypeError("Provide one or more positive scale factors.")
    if len(set(factors)) != len(factors):
        raise argparse.ArgumentTypeError("Scale factors must not contain duplicates.")
    return factors


def _directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _output_sizes(lake: Path) -> dict[str, int]:
    def total(paths: tuple[Path, ...]) -> int:
        return sum(_directory_bytes(path) for path in paths)

    return {
        "generated_inputs": _directory_bytes(ROOT / "data_raw" / "synthetic"),
        "encounters": _directory_bytes(ROOT / "data_raw" / "landing" / "encounters"),
        "bronze": _directory_bytes(lake / "bronze"),
        "silver": total((ROOT / "pipeline" / "silver" / "clean_encounters_spark",)),
        "gold": total((ROOT / "pipeline" / "gold" / "gold_weekly_cases.csv",)),
        "quality": total(
            (
                ROOT / "pipeline" / "quality" / "dq_report.json",
                ROOT / "pipeline" / "quality" / "dq_report.csv",
                ROOT / "pipeline" / "quality" / "quarantined_records.csv",
            )
        ),
        "serving": total(
            (
                ROOT / "serving" / "dashboard_summary.json",
                ROOT / "serving" / "district_boundaries.geojson",
            )
        ),
    }


def _read_last_pipeline_metrics(previous_line_count: int) -> dict[str, Any]:
    lines = RUN_LOG.read_text(encoding="utf-8").splitlines()
    new_lines = lines[previous_line_count:]
    if not new_lines:
        raise RuntimeError(f"Pipeline completed without appending metrics to {RUN_LOG}.")
    return json.loads(new_lines[-1])


def _write_results(results: list[dict[str, Any]]) -> None:
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    temporary_json = OUTPUT_JSON.with_suffix(".json.tmp")
    temporary_json.write_text(
        json.dumps({"results": results}, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_json.replace(OUTPUT_JSON)

    fields = [
        "scale_factor",
        "elapsed_seconds",
        "encounters",
        "generated_inputs",
        "bronze",
        "silver",
        "gold",
        "quality",
        "serving",
        "stage_seconds",
    ]
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for result in results:
            row = {field: result.get(field) for field in fields}
            row["stage_seconds"] = json.dumps(result["stage_seconds"], sort_keys=True)
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scale-factors",
        type=parse_scale_factors,
        default=list(DEFAULT_FACTORS),
        help="Comma-separated factors (default: 0.25,0.5,1.0).",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--acknowledge-generated-data-overwrite",
        action="store_true",
        help="Confirm that generated encounter data will be replaced at each scale.",
    )
    parser.add_argument(
        "--allow-underpowered",
        action="store_true",
        help="Allow execution below the assignment's 16 GB RAM requirement.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the plan and hardware without changing data or running Spark.",
    )
    options = parser.parse_args()
    host = collect_system_info()
    memory_bytes = host["installed_memory_bytes"]
    required_disk_bytes = int(12 * 1024**3 * max(options.scale_factors))

    plan = {
        "scale_factors": options.scale_factors,
        "seed": options.seed,
        "host": host,
        "required_free_disk_bytes": required_disk_bytes,
        "will_overwrite": [
            "data_raw/synthetic generated inputs",
            "data_raw/landing/encounters/encounters_*.csv",
            "pipeline/silver and pipeline/gold outputs",
            "serving outputs",
        ],
    }
    if options.dry_run:
        print(json.dumps(plan, indent=2))
        return

    if not options.acknowledge_generated_data_overwrite:
        parser.error(
            "Running the benchmark replaces generated data. Pass "
            "--acknowledge-generated-data-overwrite after backing up any inputs you need."
        )
    if memory_bytes is not None and memory_bytes < 16 * 1024**3 and not options.allow_underpowered:
        parser.error(
            f"This host has {memory_bytes / 1024**3:.1f} GiB RAM; the assignment requires "
            "16 GiB. Re-run on suitable hardware or explicitly pass --allow-underpowered."
        )
    if host["free_disk_bytes"] < required_disk_bytes:
        parser.error(
            f"At least {required_disk_bytes / 1024**3:.1f} GiB free disk is recommended; "
            f"only {host['free_disk_bytes'] / 1024**3:.1f} GiB is available."
        )

    results: list[dict[str, Any]] = []
    for scale_factor in options.scale_factors:
        if RUN_LOG.exists():
            previous_line_count = len(RUN_LOG.read_text(encoding="utf-8").splitlines())
        else:
            previous_line_count = 0

        lake_parent = ROOT / "lake"
        lake_parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="episcope-scale-", dir=lake_parent) as lake_dir:
            lake = Path(lake_dir)
            environment = os.environ.copy()
            environment["EPISCOPE_LAKE"] = str(lake)
            started = datetime.now(timezone.utc)
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pipeline.run_pipeline",
                    "--sf",
                    str(scale_factor),
                    "--seed",
                    str(options.seed),
                ],
                check=True,
                cwd=ROOT,
                env=environment,
            )
            ended = datetime.now(timezone.utc)
            metrics = _read_last_pipeline_metrics(previous_line_count)
            result = {
                "scale_factor": scale_factor,
                "started_at_utc": started.isoformat(),
                "completed_at_utc": ended.isoformat(),
                "elapsed_seconds": round((ended - started).total_seconds(), 3),
                "stage_seconds": {
                    key: value
                    for key, value in metrics.items()
                    if key not in {"started_at", "completed_at", "scale_factor", "seed"}
                },
                "output_bytes": _output_sizes(lake),
                "encounters_generated": sum(
                    1 for _ in (ROOT / "data_raw" / "landing" / "encounters").glob("encounters_*.csv")
                ),
                "host": host,
            }
            result.update(result.pop("output_bytes"))
        results.append(result)
        _write_results(results)
        print(f"SF={scale_factor:g}: {result['elapsed_seconds']:.1f}s; results saved to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
