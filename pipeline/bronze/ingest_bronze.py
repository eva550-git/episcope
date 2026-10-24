"""Bronze layer: raw, immutable, append-only copy of every source plus ingestion metadata.

Every record keeps its original text (all columns are strings, nothing is cleaned or cast) and gets
  _source, _batch_id, _load_time, _file_name, _file_sha256
Bronze tables are Parquet, partitioned by _batch_id:  <lake>/bronze/<table>/_batch_id=<id>/
Incremental and idempotent: a file whose (source, SHA-256) is already in the ingest log is skipped,
and a batch id is derived from the SHA-256 checksums of its files, so re-running a crashed load rewrites the same
partition instead of duplicating rows.

Sources are recognised from the file contents (header / keys), never from hard-coded file names.
Anything under a folder called ground_truth is never read.
"""
import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pyspark.sql import functions as F  # noqa: E402

from pipeline.common import LAKE, ROOT, get_spark, uri  # noqa: E402

TABLES = {
    "encounters": "bronze_encounters",   # S4
    "stock": "bronze_stock",             # S6
    "facilities": "bronze_facilities",   # S5
    "weather": "bronze_weather",         # S3
    "population": "bronze_population",   # S2
}
DEFAULT_INPUTS = [
    ROOT / "data_raw" / "landing",
    ROOT / "data_raw" / "synthetic" / "facility_registry.csv",
    ROOT / "data_raw" / "weather_daily.csv",
    ROOT / "data_raw" / "population_by_district.csv",
    ROOT / "data_raw" / "districts.geojson",
]
LOG = LAKE / "_meta" / "ingest_log.jsonl"


def sha256_of(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def classify(path):
    """Return the source name for a file, or None if it is not a recognised input."""
    suffix = path.suffix.lower()
    try:
        with open(path, "r", encoding="utf-8") as f:
            first = f.readline().strip()
    except UnicodeDecodeError:
        return None
    if suffix == ".geojson":
        return "boundaries"
    if suffix == ".csv":
        cols = {c.strip().strip('"').lower() for c in first.split(",")}
        if {"encounter_id", "report_date"} <= cols:
            return "encounters"
        if {"facility_id", "beds"} <= cols:
            return "facilities"
        if "precip_mm" in cols:
            return "weather"
        if "district" in cols and "district_id" not in cols and any(c.startswith("population") for c in cols):
            return "population"
    if suffix in (".json", ".jsonl"):
        if '"snapshot_date"' in first:
            return "stock"
        if '"FeatureCollection"' in first:
            return "boundaries"
    return None


def discover(inputs):
    files = []
    for p in inputs:
        p = Path(p)
        if p.is_file():
            files.append(p)
        elif p.is_dir():
            files.extend(sorted(x for x in p.rglob("*") if x.is_file()))
    keep = [f for f in files if "ground_truth" not in {part.lower() for part in f.parts}]
    return keep


def read_log():
    if not LOG.exists():
        return []
    return [json.loads(line) for line in LOG.read_text(encoding="utf-8").splitlines() if line.strip()]


def append_log(rows):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def read_raw(spark, source, path):
    if source == "stock":
        return spark.read.option("primitivesAsString", "true").json(uri(path))
    return (spark.read.option("header", "true").option("inferSchema", "false")
            .option("mode", "PERMISSIVE").csv(uri(path)))


def ingest_source(spark, source, files, load_time):
    """Write one batch for `source` containing the given [(path, sha256)] files."""
    checksums = sorted(checksum for _, checksum in files)
    batch_hash = hashlib.sha256("".join(checksums).encode()).hexdigest()[:16]
    batch_id = f"{source}-{batch_hash}"
    parts = []
    for path, checksum in files:
        df = (read_raw(spark, source, path)
              .withColumn("_source", F.lit(source))
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_load_time", F.lit(load_time).cast("timestamp"))
              .withColumn("_file_name", F.lit(path.name))
              .withColumn("_file_sha256", F.lit(checksum)))
        parts.append(df)
    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)
    out = LAKE / "bronze" / TABLES[source]
    df.write.mode("overwrite").partitionBy("_batch_id").parquet(uri(out))
    back = spark.read.parquet(uri(out)).where(F.col("_batch_id") == batch_id)   # partition pruning
    per_file = {r["_file_name"]: r["count"] for r in back.groupBy("_file_name").count().collect()}
    return batch_id, per_file


def copy_boundaries(files, load_time):
    """GeoJSON boundaries are kept as an untouched file copy (not tabular)."""
    out = []
    for path, checksum in files:
        batch_id = f"boundaries-{checksum[:16]}"
        dest = LAKE / "bronze" / "boundaries" / f"_batch_id={batch_id}"
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest / path.name)
        with open(path, encoding="utf-8") as f:
            n = len(json.load(f).get("features", []))
        out.append({"source": "boundaries", "file_name": path.name, "file_sha256": checksum,
                    "batch_id": batch_id, "load_time": load_time, "rows": n})
    return out


def main():
    ap = argparse.ArgumentParser(description="Ingest raw sources into the bronze layer.")
    ap.add_argument("--input", nargs="*", default=None,
                    help="files or folders to scan (default: the standard data_raw locations)")
    args = ap.parse_args()

    inputs = args.input if args.input else DEFAULT_INPUTS
    candidates = discover(inputs)
    done = {
        (row["source"], row["file_sha256"])
        for row in read_log()
        if row.get("file_sha256")
    }
    load_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    new, skipped, unknown = {}, 0, []
    for path in candidates:
        source = classify(path)
        if source is None:
            unknown.append(path.name)
            continue
        checksum = sha256_of(path)
        if (source, checksum) in done:
            skipped += 1
            continue
        new.setdefault(source, []).append((path, checksum))

    print(f"found {len(candidates)} files: {sum(len(v) for v in new.values())} new, "
          f"{skipped} already ingested, {len(unknown)} not recognised")
    if unknown:
        print("  not recognised (ignored):", ", ".join(sorted(set(unknown))[:8]))
    if not new:
        print("nothing to do (idempotent: no new files)")
        return

    log_rows = []
    if "boundaries" in new:
        copied = copy_boundaries(new.pop("boundaries"), load_time)
        log_rows += copied
        print(f"  boundaries  copied untouched: {', '.join(r['file_name'] for r in copied)} "
              f"({sum(r['rows'] for r in copied)} features)")
    spark = get_spark("bronze_ingest") if new else None
    for source, files in new.items():
        batch_id, per_file = ingest_source(spark, source, files, load_time)
        total = sum(per_file.values())
        print(f"  {source:11s} batch {batch_id}: {len(files)} files, {total:,} rows")
        for path, checksum in files:
            log_rows.append({"source": source, "file_name": path.name, "file_sha256": checksum,
                             "batch_id": batch_id, "load_time": load_time,
                             "rows": per_file.get(path.name, 0)})
    append_log(log_rows)
    print(f"logged {len(log_rows)} files -> {LOG}")
    if spark:
        spark.stop()


if __name__ == "__main__":
    main()