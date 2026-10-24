# EpiScope

EpiScope is a Big Data Analytics course project that demonstrates a synthetic disease-surveillance and preparedness-planning workflow for Bangladesh. It brings together generated encounter data, data quality checks, district-level summaries, analytics, and a D3.js dashboard. The project is an educational prototype: its data and outputs are not for clinical or operational decision-making.

## Project team

| Member | Student ID | Role |
|---|---|---|
| Eva Kalam | 0222220005101081 | Role 1 — Data Engineering |
| Aklima Akther Naima | 0222220005101084 | Role 2 — Analytics |
| Shreoshee Chowdhury Oma | 0222220005101086 | Role 3 — Visualization |

## Project status

The repository is being assembled by the three team members. Analytics and dashboard files are being contributed separately by the members responsible for those roles; this branch currently contains the data-generation and pipeline work, serving code, and performance-experiment tools. Integrate the remaining team contributions before treating the project as a complete, clean-clone submission.

The report and presentation video will be added later. The final handover also needs the team's contribution matrix and individual reflections. Benchmark results and other claims in the final report should come from measured runs, not estimates.

## Requirements

- Windows, macOS, or Linux. On Windows, WSL2 is recommended for Spark.
- Python 3.10 or newer.
- Java 8, 11, or 17 for PySpark 3.5; Java 21 is not supported by this project configuration.
- Free disk space for the chosen synthetic scale.

Python package versions are pinned in [`requirements.txt`](requirements.txt).

## Run the project

On Windows, the setup script creates a virtual environment, installs dependencies on first run, creates a private local PII salt if needed, and starts the default SF=0.10 pipeline:

```powershell
.\run_pipeline.bat
```

To select another scale factor:

```powershell
$env:SF = "0.25"
.\run_pipeline.bat
```

The script serves the project at `http://localhost:8000/`. Running the complete pipeline requires the analytics and dashboard contributions to be present.

To set up manually:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
# Set PII_SALT in .env to a private, randomly generated value.
.\.venv\Scripts\python.exe -m pipeline.run_pipeline --sf 0.10 --seed 42 --serve
```

Never commit `.env` or share its salt. To run tests:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests
```

The current working copy passed 49 tests with Java 8 configured. This test result does not establish completion of the assignment's full-scale benchmarks.

## Data and pipeline

The pipeline is organized into the following stages:

1. **Generate and prepare data:** create synthetic facilities, daily disease counts, and encounter records. Synthetic stock is a planning example, not a real inventory feed.
2. **Bronze ingestion:** identify source files from their contents, record SHA-256 checksums and batch metadata, and skip files already ingested. Ground-truth files are excluded from ingestion.
3. **Silver quality and privacy:** apply validation rules, quarantine failing records with reason codes, hash patient identifiers using a private salt, and write per-load quality reports.
4. **Gold summaries:** aggregate synthetic daily counts into weekly district-level case summaries.
5. **Serving and dashboard:** prepare small-cell-suppressed data for the dashboard. The serving and dashboard outputs are demonstrations and need validation after all team contributions are integrated.

### Reference data

`data_ref/` contains the district reference table, population data, simplified boundaries, and a compressed daily weather snapshot. The data notes and attribution reminders are included here so this repository has one README: the boundary source redistribution terms need verification, and the weather snapshot should be attributed to Open-Meteo. Check source terms before redistribution.

### Privacy and limitations

Patient identifiers are salted and hashed before the cleaned silver output. Keep the salt in the local `.env` file and outside version control. Encounter, facility, and stock records are synthetic. The stock snapshot is not observed facility inventory, and dashboard forecasts or alerts must not be treated as validated operational advice.

## Performance experiments

### Full-pipeline scale test

Preview the required SF 0.25, 0.5, and 1.0 runs and check the current host without modifying generated data:

```powershell
.\.venv\Scripts\python.exe -m pipeline.experiments.scale_benchmarks --dry-run
```

On a suitable host, first back up any generated inputs you want to keep. Then run the benchmark, which replaces generated encounter data and pipeline outputs:

```powershell
.\.venv\Scripts\python.exe -m pipeline.experiments.scale_benchmarks --acknowledge-generated-data-overwrite
```

The runner checks memory and free disk, uses a temporary lake for each run, and records per-stage timings and output sizes in `reports/scale_benchmarks.json` and `.csv`. The assignment requires a 16 GB machine; results from a lower-memory host must not be presented as meeting that target.

The current host has 7.8 GiB RAM, so the full-pipeline scale sweep has not been run. The existing SF=1 sizing record covers synthetic encounter generation only; it is not a Spark pipeline benchmark.

### Spark optimization measurements

Run the synthetic Spark microbenchmarks with:

```powershell
.\.venv\Scripts\python.exe -m pipeline.experiments.spark_optimisations --rows 1000000 --repeats 3
```

The latest local run compared a broadcast registry join with a sort-merge join, and a partitioned Parquet scan with an unpartitioned scan. It recorded about 1.21x speedup for the broadcast join in that small test. Partition pruning reduced output file size, but did not produce a measurable scan-time improvement in the same run. These figures are smoke-test measurements, not full-pipeline evidence; Spark UI screenshots and target-hardware measurements remain to be collected.

## Requirement status

| Area | Current state |
|---|---|
| Data generation and bronze ingestion | Implemented; ingestion uses SHA-256 checksums and excludes ground-truth data. |
| Spark processing | Bronze ingestion uses Spark. Some silver and gold transformations still use Pandas and need review against the heavy-processing requirement. |
| Incremental processing and lineage | Bronze file deduplication is present; complete silver late-arrival handling and gold batch lineage remain to be validated. |
| Data quality and privacy | Validation, quarantine, salted hashing, and quality reports are implemented in the pipeline code. |
| Analytics and forecasting | Being contributed by the analytics role; integrate and evaluate against the assignment's ground-truth and baseline requirements. |
| D3.js dashboard | Being contributed by the visualization role; integrate and verify linked views, accessibility, suppression, and payload size. |
| Scale and optimization evidence | Benchmark tools and a Spark microbenchmark record are included. Full SF 0.25/0.5/1.0 pipeline runs, target-hardware results, and Spark UI screenshots remain outstanding. |
| Final handover | Report and video are planned for later. Add the contribution matrix, individual reflections, final links, and final version tag before submission. |

## Repository layout

```text
data_ref/       Packaged district, population, boundary, and weather references
docs/           Assignment schema, proposal draft, and risk log
generator/      Synthetic data generation
pipeline/       Bronze ingestion, silver quality checks, gold summaries, experiments
serving/        Dashboard data export and compact serving files
analytics/      Team analytics contribution
dashboard/      Team D3.js visualization contribution
tests/          Automated project tests
reports/        Reproducible experiment evidence
```

Do not describe pending items as complete. Update this README when team code, measured results, and final handover materials are ready.
