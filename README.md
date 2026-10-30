# EpiScope

EpiScope is a Bangladesh-focused disease surveillance and preparedness planning project. It brings synthetic health data through a Spark data pipeline, applies outbreak and forecasting analytics, and presents the results in an interactive D3 dashboard. It is built for learning and demonstration; it is not a clinical or operational decision-making system.

## Project team

| Member | Student ID | Role |
|---|---|---|
| Eva Kalam | 0222220005101081 | Data Engineer|
| Aklima Akter Naima | 0222220005101084 | Analytics Engineer |
| Shreoshee Chowdhury Oma | 0222220005101086 | Visualization Engineer |

## What the project does

- **Creates reproducible sample data.** Generators produce district and facility references, daily disease counts, patient encounter records with reporting delays and data-quality defects, and a clearly labelled synthetic facility-stock snapshot.
- **Loads data into Bronze.** Source files are identified by their contents, recorded with batch metadata and SHA-256 checksums, and skipped when they have already been ingested. Ground-truth labels are kept separate from pipeline inputs.
- **Cleans and protects data in Silver.** Spark applies the configured quality rules, sends invalid records to quarantine with reason codes, hashes patient identifiers with a private local salt, and writes partitioned Parquet data and quality reports. When newer encounter batches arrive, older copies are excluded while duplicates in the latest batch remain available for the uniqueness check.
- **Builds traceable Gold summaries.** Spark keeps the latest daily-count revision for each district, disease, and date, then creates weekly district-level summaries. The output retains source batch IDs, is written as partitioned Parquet, is registered as a Spark SQL temporary view, and is also exported as CSV for serving.
- **Runs outbreak and forecast analytics.** The pipeline runs Spark-window EARS C1 detection and evaluates alerts against explicitly supplied outbreak and reporting-artefact intervals. Its weekly ridge forecast uses recent case counts and weather features, produces four forecast horizons, and is evaluated over up to 52 complete rolling origins against a seasonal-naive baseline.
- **Prepares dashboard data and resource plans.** Serving exports suppress small counts, prepare map and time-series data, and provide forecast, alert, data-quality, seasonal, and preparedness summaries. The PuLP allocator compares resource plans under budget and equity settings using the supplied stock snapshot.
- **Shows results in an interactive dashboard.** The D3 interface links district, disease, and time selections across its map, trends, seasonal views, alerts, and resource-planning panels.
- **Includes repeatable checks and experiments.** Focused automated tests cover pipeline, analytics, and serving behavior. The repository also includes scale-runner and Spark join/storage microbenchmark commands.

## Run on Windows

Use Python 3.11 and Java 8, 11, or 17 with the included PySpark 3.5 setup. From the repository folder, start the default SF=0.10 pipeline and local dashboard server with:

```powershell
.\run_pipeline.bat
```

The script creates `.venv311`, installs the pinned packages on first run, creates a private `.env` salt if needed, runs the pipeline, and serves the project at `http://localhost:8000/`.

To choose a different synthetic scale:

```powershell
$env:SF = "0.25"
.\run_pipeline.bat
```

To check the Java selection without starting the pipeline:

```powershell
.\run_pipeline.bat --check-java
```

### Manual setup

```powershell
py -3.11 -m venv .venv311
.\.venv311\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Set a private, randomly generated `PII_SALT` value in `.env`, then run:

```powershell
.\.venv311\Scripts\python.exe -m pipeline.run_pipeline --sf 0.10 --seed 42 --serve
```

Never commit `.env` or share its salt.

## Tests

Run the project tests with:

```powershell
.\.venv311\Scripts\python.exe -m pytest -q tests
```

## Performance experiments

Preview the scale benchmark configuration before running it:

```powershell
.\.venv311\Scripts\python.exe -m pipeline.experiments.scale_benchmarks --dry-run
```

Run the acknowledged scale benchmark:

```powershell
.\.venv311\Scripts\python.exe -m pipeline.experiments.scale_benchmarks --acknowledge-generated-data-overwrite
```

The scale runner replaces generated inputs and pipeline outputs, so back up any data you want to keep first. Spark microbenchmarks can be run with:

```powershell
.\.venv311\Scripts\python.exe -m pipeline.experiments.spark_optimisations --rows 1000000 --repeats 3
```

## Privacy and data

Patient identifiers are salted and hashed before cleaned Silver output is written. Keep the salt in the local `.env` file and out of version control. Encounter, facility, and generated stock records are synthetic; use an approved real stock feed before interpreting inventory figures as actual facility stock. Dashboard alerts, forecasts, and resource plans are demonstrations, not medical or operational advice.

The packaged reference files are in `data_ref/`. Review their source notes and attribution terms before redistributing them.

## Repository layout

```text
analytics/    EARS detection, forecast models, evaluation, and resource allocation
dashboard/    D3.js dashboard
data_ref/     District, population, weather, and boundary references
docs/         Data contracts and project notes
generator/    Reproducible synthetic data generators
pipeline/     Bronze, Silver, Gold, quality, and experiment stages
reports/      Recorded experiment evidence
serving/      Dashboard exports
tests/        Automated regression tests
```

Development assistance is described in [AI_USAGE.md](AI_USAGE.md).
