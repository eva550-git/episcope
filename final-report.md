# EpiScope — Final Assignment Report

**Scenario:** Synthetic disease-surveillance and preparedness-planning workflow for Bangladesh; an educational prototype, not an operational or clinical system.
**Outcome:** ⚠️ Core pipeline and supporting analytics/dashboard work are present, but assignment-scale processing and evaluation evidence remain incomplete or unverified.
**Projects affected:** 1 repository (data pipeline, analytics, serving, and dashboard)

---

## Summary

EpiScope generates synthetic facility, daily case, and encounter data; ingests encounter files into Bronze; applies Spark-based Silver quality, quarantine, and privacy processing; creates weekly Gold summaries; and exports small-cell-suppressed data for analytics and a D3 dashboard. A recent Silver validation on real Bronze input processed 2,699,025 rows with 16 enabled quality rules, retaining 2,553,555 and quarantining 145,470; the latest focused regression run passed 17 tests, while a previous working-copy run passed 51. Temporary generator evidence meets the 25-million encounter generation count, but the full Spark pipeline was not run at that scale, and outbreak, forecast, resource-allocation, and target-hardware performance outcomes have not been demonstrated with assignment-level evaluation artifacts.

## What Changed

### Packages

No package version changes were verified for this report.

### Code Modifications

- **Data generation and Bronze ingestion** — Synthetic encounter generation includes delayed reports and injected defects. A labelled synthetic S6 stock snapshot is now generated before Bronze ingestion when no stock file is supplied; existing stock input is preserved. Bronze ingestion classifies the stock CSV and generated daily counts, records SHA-256 checksums and batch metadata, supports idempotent ingestion, and excludes ground-truth data. Relevant implementation includes `generator/generate_stock.py` and `pipeline/bronze/`.
- **Silver quality and privacy** — Spark validation applies 16 enabled rules, quarantines failing rows with reasons, and hashes patient identifiers using a private local salt. The latest-version filter ignores older Bronze copies of an encounter but deliberately retains duplicates within the selected latest batch so the uniqueness rule can flag them. Partitioned Spark output defaults to `pipeline/silver/clean_encounters_spark`; the legacy `pipeline/silver/clean_encounters.parquet` is a file and was preserved rather than replaced.
- **Gold summaries** — Weekly district/disease aggregates are built with Spark from daily counts, choose the latest Bronze revision per district/disease/date, preserve contributing Bronze batch IDs in `load_batch_id`, write partitioned Parquet, and register a Spark SQL temporary view while retaining the compact CSV for serving. The Gold contract is documented in [docs/gold_schema.md](docs/gold_schema.md).
- **Analytics and dashboard** — The main pipeline now reads latest-version Bronze daily counts, runs Spark-window EARS detection and explicit outbreak/reporting-artefact evaluation, fits weather-informed weekly forecasts, and evaluates the final 52 complete origins against a seasonal-naive baseline. Forecasts and recent EARS candidate signals feed the serving export; the D3 dashboard labels the model and displays rolling-origin interval coverage when available. The heuristic forecast and weekly alert rule remain available when analytics artifacts are absent. These integrations have focused code checks but have not yet been validated through a complete end-to-end pipeline run.
- **Performance tooling and documentation** — Scale benchmark tooling supports dry-run inspection and requires explicit acknowledgement before replacing generated data. Spark microbenchmark tooling records query plans and repeated timings. README documents Java requirements, test/run commands, scale limitations, and privacy cautions.

### Git Commits

These are relevant committed history; later changes seen during report preparation are in the working tree and are not represented as commits.

| SHA | Message |
|-----|---------|
| `9c28525` | Implement analytics modules |
| `a6b49dc` | Add backend pipeline and project handover documentation |
| `5c8921b` | Add bronze ingestion with metadata, checksums and idempotent batches |
| `8fbfa12` | Add encounter record generator with reporting delays and data defects |
| `a23aeb1` | Validate Java before starting Spark on Windows |

## Assignment Requirement Status

| Requirement | Status | Evidence and limitation |
|---|---|---|
| Generate at least 25 million encounters | **Complete for generation only** | [reports/generator_sizing_evidence.json](reports/generator_sizing_evidence.json) records 26,960,544 encounters at SF=1 in a temporary workspace and confirms the minimum count. It explicitly does not validate Spark processing of that data. |
| Apply at least 12 data-quality rules | **Rule-count requirement met; scope caveat** | [pipeline/quality/dq_report.json](pipeline/quality/dq_report.json) records 16 enabled rules in the Silver run. The individual run input was 2,699,025 rows, not the 25-million generated dataset; end-to-end quality at assignment scale remains unverified. |
| Spark processing of 25-million encounters / full pipeline at required scales | **Incomplete** | The pipeline stages were successfully exercised end-to-end on the currently ingested 2,699,025-row Bronze data, with outputs isolated to a temporary directory. No SF=1 Spark run exists; required SF=0.25, 0.5, and 1.0 end-to-end runs and target-hardware results are absent. Current working-tree changes add S6 stock ingestion and Bronze daily-count lineage, but those additions have only focused tests, not a full-pipeline benchmark. |
| Outbreak detection evaluation | **Implemented; assignment evaluation unverified** | EARS and evaluation helpers/tests cover synthetic fixtures and metrics such as sensitivity, false alerts per district-year, and detection delay. No measured evaluation against the assignment's outbreak ground truth is recorded. |
| Forecasting metrics | **Implemented; pilot measured, full evaluation incomplete** | A bounded held-out four-week pilot on 67,072 synthetic weekly rows scored one origin for each of 256 district-disease series. Metrics are reported below; they are not a complete rolling-origin evaluation or assignment-ground-truth result. |
| Resource optimization | **Implemented and compared on current synthetic snapshot; assignment validation incomplete** | The dashboard export measured optimized allocation against its population-proportional baseline across five budgets and three equity floors. Results are reported below; they do not establish performance on independently validated assignment inputs. |
| Spark performance optimization benchmarks | **Partial smoke-test evidence only** | [reports/spark_optimisations.json](reports/spark_optimisations.json) records a 1-million-row synthetic microbenchmark: broadcast join speedup 1.208x; partition pruning speedup 0.997x with smaller output files. This is not a full-pipeline benchmark, host RAM was about 8 GiB versus the stated 16 GiB target, and Spark UI screenshots were not captured. |
| Dashboard/serving validation | **Implementation and focused tests present; full acceptance unverified** | Serving export tests cover output views, suppression, and a payload-size bound. Integrated browser usability, accessibility, all required linked interactions, and clean-clone submission validation are not evidenced as completed. |

## Validation Results

### Silver Spark Validation

The real-Bronze pipeline stage run used temporary output locations to preserve existing generated artifacts. Bronze ingestion was preflighted and rerun in idempotent mode (65 discovered files; 0 new, 65 already ingested, 0 unrecognized). Silver, Gold, and serving export then completed:

| Measure | Result |
|---------|--------|
| Input rows | 2,699,025 |
| Clean rows | 2,553,555 |
| Quarantined rows | 145,470 |
| Enabled quality rules | 16 |
| Gold weekly rows | 67,072 |
| Dashboard summary export | 2,540,093 bytes; district-boundaries export present |
| Elapsed stage time | 238.07 seconds |
| Regression suite | 51 passed (rerun on 2026-10-26) |

Outputs were temporary and were deleted after the run; the existing repository Silver, Gold, dashboard, and run-metrics outputs were not modified. This verifies the current stages at the available 2.7-million-row scale, but not the SF=0.25/0.5/1.0 assignment sweep or the `pipeline.run_pipeline` wrapper itself. The legacy `clean_encounters.parquet` file remains preserved; Spark Silver used its separate partitioned output location.

### Analytics-to-Serving Smoke Test

The new runner was exercised on an 80-week, one-district synthetic fixture using Python 3.11 and PySpark 3.5.4. It completed EARS outbreak/artefact evaluation, emitted one alert candidate and four model horizons, and evaluated the final 52 complete forecast origins. The serving exporter successfully consumed those forecast, alert, and evaluation artifacts and produced a dashboard summary. This verifies the integration contract on a bounded fixture only; it is not an assignment-scale model evaluation or a full `pipeline.run_pipeline` execution.

The latest focused regression run covered stock generation, Bronze ingestion, Gold and analytics revision selection, EARS, forecast evaluation helpers, and serving exports: **17 passed in 154.87 seconds**. The repository-wide test suite was not rerun.

### Recorded Spark Microbenchmarks

| Experiment | Baseline median | Optimized median | Result |
|------------|----------------:|-----------------:|--------|
| Broadcast registry join | 0.137456 s | 0.113791 s | 1.208x speedup on synthetic 1M-row microbenchmark |
| Partition-pruned Parquet scan | 0.063070 s | 0.063244 s | 0.997x speedup; partitioned output was smaller (90,212 vs 114,610 bytes) |

These measurements are not assignment-scale or full-pipeline performance evidence.

### Forecast Evaluation Pilot

A bounded held-out evaluation used the same forecast-generation and horizon-summary helpers as the rolling-origin evaluator, with one final four-week origin per district-disease series. It used 67,072 weekly rows, 256 series, and scored 1,024 forecasts. This is a pilot, not the complete rolling-origin metric run.

| Horizon | MAE | 80% interval coverage | Seasonal-naive MAE |
|--------:|----:|-----------------------:|--------------------:|
| 1 week | 2.54 | 96.1% | 3.88 |
| 2 weeks | 2.56 | 99.6% | 3.64 |
| 3 weeks | 3.27 | 99.6% | 2.98 |
| 4 weeks | 6.76 | 99.6% | 5.03 |

The pilot indicates the model beats the seasonal-naive MAE at horizons 1 and 2 but not 3 and 4. The Spark `applyInPandas` path could not be run in this Python 3.12 / PySpark 3.5.4 environment because the runtime lacks `distutils`; a full local multi-origin attempt was stopped after exceeding the available runtime. The pipeline is configured to run with Python 3.11, which successfully ran the grouped forecast smoke test. The complete pipeline evaluation and its newly generated metrics have not yet been run; the single-origin pilot must not be presented as the assignment's complete forecasting evaluation.

### Resource Allocation Comparison

The dashboard exporter was rerun into a temporary directory using the current synthetic Gold mart and compared optimized allocation with the code's population-proportional baseline. At the default 60% budget scenario, optimized fill was **94.7%** versus **75.2%** for baseline; the reported equity index was **94.6** versus **90.3**. Across budget fractions 20%, 40%, 60%, 80%, and 100%, optimized fill was 93.4%, 94.0%, 94.7%, 95.3%, and 95.9%; baseline fill was 57.9%, 69.6%, 75.2%, 81.7%, and 87.5%. These are measured synthetic-snapshot comparisons, not independent or assignment-scale validation.

## Practical Run Instructions

From `C:\episcope`, use Python 3.10+ and Java 8, 11, or 17 (the documented Spark setup does not support Java 21). Windows users can check Java without starting the pipeline:

```powershell
.\run_pipeline.bat --check-java
```

To create/use the local environment, configure a private `PII_SALT` in the ignored `.env` file, and run the default SF=0.10 pipeline with serving:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
# Set a private, randomly generated PII_SALT in .env; never commit or share it.
.\.venv\Scripts\python.exe -m pipeline.run_pipeline --sf 0.10 --seed 42 --serve
```

Alternatively, `.\run_pipeline.bat` performs the documented Windows setup and runs the default scale. Set `$env:SF = "0.25"` first to choose another scale. The pipeline writes generated and derived outputs; back up data you need to preserve before running it. Start the dashboard at `http://localhost:8000/` after the pipeline completes.

Run regression tests with:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests
```

Inspect the required scale benchmark without modifying generated data:

```powershell
.\.venv\Scripts\python.exe -m pipeline.experiments.scale_benchmarks --dry-run
```

The actual scale benchmark replaces generated encounter data and pipeline outputs and requires `--acknowledge-generated-data-overwrite`. Do not run it on data that has not been backed up. Only run the documented full sweep on a suitable host (the assignment target is 16 GB RAM); this report does not include that sweep.

Run the synthetic Spark microbenchmark separately if needed:

```powershell
.\.venv\Scripts\python.exe -m pipeline.experiments.spark_optimisations --rows 1000000 --repeats 3
```

## Decisions and Constraints

- Encounter, facility, and stock inputs are synthetic; dashboard values and planning outputs must not be treated as operational or clinical advice.
- Ground-truth evaluation data is supplied explicitly to evaluation helpers rather than automatically searched or loaded by the detector.
- The Silver partitioned Spark output uses a separate default path to avoid overwriting the legacy single-file Parquet artifact.
- Full scale benchmarking is opt-in because it replaces generated and derived project data. The available-scale pipeline stages were run with temporary outputs; no assignment-scale benchmark or full pipeline wrapper run was performed.

## Known Gaps & Follow-up Items

- **Full-scale Spark processing** — Run and record full-pipeline SF=0.25, 0.5, and 1.0 results on the required 16-GB-or-better host, with hardware, stage timings, and output sizes. The successful 2.7-million-row stage run and generator-only result of 26.96 million encounters do not satisfy this evidence requirement.
- **Outbreak evaluation** — Run the detector/evaluator against the assignment's outbreak truth and report sensitivity, false-alert rate, delay, and threshold trade-offs; document the data and method.
- **Forecast evaluation** — Run the complete rolling-origin evaluation on the assignment dataset; the current bounded pilot is not a substitute, and the Spark pandas-UDF path needs a compatible Python/PySpark environment.
- **Allocation evaluation** — Repeat and document the optimized-versus-population-proportional comparison on the accepted assignment demand/stock inputs; current figures use synthetic snapshot data.
- **Spark performance evidence** — Capture Spark UI evidence and repeat benchmark comparisons on target hardware. Current microbenchmarks are synthetic and do not establish end-to-end improvement.
- **Final integration/handover** — Resolve stale README status statements, verify clean-clone setup, check dashboard accessibility and linked-view acceptance, and complete the proposal's group number, contribution matrix, individual reflections, report/video links, and final version tag. Verify reference-data attribution and redistribution terms before submission.
- **Working-tree state** — The repository had existing modified and untracked work, including analytics/dashboard files, Spark quality code, tests, and generated outputs. The validation used temporary outputs and did not modify the existing generated artifacts. The report and working-copy additions have not been committed or pushed.
