# EpiScope proposal (team draft)

**Course:** CSE 4345 Big Data Analytics, Spring 2026
**Group:** [Add group number]
**Members:** Eva Kalam (0222220005101081), Aklima Akther Naima (0222220005101084), Shreoshee Chowdhury Oma (0222220005101086)

## Problem and stakeholders

EpiScope is a synthetic teaching prototype for exploring district-level infectious-disease surveillance and preparedness planning in Bangladesh. Public-health decision-makers need timely signals, interpretable trends, and an auditable way to compare resource-allocation scenarios. The project uses generated encounter and facility data with reference geography, population, and weather inputs; it does not use real patient records or represent an operational surveillance service.

The main stakeholders are national and district public-health planners, health facilities that report case information, and communities affected by disease and by decisions made from surveillance data. Key design tensions include early alerts versus false alarms, national efficiency versus district equity, useful detail versus privacy, and model sophistication versus reproducibility on student hardware.

## Proposed design

The repository separates data generation, raw bronze ingestion, quality-controlled silver data, aggregate gold marts, analytics, and a dashboard-serving layer. Patient identifiers are salted and hashed before the cleaned layer, quality failures are quarantined with reason codes, and dashboard aggregates apply small-cell suppression. The intended dashboard uses D3.js and coordinates geographic, temporal, preparedness, allocation, and alert views.

The current implementation uses Spark for bronze ingestion and Parquet storage, with Python/Pandas transformations in parts of silver and gold. This is a known deviation from the assignment's requirement to use Spark for heavy transformations; it must be addressed or explicitly evaluated before the final submission. The analytics and dashboard directories are being completed by team members and are intentionally outside this scoped contribution.

## Data and evaluation plan

Encounter, facility, and stock examples are synthetic. Reference data and their provenance should be documented in the final report. Planned checks include input-schema and domain validation, quarantine counts, privacy checks, detector performance against injected outbreak labels, rolling-origin forecast evaluation against a seasonal-naive baseline, allocation comparison with a population-proportional baseline, and full-pipeline scale runs at SF 0.25, 0.5, and 1.0. Results must be measured and reported with the hardware and limitations; generator-only counts are not Spark performance evidence.

## Roles and risks

| Role | Assigned member |
|---|---|
| Role 1 — Data generation, ingestion, data quality, and scale experiments | Eva Kalam (0222220005101081) |
| Role 2 — Detection, forecasting, and optimization | Aklima Akther Naima (0222220005101084) |
| Role 3 — Serving, dashboard, and usability evaluation | Shreoshee Chowdhury Oma (0222220005101086) |

See [`risk_log.md`](risk_log.md) for the initial risks and mitigations. The group must add its group number and review the scope and decisions before treating this draft as its M1 proposal.
