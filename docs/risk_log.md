# EpiScope risk log (team draft)

| ID | Risk | Likelihood / impact | Mitigation and evidence needed | Owner |
|---|---|---|---|---|
| R1 | Synthetic or delayed reports do not represent real reporting behavior | Medium / High | Label the data as synthetic, inject known defects, and discuss external-validity limits | [Assign] |
| R2 | Small cells or linked aggregates reveal suppressed counts | Medium / High | Apply suppression to every served output, test exports, and avoid totals that recover hidden values | [Assign] |
| R3 | Weak or committed PII salt exposes linkable identifiers | Low / High | Require a high-entropy local salt, keep `.env` ignored, scan staged files, and test fail-closed behavior | [Assign] |
| R4 | SF=1 data exhausts student laptop memory, disk, or time | High / High | Check resources first, benchmark incrementally, use Spark local mode with bounded settings, and record failures honestly | [Assign] |
| R5 | Forecast or detector errors cause misleading preparedness conclusions | Medium / High | Compare against stated baselines, report uncertainty and false-alert trade-offs, and label outputs as non-operational | [Assign] |
| R6 | Synthetic stock data is mistaken for real facility inventory | Medium / High | Label stock as synthetic in the dashboard and report; do not present allocations as operational recommendations | [Assign] |
| R7 | Team contributions or final repository state are incomplete at handover | Medium / High | Integrate member branches early, run tests from a clean clone, verify required files, and tag only the reviewed final state | [Assign] |

This is an initial project risk register. The team should review likelihood and impact, assign named owners, record status and dates, and update mitigations as implementation and testing proceed.
