# Gold schema contract (v0.1)

## gold_weekly_cases (grain: epi_year x epi_week x district x disease)
| column | type | description |
|---|---|---|
| epi_year | int | ISO year |
| epi_week | int | 1..53 |
| week_start | date | Monday of the epi-week |
| division | string | 8 divisions |
| district | string | 64 districts |
| disease | string | dengue, diarrhoea, ili, malaria |
| cases | int | total reported cases |
| lab_confirmed | int | lab confirmed cases |
| admitted | int | admitted cases |
| deaths | int | deaths |
| reports_received | int | facilities that reported |
| facilities_expected | int | facilities in the district |
| population | int | district population (denominator) |
| load_batch_id | string | lineage to bronze batch |

## Diseases (ICD-10 mapping)
- dengue: A90, A91
- diarrhoea: A00, A09
- ili: J10, J11
- malaria: B50-B54