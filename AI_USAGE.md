# Development tools and assistance

This note records development assistance used on the EpiScope repository. The team should update it before submission to accurately describe each member's tools and contributions.

| Tool | Purpose |
|---|---|
| GitHub Copilot Chat in VS Code | Reviewed the repository against the assignment requirements and assisted with SHA-256 ingestion checks, Spark experiment scripts, focused tests, and documentation. |

## Example prompts

- “Compare this repository with the assignment requirements and list incomplete deliverables. Do not claim results that have not been measured.”
- “Add repeatable Spark performance experiments that record query plans and timings and protect existing generated data.”
- “Test bronze input classification, ground-truth exclusion, and SHA-256 checksums.”

Suggestions were reviewed and tested where possible. The complete working-copy test suite passed locally (`49 passed`, with Java 8 configured). Rerun the tests after team integration. Performance and scale results must be measured on the stated hardware.

Each member should add other tools they used, their purpose, and example prompts before submission.
