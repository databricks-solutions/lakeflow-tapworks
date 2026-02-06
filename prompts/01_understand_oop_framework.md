# Step 1 — Understand the OOP framework

> **Audience: Human** — Read this file as a workflow/playbook.
>
> **Audience: Agent** — You can paste the “Agent prompt” block below into Cursor/Claude to have it generate the requested design note.

## Goal
Build an accurate mental model of the OOP connector framework and how it generates Databricks Asset Bundles (DABs).

## Agent prompt (copy/paste into Cursor/Claude)

```
Read the repository files listed below and produce a short design note that explains:
- entry points users run
- how CSV config becomes output/<project>/databricks.yml + resources/*.yml
- database vs SaaS connector differences
- grouping rules (prefix+subgroup -> pipeline_group; gateways for DBs)
- naming conventions for pipeline/job resources
- known constraints / gotchas (UC connections, CDC prereqs, ownership)

Ground your answer in the source of truth: core/connectors.py. Include function names where relevant.
```

## Required outputs
Produce a short design note that includes:

- **Entry points**: which scripts/users call
- **Core workflow**: how config becomes `output/<project>/databricks.yml` + `resources/*.yml`
- **Connector taxonomy**: database vs SaaS connectors and what differs
- **Grouping rules**: how `prefix` + `subgroup` becomes `pipeline_group` (and gateways for DBs)
- **Naming conventions**: how pipeline/job resource names are formed
- **Known constraints**: UC connection quirks, CDC prerequisites, UC table ownership rules

## Where to read (must-reference files)

- `ARCHITECTURE.md` (high-level design)
- `core/connectors.py` (the implementation; treat as source of truth)
- Connector implementations:
  - `sqlserver/connector.py` (database connector reference)
  - `postgres/connector.py` (database connector reference)
  - `salesforce/connector.py` (SaaS connector reference)
  - `google_analytics/connector.py` (SaaS connector reference)
- Utility helpers:
  - `utilities/utilities.py`

## “Golden path” summary (expected)

You should be able to explain, at minimum:

- `BaseConnector.run_complete_pipeline_generation(...)` is the **golden entrypoint** for all connectors.
- Input config normalization is shared (required/optional columns + defaults + overrides).
- Load balancing assigns `pipeline_group` (and gateway grouping for DB connectors).
- YAML generation writes a per-project DAB layout:

```text
output/<project_name>/
  databricks.yml
  resources/
    (gateways.yml)  # database only
    pipelines.yml
    jobs.yml
```

## Acceptance criteria
- You can point to the exact functions in `core/connectors.py` responsible for:
  - normalization
  - grouping/load balancing
  - YAML file generation and output layout
- You can articulate what differs for database vs SaaS connectors.

