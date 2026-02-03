# Step 2 — Add or update a connector (OOP)

> **Audience: Human** — Read this file as a workflow/playbook.
>
> **Audience: Agent** — Use the “Agent prompt” block below to drive implementation.

## Goal
Implement a new connector (or refactor an existing one) in a way that fully conforms to the shared OOP framework in `core/connectors.py`.

## Agent prompt (copy/paste into Cursor/Claude)

```
Implement/update a connector following the repo's OOP architecture:
- Choose the right base class (DatabaseConnector vs SaaSConnector)
- Define required_columns and default_values (including schedule/prefix/subgroup)
- Implement YAML generation (databricks.yml + resources/*.yml; include gateways.yml for DB connectors)
- Do NOT re-implement shared normalization/grouping outside core/connectors.py; use run_complete_pipeline_generation()
- Add/update examples/basic/pipeline_config.csv and document any connector-specific CSV columns in the connector README
```

## Decide which base class to use

- **Database source** → subclass `DatabaseConnector`
  - Must generate **gateways + ingestion pipelines**
  - Must support two-level splitting (gateways then pipelines)
- **SaaS source** → subclass `SaaSConnector`
  - Must generate **ingestion pipelines only**

## Required outputs

Create or update these files for a connector named `myconnector`:

- `myconnector/connector.py`
- `myconnector/pipeline_generator.py` (CLI; see Step 4)
- `myconnector/README.md`
- `myconnector/examples/basic/pipeline_config.csv`

## Rules (do not violate)

- Do not duplicate core logic from `core/connectors.py` in each connector.
- Connector code should focus on **connector-specific** concerns:
  - required CSV columns
  - default values
  - generating YAML structures for gateways/pipelines/jobs using connector-specific schema expectations
- Keep CSV formats consistent with the repo style (see templates).

## CSV schema design

### Database connector (typical)

- Required:
  - `source_database,source_schema,source_table_name`
  - `target_catalog,target_schema,target_table_name`
  - `connection_name`
- Optional (typical):
  - `project_name,prefix,subgroup,schedule`
  - `gateway_catalog,gateway_schema,gateway_worker_type,gateway_driver_type`

### SaaS connector (typical)

- Required:
  - `source_database,source_schema,source_table_name` (or equivalent for SaaS)
  - `target_catalog,target_schema,target_table_name`
  - `connection_name`
- Optional:
  - `project_name,prefix,subgroup,schedule`
  - connector-specific configuration columns (e.g. `include_columns`, `exclude_columns`)

## Common Databricks constraints to encode in docs/tests

- **UC table ownership**: avoid reusing a target schema/table already owned by another pipeline.
- If a connector supports column filtering (SFDC), document required system columns that must be present.
- For CDC database ingestion, document prerequisites (e.g., PostgreSQL logical decoding + replication privilege).

## Acceptance criteria

- The new connector can generate a DAB project under `output/<project_name>/`.
- The generated bundle validates with `databricks bundle validate`.
- The connector README includes a working “Quickstart” for:
  - creating a UC connection (if applicable)
  - preparing a CSV
  - generating the bundle
  - deploying it

