# Validation

Tapworks validates input configuration before generating DAB files. This document covers the validation rules applied during pipeline generation.

## Required Fields

Each connector defines its own `required_columns` (e.g., `source_schema`, `target_catalog`, `connection_name`). If any required column is missing from the input, a `ConfigurationError` is raised.

### `project_name`

`project_name` is always required and has no default. It must be provided via:
- A column in the input config (CSV/DataFrame)
- `default_values` parameter
- `override_config` parameter

```python
# Providing via default_values
run_pipeline_generation(
    connector_name='salesforce',
    input_source='tables.csv',
    output_dir='output',
    targets={...},
    default_values={'project_name': 'sfdc_prod'},
)
```

## Resource Naming

Resource names are derived from `project_name`, `prefix`, and `subgroup`:

```
project_name  →  required, no default
prefix        →  falls back to project_name if not specified
subgroup      →  defaults to "01" if not specified
pipeline_group = {prefix}_{subgroup}
```

| Resource | Pattern | Example |
|---|---|---|
| Pipeline (resource name) | `pipeline_{pipeline_group}` | `pipeline_sales_02` |
| Pipeline (display name) | `{pipeline_group}` | `sales_02` |
| Job (resource name) | `job_{pipeline_group}` | `job_sales_02` |
| Job (display name) | `{pipeline_group}_scheduler` | `sales_02_scheduler` |

Resource names are validated to contain only alphanumeric characters, underscores, and hyphens.

> **Important:** Prefixes must be unique per workspace. Using the same prefix across different projects deployed to the same workspace will cause resource name collisions. Use distinct prefixes (or distinct `project_name` values if relying on the prefix fallback) for each project.

## Subgroup Validation

Subgroups control how tables are grouped into pipelines within a prefix. Tapworks enforces consistent usage:

- **All empty** — defaults every row to `"01"` (auto-grouping)
- **All explicit** — preserves user-defined subgroups
- **Mixed (some empty, some explicit) within the same prefix** — raises a `ValidationError`

This prevents accidental mis-grouping where some tables get auto-assigned while others are manually placed.

## SCD Type Validation

If an `scd_type` column is provided, values are validated against the connector's supported types (typically `SCD_TYPE_1` and `SCD_TYPE_2`). Validation is case-insensitive. Empty or invalid values are ignored (treated as unset).

## Cron Expression Validation

Schedule values are validated as cron expressions (5-field format: `minute hour day month weekday`). Invalid expressions generate a warning and the job is skipped.

## Targets Validation

At least one target environment must be provided. Each target must include a `workspace_host`. An empty `targets` dict raises a `ConfigurationError`.
