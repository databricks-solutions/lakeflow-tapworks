# Usage Guide

This guide shows how to use Lakehouse Tapworks through both command line (CLI) and notebook/programmatic interfaces.

## Quick Start - Unified CLI

Install the package first, then use the `tapworks` command:

```bash
# Install (from repo root)
pip install -e .

# List available connectors
tapworks --list

# Show connector info (required columns, defaults)
tapworks salesforce --info

# Generate pipelines using settings file
tapworks salesforce --input-config tables.csv --output-dir output --settings settings.json

# Generate pipelines using inline JSON
tapworks sql_server --input-config tables.csv --output-dir output \
  --targets '{"dev": {"workspace_host": "https://..."}}' \
  --default-values '{"project_name": "my_project"}'
```

---

## Quick Start - Unified Notebook

Use `notebook_runner.py` in Databricks for a single notebook entry point:

```python
# Configuration - edit these values
connector_name = "salesforce"
input_source = "main.config.pipeline_tables"  # Delta table or CSV path
output_dir = "/Workspace/Users/you@company.com/dab_output"

targets = {
    "dev": {"workspace_host": "https://dev.cloud.databricks.com"},
    "prod": {"workspace_host": "https://prod.cloud.databricks.com"},
}

default_values = {"project_name": "my_project", "schedule": "0 */6 * * *"}

# Run pipeline generation
from core.runner import run_pipeline_generation

result_df = run_pipeline_generation(
    connector_name=connector_name,
    input_source=input_source,
    output_dir=output_dir,
    targets=targets,
    default_values=default_values,
    spark_session=spark,
)
display(result_df)
```

---

## Configuration Options

| Parameter | Description |
|-----------|-------------|
| `targets` | Target environments (dev, prod) with workspace settings |
| `default_values` | Default values for optional columns - fills missing/empty values (supports group-based) |
| `override_input_config` | Force override values for ALL rows (supports group-based) |
| `max_tables_per_pipeline` | Maximum tables per pipeline (default: 250) |
| `max_tables_per_gateway` | Maximum tables per gateway - database connectors only (default: 250) |

### Row Order and Load Balancing

When a prefix has more tables than `max_tables_per_pipeline` (or `max_tables_per_gateway`), Tapworks splits them into chunks based on their row position in the input config. This has important implications:

- **Row order determines chunk assignment.** The first 250 rows for a prefix go to the first pipeline, the next 250 to the second, etc.
- **Rows don't need to be contiguous.** Tables for the same prefix can be scattered throughout the config — Tapworks collects them by prefix, but preserves their relative order when splitting.
- **Append new tables to the end of their prefix.** Inserting rows in the middle shifts which tables belong to which pipeline. In DABs, a table moving to a different pipeline means the old pipeline is removed and recreated — **this causes data loss**. Always add new tables after the existing rows for that prefix.

---

## Defaults and Overrides

Both `default_values` and `override_input_config` support two formats:

### Simple Format (All Rows)

```python
default_values = {
    'schedule': '0 */6 * * *',
    'pause_status': 'UNPAUSED',
}
```

### Group-Based Format (Per Pipeline Group)

```python
default_values = {
    '*': {'schedule': '0 */6 * * *'},        # Global fallback
    'sales': {'schedule': '*/15 * * * *'},   # All sales pipelines
    'sales_2': {'schedule': '*/30 * * * *'}, # Only sales_2 subgroup
    'hr': {'schedule': '0 0 * * *'},         # HR pipelines
}

override_config = {
    '*': {'pause_status': 'UNPAUSED'},
    'finance': {'pause_status': 'PAUSED'},   # Pause finance for audit
}
```

### Matching Precedence

Config keys are matched in this order (most specific wins):
1. `pipeline_group` (prefix_subgroup) - e.g., `'sales_2'`
2. `prefix` - e.g., `'sales'`
3. `project_name` - e.g., `'my_project'`
4. `'*'` (global fallback)

### Defaults vs Overrides

| Parameter | Behavior |
|-----------|----------|
| `default_values` | Fill missing/empty values only |
| `override_config` | Overwrite all values (ignores CSV) |

See [examples/features/group_based_config](./examples/features/group_based_config) (<a href="$./examples/features/group_based_config">Databricks</a>) for detailed examples.

---

## Settings File Format

```json
{
  "targets": {
    "dev": {
      "workspace_host": "https://dev.cloud.databricks.com",
      "root_path": "/Shared/pipelines/dev"
    },
    "prod": {
      "workspace_host": "https://prod.cloud.databricks.com",
      "root_path": "/Shared/pipelines/prod"
    }
  },
  "default_values": {
    "project_name": "my_project",
    "schedule": "0 */6 * * *"
  },
  "override_input_config": {
    "pause_status": "PAUSED"
  },
  "max_tables_per_pipeline": 250
}
```

---

## Connector Reference

Use `tapworks <connector> --info` to see required columns and defaults for any connector.

### SaaS Connectors

**Salesforce**:
```bash
tapworks salesforce --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`, `pipeline_catalog`, `pipeline_schema`

Optional: `include_columns`, `exclude_columns`, `primary_keys` (comma-separated; supports composite keys)

**Google Analytics 4**:
```bash
tapworks google_analytics --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_catalog`, `source_schema`, `tables`, `target_catalog`, `target_schema`, `connection_name`, `pipeline_catalog`, `pipeline_schema`

**ServiceNow**:
```bash
tapworks servicenow --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`, `pipeline_catalog`, `pipeline_schema`

**Workday Reports**:
```bash
tapworks workday_reports --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_url`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`, `primary_keys`, `pipeline_catalog`, `pipeline_schema`

### Database Connectors

Database connectors support two-level load balancing with gateways.

**SQL Server**:
```bash
tapworks sql_server --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`, `pipeline_catalog`, `pipeline_schema`

Optional: `gateway_catalog`, `gateway_schema`, `gateway_worker_type`, `gateway_driver_type`

**PostgreSQL**:
```bash
tapworks postgresql --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`, `pipeline_catalog`, `pipeline_schema`, `slot_name`, `publication_name`

Optional: `gateway_catalog`, `gateway_schema`, `gateway_worker_type`, `gateway_driver_type`

---

## Programmatic Usage

You can also use connectors directly in Python:

```python
from tapworks.core import get_connector, run_pipeline_generation

# Option 1: Use the unified runner
result = run_pipeline_generation(
    connector_name='salesforce',
    input_source='tables.csv',
    output_dir='output',
    targets={'dev': {'workspace_host': 'https://...'}},
)

# Option 2: Use connector directly
connector = get_connector('salesforce')
result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='output',
    targets={'dev': {'workspace_host': 'https://...'}},
)
```

---

## Example Notebooks

Each connector folder contains an `example_notebook.ipynb`:
- `examples/connectors/salesforce/example_notebook.ipynb`
- `examples/connectors/sql_server/example_notebook.ipynb`
- `examples/connectors/postgresql/example_notebook.ipynb`
- `examples/connectors/google_analytics/example_notebook.ipynb`
- `examples/connectors/servicenow/example_notebook.ipynb`
- `examples/connectors/workday_reports/example_notebook.ipynb`
