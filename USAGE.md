# Usage Guide

This guide shows how to use Lakehouse Tapworks through both command line (CLI) and notebook/programmatic interfaces.

## Quick Start - Unified CLI

The unified CLI (`cli.py`) is the single entry point for all connectors:

```bash
# List available connectors
python cli.py --list

# Show connector info (required columns, defaults)
python cli.py salesforce --info

# Generate pipelines using settings file
python cli.py salesforce --input-config tables.csv --output-dir output --settings settings.json

# Generate pipelines using inline JSON
python cli.py sql_server --input-config tables.csv --output-dir output \
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
| `default_values` | Default values for optional columns (fills missing/empty values) |
| `override_input_config` | Force override values for ALL rows |
| `max_tables_per_pipeline` | Maximum tables per pipeline (default: 250) |
| `max_tables_per_gateway` | Maximum tables per gateway - database connectors only (default: 250) |

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

Use `python cli.py <connector> --info` to see required columns and defaults for any connector.

### SaaS Connectors

**Salesforce**:
```bash
python cli.py salesforce --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`

Optional: `include_columns`, `exclude_columns`, `primary_keys` (comma-separated; supports composite keys)

**Google Analytics 4**:
```bash
python cli.py google_analytics --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`

**ServiceNow**:
```bash
python cli.py servicenow --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`

**Workday Reports**:
```bash
python cli.py workday_reports --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_url`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`, `primary_keys`

### Database Connectors

Database connectors support two-level load balancing with gateways.

**SQL Server**:
```bash
python cli.py sql_server --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`

Optional: `gateway_catalog`, `gateway_schema`, `gateway_worker_type`, `gateway_driver_type`

**PostgreSQL**:
```bash
python cli.py postgresql --input-config tables.csv --output-dir output --settings settings.json
```
Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `connection_name`

Optional: `gateway_catalog`, `gateway_schema`, `gateway_worker_type`, `gateway_driver_type`

---

## Programmatic Usage

You can also use connectors directly in Python:

```python
from core import get_connector, run_pipeline_generation

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
- `salesforce/example_notebook.ipynb`
- `sql_server/example_notebook.ipynb`
- `postgresql/example_notebook.ipynb`
- `google_analytics/example_notebook.ipynb`
- `servicenow/example_notebook.ipynb`
- `workday_reports/example_notebook.ipynb`
