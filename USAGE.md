# Usage Guide

This guide shows how to use Lakehouse Tapworks through both command line (CLI) and notebook/programmatic interfaces.

## Configuration Methods

Both CLI and programmatic interfaces support the same configuration options:

| Parameter | Description |
|-----------|-------------|
| `targets` | Target environments (dev, prod) with workspace settings |
| `default_values` | Default values for optional CSV columns |
| `override_input_config` | Force override values (ignores CSV) |
| `max_tables_per_pipeline` | Maximum tables per pipeline (default: 250) |
| `max_tables_per_gateway` | Maximum tables per gateway - database connectors only (default: 250) |

---

## SaaS Connectors

### Salesforce

**CLI - Using config file:**
```bash
python salesforce/pipeline_generator.py \
  --input-csv salesforce/examples/basic/pipeline_config.csv \
  --config config.json
```

**CLI - Using inline JSON:**
```bash
python salesforce/pipeline_generator.py \
  --input-csv salesforce/examples/basic/pipeline_config.csv \
  --targets '{"dev": {"workspace_host": "https://my-workspace.cloud.databricks.com"}}' \
  --default-values '{"project_name": "sfdc_ingestion", "schedule": "*/30 * * * *"}'
```

**CLI - Legacy arguments:**
```bash
python salesforce/pipeline_generator.py \
  --input-csv salesforce/examples/basic/pipeline_config.csv \
  --project-name sfdc_ingestion \
  --workspace-host https://my-workspace.cloud.databricks.com \
  --schedule "*/30 * * * *" \
  --output-dir dab_deployment
```

**Notebook/Programmatic:**
```python
from salesforce.connector import SalesforceConnector
from utilities import load_input_csv

connector = SalesforceConnector()
df = load_input_csv('salesforce/examples/basic/pipeline_config.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='dab_deployment',
    targets={
        'dev': {'workspace_host': 'https://my-workspace.cloud.databricks.com'},
        'prod': {'workspace_host': 'https://prod-workspace.cloud.databricks.com'}
    },
    default_values={
        'project_name': 'sfdc_ingestion',
        'schedule': '*/30 * * * *'
    },
    override_input_config=None,  # Optional: force override values
    max_tables_per_pipeline=250
)
```

---

### Google Analytics 4

**CLI - Using config file:**
```bash
python google_analytics/pipeline_generator.py \
  --input-csv google_analytics/examples/basic/pipeline_config.csv \
  --config config.json
```

**CLI - Using inline JSON:**
```bash
python google_analytics/pipeline_generator.py \
  --input-csv google_analytics/examples/basic/pipeline_config.csv \
  --targets '{"dev": {"workspace_host": "https://my-workspace.cloud.databricks.com"}}' \
  --default-values '{"project_name": "ga4_ingestion", "schedule": "0 */6 * * *"}'
```

**CLI - Legacy arguments:**
```bash
python google_analytics/pipeline_generator.py \
  --input-csv google_analytics/examples/basic/pipeline_config.csv \
  --project-name ga4_ingestion \
  --workspace-host https://my-workspace.cloud.databricks.com \
  --schedule "0 */6 * * *" \
  --output-dir dab_project
```

**Notebook/Programmatic:**
```python
from google_analytics.connector import GoogleAnalyticsConnector
from utilities import load_input_csv

connector = GoogleAnalyticsConnector()
df = load_input_csv('google_analytics/examples/basic/pipeline_config.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='dab_project',
    targets={
        'dev': {'workspace_host': 'https://my-workspace.cloud.databricks.com'}
    },
    default_values={
        'project_name': 'ga4_ingestion',
        'schedule': '0 */6 * * *'
    },
    max_tables_per_pipeline=250
)
```

---

### ServiceNow

**CLI - Using config file:**
```bash
python servicenow/pipeline_generator.py \
  --input-csv servicenow/examples/basic/pipeline_config.csv \
  --config config.json
```

**CLI - Using inline JSON:**
```bash
python servicenow/pipeline_generator.py \
  --input-csv servicenow/examples/basic/pipeline_config.csv \
  --targets '{"dev": {"workspace_host": "https://my-workspace.cloud.databricks.com"}}' \
  --default-values '{"project_name": "snow_ingestion", "schedule": "*/15 * * * *"}'
```

**CLI - Legacy arguments:**
```bash
python servicenow/pipeline_generator.py \
  --input-csv servicenow/examples/basic/pipeline_config.csv \
  --project-name snow_ingestion \
  --workspace-host https://my-workspace.cloud.databricks.com \
  --output-dir dab_deployment
```

**Notebook/Programmatic:**
```python
from servicenow.connector import ServiceNowConnector
from utilities import load_input_csv

connector = ServiceNowConnector()
df = load_input_csv('servicenow/examples/basic/pipeline_config.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='dab_deployment',
    targets={
        'dev': {'workspace_host': 'https://my-workspace.cloud.databricks.com'}
    },
    default_values={
        'project_name': 'snow_ingestion',
        'schedule': '*/15 * * * *',
        'scd_type': 'SCD_TYPE_2'  # Optional: set default SCD type
    },
    max_tables_per_pipeline=250
)
```

---

### Workday Reports

**CLI - Using config file:**
```bash
python workday_reports/pipeline_generator.py \
  --input-csv workday_reports/examples/basic/pipeline_config.csv \
  --config config.json
```

**CLI - Using inline JSON:**
```bash
python workday_reports/pipeline_generator.py \
  --input-csv workday_reports/examples/basic/pipeline_config.csv \
  --targets '{"dev": {"workspace_host": "https://my-workspace.cloud.databricks.com"}}' \
  --default-values '{"project_name": "workday_ingestion", "schedule": "0 */6 * * *"}'
```

**CLI - Legacy arguments:**
```bash
python workday_reports/pipeline_generator.py \
  --input-csv workday_reports/examples/basic/pipeline_config.csv \
  --project-name workday_ingestion \
  --workspace-host https://my-workspace.cloud.databricks.com \
  --output-dir dab_deployment
```

**Notebook/Programmatic:**
```python
from workday_reports.connector import WorkdayReportsConnector
from utilities import load_input_csv

connector = WorkdayReportsConnector()
df = load_input_csv('workday_reports/examples/basic/pipeline_config.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='dab_deployment',
    targets={
        'dev': {'workspace_host': 'https://my-workspace.cloud.databricks.com'}
    },
    default_values={
        'project_name': 'workday_ingestion',
        'schedule': '0 */6 * * *'
    },
    max_tables_per_pipeline=250
)
```

---

## Database Connectors

Database connectors support two-level load balancing with gateways and pipelines.

### SQL Server

**CLI - Using config file:**
```bash
python sqlserver/pipeline_generator.py \
  --input-csv sqlserver/examples/basic/pipeline_config.csv \
  --config config.json
```

**CLI - Using inline JSON:**
```bash
python sqlserver/pipeline_generator.py \
  --input-csv sqlserver/examples/basic/pipeline_config.csv \
  --targets '{"dev": {"workspace_host": "https://my-workspace.cloud.databricks.com", "root_path": "/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}"}}' \
  --default-values '{"project_name": "sqlserver_ingestion", "gateway_worker_type": "m5d.large"}'
```

**CLI - Legacy arguments:**
```bash
python sqlserver/pipeline_generator.py \
  --input-csv sqlserver/examples/basic/pipeline_config.csv \
  --project-name sqlserver_ingestion \
  --workspace-host https://my-workspace.cloud.databricks.com \
  --root-path '/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}' \
  --worker-type m5d.large \
  --driver-type c5a.8xlarge \
  --max-tables-gateway 500 \
  --max-tables-pipeline 250 \
  --output-dir dab_project
```

**Notebook/Programmatic:**
```python
from sqlserver.connector import SQLServerConnector
from utilities import load_input_csv

connector = SQLServerConnector()
df = load_input_csv('sqlserver/examples/basic/pipeline_config.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='dab_project',
    targets={
        'dev': {
            'workspace_host': 'https://my-workspace.cloud.databricks.com',
            'root_path': '/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}'
        },
        'prod': {
            'workspace_host': 'https://prod-workspace.cloud.databricks.com',
            'root_path': '/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}'
        }
    },
    default_values={
        'project_name': 'sqlserver_ingestion',
        'schedule': '*/15 * * * *',
        'gateway_worker_type': 'm5d.large',
        'gateway_driver_type': 'c5a.8xlarge'
    },
    max_tables_per_gateway=500,
    max_tables_per_pipeline=250
)
```

---

### PostgreSQL

**CLI - Using config file:**
```bash
python postgres/pipeline_generator.py \
  --input-csv postgres/examples/basic/pipeline_config.csv \
  --config config.json
```

**CLI - Using inline JSON:**
```bash
python postgres/pipeline_generator.py \
  --input-csv postgres/examples/basic/pipeline_config.csv \
  --targets '{"dev": {"workspace_host": "https://my-workspace.cloud.databricks.com", "root_path": "/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}"}}' \
  --default-values '{"project_name": "postgres_ingestion", "gateway_worker_type": "m6d.xlarge"}'
```

**CLI - Legacy arguments:**
```bash
python postgres/pipeline_generator.py \
  --input-csv postgres/examples/basic/pipeline_config.csv \
  --project-name postgres_ingestion \
  --workspace-host https://my-workspace.cloud.databricks.com \
  --root-path '/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}' \
  --worker-type m6d.xlarge \
  --driver-type m6d.xlarge \
  --max-tables-gateway 500 \
  --max-tables-pipeline 250 \
  --output-dir dab_project
```

**Notebook/Programmatic:**
```python
from postgres.connector import PostgreSQLConnector
from utilities import load_input_csv

connector = PostgreSQLConnector()
df = load_input_csv('postgres/examples/basic/pipeline_config.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='dab_project',
    targets={
        'dev': {
            'workspace_host': 'https://my-workspace.cloud.databricks.com',
            'root_path': '/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}'
        }
    },
    default_values={
        'project_name': 'postgres_ingestion',
        'schedule': '*/15 * * * *',
        'gateway_worker_type': 'm6d.xlarge',
        'gateway_driver_type': 'm6d.xlarge'
    },
    max_tables_per_gateway=500,
    max_tables_per_pipeline=250
)
```

---

## Config File Format

Config files can be JSON or YAML format.

**JSON Example (`config.json`):**
```json
{
  "targets": {
    "dev": {
      "workspace_host": "https://dev-workspace.cloud.databricks.com",
      "root_path": "/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}"
    },
    "prod": {
      "workspace_host": "https://prod-workspace.cloud.databricks.com",
      "root_path": "/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}"
    }
  },
  "default_values": {
    "project_name": "my_ingestion",
    "schedule": "*/15 * * * *",
    "gateway_worker_type": "m5d.large",
    "gateway_driver_type": "c5a.8xlarge"
  },
  "override_input_config": {
    "schedule": null
  },
  "max_tables_per_gateway": 500,
  "max_tables_per_pipeline": 250
}
```

**YAML Example (`config.yaml`):**
```yaml
targets:
  dev:
    workspace_host: https://dev-workspace.cloud.databricks.com
    root_path: /Users/user@company.com/.bundle/${bundle.name}/${bundle.target}
  prod:
    workspace_host: https://prod-workspace.cloud.databricks.com
    root_path: /Users/user@company.com/.bundle/${bundle.name}/${bundle.target}

default_values:
  project_name: my_ingestion
  schedule: "*/15 * * * *"
  gateway_worker_type: m5d.large
  gateway_driver_type: c5a.8xlarge

override_input_config:
  schedule: null

max_tables_per_gateway: 500
max_tables_per_pipeline: 250
```

---

## Configuration Precedence

When using CLI, configuration is merged with the following precedence (highest to lowest):

1. **Inline JSON arguments** (`--targets`, `--default-values`, `--override`)
2. **Config file** (`--config`)
3. **Legacy CLI arguments** (`--project-name`, `--workspace-host`, etc.)

This allows you to use a base config file and override specific values:

```bash
python salesforce/pipeline_generator.py \
  --input-csv data.csv \
  --config base_config.json \
  --override '{"schedule": "0 0 * * *"}'  # Override just the schedule
```

---

## Advanced: Override vs Default Values

- **`default_values`**: Used when CSV column is missing or empty
- **`override_input_config`**: Always applied, ignoring CSV values

**Example:**
```python
# CSV has schedule="*/15 * * * *" for some rows

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='output',
    targets={'dev': {'workspace_host': '...'}},

    # This sets schedule for rows where it's missing
    default_values={'schedule': '0 */6 * * *'},

    # This forces ALL rows to use this schedule (ignores CSV)
    override_input_config={'schedule': '0 0 * * *'}
)
```

---

## Verbose Mode

Add `--verbose` or `-v` to see detailed logging:

```bash
python salesforce/pipeline_generator.py \
  --input-csv data.csv \
  --config config.json \
  --verbose
```
