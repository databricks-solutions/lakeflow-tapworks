# ServiceNow Connector

Automated pipeline generation for ServiceNow to Databricks ingestion using Lakeflow Connect.

## Quick Start

### Option 1: Interactive Notebook (Recommended)

1. Upload `pipeline_setup.ipynb` to your Databricks workspace
2. Configure parameters in the notebook widgets
3. Run all cells to generate and verify pipeline configuration
4. Deploy using Databricks Asset Bundles

### Option 2: Python Script

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create ServiceNow connection in Databricks

# 3. Prepare your input CSV (see examples/basic/pipeline_config.csv)

# 4. Run the generator
python pipeline_generator.py \
  --input-csv examples/basic/pipeline_config.csv \
  --project-name servicenow_ingestion \
  --workspace-host https://your-workspace.cloud.databricks.com \
  --output-dir dab_deployment

# 5. Deploy
cd dab_deployment/servicenow_ingestion
databricks bundle deploy -t dev
```

## Input CSV Format

Your CSV should include these columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `source_database` | Yes | Always 'SERVICENOW' | `SERVICENOW` |
| `source_schema` | Yes | Schema name | `default` (for standard tables) |
| `source_table_name` | Yes | ServiceNow table name | `incident`, `sys_user`, `cmdb_ci_server` |
| `target_catalog` | Yes | Target catalog | `main` |
| `target_schema` | Yes | Target schema | `servicenow` |
| `target_table_name` | Yes | Target table name | `incident` |
| `prefix` | No | Pipeline grouping prefix | `business_unit1`, `business_unit2` |
| `priority` | No | Priority level (maps to subgroup) | `01`, `02`, `03` |
| `connection_name` | Yes | Connection name | `servicenow_connection` |
| `schedule` | No | Cron schedule | `*/15 * * * *` (default) |
| `include_columns` | No | Columns to include | `"sys_id,sys_updated_on,name"` |
| `exclude_columns` | No | Columns to exclude | `"user_password,last_password"` |
| `scd_type` | No | SCD type | `SCD_TYPE_1` or `SCD_TYPE_2` (default: `SCD_TYPE_2`) |

**Example CSV:**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,prefix,priority,connection_name,schedule,include_columns,exclude_columns,scd_type
SERVICENOW,default,incident,main,servicenow,incident,business_unit1,01,servicenow_connection,*/15 * * * *,,,SCD_TYPE_2
SERVICENOW,default,sys_user,main,servicenow,sys_user,business_unit1,02,servicenow_connection,*/15 * * * *,,"user_password,last_password,phone",SCD_TYPE_2
SERVICENOW,default,cmdb_ci_server,main,servicenow,cmdb_ci_server,business_unit2,01,servicenow_connection,*/30 * * * *,,,SCD_TYPE_2
```

## Key Features

- **No Gateway Required** - Direct OAuth connection to ServiceNow
- **Flexible Grouping** - Organize tables by business unit (prefix) and priority
- **Per-Pipeline Schedules** - Configure individual schedules via CSV
- **Column Filtering** - Include or exclude specific columns per table
- **SCD Type Support** - Choose between SCD Type 1 (overwrite) or Type 2 (change tracking)
- **Standard and Custom Tables** - Support for both ServiceNow standard and custom tables

## Pipeline Grouping

Pipelines are created based on `(prefix, priority)` combinations:

```
prefix=business_unit1, priority=01  → Pipeline: business_unit1_01
prefix=business_unit1, priority=02  → Pipeline: business_unit1_02
prefix=business_unit2, priority=01  → Pipeline: business_unit2_01
```

**Benefits:**
- Separate pipelines by business unit or domain
- Prioritize critical tables
- Apply different schedules per group
- Manage up to 250 tables per pipeline

## SCD Type Configuration

The ServiceNow connector supports two Slowly Changing Dimension types:

- **SCD_TYPE_1**: Overwrites existing data (no history tracking)
  - Best for: Dimension tables, lookup tables, reference data
  - Example: Country codes, product categories

- **SCD_TYPE_2**: Tracks historical changes (maintains history)
  - Best for: Transactional tables, audit tables, time-sensitive data
  - Example: User records, incident records, configuration items
  - Automatically tracks: `_databricks_deleted`, `sys_updated_on`, `sys_created_on`

## Generated Output

The tool generates a Databricks Asset Bundle structure:

```
dab_deployment/{project_name}/
├── databricks.yml           # Bundle configuration
└── resources/
    ├── pipelines.yml        # Pipeline definitions
    └── jobs.yml             # Scheduled jobs
```

## Prerequisites

- Databricks workspace with Unity Catalog
- **ServiceNow connection created**
- Databricks CLI installed and authenticated
- Python 3.8+ with required packages

### Creating ServiceNow Connection

- https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/servicenow-source-setup

## Configuration

The `pipeline_generator.py` script accepts these parameters:

```bash
python pipeline_generator.py \
  --input-csv <path_to_csv> \
  --project-name <project_name> \
  --workspace-host <databricks_workspace_url> \
  [--schedule <cron_schedule>] \
  [--max-tables <max_tables_per_pipeline>] \
  [--output-dir <output_directory>]
```

### Parameters:
- `--input-csv` - Path to input CSV file (required)
- `--project-name` - Project name for the bundle (required)
- `--workspace-host` - Databricks workspace URL (required)
- `--schedule` - Default cron schedule (default: `*/15 * * * *`)
- `--max-tables` - Maximum tables per pipeline (default: 250)
- `--output-dir` - Output directory for DAB project (default: `dab_deployment`)

## Examples

See the `examples/` directory for sample configurations:

- `examples/basic/` - Basic ServiceNow tables with various configurations

## Deployment

```bash
# Navigate to generated DAB project
cd dab_deployment/{project_name}

# Validate the bundle
databricks bundle validate -t dev

# Deploy to workspace
databricks bundle deploy -t dev

# Verify deployment
databricks bundle status -t dev
```

Jobs are created with schedules enabled. To manage job schedules:

```bash
# List jobs
databricks jobs list | grep "Pipeline Scheduler"

# Pause a job if needed
databricks jobs update <job_id> --pause-status PAUSED
```


## Troubleshooting

### Connection Issues
- Ensure ServiceNow connection is active in Databricks UI
- Verify ServiceNow instance URL is correct
- Check OAuth credentials are valid

### Pipeline Failures
- Verify table names exist in ServiceNow
- Check include/exclude columns match actual table schema
- Review pipeline logs in Databricks UI

### Performance Optimization
- Use column filtering (include_columns) to reduce data volume
- Adjust schedules based on data update frequency
- Group tables by update frequency using priority levels

## Project Structure

```
servicenow/
├── README.md
├── requirements.txt
├── connector.py                      # ServiceNow connector class
├── pipeline_generator.py             # CLI tool for pipeline generation
├── pipeline_setup.ipynb              # Interactive notebook (coming soon)
└── examples/
    └── basic/
        └── pipeline_config.csv       # Example configuration
```

## Support

For issues or questions:
1. Check the examples/ directory for reference configurations
2. Review the Databricks Lakeflow Connect documentation
3. Verify your ServiceNow connection is properly configured
4. Check the generated YAML files for any misconfigurations
