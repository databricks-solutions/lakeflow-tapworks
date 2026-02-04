# Workday Reports Connector

Automated pipeline generation for Workday Reports to Databricks ingestion using Lakeflow Connect.

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

# 2. Create Workday connection in Databricks

# 3. Prepare your input CSV (see examples/basic/pipeline_config.csv)

# 4. Run the generator
python pipeline_generator.py \
  --input-csv examples/basic/pipeline_config.csv \
  --project-name workday_reports_ingestion \
  --workspace-host https://your-workspace.cloud.databricks.com \
  --output-dir dab_deployment

# 5. Deploy
cd dab_deployment/workday_reports_ingestion
databricks bundle deploy -t dev
```

## Input CSV Format

Your CSV should include these columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `source_url` | Yes | Workday report URL | `https://wd2-impl...?format=json` |
| `target_catalog` | Yes | Target catalog | `main` |
| `target_schema` | Yes | Target schema | `workday` |
| `target_table_name` | Yes | Destination table name | `all_active_employees` |
| `connection_name` | Yes | Connection name | `workday_connection` |
| `primary_keys` | Yes | Comma-separated primary keys | `Employee_ID` or `Worker_ID,Effective_Date` |
| `prefix` | No | Pipeline grouping prefix | `hr`, `finance`, `recruiting` |
| `priority` | No | Priority level (maps to subgroup) | `01`, `02`, `03` |
| `schedule` | No | Cron schedule | `0 */6 * * *` (default: every 6 hours) |
| `include_columns` | No | Columns to include | `"Employee_ID,Name,Department"` |
| `exclude_columns` | No | Columns to exclude | `"Salary_Amount,Bonus_Amount"` |
| `scd_type` | No | SCD type | `SCD_TYPE_1` (default) or `SCD_TYPE_2` |

**Example CSV:**
```csv
source_url,target_catalog,target_schema,target_table_name,prefix,priority,connection_name,schedule,primary_keys,exclude_columns,scd_type
https://wd2-impl-services1.workday.com/ccx/service/.../All_Active_Employees_Data?format=json,main,workday,all_active_employees,hr,01,workday_connection,0 */6 * * *,Employee_ID,,SCD_TYPE_2
https://wd2-impl-services1.workday.com/ccx/service/.../Worker_Profile_Data?format=json,main,workday,worker_profile,hr,01,workday_connection,0 */6 * * *,Worker_ID,,SCD_TYPE_2
https://wd2-impl-services1.workday.com/ccx/service/.../Cost_Center_Data?format=json,main,workday,cost_centers,finance,01,workday_connection,0 0 * * *,Cost_Center_ID,,SCD_TYPE_1
```

## Key Features

- **No Gateway Required** - Direct OAuth connection to Workday
- **Flexible Grouping** - Organize reports by business unit (prefix) and priority
- **Per-Pipeline Schedules** - Configure individual schedules via CSV
- **Column Filtering** - Include or exclude specific columns per report
- **SCD Type Support** - Choose between SCD Type 1 (overwrite) or Type 2 (change tracking)
- **Primary Key Configuration** - Required for all Workday reports
- **Multi-column Primary Keys** - Support for composite keys

## Pipeline Grouping

Pipelines are created based on `(prefix, priority)` combinations:

```
prefix=hr, priority=01        → Pipeline: hr_01
prefix=hr, priority=02        → Pipeline: hr_02
prefix=finance, priority=01   → Pipeline: finance_01
```

**Benefits:**
- Separate pipelines by business unit or domain
- Prioritize critical reports
- Apply different schedules per group
- Manage up to 250 reports per pipeline

## Primary Keys

**Important:** All Workday reports require primary keys for proper data management.

### Single Primary Key
```csv
primary_keys
Employee_ID
```

### Composite Primary Key (Multiple Columns)
```csv
primary_keys
"Employee_ID,Effective_Date"
```

Common primary keys for Workday reports:
- **Employee reports**: `Employee_ID` or `Worker_ID`
- **Time-based reports**: `Worker_ID,Effective_Date` or `Employee_ID,Transaction_Date`
- **Organization reports**: `Organization_ID`
- **Requisition reports**: `Requisition_ID`

## SCD Type Configuration

The Workday Reports connector supports two Slowly Changing Dimension types:

- **SCD_TYPE_1**: Overwrites existing data (no history tracking) - **DEFAULT**
  - Best for: Dimension tables, lookup tables, reference data
  - Example: Organization hierarchy, cost centers, location data

- **SCD_TYPE_2**: Tracks historical changes (maintains history)
  - Best for: Transactional data, employee records, time-sensitive data
  - Example: Employee data, compensation history, job changes
  - Tracks changes using primary keys and timestamps

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
- **Workday connection created** 
- Databricks CLI installed and authenticated
- Python 3.8+ with required packages

### Creating Workday Connection
- https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/workday-reports-source-setup

## Configuration

The `pipeline_generator.py` script accepts these parameters:

```bash
python pipeline_generator.py \
  --input-csv <path_to_csv> \
  --project-name <project_name> \
  --workspace-host <databricks_workspace_url> \
  [--schedule <cron_schedule>] \
  [--max-reports <max_reports_per_pipeline>] \
  [--output-dir <output_directory>]
```

### Parameters:
- `--input-csv` - Path to input CSV file (required)
- `--project-name` - Project name for the bundle (required)
- `--workspace-host` - Databricks workspace URL (required)
- `--schedule` - Default cron schedule (default: `0 */6 * * *` - every 6 hours)
- `--max-reports` - Maximum reports per pipeline (default: 250)
- `--output-dir` - Output directory for DAB project (default: `dab_deployment`)

## Examples

See the `examples/` directory for sample configurations:

- `examples/basic/` - Basic Workday reports with various configurations

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
databricks jobs list | grep "Workday Reports Pipeline Scheduler"

# Pause a job if needed
databricks jobs update <job_id> --pause-status PAUSED
```

## Troubleshooting

### Connection Issues
- Ensure Workday connection is active in Databricks UI
- Verify connection name matches CSV configuration
- Check OAuth credentials are valid

### Pipeline Failures
- Verify report URLs are accessible
- Check primary_keys are correctly specified
- Ensure report format is JSON (`?format=json` in URL)
- Review pipeline logs in Databricks UI

### Primary Key Issues
- Every report must have at least one primary key
- For composite keys, use comma-separated values: `"Key1,Key2"`
- Primary keys must exist in the report data

### Performance Optimization
- Use column filtering (include_columns) to reduce data volume
- Adjust schedules based on data update frequency
- Group reports by update frequency using priority levels
- Consider SCD_TYPE_1 for dimension tables (faster)

## Report URL Format

Workday report URLs follow this pattern:
```
https://[instance].workday.com/ccx/service/customreport2/[tenant]/[ReportName]?format=json
```

**Example:**
```
https://wd2-impl-services1.workday.com/ccx/service/customreport2/acme/All_Active_Employees_Data?format=json
```

**URL Components:**
- `instance`: Your Workday instance (e.g., `wd2-impl-services1`)
- `tenant`: Your Workday tenant name (e.g., `acme`)
- `ReportName`: The custom report name (e.g., `All_Active_Employees_Data`)
- `format=json`: Required parameter for JSON output

## Project Structure

```
workday_reports/
├── README.md
├── requirements.txt
├── connector.py                      # Workday Reports connector class
├── pipeline_generator.py             # CLI tool for pipeline generation
├── pipeline_setup.ipynb              # Interactive notebook
└── examples/
    └── basic/
        └── pipeline_config.csv       # Example configuration
```

## Support

For issues or questions:
1. Check the examples/ directory for reference configurations
2. Review the [Databricks Lakeflow Connect documentation](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/workday-reports-pipeline.html)
3. Verify your Workday connection is properly configured
4. Check the generated YAML files for any misconfigurations
