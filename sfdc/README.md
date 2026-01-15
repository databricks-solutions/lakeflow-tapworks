# Salesforce Connector

Automated pipeline generation for Salesforce to Databricks ingestion using Lakeflow Connect.

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

# 2. Create Salesforce connection in Databricks (via UI - OAuth required)

# 3. Prepare your input CSV (see examples/sales/pipeline_config.csv)

# 4. Run the generator
python pipeline_generator.py

# 5. Deploy
cd examples/sales/deployment
databricks bundle deploy -t prod
```

## Input CSV Format

Your CSV should include these columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `source_schema` | No | Schema type | `standard` or `custom` |
| `source_table_name` | Yes | Salesforce object name | `Account`, `Contact`, `MyCustom__c` |
| `target_catalog` | Yes | Target catalog | `sfdc_catalog` |
| `target_schema` | Yes | Target schema | `salesforce` |
| `target_table_name` | Yes | Target table name | `Account` |
| `prefix` | Yes | Pipeline grouping prefix | `sales`, `marketing` |
| `priority` | Yes | Priority level | `01`, `02`, `03` |
| `connection_name` | No | Connection name | `sfdc_connection` |
| `schedule` | No | Cron schedule | `*/15 * * * *` |
| `include_columns` | No | Columns to include | `"Id,Name,Type"` |
| `exclude_columns` | No | Columns to exclude | `"SystemModstamp"` |

**Example CSV:**
```csv
source_schema,source_table_name,target_catalog,target_schema,target_table_name,prefix,priority,connection_name,schedule,include_columns,exclude_columns
standard,Account,sfdc_catalog,salesforce,Account,sales,01,sfdc_connection,*/15 * * * *,"Id,Name,Type",
standard,Contact,sfdc_catalog,salesforce,Contact,sales,01,sfdc_connection,*/15 * * * *,,
standard,Opportunity,sfdc_catalog,salesforce,Opportunity,sales,02,sfdc_connection,*/30 * * * *,,"Description"
```

## Key Features

- **No Gateway Required** - Direct OAuth connection to Salesforce
- **Flexible Grouping** - Organize objects by business unit (prefix) and priority
- **Per-Pipeline Schedules** - Configure individual schedules via CSV
- **Column Filtering** - Include or exclude specific columns per object
- **Standard and Custom Objects** - Support for both standard and custom Salesforce objects

## Pipeline Grouping

Pipelines are created based on `(prefix, priority)` combinations:

```
prefix=sales, priority=01  → Pipeline: sales_01
prefix=sales, priority=02  → Pipeline: sales_02
prefix=marketing, priority=01 → Pipeline: marketing_01
```

**Benefits:**
- Separate pipelines by business unit or domain
- Prioritize critical objects
- Apply different schedules per group

## Generated Output

The tool generates a Databricks Asset Bundle structure:

```
examples/{example_name}/deployment/
├── databricks.yml           # Bundle configuration
└── resources/
    ├── sfdc_pipeline.yml    # Pipeline definitions
    └── jobs.yml             # Scheduled jobs
```

## Prerequisites

- Databricks workspace with Unity Catalog
- **Salesforce connection created via UI** (OAuth 2.0 authentication - cannot be automated)
- Databricks CLI installed and authenticated
- Python 3.8+ with required packages

### Creating Salesforce Connection (Manual)

**Important:** Salesforce connections require OAuth 2.0 and must be created via the Databricks UI.

1. Go to Databricks workspace → **Catalog → Connections**
2. Click **"Create Connection"**
3. Select **Salesforce** as connection type
4. Enter connection name (e.g., `sfdc_connection`)
5. Complete OAuth authorization in browser
6. Verify connection status is "Active"

## Configuration

The `pipeline_generator.py` script accepts these parameters:

- `input_csv` - Path to input CSV file
- `default_connection_name` - Default connection name (default: "sfdc_connection")
- `default_schedule` - Default cron schedule (default: "*/15 * * * *")
- `output_yaml` - Path for generated YAML file
- `output_config` - Path for generated configuration CSV

## Examples

See the `examples/` directory for sample configurations:

- `examples/sales/` - Sales department objects with schedules

## Deployment

```bash
cd examples/{example_name}/deployment
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

Jobs are created in **PAUSED** state. To enable scheduling:

```bash
# List jobs
databricks jobs list | grep sfdc_pipeline

# Unpause job
databricks jobs update <job_id> --schedule-pause-status UNPAUSED
```

## Project Structure

```
sfdc/
├── README.md
├── requirements.txt
├── load_balancing/
│   ├── load_balancer.py           # Pipeline grouping logic
│   └── examples/
│       └── sfdc_config.csv        # Example input
├── deployment/
│   └── connector_settings_generator.py  # YAML generation
├── examples/
│   └── sales/                     # Example
│       ├── pipeline_config.csv
│       └── deployment/
│           ├── databricks.yml
│           └── resources/
│               ├── sfdc_pipeline.yml
│               └── jobs.yml
├── pipeline_generator.py          # Unified workflow
└── pipeline_setup.ipynb           # Interactive notebook
```

## Support

For detailed information, see the comprehensive documentation in `_legacy/README.md`.
