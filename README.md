# Lakehouse Tapworks

Automated pipeline generation toolkit for Databricks Lakeflow Connect ingestion.

## Overview

Lakehouse Tapworks generates Lakeflow connect ingestion pipelines from user defined configurations. It handles load balancing and produces deployment-ready Databricks Asset Bundle (DAB) files.

**Supported Connectors:**
- **SQL Server** - Database connector with gateway support
- **Salesforce** - SaaS connector for Salesforce objects
- **Google Analytics 4** - SaaS connector via BigQuery integration

**Key principle:** One project = One DAB package = One deployment unit

### Prefix + Subgroup Grouping

Tables are organized using **prefix** and **subgroup** to control pipeline grouping:

```csv
source_table_name,prefix,subgroup
customers,sales,01
orders,sales,01
invoices,finance,02
```

- **prefix**: Logical grouping identifier (e.g., 'sales', 'finance', 'marketing')
- **subgroup**: subgroups, one groups might break into smaller ones ('01', '02', '03', etc.)

**Grouping behavior:**
- Tables with the same `(prefix, subgroup)` combination are grouped together
- If prefix is missing/empty, defaults to `project_name`
- If subgroup is missing/empty, defaults to `'01'`

**Example grouping:**

| Prefix | Subgroup | Result Group | Execution Order |
|--------|----------|--------------|-----------------|
| sales  | 01       | sales_01     | 1st             |
| sales  | 02       | sales_02     | 2nd             |
| finance| 01       | finance_01   | 1st (parallel)  |

### Load Balancing

The toolkit automatically splits tables into multiple pipelines based on prefix, subgroup and max number of tables specified for diferent components:

**SaaS Connectors (Salesforce, GA4):**
- Single-level splitting: `prefix_subgroup` → pipelines
- Splits when group exceeds `max_tables_per_pipeline` (default: 250)
- Creates: `sales_01_g01`, `sales_01_g02`, etc.

**Database Connectors (SQL Server):**
- Two-level splitting: `prefix_subgroup` → gateways → pipelines
- First splits into gateways at `max_tables_per_gateway` (default: 250)
- Then splits each gateway into pipelines at `max_tables_per_pipeline` (default: 250)
- Creates: `sales_01_gw01_g01`, `sales_01_gw01_g02`, `sales_01_gw02_g01`, etc.

**Example:** 600 tables with prefix='sales', subgroup='01':
```
Database connector:
  sales_01 → sales_01_gw01 (250 tables) → sales_01_gw01_g01 (250 tables)
          → sales_01_gw02 (250 tables) → sales_01_gw02_g01 (250 tables)
          → sales_01_gw03 (100 tables) → sales_01_gw03_g01 (100 tables)

SaaS connector:
  sales_01 → sales_01_g01 (250 tables)
          → sales_01_g02 (250 tables)
          → sales_01_g03 (100 tables)
```

## Quick Start

```bash
# 1. Navigate to connector directory
cd sqlserver  # or salesforce, google_analytics

# 2. Prepare your input CSV with tables to ingest
# See "Input CSV Format" section below

# 3. Run the pipeline generator
python pipeline_generator.py \
  --input-csv my_tables.csv \
  --project-name my_project \
  --workspace-host https://my-workspace.cloud.databricks.com \
  --root-path '/Users/me/.bundle/${bundle.name}/${bundle.target}'

# 4. Deploy to Databricks
cd dab_project/my_project
databricks bundle deploy -t dev
```

## Core Concepts

### Project Organization

Each unique `project_name` creates a separate Databricks Asset Bundle (DAB) package:

```
output/
├── project_a/
│   ├── databricks.yml          # Root DAB configuration
│   └── resources/
│       ├── gateways.yml        # Gateway definitions (database connectors only)
│       ├── pipelines.yml       # Pipeline definitions
│       └── jobs.yml            # Scheduled jobs
└── project_b/
    ├── databricks.yml
    └── resources/
        └── ...
```


## Configuration Hierarchy

Configuration values are applied in this order (later values override earlier ones):

```
1. Configuration column values (per row)
   ↓
2. default_values parameter (function argument)
   ↓
3. override_input_config parameter (function argument)
```

**Example:**

```python
# CSV has: project_name='csv_project', prefix='sales'
# Built-in default: project_name='sqlserver_ingestion'

run_complete_pipeline_generation(
    df=input_df,
    default_values={'project_name': 'default_project'},  # replaces config empty/missing values
    override_input_config={'prefix': 'marketing'}         # Overrides default or config values
)

# Result: project_name='csv_project' (CSV wins over default_values)
#         prefix='marketing' (override_input_config wins over everything)
```

**Common patterns:**

1. **Set project-wide defaults:**
   ```python
   default_values={
       'project_name': 'my_project',
       'gateway_worker_type': 'm5d.large'
   }
   ```

2. **Force all rows to use same value:**
   ```python
   override_input_config={
       'connection_name': 'prod_sql_server'
   }
   ```

3. **Mix CSV and defaults:**
   ```python
   # CSV has connection_name for some rows, empty for others
   default_values={'connection_name': 'default_connection'}
   # Empty rows get default, populated rows keep CSV value
   ```

## Input CSV Format

### SQL Server

**Required columns:**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name
salesdb,dbo,customers,bronze,sales,customers,sql_server_prod
```

**Optional columns:**
- `project_name`: Project identifier (default: 'sqlserver_ingestion')
- `prefix`: Grouping prefix (default: project_name)
- `subgroup`: Execution subgroup (default: '01')
- `gateway_catalog`: Gateway storage catalog (default: target_catalog)
- `gateway_schema`: Gateway storage schema (default: target_schema)
- `gateway_worker_type`: Worker node type (e.g., 'm5d.large', default: None)
- `gateway_driver_type`: Driver node type (e.g., 'c5a.8xlarge', default: None)
- `schedule`: Cron schedule (e.g., '0 0 * * *', default: None)

### Salesforce

**Required columns:**
```csv
source_object,target_catalog,target_schema,target_table_name,connection_name
Account,bronze,salesforce,accounts,salesforce_prod
```

**Optional columns:**
- `project_name`: Project identifier (default: 'salesforce_ingestion')
- `prefix`: Grouping prefix (default: project_name)
- `subgroup`: Execution subgroup (default: '01')
- `schedule`: Cron schedule (default: None)

### Google Analytics 4

**Required columns:**
```csv
source_table,target_catalog,target_schema,target_table_name,connection_name
events_20240101,bronze,ga4,events_20240101,ga4_bigquery_prod
```

**Optional columns:**
- `project_name`: Project identifier (default: 'ga4_ingestion')
- `prefix`: Grouping prefix (default: project_name)
- `subgroup`: Execution subgroup (default: '01')
- `schedule`: Cron schedule (default: None)

## Usage Examples

### Example 1: Basic SQL Server Ingestion

```python
from utilities import load_input_csv
from sqlserver.pipeline_generator import run_complete_pipeline_generation

# Load CSV
input_df = load_input_csv('my_tables.csv')

# Generate pipelines
run_complete_pipeline_generation(
    df=input_df,
    output_dir='output',
    targets={
        'dev': {
            'workspace_host': 'https://dev.databricks.com',
            'root_path': '/Users/me/.bundle/${bundle.name}/${bundle.target}'
        }
    },
    default_values={'project_name': 'sales_ingestion'}
)
```

**CSV content:**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name
salesdb,dbo,customers,bronze,sales,customers,sql_prod
salesdb,dbo,orders,bronze,sales,orders,sql_prod
```

**Result:** Creates `output/sales_ingestion/` with one gateway and one pipeline for 2 tables.

### Example 2: Multiple Projects in One CSV

```python
# CSV has project_name column with 'sales' and 'finance' values
run_complete_pipeline_generation(
    df=input_df,
    output_dir='output',
    targets={
        'prod': {
            'workspace_host': 'https://prod.databricks.com',
            'root_path': '/Workspace/prod/${bundle.name}'
        }
    }
)
```

**Result:** Creates two separate DAB packages:
- `output/sales/databricks.yml`
- `output/finance/databricks.yml`

### Example 3: Priority-Based Execution

```python
# CSV with subgroup column
```
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name,subgroup
salesdb,dbo,customers,bronze,sales,customers,sql_prod,01
salesdb,dbo,orders,bronze,sales,orders,sql_prod,01
salesdb,dbo,invoices,bronze,sales,invoices,sql_prod,02
```

```python
run_complete_pipeline_generation(
    df=input_df,
    output_dir='output',
    targets={'dev': {...}},
    default_values={'project_name': 'sales'}
)
```

**Result:** Two pipeline groups:
- `sales_01`: customers, orders (runs first)
- `sales_02`: invoices (runs after sales_01 completes)

### Example 4: Custom Gateway Configuration

```python
# SQL Server with specific node types
run_complete_pipeline_generation(
    df=input_df,
    output_dir='output',
    targets={'prod': {...}},
    default_values={
        'project_name': 'high_performance',
        'gateway_worker_type': 'm5d.xlarge',
        'gateway_driver_type': 'c5a.4xlarge'
    }
)
```

**Result:** All gateways use m5d.xlarge workers and c5a.4xlarge drivers. If CSV has `gateway_worker_type` or `gateway_driver_type` columns with values, those override the defaults per row.

### Example 5: Multiple Environments

```python
run_complete_pipeline_generation(
    df=input_df,
    output_dir='output',
    targets={
        'dev': {
            'workspace_host': 'https://dev.databricks.com',
            'root_path': '/Users/dev/.bundle/${bundle.name}/${bundle.target}'
        },
        'staging': {
            'workspace_host': 'https://staging.databricks.com',
            'root_path': '/Users/staging/.bundle/${bundle.name}/${bundle.target}'
        },
        'prod': {
            'workspace_host': 'https://prod.databricks.com',
            'root_path': '/Workspace/prod/${bundle.name}'
        }
    }
)
```

**Usage:**
```bash
# Deploy to dev
cd output/my_project
databricks bundle deploy -t dev

# Deploy to prod
databricks bundle deploy -t prod
```

### Example 6: Override All Connection Names

```python
# CSV has mixed or missing connection_name values
run_complete_pipeline_generation(
    df=input_df,
    output_dir='output',
    targets={'prod': {...}},
    override_input_config={
        'connection_name': 'prod_sql_server'
    }
)
```

**Result:** Every row uses 'prod_sql_server' regardless of CSV values.

## Load Balancing Configuration

Control how tables are split into pipelines and gateways:

```python
run_complete_pipeline_generation(
    df=input_df,
    output_dir='output',
    targets={'dev': {...}},
    max_tables_per_gateway=500,    # SQL Server only (default: 250)
    max_tables_per_pipeline=100,   # All connectors (default: 250)
)
```

**Guidelines:**
- **Databricks limit**: 500 tables per pipeline maximum
- **Recommended**: 250 tables per pipeline for optimal performance
- **Gateway limit**: 250 tables recommended for SQL Server gateways
- **Large datasets**: Lower values (50-100) for better parallelism

**Example:** 1000 tables with `max_tables_per_gateway=300`, `max_tables_per_pipeline=100`:
```
1000 tables → 4 gateways (300+300+300+100)
Each gateway → 3-4 pipelines (100 tables each)
Total: 4 gateways, 10 pipelines
```

## Output Structure

After running the generator:

```
output/
└── {project_name}/
    ├── databricks.yml              # Root DAB configuration
    │                                # - Project metadata
    │                                # - Target environments (dev, prod, etc.)
    │                                # - Resource includes
    └── resources/
        ├── gateways.yml            # Gateway definitions (SQL Server only)
        │                           # - Connection settings
        │                           # - Cluster configuration
        │                           # - Storage locations
        ├── pipelines.yml           # Pipeline definitions
        │                           # - Table mappings
        │                           # - Ingestion settings
        │                           # - Target catalogs/schemas
        └── jobs.yml                # Scheduled jobs
                                    # - Cron schedules
                                    # - Pipeline triggers
```

## Deployment

### Deploy to Databricks

```bash
cd output/{project_name}

# Validate bundle
databricks bundle validate -t dev

# Deploy to development
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

### Update Existing Deployment

```bash
# Regenerate with new CSV
python pipeline_generator.py --input-csv updated_tables.csv ...

# Deploy updates
cd output/{project_name}
databricks bundle deploy -t prod
```

### Monitor Pipelines

```bash
# List deployed resources
databricks bundle resources list -t prod

# View pipeline runs
databricks pipelines list

# Check job status
databricks jobs list
```

## Advanced Configuration

### Per-Table Custom Settings

Specify unique settings for each table in the CSV:

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name,prefix,subgroup,gateway_worker_type
salesdb,dbo,big_table,bronze,sales,big_table,sql_prod,sales,01,m5d.2xlarge
salesdb,dbo,small_table,bronze,sales,small_table,sql_prod,sales,01,m5d.large
```

**Result:** `big_table` gets its own gateway with m5d.2xlarge workers, `small_table` gets m5d.large.

### Mixed Gateway Configurations

Combine specified and default values:

```python
# CSV has gateway_worker_type for some rows, empty for others
default_values={'gateway_worker_type': 'm5d.large'}

# Result:
# - Rows with gateway_worker_type in CSV: use CSV value
# - Rows with empty gateway_worker_type: use m5d.large
# - Rows with gateway_worker_type=None in CSV: serverless (no cluster)
```

### Programmatic Usage

Import functions directly in your scripts:

```python
from utilities import load_input_csv, process_input_config
from utilities.load_balancing import generate_database_pipeline_config
from sqlserver.deployment.connector_settings_generator import generate_yaml_files

# Load and normalize
df = load_input_csv('input.csv')
normalized_df = process_input_config(
    df=df,
    required_columns=['source_database', 'source_schema', ...],
    default_values={'project_name': 'my_project'}
)

# Custom load balancing
pipeline_df = generate_database_pipeline_config(
    df=normalized_df,
    max_tables_per_gateway=300,
    max_tables_per_pipeline=150
)

# Generate YAML
generate_yaml_files(
    df=pipeline_df,
    output_dir='custom_output',
    targets={'prod': {...}}
)
```

## Architecture

### Two-Stage Process

```
Stage 1: Load Balancing
  Input CSV → Normalization → Grouping (prefix+subgroup) → Splitting (capacity-based) → Pipeline Config

Stage 2: YAML Generation
  Pipeline Config → Gateway YAML + Pipeline YAML + Jobs YAML → DAB Package
```

### Connector Patterns

**SaaS Connectors (Salesforce, GA4):**
- Direct ingestion (no gateway)
- Single-level grouping: prefix+subgroup → pipelines
- OAuth/service account authentication

**Database Connectors (SQL Server):**
- Gateway-based ingestion
- Two-level grouping: prefix+subgroup → gateways → pipelines
- SQL connection via Databricks connection

### Design Principles

1. **Configuration over Code**: CSV-driven, minimal code changes
2. **Idempotent**: Re-running with same input produces same output
3. **Composable**: Functions can be used independently
4. **Extensible**: Easy to add new connectors following shared patterns
5. **Safe**: Validates inputs, provides clear error messages

## Common Patterns

### Pattern 1: Single Large Database

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name,project_name
salesdb,dbo,table001,bronze,sales,table001,sql_prod,sales
salesdb,dbo,table002,bronze,sales,table002,sql_prod,sales
... (1000+ tables)
```

**Behavior:** Automatically splits into multiple gateways and pipelines, all under `sales` project.

### Pattern 2: Multiple Databases, Same Project

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name,project_name,prefix
salesdb,dbo,customers,bronze,sales,customers,sql_prod,company_data,sales
financedb,dbo,invoices,bronze,finance,invoices,sql_prod,company_data,finance
```

**Behavior:** Single project with two groups (sales, finance), separate pipelines for each prefix.

### Pattern 3: Incremental Rollout

```python
# Week 1: Start with subgroup 01 tables
df_subgroup_01 = df[df['subgroup'] == '01']
run_complete_pipeline_generation(df_subgroup_01, ...)

# Week 2: Add subgroup 02 tables
df_all = df[df['subgroup'].isin(['01', '02'])]
run_complete_pipeline_generation(df_all, ...)
```

**Behavior:** Adds new pipelines without affecting existing subgroup 01 pipelines.

## Troubleshooting

### Error: "project_name is required"

**Cause:** No project_name in CSV, default_values, or override_input_config.

**Fix:**
```python
default_values={'project_name': 'my_project'}
```

### Error: "connection_name is required"

**Cause:** CSV missing connection_name column or has empty values.

**Fix:** Add connection_name to CSV or use default_values/override_input_config.

### Too Many Pipelines Created

**Cause:** max_tables_per_pipeline or max_tables_per_gateway too low.

**Fix:** Increase limits:
```python
max_tables_per_gateway=500,
max_tables_per_pipeline=250
```

### Gateway Without Cluster Config

**Expected behavior:** If gateway_worker_type and gateway_driver_type are both None/empty, gateway uses serverless compute (no cluster section in YAML).

## Contributing

To add a new connector, see [CONNECTOR_DEVELOPMENT_GUIDE.md](CONNECTOR_DEVELOPMENT_GUIDE.md).

## Support

For issues, questions, or contributions, please open an issue in the repository.
