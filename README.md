# Lakehouse Tapworks

Automated pipeline generation toolkit for Databricks Lakeflow Connect ingestion.

## Overview

Lakehouse Tapworks generates Lakeflow connect ingestion pipelines from user defined configurations. It handles load balancing and produces deployment-ready Databricks Asset Bundle (DAB) files.

**Supported Connectors:**
- **SQL Server** - Database connector with gateway support
- **Salesforce** - SaaS connector for Salesforce objects
- **Google Analytics 4** - SaaS connector via BigQuery integration

### Config columns
**Main columns**
```Project_name 
Source database
Source table
Target catalog
Target Schema
Target Table name
```

**Additional major columns**
```Prefix (if missing replaced with project name)
Subgroup (specifies spliting of the pipeline group specified by Prefix into smaller groups)
max tables per pipeline
max table per gateway (for database connectors)
```

For each project name a separate DAB package is genrated. If a single project_name all pipelines are deplyed using a single databricks.yml

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


### max tables per pipeline/gateway
groups deinfed by project_name, prefix and subgroups will be split further if the number of tables assigned to each subgroups is more than the max table specified. 


### DAB Project Organization

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


## Configuration 

Config values can be specified in the columns read from the config (can be a csv file or a delta table for example)
These values when passed to the run_complete function as a dataframe can be replaced by vluaes specified in override_vaules. Also if columns are missing from the config or have empty values can be replaces by values passed using default_values.

Doing this you can only specify what's different between pipelines using the config then use default_values to specify what's shared across pipelines.
Also can override what's in the config if needed. For example you can pause/unpause the pipelines by passing pause_state:paused/unpaused to run_complete_pipeline then redeployt the resources.


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
