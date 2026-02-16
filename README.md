# Lakehouse Tapworks

Automated DAB (Databricks Asset Bundle) generation toolkit for Lakeflow Connect pipelines.

## Problem

Manually creating and maintaining DABs for Lakeflow connectors doesn't scale. Common challenges include:

- **Manual table management** - Adding hundreds or thousands of tables to DABs by hand is error-prone and time-consuming
- **Load balancing** - Distributing tables across pipelines based on size, SLAs, or performance metrics is impossible to do manually at scale
- **Naming conventions** - Table mapping for sources with unsupported characters (e.g., SAP tables with "/") or enforcing naming standards requires automation
- **DAB syntax errors** - Minor syntax mistakes (e.g., missing spaces) cause cryptic errors that generate support tickets

## Solution

Tapworks reads from a simple configuration (CSV, Delta table, or any DataFrame source) and automatically generates complete DAB packages with load balancing, validation, and proper syntax.


## How It Works

1. **Define your config** - Specify source/target mappings in a simple format:

   ```csv
   source_schema,source_table,target_catalog,target_schema,target_table,connection_name
   dbo,customers,bronze,sales,customers,sql_server_conn
   ```

2. **Run the generator** - From CLI or notebook:

   **CLI (recommended):**
   ```bash
   # List available connectors
   python cli.py --list

   # Show connector requirements
   python cli.py sql_server --info

   # Generate DAB files
   python cli.py sql_server --input-config tables.csv --output-dir output \
     --targets '{"dev": {"workspace_host": "https://your-workspace.databricks.com"}}' 
   ```

   **Notebook / Python:**
   ```python
   from core import run_pipeline_generation

   result = run_pipeline_generation(
       connector_name='sql_server',
       input_source='tables.csv',  # or Delta table or DataFrame
       output_dir='output',
       targets={'dev': {'workspace_host': 'https://...'}},
       default_values={'project_name': 'my_project'},
   )
   ```

3. **Deploy** - Use the generated DAB files with `databricks bundle deploy`

## Load Balancing

Tapworks automatically distributes tables across pipelines and gateways.

### Hierarchy

**SaaS connectors** (Salesforce, GA4, ServiceNow, Workday):
```
Project (DAB Package)
└── Prefix + Subgroup (logical grouping)
    └── Pipeline(s) - auto-split if > 250 tables
```

**Database connectors** (SQL Server, PostgreSQL):
```
Project (DAB Package)
└── Prefix + Subgroup (logical grouping)
    └── Gateway(s) - auto-split if > 250 tables
        └── Pipeline(s) - auto-split if > 250 tables per gateway
```

### Auto-Distribution

Tables are automatically split based on configurable limits (default: 250 tables per pipeline/gateway):

**SaaS connector example** (600 tables):
```
              Input: 600 tables, prefix="sales", subgroup="01"
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
            ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
            │   Pipeline    │ │   Pipeline    │ │   Pipeline    │
            │ sales_01_g01  │ │ sales_01_g02  │ │ sales_01_g03  │
            │ (250 tables)  │ │ (250 tables)  │ │ (100 tables)  │
            └───────────────┘ └───────────────┘ └───────────────┘
```

**Database connector example** (600 tables):
```
              Input: 600 tables, prefix="sales", subgroup="01"
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
            ┌───────────────┐                   ┌───────────────┐
            │    Gateway    │                   │    Gateway    │
            │ sales_01_gw01 │                   │ sales_01_gw02 │
            │ (500 tables)  │                   │ (100 tables)  │
            └───────┬───────┘                   └───────┬───────┘
                    │                                   │
          ┌─────────┴─────────┐                         │
          ▼                   ▼                         ▼
   ┌───────────────┐   ┌───────────────┐        ┌───────────────┐
   │   Pipeline    │   │   Pipeline    │        │   Pipeline    │
   │  sales_01_    │   │  sales_01_    │        │  sales_01_    │
   │  gw01_g01     │   │  gw01_g02     │        │  gw02_g01     │
   │ (250 tables)  │   │ (250 tables)  │        │ (100 tables)  │
   └───────────────┘   └───────────────┘        └───────────────┘
```

### Manual Subgroups

Use subgroups to isolate specific tables (e.g., critical or high-volume tables).
**Note:** When using subgroups, all tables in a prefix must have explicit subgroups.

```
                    prefix="sales"
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
      subgroup="01"                  subgroup="02"
    (5 critical tables)          (595 remaining tables)
          │                               │
          ▼                               ▼
  ┌───────────────┐       ┌───────────────┬───────────────┬───────────────┐
  │   Pipeline    │       │   Pipeline    │   Pipeline    │   Pipeline    │
  │ sales_01_g01  │       │ sales_02_g01  │ sales_02_g02  │ sales_02_g03  │
  │  (5 tables)   │       │ (250 tables)  │ (250 tables)  │  (95 tables)  │
  └───────────────┘       └───────────────┴───────────────┴───────────────┘
```

## Defaults and Overrides

- **default_values** - Fill missing/empty columns with defaults (e.g., set schedule for rows that don't have one)
- **override_config** - Force values for ALL rows, ignoring what's in the input (e.g., pause all jobs)

### Simple Configuration

Apply the same values to all rows using a flat dictionary:

```python
default_values = {
    'schedule': '0 */6 * * *',
    'pause_status': 'UNPAUSED',
}
```

### Group-Based Configuration

Apply different values per pipeline group using nested dictionaries:

```python
default_values = {
    '*': {'schedule': '0 */6 * * *'},        # Global fallback
    'sales': {'schedule': '*/15 * * * *'},   # All sales pipelines
    'hr': {'schedule': '0 0 * * *'},         # HR pipelines
}

override_config = {
    '*': {'pause_status': 'UNPAUSED'},
    'finance': {'pause_status': 'PAUSED'},   # Pause finance for audit
}
```

**Matching precedence** (most specific wins):
1. `pipeline_group` (prefix_subgroup) - e.g., `'sales_2'`
2. `prefix` - e.g., `'sales'`
3. `project_name` - e.g., `'my_project'`
4. `'*'` (global)

See [examples/group_based_config](examples/group_based_config) for detailed examples.

### CLI Examples

**Inline JSON:**
```bash
python cli.py salesforce --input-config tables.csv --output-dir output \
  --targets '{"dev": {"workspace_host": "https://dev.cloud.databricks.com"}}' \
  --default-values '{"project_name": "sfdc_prod", "schedule": "0 */6 * * *"}' \
  --override '{"pause_status": "PAUSED"}'
```

**Settings file (settings.json):**
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
    "project_name": "sfdc_prod",
    "schedule": "0 */6 * * *"
  },
  "override_input_config": {
    "pause_status": "PAUSED"
  }
}
```

```bash
python cli.py salesforce --input-config tables.csv --output-dir output --settings settings.json
```

### Notebook Example

```python
from core import run_pipeline_generation

result = run_pipeline_generation(
    connector_name='salesforce',
    input_source='config.csv',
    output_dir='output',
    targets={
        'dev': {'workspace_host': 'https://dev.cloud.databricks.com'},
        'prod': {'workspace_host': 'https://prod.cloud.databricks.com'},
    },
    # Fill missing values
    default_values={
        'project_name': 'sfdc_prod',
        'schedule': '0 */6 * * *',
    },
    # Override ALL rows (e.g., pause all jobs during maintenance)
    override_config={
        'pause_status': 'PAUSED',
    },
)
```

## Target Environement

- Using target it is possible to specify different workspaces for deployment (e.g., dev, staging, prod)

## Output Structure

```
output/<project_name>/
  databricks.yml
  resources/
    gateways.yml    # database connectors only
    pipelines.yml
    jobs.yml
```

## Documentation

- [USAGE.md](USAGE.md) - CLI and notebook usage examples for all connectors
- [ARCHITECTURE.md](ARCHITECTURE.md) - Technical architecture and class hierarchy

**Quick reference:**
```bash
# Show connector requirements (columns, defaults, SCD types)
python cli.py <connector> --info

# Examples
python cli.py salesforce --info
python cli.py sql_server --info
```
