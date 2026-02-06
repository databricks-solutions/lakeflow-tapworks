# Lakehouse Tapworks

Automated DAB (Databricks Asset Bundle) generation toolkit for Lakeflow Connect pipelines.

## Problem

Manually creating and maintaining DABs for Lakeflow connectors doesn't scale. Common challenges include:

- **Migration complexity** - Customers have existing metadata from tools like ADF that they want to leverage automatically
- **Manual table management** - Adding hundreds or thousands of tables to DABs by hand is error-prone and time-consuming
- **Load balancing** - Distributing tables across pipelines based on size, SLAs, or performance metrics is impossible to do manually at scale
- **Naming conventions** - Table mapping for sources with unsupported characters (e.g., SAP tables with "/") or enforcing naming standards requires automation
- **Config validation** - Catching issues like missing catalogs, schemas, or connections before deployment prevents pipeline failures
- **DAB syntax errors** - Minor syntax mistakes (e.g., missing spaces) cause cryptic errors that generate support tickets

## Solution

Tapworks reads from a simple configuration (CSV, Delta table, or any DataFrame source) and automatically generates complete DAB packages with load balancing, validation, and proper syntax.

**Supported connectors:** SQL Server, PostgreSQL, Salesforce, Google Analytics 4, ServiceNow, Workday

## How It Works

1. **Define your config** - Specify source/target mappings in a simple format:

   ```csv
   source_schema,source_table,target_catalog,target_schema,target_table,connection_name
   dbo,customers,bronze,sales,customers,sqlserver_conn
   ```

2. **Run the generator** - From CLI or notebook:

   **CLI (recommended):**
   ```bash
   # List available connectors
   python cli.py --list

   # Show connector requirements
   python cli.py sqlserver --info

   # Generate DAB files
   python cli.py sqlserver --input config.csv --config config.yaml
   ```

   **Notebook / Python:**
   ```python
   from core import run_pipeline_generation

   result = run_pipeline_generation(
       connector_name='sqlserver',
       input_source='config.csv',  # or Delta table or DataFrame
       output_dir='output',
       targets={'dev': {'workspace_host': 'https://...'}},
       default_values={'project_name': 'my_project'},
   )
   ```

3. **Deploy** - Use the generated DAB files with `databricks bundle deploy`

## Connector Aliases

Use short aliases for convenience:

| Alias | Connector |
|-------|-----------|
| `sf` | salesforce |
| `sql`, `mssql` | sqlserver |
| `pg`, `postgresql` | postgres |
| `ga`, `ga4` | google_analytics |
| `snow` | servicenow |
| `wd`, `workday` | workday_reports |

```bash
python cli.py sf --input config.csv --config config.yaml
python cli.py pg --input config.csv --config config.yaml
```

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
            ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
            │  Pipeline    │  │  Pipeline    │  │  Pipeline    │
            │  sales_01_g1 │  │  sales_01_g2 │  │  sales_01_g3 │
            │  (250 tables)│  │  (250 tables)│  │  (100 tables)│
            └──────────────┘  └──────────────┘  └──────────────┘
```

**Database connector example** (600 tables):
```
              Input: 600 tables, prefix="sales", subgroup="01"
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
            ┌──────────────┐                    ┌──────────────┐
            │  Gateway     │                    │  Gateway     │
            │  sales_01_gw1│                    │  sales_01_gw2│
            │  (500 tables)│                    │  (100 tables)│
            └──────┬───────┘                    └──────┬───────┘
                   │                                   │
          ┌────────┴────────┐                          │
          ▼                 ▼                          ▼
   ┌──────────────┐  ┌──────────────┐          ┌──────────────┐
   │  Pipeline    │  │  Pipeline    │          │  Pipeline    │
   │  sales_01    │  │  sales_01    │          │  sales_01    │
   │  _gw1_g1     │  │  _gw1_g2     │          │  _gw2_g1     │
   │  (250 tables)│  │  (250 tables)│          │  (100 tables)│
   └──────────────┘  └──────────────┘          └──────────────┘
```

### Manual Subgroups

Use subgroups to isolate specific tables (e.g., critical or high-volume tables):

```
                    prefix="sales"
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
    subgroup="critical"             subgroup="01" (auto)
          │                               │
          ▼                               ▼
  ┌──────────────┐               ┌──────────────┐
  │  Pipeline    │               │  Pipeline(s) │
  │  (5 critical │               │  (remaining  │
  │   tables)    │               │   tables)    │
  └──────────────┘               └──────────────┘
```

## Additional Features

- **Defaults and overrides** - Set default values for missing config columns or override existing values globally (e.g., pause all jobs)
- **Multi-environment** - Generate DABs for dev, staging, prod from the same config
- **Flexible storage** - Config can live in CSV, Delta tables, or any DataFrame-compatible source

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
- [IMPROVEMENTS.md](IMPROVEMENTS.md) - Roadmap and future improvements

**Quick reference:**
```bash
# Show connector requirements (columns, defaults, SCD types)
python cli.py <connector> --info

# Examples
python cli.py salesforce --info
python cli.py sqlserver --info
```
