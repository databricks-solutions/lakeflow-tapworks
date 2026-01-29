# Lakehouse Tapworks

Automated pipeline generation toolkit for Databricks Lakeflow Connect ingestion.

## Overview

Lakehouse Tapworks generates Lakeflow Connect ingestion pipelines from user-defined configurations. It handles load balancing and produces deployment-ready Databricks Asset Bundle (DAB) files.

**Supported connectors:**
- **SQL Server** - Database connector with gateway support
- **PostgreSQL** - Database connector with gateway support
- **Salesforce** - SaaS connector for Salesforce objects
- **Google Analytics 4** - SaaS connector via BigQuery integration

## Load balancing

- **SaaS connectors (Salesforce, GA4)**: split into pipelines only (by `prefix` + `subgroup`)
- **Database connectors (SQL Server, PostgreSQL)**: split into gateways then pipelines (by `prefix` + `subgroup`)

## Configuration hierarchy

When you run `run_complete_pipeline_generation(df=...)`:

1. Built-in defaults (per connector)
2. Values in the input dataframe (CSV / Delta / etc.)
3. `default_values={...}` (fills empty/missing)
4. `override_input_config={...}` (forces values for all rows)

## Input CSV formats

### SQL Server / PostgreSQL (database connectors)

**Required columns:**

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name
mydb,public,customers,bronze,sales,customers,my_db_connection
```

**Optional columns:**
- `project_name` (default: `<connector>_ingestion`, Postgres uses `postgres_ingestion`)
- `prefix` (default: project_name)
- `subgroup` (default: `01`)
- `gateway_catalog` (default: target_catalog)
- `gateway_schema` (default: target_schema)
- `gateway_worker_type` (default: None)
- `gateway_driver_type` (default: None)
- `schedule` (default: None)

### Salesforce (SaaS connector)

See `salesforce/README.md`.

### Google Analytics 4 (SaaS connector)

See `google_analytics/README.md`.

## Output structure

Each unique `project_name` produces a separate DAB package:

```
output/<project_name>/
  databricks.yml
  resources/
    (gateways.yml)   # database connectors only
    pipelines.yml
    jobs.yml
```

