# Lakehouse Tapworks

Automated pipeline deployment template generation toolkit for Databricks Lakeflow Connectors.

## Overview

Lakehouse Tapworks generates deployment-ready Databricks Asset Bundle (DAB) Lakeflow Connect ingestion pipelines from user-defined configurations. It automatically handles load balancing to cater for performance based on metrics like number pf tables per pieplien and user inputs.

**Supported connectors:**
- **SQL Server** 
- **PostgreSQL** 
- **Salesforce** 
- **Google Analytics 4** 
- **Service Now**
- **Workday**

## Load balancing

The user defines a project name. For each project name in the config a new DAB package is created.

The user can define a prefix (e.g., business unit 1 or sales) for each subset of tables. This defines a logical grouping between the pipelines that will process those tables.

For each of these logical groups:
Split the tables according to max number of tables per pipeline between pipelines (default 250)
By default each pipeline is linked to one gateway if this is a database connector
If the max number of tables per gateway (default 250) is larger than max tables per pipeline, then multiple pipelines can share the same gateway.

If a user wants to manually dedicate a pipeline to process a subset of the tables (e.g., critical tables, or large tables) they can specify a subgroup in the config for the prefix. Load balancer prioritises the subgroup over the default laid balancing if present. 


Extra features

It is possible to specify all or a subset of config columns, specify defaults to replace missing data (for example, send a single value for gateway driver node instead of specifying in the config), or override values to override what’s specified in the config if needed (e.g., this can be used to pause/unpause the jobs using 

Config can be stored in different forms as long as the data can be passed to the tapwork main function as a dataframe. We’re currently using CSV for ease of demonstration, but this can be a delta or lakebase table that can be maintained using a Databricks App.
It is possible to pass configurations for different environments to the main function to generate databricks.xml accordingly and deploy the pipelines to different environments (e.g., dev, prod, …)




## Input config formats

### SQL Server / PostgreSQL (database connectors)

**Required columns:**

```
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

