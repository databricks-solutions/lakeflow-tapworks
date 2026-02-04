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

2. **Run the generator** - From a notebook or CLI (for CI/CD integration):

   ```python
   from sqlserver.connector import SQLServerConnector
   connector = SQLServerConnector(config_df)
   connector.generate()
   ```

3. **Deploy** - Use the generated DAB files with `databricks bundle deploy`

## Load Balancing

Tapworks automatically distributes tables across pipelines and gateways:

- **Project** - Each unique project name creates a separate DAB package
- **Prefix** - Logical groupings (e.g., by business unit) that map to pipelines
- **Auto-splitting** - Tables are distributed based on configurable limits (default: 250 tables per pipeline)
- **Gateway sharing** - Multiple pipelines can share gateways when limits allow
- **Subgroups** - Override automatic balancing for specific tables (e.g., isolate critical or large tables)

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

See connector-specific READMEs for detailed configuration options:
- [SQL Server](sqlserver/README.md)
- [PostgreSQL](postgres/README.md)
- [Salesforce](salesforce/README.md)
- [Google Analytics 4](google_analytics/README.md)
- [ServiceNow](servicenow/README.md)
- [Workday](workday_reports/README.md)
