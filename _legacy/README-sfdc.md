# Salesforce Lakeflow Connect

Databricks Asset Bundle (DAB) for ingesting Salesforce data via Lakeflow Connect.

## Overview

This connector enables automated ingestion of Salesforce objects (Account, Contact, Opportunity, etc.) into Databricks Delta tables using Lakeflow Connect.

**Architecture:**
```
Salesforce (SaaS) → Connection (OAuth) → Ingestion Pipeline → Delta Tables
```

**Note:** Salesforce is a SaaS connector with direct API access. Unlike on-premises databases (Postgres, MySQL, SQL Server), it does NOT require a gateway.

## Quick Start

### 1. Prerequisites

- Databricks workspace with Unity Catalog
- Databricks CLI installed and authenticated
- Appropriate permissions (catalog, schema, pipeline creation)

### 2. Create Salesforce Connection (Manual - UI Only)

**Important:** Salesforce connections require OAuth 2.0 and CANNOT be created via DABs or SDK.

**Steps:**
1. Go to Databricks workspace → **Catalog → Connections**
2. Click **"Create Connection"**
3. Select connection type: **Salesforce**
4. Enter connection name (e.g., `sfdc_connection`)
5. Complete OAuth authorization flow in browser
6. Verify connection status is "Active"

**Why manual?** OAuth 2.0 requires interactive browser-based authentication and cannot be automated programmatically.

### 3. Create Catalog and Schema

```bash
# Default catalog and schema
python create_salesforce_catalog.py

# Or specify custom names
python create_salesforce_catalog.py salesforce_connector salesforce_dev
```

Creates:
- Catalog: `salesforce_connector`
- Schema: `salesforce_connector.salesforce_dev`

### 4. Configure Objects to Ingest

```bash
# Copy example configuration
cp dab/examples/example_config.csv my_salesforce_config.csv

# Edit my_salesforce_config.csv:
# - Add/remove Salesforce objects
# - Set pipeline_group (1 = single pipeline, 2+ = multiple pipelines)
# - Update connection_name if different
```

**Example config:**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,pipeline_group,connection_name,schedule,include_columns,exclude_columns
Salesforce,standard,Account,salesforce_connector,salesforce,Account,1,sfdc_connection,*/15 * * * *,"Id,Name,Type,Industry",
Salesforce,standard,Contact,salesforce_connector,salesforce,Contact,1,sfdc_connection,*/15 * * * *,,
Salesforce,standard,Opportunity,salesforce_connector,salesforce,Opportunity,2,sfdc_connection,*/30 * * * *,,"Description,SystemModstamp"
```

### 5. Create databricks.yml

```bash
# Copy example template to dab directory
cp dab/examples/example_databricks.yml dab/databricks.yml

# Edit dab/databricks.yml and customize:
# - Connection name (sfdc_connection_name)
# - Catalog and schema names (if different from defaults)
# - Target environments (dev/prod)
```

### 6. Generate Pipeline and Job YAML

```bash
python dab/generate_salesforce_pipeline.py my_salesforce_config.csv
```

This generates `dab/resources/sfdc_pipeline.yml` with:
- **Pipeline definitions** for data ingestion
- **Job definitions** for automatic scheduling (based on `schedule` column in CSV)

**See example output:** `dab/examples/example_generated_pipeline.yml` shows the generated pipelines and jobs.

### 7. Deploy

```bash
cd dab
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

DABs will create:
1. **Pipelines** - Lakeflow Connect ingestion pipelines
2. **Jobs** - Scheduled jobs that trigger the pipelines automatically

### 8. Enable Scheduling

Jobs are created in **PAUSED** state by default. To enable automatic scheduling:

```bash
# List jobs to find job ID
databricks jobs list | grep sfdc_pipeline_scheduler

# Unpause the job to enable scheduling
databricks jobs update <job_id> --schedule-pause-status UNPAUSED
```

**Or manually trigger:**
```bash
# Run pipeline immediately (without waiting for schedule)
databricks pipelines start-update <pipeline_id>
```

## Pipeline Groups

Use the `pipeline_group` column in your CSV to organize tables:

- **Single pipeline** (all tables in one pipeline):
  ```csv
  Account,1,...
  Contact,1,...
  Opportunity,1,...
  ```
  Result: 1 pipeline with 3 objects

- **Multiple pipelines** (separate by domain/frequency):
  ```csv
  Account,1,...        # Pipeline 1: Core CRM
  Contact,1,...
  Opportunity,2,...    # Pipeline 2: Sales
  Lead,2,...
  Case,3,...           # Pipeline 3: Support
  Task,3,...
  ```
  Result: 3 pipelines with different objects

## Project Structure

```
sfdc/
├── create_salesforce_catalog.py       # Create Unity Catalog catalog/schema
├── README.md                          # This file
└── dab/
    ├── generate_salesforce_pipeline.py # Generate pipeline YAML from CSV
    ├── databricks.yml                  # DABs configuration (copy from examples/)
    ├── resources/
    │   └── sfdc_pipeline.yml           # Generated pipeline (git ignored)
    └── examples/
        ├── example_databricks.yml      # DABs configuration template
        └── example_config.csv          # Table configuration template
```

## Configuration Reference

### CSV Columns

| Column | Description | Example |
|--------|-------------|---------|
| `source_database` | Source system | `Salesforce` |
| `source_schema` | Schema type | `standard` or `custom` |
| `source_table_name` | Salesforce object name | `Account`, `Contact`, `MyCustom__c` |
| `target_catalog` | Destination catalog | `salesforce_connector` |
| `target_schema` | Destination schema | `salesforce`, `salesforce_dev` |
| `target_table_name` | Destination table name | `Account` (usually same as source) |
| `pipeline_group` | Pipeline grouping | `1`, `2`, `3`, etc. |
| `connection_name` | Connection reference | `sfdc_connection` |
| `schedule` | Cron schedule for automatic execution | `*/15 * * * *` (every 15 min) |
| `include_columns` | Columns to include (optional) | `"Id,Name,Type"` or leave empty for all |
| `exclude_columns` | Columns to exclude (optional) | `"SystemModstamp"` or leave empty |

### Schedule Column Details

The `schedule` column uses standard cron format (5 fields) which is automatically converted to Quartz cron format (6 fields) for Databricks jobs:

**Standard Cron → Quartz Cron Conversion:**
```
*/15 * * * *    →  0 */15 * * * ?    (every 15 minutes)
0 */6 * * *     →  0 0 */6 * * ?     (every 6 hours)
0 0 * * *       →  0 0 0 * * ?       (daily at midnight)
0 9 * * 1       →  0 0 9 ? * 1       (weekly Monday at 9am)
```

**How it works:**
1. Generator reads `schedule` from CSV
2. Creates a Databricks job for each pipeline group
3. Job automatically triggers the pipeline on schedule
4. Uses DABs variable syntax to reference pipeline: `${resources.pipelines.PIPELINE_NAME.id}`

**Per-group scheduling:**
- All tables in same `pipeline_group` must have same schedule
- Different groups can have different schedules
- If schedules differ within a group, first table's schedule is used (with warning)

**No schedule:**
- If `schedule` column is empty or missing, pipelines are created without jobs
- You must manually trigger pipelines or create jobs separately

### Deployment Targets

Configure different environments in `dab/databricks.yml`:

```yaml
targets:
  dev:
    mode: development
    variables:
      dest_catalog: salesforce_connector
      dest_schema: salesforce_dev

  prod:
    mode: production
    variables:
      dest_catalog: salesforce_connector
      dest_schema: salesforce_prod
```

**Deploy:**
```bash
# Development
databricks bundle deploy -t dev

# Production
databricks bundle deploy -t prod
```

## Monitoring

### Check Pipeline Status

```bash
# List pipelines
databricks bundle resources -t dev

# Get pipeline details
databricks pipelines get --pipeline-id <pipeline_id>

# View recent updates
databricks pipelines list-updates --pipeline-id <pipeline_id>
```

### Query Data

```sql
-- Check ingested tables
SHOW TABLES IN salesforce_connector.salesforce_dev;

-- Query Salesforce data
SELECT * FROM salesforce_connector.salesforce_dev.Account LIMIT 10;

-- Check row counts
SELECT
  'Account' as object, COUNT(*) as rows FROM salesforce_connector.salesforce_dev.Account
UNION ALL
SELECT
  'Contact' as object, COUNT(*) as rows FROM salesforce_connector.salesforce_dev.Contact;
```

## Troubleshooting

### Connection Not Found

**Error:** `Connection 'sfdc_connection' does not exist`

**Solution:**
1. Verify connection exists: `databricks connections get sfdc_connection`
2. Check connection name matches `databricks.yml`
3. Grant permissions: `GRANT USE CONNECTION ON CONNECTION sfdc_connection TO <principal>`

### OAuth Token Expired

**Symptom:** Pipeline fails with authentication error

**Solution:**
1. Go to Catalog → Connections → `sfdc_connection`
2. Click "Re-authenticate"
3. Complete OAuth flow again
4. Restart pipeline

### Pipeline Not Starting

**Check:**
1. Connection is active (Catalog → Connections)
2. Catalog and schema exist
3. Permissions granted on connection
4. No errors in pipeline event log

## DABs Configuration

### databricks.yml Structure

The `dab/databricks.yml` file defines:

**Bundle Settings:**
```yaml
bundle:
  name: salesforce_lakeflow_connect

workspace:
  host: https://your-workspace.cloud.databricks.com

include:
  - resources/*.yml  # Includes generated pipeline YAML
```

**Variables:**
```yaml
variables:
  dest_catalog:
    description: "Catalog for Salesforce data"
    default: salesforce_connector

  dest_schema:
    description: "Schema for Salesforce tables"
    default: salesforce

  sfdc_connection_name:
    description: "Name of existing Salesforce connection (created via UI)"
    default: sfdc_connection
```

**Key Points:**
- Variables are referenced in pipeline YAML as `${var.variable_name}`
- Connection must already exist (created via UI)
- Catalog/schema must be created before deployment
- Use targets to override variables per environment

## Additional Resources

- **Databricks Connections:** https://docs.databricks.com/en/connect/index.html
- **Lakeflow Connect Guide:** https://docs.databricks.com/en/ingestion/index.html
- **Databricks Asset Bundles:** https://docs.databricks.com/en/dev-tools/bundles/index.html
