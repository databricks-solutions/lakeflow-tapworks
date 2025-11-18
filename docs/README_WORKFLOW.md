# Complete Salesforce Lakeflow Connect Workflow

End-to-end guide for setting up Salesforce ingestion using Databricks Asset Bundles with SDK-based connection creation and secrets management.

## Overview

This workflow uses:
- **Databricks Secrets** for credential storage (no hardcoding)
- **Databricks SDK** for programmatic connection creation
- **Lakeflow Connect** for serverless Salesforce ingestion
- **Asset Bundles (DABs)** for infrastructure-as-code deployment

## Complete Workflow

```
1. Setup Secrets         2. Create Connection    3. Generate YAML        4. Deploy Pipeline
   (secret store)            (SDK + secrets)          (CSV → YAML)           (DABs)
        ↓                          ↓                        ↓                      ↓
   Databricks          →     Connection API    →     Python Script    →    databricks bundle
   Secrets Scope              (w/ secret refs)         (serverless)           deploy -t dev
```

## Step 1: Setup Secrets

Store Salesforce credentials securely in Databricks secrets:

```bash
# Interactive setup (recommended)
./setup_salesforce_secrets.sh salesforce_secrets

# Or manual setup
databricks secrets create-scope salesforce_secrets

databricks secrets put-secret salesforce_secrets salesforce-username \
  --string-value "your@email.com"

databricks secrets put-secret salesforce_secrets salesforce-password \
  --string-value "your_password"

databricks secrets put-secret salesforce_secrets salesforce-security-token \
  --string-value "your_token"
```

**Verify secrets:**
```bash
databricks secrets list-secrets salesforce_secrets
```

## Step 2: Create Connection

Create Databricks connection using SDK with secret references:

```bash
# Install dependencies
pip install databricks-sdk pyyaml

# Create connection
python create_salesforce_connection.py \
  sfdc_dabs_pipeline_testing \
  salesforce_secrets
```

**What this does:**
1. Connects to Databricks workspace
2. Verifies secret scope and secrets exist
3. Creates connection with secret references:
   ```
   user: {{secrets/salesforce_secrets/salesforce-username}}
   password: {{secrets/salesforce_secrets/salesforce-password}}
   securityToken: {{secrets/salesforce_secrets/salesforce-security-token}}
   ```
4. Returns connection ID

**Options:**
```bash
# Recreate existing connection
python create_salesforce_connection.py my_sfdc salesforce_secrets --force

# Use different secret scope
python create_salesforce_connection.py my_sfdc prod_salesforce_secrets
```

## Step 3: Configure Tables

Create CSV with tables to ingest:

**salesforce_sample_config.csv:**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,pipeline_group,gateway,connection_name,schedule
Salesforce,standard,Account,main,salesforce,Account,1,0,sfdc_dabs_pipeline_testing,*/30 * * * *
Salesforce,standard,Contact,main,salesforce,Contact,1,0,sfdc_dabs_pipeline_testing,*/30 * * * *
Salesforce,standard,Opportunity,main,salesforce,Opportunity,2,0,sfdc_dabs_pipeline_testing,*/30 * * * *
Salesforce,standard,Lead,main,salesforce,Lead,2,0,sfdc_dabs_pipeline_testing,*/30 * * * *
```

**Key fields:**
- `connection_name`: Must match Step 2
- `pipeline_group`: Groups tables into separate pipelines (1, 2, 3, etc.)
- `target_catalog`/`target_schema`: Where Delta tables will be created

## Step 4: Generate Pipeline YAML

Generate DAB YAML configuration:

```bash
python generate_sfdc_yaml_simple.py salesforce_sample_config.csv
```

**Output:** `salesforce_dab/resources/sfdc_pipeline.yml`

**Generated structure:**
```yaml
resources:
  pipelines:
    pipeline_sfdc_ingestion_group_1:  # Group 1 tables
      catalog: ${var.dest_catalog}
      ingestion_definition:            # ← Serverless!
        connection_name: sfdc_dabs_pipeline_testing
        objects:
          - table:
              source_schema: objects
              source_table: Account
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}

    pipeline_sfdc_ingestion_group_2:  # Group 2 tables
      catalog: ${var.dest_catalog}
      ingestion_definition:
        connection_name: sfdc_dabs_pipeline_testing
        objects:
          - table:
              source_schema: objects
              source_table: Opportunity
              ...
```

## Step 5: Deploy with DABs

Deploy serverless pipelines:

```bash
cd salesforce_dab

# Validate configuration
databricks bundle validate -t dev

# Deploy
databricks bundle deploy -t dev --auto-approve
```

**Output:**
```
Deploying resources...
  Pipeline: [dev your_username] sfdc_ingestion_pipeline_group_1 (25f7f097-aa09-45e3-834f-5f5b2bd31a1f)
  Pipeline: [dev your_username] sfdc_ingestion_pipeline_group_2 (8a3b2c1d-4e5f-6a7b-8c9d-0e1f2a3b4c5d)
```

## Step 6: Run Pipelines

Start ingestion:

```bash
# Start pipeline
databricks pipelines start-update 25f7f097-aa09-45e3-834f-5f5b2bd31a1f

# Check status
databricks pipelines get 25f7f097-aa09-45e3-834f-5f5b2bd31a1f
```

**Or in UI:**
- Navigate to **Workflows** → **Delta Live Tables**
- Find pipeline: `[dev your_username] sfdc_ingestion_pipeline_group_1`
- Click **Start**

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Databricks Workspace                         │
│                                                                   │
│  ┌──────────────┐      ┌────────────────┐                       │
│  │   Secrets    │      │  Connection    │                       │
│  │   Scope      │─────→│  (with secret  │                       │
│  │              │      │   references)  │                       │
│  └──────────────┘      └────────┬───────┘                       │
│                                  │                                │
│                                  ↓                                │
│                      ┌────────────────────┐                      │
│                      │ Serverless Pipeline│                      │
│                      │  (Lakeflow Connect)│                      │
│                      └─────────┬──────────┘                      │
│                                │                                  │
│                                ↓                                  │
│                      ┌────────────────────┐                      │
│                      │   Delta Tables     │                      │
│                      │ (main.salesforce.*)│                      │
│                      └────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
                                 ↑
                                 │
                         Salesforce API
```

## Security Features

 **No hardcoded credentials** - All stored in Databricks secrets
 **Secret references** - Connection uses `{{secrets/<scope>/<key>}}` format
 **IAM-based access** - Uses Databricks CLI authentication
 **Audit logging** - All secret access is logged
 **Rotation support** - Update secrets without changing code

## Cost Optimization

| Component | Cost |
|-----------|------|
| **Secrets** | Free (part of workspace) |
| **Connection** | Free (metadata only) |
| **Serverless pipeline** | ~$1-2 per run (pay-per-use) |
| **Delta storage** | Standard storage costs |

**No gateway needed** = No 24/7 compute costs (~$20-50/day saved)

## Multi-Environment Setup

### Development Environment

```bash
# Dev secrets
./setup_salesforce_secrets.sh salesforce_dev

# Dev connection
python create_salesforce_connection.py sfdc_dev salesforce_dev

# Update CSV with connection_name: sfdc_dev
python generate_sfdc_yaml_simple.py dev_config.csv

# Deploy to dev
cd salesforce_dab && databricks bundle deploy -t dev
```

### Production Environment

```bash
# Prod secrets
./setup_salesforce_secrets.sh salesforce_prod

# Prod connection
python create_salesforce_connection.py sfdc_prod salesforce_prod

# Update CSV with connection_name: sfdc_prod
python generate_sfdc_yaml_simple.py prod_config.csv

# Deploy to prod
cd salesforce_dab && databricks bundle deploy -t prod
```

## Troubleshooting

### Connection Creation Fails

```bash
# Check secret scope exists
databricks secrets list-scopes

# Verify secrets
databricks secrets list-secrets <scope_name>

# Check connection status
databricks connections get <connection_name>
```

### Pipeline Deployment Fails

```bash
# Validate bundle
databricks bundle validate -t dev

# Check connection exists
databricks connections list | grep <connection_name>

# Verify catalog permissions
databricks catalogs get <catalog_name>
```

### Pipeline Run Fails

Common issues:
1. **Invalid credentials** - Rotate secrets:
   ```bash
   databricks secrets put-secret salesforce_secrets salesforce-password \
     --string-value "new_password"
   ```

2. **Connection not found** - Verify connection_name in CSV matches Step 2

3. **Permission denied** - Grant catalog permissions:
   ```sql
   GRANT USE CATALOG ON CATALOG main TO `user@company.com`;
   GRANT CREATE SCHEMA ON CATALOG main TO `user@company.com`;
   ```

## Files Created

```
lakehouse-tapworks/
├── setup_salesforce_secrets.sh          # Interactive secret setup
├── create_salesforce_connection.py      # SDK connection creator
├── generate_sfdc_yaml_simple.py         # YAML generator
├── salesforce_sample_config.csv         # Table configuration
├── example_multi_group.csv              # Multi-pipeline example
└── salesforce_dab/
    ├── databricks.yml                   # Main DAB config
    └── resources/
        └── sfdc_pipeline.yml            # Generated pipelines
```

## Commands Quick Reference

```bash
# 1. Setup secrets
./setup_salesforce_secrets.sh salesforce_secrets

# 2. Create connection
python create_salesforce_connection.py my_sfdc salesforce_secrets

# 3. Generate YAML
python generate_sfdc_yaml_simple.py config.csv

# 4. Deploy
cd salesforce_dab && databricks bundle deploy -t dev

# 5. Run
databricks pipelines start-update <pipeline_id>

# Check status
databricks pipelines get <pipeline_id>

# View data
databricks sql "SELECT * FROM main.salesforce.Account LIMIT 10"
```

## Advanced Topics

### Custom Secret Keys

If using different secret key names:

**Edit `create_salesforce_connection.py` line ~107:**
```python
options={
    "user": f"{{{{secrets/{secret_scope}/sfdc_user}}}}",      # Custom key
    "password": f"{{{{secrets/{secret_scope}/sfdc_pass}}}}",  # Custom key
    "securityToken": f"{{{{secrets/{secret_scope}/sfdc_token}}}}"  # Custom key
}
```

### Multiple Salesforce Orgs

```bash
# Production org
./setup_salesforce_secrets.sh sfdc_prod
python create_salesforce_connection.py sfdc_prod sfdc_prod

# Sandbox org
./setup_salesforce_secrets.sh sfdc_sandbox
python create_salesforce_connection.py sfdc_sandbox sfdc_sandbox

# Use different CSV files
python generate_sfdc_yaml_simple.py prod_config.csv prod_dab/resources
python generate_sfdc_yaml_simple.py sandbox_config.csv sandbox_dab/resources
```

### Scheduled Runs

Add schedule to pipeline (requires CLI or UI):

```bash
# Via CLI (not yet supported in DABs)
databricks pipelines update <pipeline_id> --trigger "cron: 0 */4 * * *"

# Or via UI:
# Workflows → DLT → Pipeline → Schedule → Add trigger
```

## Support

For issues or questions:
1. Check [Databricks Lakeflow Connect docs](https://docs.databricks.com/ingestion/lakeflow-connect/)
2. Review [Unity Catalog Connections docs](https://docs.databricks.com/data-governance/unity-catalog/connections.html)
3. Contact Databricks support

## License

Internal Databricks tool for customer engagements.
