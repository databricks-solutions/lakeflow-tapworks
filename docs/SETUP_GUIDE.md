# Complete Salesforce Lakeflow Connect Setup Guide

## Summary

This guide shows how to set up Salesforce Lakeflow Connect with Databricks.

## Quick Start (3 Steps)

### Step 1: Create Salesforce Connection via Databricks UI

Salesforce connections for Unity Catalog must be created via the Databricks UI (cannot be automated via SDK).

1. Go to your Databricks workspace
2. Navigate to: **Catalog** → **External Data** → **Connections**
3. Click **Create Connection**
4. Select **Salesforce**
5. Enter connection details:
   - **Name:** `sfdc_dabs_pipeline_testing` (or your preferred name)
   - **Description:** Salesforce connection for Lakeflow Connect
6. Click **Create** and complete OAuth authorization
7. Verify connection is active

### Step 2: Create Catalog and Schema (Optional)

```bash
cd /path/to/your/lakehouse-tapworks
source venv/bin/activate
python create_salesforce_catalog.py
```

This creates:
- Catalog: `salesforce_connector`
- Schema: `salesforce_connector.salesforce`

### Step 3: Generate Pipeline YAML

```bash
python generate_sfdc_yaml_simple.py salesforce_sample_config.csv
```

Output: `salesforce_dab/resources/sfdc_pipeline.yml`

## Files in Repository

### Core Scripts

| File | Purpose |
|------|---------|
| `create_salesforce_catalog.py` | Creates Unity Catalog catalog and schema |
| `generate_salesforce_pipeline.py` | Generates DAB YAML from CSV configuration |
| `test_yaml_generator.py` | Tests for YAML generation |

### Documentation

| File | Purpose |
|------|---------|
| `SALESFORCE_CONNECTED_APP_SETUP.md` | Guide for setting up Salesforce Connected App (for reference) |
| `README_WORKFLOW.md` | Workflow documentation |
| `README_TESTS.md` | Test suite documentation |
| `README_GENERATOR.md` | YAML generator documentation |
| `SETUP_GUIDE.md` | This file |

### Test Suite

| File | Purpose |
|------|---------|
| `test_yaml_generator.py` | Tests YAML generation |
| `run_all_tests.sh` | Test runner |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│    Databricks UI: Create Salesforce Connection (OAuth)         │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  1. Navigate to Catalog → Connections                      │ │
│  │  2. Create Salesforce connection                           │ │
│  │  3. Complete OAuth authorization                           │ │
│  │  4. Connection name: sfdc_dabs_pipeline_testing            │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────┬───────────────────────────────────────────┘
                       │ Connection created
                       ↓
┌─────────────────────────────────────────────────────────────────┐
│              DAB YAML Pipeline Configuration                    │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  resources:                                                │ │
│  │    pipelines:                                              │ │
│  │      pipeline_sfdc_ingestion_group_1:                      │ │
│  │        ingestion_definition:                               │ │
│  │          connection_name: sfdc_dabs_pipeline_testing       │ │
│  │          objects:                                          │ │
│  │            - table: Account                                │ │
│  │            - table: Contact                                │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────┬───────────────────────────────────────────┘
                       │ Deploy & Run
                       ↓
┌─────────────────────────────────────────────────────────────────┐
│         Salesforce Data → Unity Catalog Tables                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  Catalog: salesforce_connector                             │ │
│  │  Schema: salesforce                                        │ │
│  │  Tables: Account, Contact, Opportunity, etc.               │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Connection Requirements

**Databricks Unity Catalog Salesforce connections require OAuth** and must be created through the Databricks UI.

**Why UI is Required:**
1. **OAuth Authorization Flow**: Interactive login with Salesforce required
2. **Token Management**: Databricks securely manages refresh tokens
3. **Security**: No credentials stored in code or configuration files

## Deployment Workflow

Once connection is created:

```bash
# 1. Configure tables in CSV
vim salesforce_sample_config.csv

# 2. Generate YAML
python generate_sfdc_yaml_simple.py salesforce_sample_config.csv

# 3. Validate DAB
cd salesforce_dab && databricks bundle validate -t dev

# 4. Deploy
databricks bundle deploy -t dev

# 5. Run pipeline
databricks pipelines start-update <pipeline_id>
```

**Note:** The YAML only references the connection by NAME (`sfdc_dabs_pipeline_testing`), so it's portable across environments.

## Multi-Environment Setup

For dev/staging/prod:

```bash
# Dev environment
python create_salesforce_connection_full_oauth.py sfdc_dev salesforce_dev
python generate_sfdc_yaml_simple.py dev_config.csv
cd salesforce_dab && databricks bundle deploy -t dev

# Prod environment
python create_salesforce_connection_full_oauth.py sfdc_prod salesforce_prod
python generate_sfdc_yaml_simple.py prod_config.csv
cd salesforce_dab && databricks bundle deploy -t prod
```

## Next Steps

**Setup:**
1. Create Salesforce connection via Databricks UI
2. Create catalog and schema (optional): `python create_salesforce_catalog.py`
3. Configure tables in `salesforce_config.csv`

**Deploy:**
4. Generate pipeline YAML: `python generate_salesforce_pipeline.py salesforce_config.csv`
5. Validate DAB: `cd salesforce_dab && databricks bundle validate -t dev`
6. Deploy: `databricks bundle deploy -t dev`
7. Run pipelines via Databricks UI or CLI

## Troubleshooting

### Connection Issues

**Error:** `Connection 'sfdc_dabs_pipeline_testing' not found`
**Fix:** Create Salesforce connection via Databricks UI first

**Error:** `Permission denied` for connection
**Fix:** Grant connection permissions:
```sql
GRANT USE CONNECTION ON CONNECTION sfdc_dabs_pipeline_testing TO `user@company.com`;
```

### Pipeline Deployment Fails

**Error:** `Catalog or schema not found`
**Fix:** Run `python create_salesforce_catalog.py` or update target_catalog/target_schema in CSV

**Error:** `Permission denied` for catalog
**Fix:** Grant catalog permissions:
```sql
GRANT USE CATALOG ON CATALOG salesforce_connector TO `user@company.com`;
GRANT USE SCHEMA ON SCHEMA salesforce_connector.salesforce TO `user@company.com`;
```

## Support

- Databricks Lakeflow Connect docs: https://docs.databricks.com/ingestion/lakeflow-connect/
- Unity Catalog Connections: https://docs.databricks.com/data-governance/unity-catalog/connections.html
- Salesforce OAuth: https://help.salesforce.com/articleView?id=sf.remoteaccess_oauth_username_password_flow.htm
