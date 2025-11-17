# DABs Unity Catalog Connections Investigation

## Date: November 15, 2025

## Executive Summary

**Finding:** Databricks Asset Bundles (DABs) CLI v0.228.0 **does NOT support Unity Catalog connections** as a resource type. This applies to ALL connection types, regardless of authentication method.

## Investigation Scope

Tested connection creation via DABs for three different connection types:
1. **PostgreSQL** - Username/password authentication (static credentials)
2. **Google Analytics 4 (GA4)** - Service account JSON authentication (static credentials)
3. **Salesforce** - OAuth 2.0 authentication (interactive flow)

## Test Environment

- **Databricks CLI Version:** v0.228.0
- **Workspace:** https://e2-demo-field-eng.cloud.databricks.com
- **User:** pravin.varma@databricks.com
- **Git Branch:** feature/postgres (PostgreSQL/GA4), feature/sfdc (Salesforce)

## Test Results

### PostgreSQL Connection Test

**Configuration:** `postgres/dab/resources/connections.yml`
```yaml
resources:
  connections:
    postgres_connection:
      name: ${var.postgres_connection_name}
      connection_type: POSTGRESQL
      options:
        host: "${var.postgres_host}/${var.postgres_database}"
        port: "${var.postgres_port}"
        user: "{{secrets/${var.secret_scope}/${var.postgres_username_key}}}"
        password: "{{secrets/${var.secret_scope}/${var.postgres_password_key}}}"
```

**Validation Result:**
```
Warning: unknown field: connections
  at resources
  in resources/connections.yml:24:3
```

**Outcome:** [NO] Failed - `connections` not recognized as valid resource type

### GA4 Connection Test

**Configuration:** `ga4/dab/resources/connections.yml`
```yaml
resources:
  connections:
    ga4_connection:
      name: ${var.ga4_connection_name}
      connection_type: GA4_RAW_DATA
      options:
        service_account_json: "{{secrets/${var.secret_scope}/service_account_json}}"
```

**Validation Result:**
```
Warning: unknown field: connections
  at resources
  in resources/connections.yml:24:3
```

**Outcome:** [NO] Failed - `connections` not recognized as valid resource type

### Salesforce Connection Test

**Configuration:** `salesforce_dab/resources/connections.yml`
```yaml
resources:
  connections:
    sfdc_connection:
      name: ${var.sfdc_connection_name}
      connection_type: SALESFORCE
      options:
        salesforce_url: "${var.salesforce_url}"
        client_id: "{{secrets/${var.secret_scope}/client_id}}"
        client_secret: "{{secrets/${var.secret_scope}/client_secret}}"
        username: "{{secrets/${var.secret_scope}/username}}"
        password: "{{secrets/${var.secret_scope}/password}}}"
```

**Validation Result:**
```
Warning: unknown field: connections
  at resources
  in resources/connections.yml:2:3
```

**Outcome:** [NO] Failed - `connections` not recognized as valid resource type

## Root Cause Analysis

### Initial Hypothesis (INCORRECT)
Initially hypothesized that only OAuth-based connections (Salesforce) failed due to interactive authorization requirements, while static credential connections (PostgreSQL, GA4) would work.

### Actual Root Cause (CONFIRMED)
**Databricks CLI v0.228.0 does not support `connections` as a DABs resource type.**

This is a **platform limitation**, not an authentication limitation. The DABs framework currently does not include connections in its supported resource types schema.

## Supported DABs Resource Types (CLI v0.228.0)

Based on validation results, current supported resource types include:
- [YES] `pipelines` - Delta Live Tables pipelines
- [YES] `jobs` - Databricks jobs/workflows
- [YES] `clusters` - Compute clusters
- [YES] `permissions` - Access control
- [YES] `experiments` - MLflow experiments
- [YES] `models` - MLflow models
- [YES] `model_serving_endpoints` - Model serving
- [NO] `connections` - Unity Catalog connections (NOT SUPPORTED)

## Comparison: DABs vs SDK vs UI

| Connection Type | DABs Support | SDK Support | UI Support | Authentication Method |
|----------------|--------------|-------------|------------|----------------------|
| PostgreSQL | [NO] No (CLI v0.228.0) | [YES] **Yes** (Confirmed) | [YES] Yes | Static (username/password) |
| GA4 | [NO] No (CLI v0.228.0) | [YES] **Yes** (Confirmed) | [YES] Yes | Static (service account JSON) |
| Salesforce | [NO] No (CLI v0.228.0) | [NO] No (OAuth limitation) | [YES] Yes | OAuth 2.0 (interactive flow) |
| Pipelines | [YES] Yes | [YES] Yes | [YES] Yes | N/A |
| Jobs | [YES] Yes | [YES] Yes | [YES] Yes | N/A |
| Models | [YES] Yes | [YES] Yes | [YES] Yes | N/A |

**Key Insight:** Static credential connections (PostgreSQL, GA4) CAN be created programmatically via SDK, while OAuth-based connections (Salesforce) require UI for interactive authorization.

## Current Workarounds

### Option 1: Manual UI Creation
1. Go to Databricks workspace
2. Navigate to Catalog → Connections
3. Click "Create Connection"
4. Select connection type
5. Complete authentication (password/OAuth/service account)

**Advantages:**
- Fully supported
- Works for all connection types

**Disadvantages:**
- Not infrastructure-as-code
- Manual process
- Cannot automate across environments

### Option 2: Databricks SDK (Python) - CONFIRMED WORKING

**PostgreSQL & GA4 Connections:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

w = WorkspaceClient()

# PostgreSQL - WORKS [YES]
connection = w.connections.create(
    name="postgres_connection",
    connection_type=ConnectionType.POSTGRESQL,
    options={
        "host": "localhost/postgres",
        "port": "5432",
        "user": "{{secrets/postgres_secrets/postgres_username}}",
        "password": "{{secrets/postgres_secrets/postgres_password}}"
    }
)

# GA4 - WORKS [YES]
connection = w.connections.create(
    name="ga4_connection",
    connection_type=ConnectionType.GA4_RAW_DATA,
    options={
        "service_account_json": "{{secrets/ga4_secrets/service_account_json}}"
    }
)
```

**Test Results (Confirmed November 15, 2025):**
- [YES] PostgreSQL: `postgres_sdk_verify_test` created successfully
- [YES] GA4: `ga4_sdk_verify_test` created successfully
- [NO] Salesforce: Fails (requires OAuth refresh_token from interactive flow)

**Working Scripts:**
- `postgres/create_postgres_connection.py` - Tested and working
- `ga4/create_ga4_connection.py` - Tested and working
- `salesforce_dab/scripts/create_connection.py` - Non-functional (OAuth limitation)

**Advantages:**
- [YES] Programmatic creation for static credentials
- [YES] Can be scripted and automated
- [YES] Works with secrets (no hardcoding)
- [YES] Reliable for PostgreSQL and GA4

**Disadvantages:**
- Not declarative like DABs
- Separate tooling from DABs workflow
- Still doesn't solve OAuth challenge (Salesforce)
- Must run before DABs deployment

### Option 3: Reference Pre-Created Connections in DABs

**Best Current Practice:**

1. Create connections manually (UI or SDK)
2. Use DABs to deploy pipelines/jobs that reference connections

```yaml
# databricks.yml
variables:
  postgres_connection_name:
    default: postgres_lakeflow_connection  # Reference existing
  ga4_connection_name:
    default: ga4_demo_connection  # Reference existing
  sfdc_connection_name:
    default: sfdc-pravin-dabs  # Reference existing

# resources/pipeline.yml
resources:
  pipelines:
    my_pipeline:
      ingestion_definition:
        connection_name: ${var.postgres_connection_name}
```

**Advantages:**
- Pipelines remain infrastructure-as-code
- Works with DABs workflow
- Separates connection management from pipeline management

**Disadvantages:**
- Connections not version controlled
- Manual pre-requisite step
- Environment setup complexity

## Existing Connections (pravin.varma@databricks.com)

### Production Connections
| Connection Name | Type | Status | Created Via |
|----------------|------|--------|-------------|
| `ga4_demo_connection` | GA4_RAW_DATA | [YES] Active | UI |
| `sfdc-pravin-dabs` | SALESFORCE | [YES] Active | UI |

### Test Connections (SDK Verification)
| Connection Name | Type | Status | Created Via | Purpose |
|----------------|------|--------|-------------|---------|
| `postgres_sdk_test_connection` | POSTGRESQL | [YES] Active | SDK | Initial SDK test |
| `postgres_sdk_verify_test` | POSTGRESQL | [YES] Active | SDK | Confirmation test |
| `ga4_sdk_verify_test` | GA4_RAW_DATA | [YES] Active | SDK | Confirmation test |

**Confirmation Date:** November 15, 2025
**Test Scripts Used:**
- `postgres/create_postgres_connection.py` (Working)
- `ga4/create_ga4_connection.py` (Working)

## Recommendations

### Short-Term (Current State)

1. **Document pre-requisites clearly:**
   - State that connections must be created via UI before DABs deployment
   - Provide connection creation instructions in READMEs

2. **Use variable references:**
   - Define connection names as variables in databricks.yml
   - Reference variables in pipeline/gateway definitions
   - Makes it easy to change connection names per environment

3. **Create setup scripts:**
   - Provide SDK scripts for connection creation (documentation/reference)
   - Note that Salesforce still requires UI for OAuth

4. **Maintain connections.yml for documentation:**
   - Keep connection YAML files as reference documentation
   - Clearly mark as "non-functional but kept for future compatibility"
   - Update when DABs adds support

### Long-Term (Future Improvements)

1. **Monitor Databricks releases:**
   - Watch for DABs connection support in CLI updates
   - Test immediately when support is added
   - Update documentation accordingly

2. **Feature request:**
   - Request connections support from Databricks
   - Provide use cases and priority justification
   - Reference this investigation

3. **Fallback patterns:**
   - Terraform Databricks provider may have connection support
   - Consider hybrid approach (Terraform for connections, DABs for pipelines)
   - Evaluate cost/benefit of multi-tool approach

## Documentation Updates

### Files Created
- `docs/DABS_CONNECTIONS_INVESTIGATION.md` (this file)
- `docs/SALESFORCE_DABS_INVESTIGATION.md` (Salesforce-specific details)
- `postgres/dab/resources/connections.yml` (reference only)
- `postgres/dab/examples/connections_example.yml` (documentation)
- `ga4/dab/resources/connections.yml` (reference only)
- `ga4/dab/examples/connections_example.yml` (documentation)
- `salesforce_dab/resources/connections.yml` (reference only)
- `salesforce_dab/examples/connections_example.yml` (documentation)

### Files Modified
All three databricks.yml files updated with:
- Connection name variables
- Secret scope variables
- Connection option variables (host, port, etc.)

### README Updates
All three READMEs updated with:
- DABs structure explanation
- Connection pre-requisite sections
- Manual UI creation instructions
- Variable reference patterns

## Conclusion

**Key Finding:** DABs CLI v0.228.0 does not support Unity Catalog connections as a resource type. This is a platform limitation affecting ALL connection types, regardless of authentication method.

**Impact:**
- Cannot deploy connections via DABs
- Connections must be created separately (UI or SDK)
- Infrastructure-as-code workflow is incomplete for full Lakeflow Connect setup

**Mitigation:**
- Use pre-created connections with variable references
- Document connection setup as pre-requisite
- Keep connection YAML files for future compatibility

**Future Hope:**
- Databricks may add connection support in future CLI versions
- Monitor releases and update when available
- Configuration files ready for quick migration

## Test Commands Used

```bash
# PostgreSQL validation
cd postgres/dab && databricks bundle validate -t dev

# GA4 validation
cd ga4/dab && databricks bundle validate -t dev

# Salesforce validation
cd salesforce_dab && databricks bundle validate -t dev

# Check CLI version
databricks version

# List existing connections
databricks connections list

# Get specific connection details
databricks connections get ga4_demo_connection
databricks connections get sfdc-pravin-dabs
```

## Related Documentation

- PostgreSQL: `postgres/README.md`, `postgres/dab/examples/connections_example.yml`
- GA4: `ga4/README.md`, `ga4/dab/examples/connections_example.yml`
- Salesforce: `salesforce_dab/README.md`, `salesforce_dab/examples/connections_example.yml`
- Salesforce OAuth Details: `docs/SALESFORCE_DABS_INVESTIGATION.md`

## Contact

For questions or updates: pravin.varma@databricks.com

---

**Investigation Completed:** November 15, 2025
**Last Updated:** November 15, 2025
**Status:** Conclusive - DABs connections not supported in CLI v0.228.0
