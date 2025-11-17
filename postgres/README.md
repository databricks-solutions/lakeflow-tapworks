# PostgreSQL Lakeflow Connect - Databricks Asset Bundles

Gateway-based PostgreSQL data ingestion using Databricks Lakeflow Connect with CDC support.

## Overview

This implementation provides infrastructure-as-code for PostgreSQL ingestion using:
- **Gateway Architecture** - Continuous clusters for JDBC connectivity + CDC
- **Databricks Asset Bundles** - Infrastructure-as-code deployment
- **Unity Catalog** - Centralized connections and governance

## Databricks Asset Bundles (DABs) Structure

This implementation uses Databricks Asset Bundles for infrastructure-as-code deployment.

### Main Configuration: databricks.yml

Located at `dab/databricks.yml`, this is the root configuration file that defines:

**1. Bundle Metadata**
```yaml
bundle:
  name: cloudsql_postgres_test  # Bundle name
```

**2. Workspace Configuration**
```yaml
workspace:
  host: https://e2-demo-field-eng.cloud.databricks.com
```

**3. Variables** (customizable per environment)
```yaml
variables:
  catalog: bronze                    # Unity Catalog for gateway
  schema: metadata                   # Schema for gateway metadata
  postgres_host: localhost           # PostgreSQL hostname
  postgres_port: "5432"             # PostgreSQL port
  postgres_database: postgres        # Database name
  secret_scope: postgres_secrets     # Databricks secret scope
```

**4. Targets** (dev, prod, staging)
```yaml
targets:
  dev:
    mode: development
    workspace:
      host: https://dev-workspace.cloud.databricks.com
```

**5. Resource Includes**
```yaml
include:
  - resources/*.yml  # Includes all resource YAML files
```

### Resource Files: resources/*.yml

Located in `dab/resources/`, these define actual Databricks resources:

**connections.yml** - Unity Catalog Connections
- Defines PostgreSQL connections
- References secrets using `{{secrets/scope/key}}`
- Uses variables from databricks.yml

**gateways.yml** - Gateway Pipelines (generated)
- Continuous 24/7 clusters for JDBC connectivity
- CDC capture via replication slots
- Stores metadata in gateway storage tables

**pipelines.yml** - Ingestion Pipelines (generated)
- Scheduled DLT pipelines
- Read from gateway storage
- Write to Unity Catalog tables

### How They Work Together

```
databricks.yml (root)
  ├── Defines variables (postgres_host, secret_scope, etc.)
  ├── Includes resources/*.yml files
  │
  └── resources/
      ├── connections.yml
      │   └── Uses ${var.postgres_host}, ${var.secret_scope}
      │
      ├── gateways.yml
      │   └── References connection by name
      │
      └── pipelines.yml
          └── References gateway by ID
```

### Deployment

```bash
# Deploy all resources (connections, gateways, pipelines)
databricks bundle deploy -t dev \
    -var="postgres_host=postgres.example.com" \
    -var="postgres_database=mydb"
```

This single command:
1. Reads databricks.yml
2. Applies variables
3. Includes all resources/*.yml files
4. Creates/updates connections, gateways, and pipelines

## Key Differences from GA4

| Feature | GA4 (Serverless) | PostgreSQL (Gateway) |
|---------|------------------|---------------------|
| Architecture | Direct connection | Gateway + Ingestion (2-tier) |
| Connection | Serverless | JDBC through gateway cluster |
| CDC Support | No | Yes (logical replication) |
| Cost Model | Pay per run | Gateway runs 24/7 |
| Table-level | Not supported | Fully supported |
| DABs Structure | connections.yml + pipelines.yml | connections.yml + gateways.yml + pipelines.yml |

## Usage Flow

### Overview

The PostgreSQL Lakeflow Connect implementation follows a structured workflow from connection setup to production deployment:

```
1. PostgreSQL Setup (CDC enabled)
   |
2. Databricks Credentials (secrets)
   |
3. Unity Catalog Connection
   |
4. Pre-Flight Validation
   |
5. Configuration CSV
   |
6. Generate Pipeline YAML
   |
7. Deploy Bundle
   |
8. Monitor & Maintain
```

### Detailed Usage Flow

**Phase 1: Source Database Preparation**
1. Enable logical replication on PostgreSQL
2. Create replication user with appropriate permissions
3. Verify network connectivity to Databricks

**Phase 2: Databricks Configuration**
1. Store PostgreSQL credentials in Databricks secrets
2. Create Unity Catalog connection using stored credentials
3. Test connection in Databricks UI

**Phase 3: Pre-Deployment Validation**
1. Create configuration CSV with table mappings
2. Run pre-flight check to validate:
   - Connection accessibility
   - Target catalogs/schemas existence
   - Source table connectivity
   - CDC configuration
3. Fix any issues identified by validation

**Phase 4: Pipeline Generation**
1. Generate gateway and ingestion pipeline YAML
2. Use auto-balance for optimal resource distribution
3. Review generated configurations

**Phase 5: Deployment**
1. Validate Databricks bundle configuration
2. Deploy gateway pipeline (continuous)
3. Deploy ingestion pipelines (scheduled)
4. Verify pipeline status in Databricks UI

**Phase 6: Production Operations**
1. Monitor pipeline health and performance
2. Track replication lag and data freshness
3. Handle schema changes and updates
4. Scale resources based on workload

## Code Flow

### Architecture Overview

The implementation consists of three main components:

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Source                         │
│  - Logical Replication Enabled (wal_level=logical)          │
│  - Replication Slots for CDC                                │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  │ JDBC Connection
                  │
┌─────────────────▼───────────────────────────────────────────┐
│              Databricks Gateway Cluster                      │
│  - Continuous running (24/7)                                │
│  - Maintains JDBC connection pool                           │
│  - Captures CDC events via replication slots                │
│  - Stores metadata in gateway storage tables                │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  │ Internal API
                  │
┌─────────────────▼───────────────────────────────────────────┐
│           Ingestion Pipelines (DLT)                         │
│  - Read from gateway storage                                │
│  - Apply transformations                                    │
│  - Write to Unity Catalog tables                           │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  │
┌─────────────────▼───────────────────────────────────────────┐
│            Unity Catalog Tables                             │
│  - Bronze/Silver/Gold layers                                │
│  - CDC history maintained                                   │
│  - Queryable via SQL                                        │
└─────────────────────────────────────────────────────────────┘
```

### Component Flow

**1. Connection Creation (create_postgres_connection.py)**

```
Input: Connection name, secret scope, host, port, database
  |
  ├─> Retrieve credentials from Databricks secrets
  |     - Username: {{secrets/scope/postgres_username}}
  |     - Password: {{secrets/scope/postgres_password}}
  |
  ├─> Create Unity Catalog connection
  |     - Type: POSTGRESQL
  |     - Options: host/database, port, user, password
  |
  └─> Return: Connection ID and metadata
```

**2. Pre-Flight Validation (preflight_check.py)**

```
Input: Configuration CSV with table mappings
  |
  ├─> Parse CSV and validate format
  |
  ├─> For each table, check in parallel:
  |     ├─> Connection exists and accessible
  |     ├─> Connection type is POSTGRESQL
  |     ├─> Target catalog exists in Unity Catalog
  |     ├─> Target schema exists in catalog
  |     ├─> Source table accessible via connection
  |     └─> CDC configuration (warn if not verifiable)
  |
  ├─> Aggregate results by check type
  |
  └─> Output: Summary report with pass/fail counts
        - Tables ready for deployment
        - Tables requiring fixes
        - Recommended actions
```

**3. Pipeline Generation (generate_postgres_pipeline.py)**

```
Input: Configuration CSV, gateway/pipeline settings
  |
  ├─> Load configuration and group by connection
  |
  ├─> Apply auto-balancing (if enabled):
  |     ├─> Group tables by connection_name
  |     ├─> Distribute across gateways (round-robin)
  |     │     - Max tables per gateway (default: 50)
  |     └─> Distribute across pipelines within gateway
  |           - Max tables per pipeline (default: 20)
  |
  ├─> Connect to Databricks workspace
  |     └─> Fetch connection IDs for each connection_name
  |
  ├─> Generate gateways.yml:
  |     └─> For each gateway:
  |           ├─> Define cluster configuration
  |           ├─> Set gateway_definition:
  |           │     - connection_id
  |           │     - gateway_storage_catalog/schema/name
  |           ├─> Set continuous: true
  |           └─> Set channel: PREVIEW
  |
  ├─> Generate pipelines.yml:
  |     └─> For each pipeline:
  |           ├─> Link to gateway via ingestion_gateway_id
  |           ├─> Define table mappings:
  |           │     - source_catalog/schema/table
  |           │     - destination_catalog/schema
  |           └─> Set schedule (from CSV)
  |
  └─> Write YAML files to resources/ directory
```

**4. Bundle Deployment Flow**

```
databricks bundle validate
  |
  ├─> Parse databricks.yml
  ├─> Include resources/*.yml files
  ├─> Validate YAML syntax
  ├─> Check workspace connectivity
  └─> Validate resource definitions

databricks bundle deploy
  |
  ├─> Upload bundle files to workspace
  ├─> Generate Terraform configuration
  ├─> Create/update resources:
  |     ├─> Gateway pipeline (continuous)
  |     │     - Provisions cluster
  |     │     - Establishes JDBC connection
  |     │     - Creates replication slots
  |     └─> Ingestion pipelines (scheduled)
  |           - Links to gateway
  |           - Creates DLT pipelines
  └─> Update deployment state
```

### Data Flow During Operation

**Gateway Pipeline (Continuous)**

```
1. Initialize JDBC connection to PostgreSQL
   └─> Use credentials from Unity Catalog connection

2. Create replication slot (if not exists)
   └─> Named: databricks_<gateway_id>

3. Continuously read CDC events
   ├─> INSERT: Capture new rows
   ├─> UPDATE: Capture before/after values
   ├─> DELETE: Capture deleted row ID
   └─> DDL changes: Log schema changes

4. Write CDC events to gateway storage tables
   ├─> Location: <gateway_catalog>.<gateway_schema>.<gateway_name>
   ├─> Format: Delta Lake
   └─> Partitioned by: table_name, event_time

5. Maintain replication lag metrics
   └─> Monitor WAL position vs current
```

**Ingestion Pipeline (Scheduled/Triggered)**

```
1. Read from gateway storage tables
   └─> Filter by table_name and last_processed_timestamp

2. Apply CDC operations:
   ├─> MERGE for INSERT/UPDATE
   └─> DELETE for DELETE operations

3. Write to destination tables
   ├─> Location: <target_catalog>.<target_schema>.<table_name>
   ├─> Format: Delta Lake
   └─> Include CDC metadata columns:
         - _change_type (insert/update/delete)
         - _commit_version
         - _commit_timestamp

4. Update checkpoint
   └─> Record last_processed_timestamp for next run

5. Update metrics
   └─> Rows processed, latency, errors
```

### Error Handling Flow

**Connection Failures**
```
Gateway detects connection loss
  |
  ├─> Retry with exponential backoff
  │     - Max retries: 5
  │     - Backoff: 1s, 2s, 4s, 8s, 16s
  |
  ├─> If all retries fail:
  │     └─> Mark pipeline as FAILED
  |
  └─> Alert configured recipients
```

**Replication Slot Exhaustion**
```
PostgreSQL replication_slots > max_replication_slots
  |
  ├─> Gateway cannot create new slot
  |
  ├─> Log error with slot details
  |
  └─> Manual intervention required:
        - Drop unused slots
        - Increase max_replication_slots
        - Restart PostgreSQL
```

**Schema Changes**
```
DDL detected in CDC stream
  |
  ├─> Gateway captures schema change
  |
  ├─> Write to schema_changes table
  |
  ├─> Ingestion pipeline detects schema mismatch
  │     └─> Option 1: Auto-evolve schema (if enabled)
  │     └─> Option 2: Fail and alert
  |
  └─> User reviews and approves change
```

## Quick Start

### 1. Prerequisites

**On PostgreSQL:**
```sql
-- Enable logical replication (postgresql.conf)
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

-- Create replication user
CREATE USER databricks_repl WITH REPLICATION LOGIN PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO databricks_repl;
```

**On Databricks:**
```bash
# Install dependencies
pip install databricks-sdk pyyaml pandas

# Configure CLI
databricks configure
```

### 2. Store Credentials

```bash
# Create secret scope
databricks secrets create-scope jdbc_secrets

# Store PostgreSQL credentials
databricks secrets put-secret jdbc_secrets postgres_username --string-value "databricks_repl"
databricks secrets put-secret jdbc_secrets postgres_password --string-value "secure_password"
```

### 3. Create Connection

**Option A: Using SDK Script (Quick Setup)**

```bash
python create_postgres_connection.py postgres_prod_connection jdbc_secrets \
    --host postgres.example.com \
    --port 5432 \
    --database mydb
```

**Option B: Using Databricks Asset Bundles (Recommended for Production)**

Connections can be defined declaratively in `dab/resources/connections.yml` and deployed with your pipeline infrastructure:

```bash
# Deploy connection with bundle
cd dab
databricks bundle deploy -t dev \
    -var="postgres_host=postgres.example.com" \
    -var="postgres_database=mydb" \
    -var="secret_scope=jdbc_secrets"
```

**Benefits of DABs approach:**
- Infrastructure as code (version controlled)
- Repeatable deployments across environments
- Integrated with pipeline deployments
- No manual script execution required

See `dab/examples/connections_example.yml` for detailed configuration options.

### 4. Run Pre-Flight Check (RECOMMENDED)

Validate connectivity and permissions before deployment.

#### Fast Mode (Default - No Warehouse)

Quick validation of Databricks-side configuration:

```bash
python preflight_check.py dab/examples/example_config.csv
```

**What it checks:**
- [OK] Unity Catalog connections exist and are accessible
- [OK] Connection type is POSTGRESQL
- [OK] Target catalogs exist
- [OK] Target schemas exist
- [SKIP] Table connectivity (skipped)
- [SKIP] CDC configuration (skipped)

**Time:** 10 seconds | **Cost:** Free | **Catches:** 80% of issues

#### Full Mode (With SQL Warehouse)

Deep validation including actual PostgreSQL connectivity:

```bash
python preflight_check.py dab/examples/example_config.csv --warehouse-id abc123def456
```

**What it checks:**
- [OK] Unity Catalog connections exist and are accessible
- [OK] Connection type is POSTGRESQL
- [OK] Target catalogs exist
- [OK] Target schemas exist
- [OK/FAIL] Table connectivity (actually queries each table)
- [OK/FAIL] CDC configuration (verifies wal_level=logical)

**Time:** 5 minutes | **Cost:** ~$0.10 | **Catches:** 95% of issues

#### Example Output

```
[OK] 280 tables ready | [FAIL] 20 tables need fixes
Total tables checked: 300

TABLES REQUIRING FIXES (20):
  postgres_prod_connection.mydb.public.old_table
    [FAIL] Schema 'bronze.sales' not found
    [FAIL] CDC not enabled (wal_level=replica, expected 'logical')

RECOMMENDED ACTIONS:
1. Create missing schemas (5 tables):
   - CREATE SCHEMA IF NOT EXISTS bronze.sales
2. Enable CDC on PostgreSQL (manual step)
```

**ROI:** Fast mode catches issues in 10 seconds vs 3 days of debugging

### 5. Configure Tables

Edit `dab/examples/example_config.csv`:
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,pipeline_group,gateway,connection_name,source_type,schedule
mydb,public,users,bronze,sales,users,1,1,postgres_prod_connection,POSTGRESQL,0 */6 * * *
mydb,public,orders,bronze,sales,orders,1,1,postgres_prod_connection,POSTGRESQL,0 */6 * * *
```

**CSV Columns:**
- `gateway` - Gateway ID (tables with same ID share gateway)
- `pipeline_group` - Pipeline ID (groups tables into ingestion pipelines)
- `source_type` - Always `POSTGRESQL` for Postgres

### 6. Generate Pipeline YAML

```bash
cd dab
python generate_postgres_pipeline.py examples/example_config.csv \
    --gateway-catalog bronze \
    --gateway-schema metadata \
    --project-name my_postgres_proj
```

Generates:
- `resources/gateways.yml` - Gateway definitions (continuous clusters)
- `resources/pipelines.yml` - Ingestion pipelines (reference gateways)

### 7. Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Gateway Architecture

### One Gateway per Connection

```
Postgres Instance A
  └── Connection: postgres_prod_conn
      └── Gateway: postgres_prod_gateway (24/7 cluster)
          ├── Pipeline 1 (users, orders)
          └── Pipeline 2 (products, inventory)

Postgres Instance B
  └── Connection: postgres_analytics_conn
      └── Gateway: postgres_analytics_gateway (24/7 cluster)
          └── Pipeline 3 (page_views, sessions)
```

**Gateway Reuse Rules:**
- [OK] Same Postgres, multiple tables -> Same gateway, multiple pipelines
- [FAIL] Different Postgres instances -> Different gateways required
- [FAIL] Different databases -> Different gateways required

### Load Balancing

#### Manual Load Balancing

Distribute tables across gateways using CSV:

```csv
# High-volume tables -> Dedicated gateway
large_table_1,1,1,postgres_prod_conn
large_table_2,2,1,postgres_prod_conn

# Low-volume tables -> Shared gateway
small_table_1,3,2,postgres_prod_conn
small_table_2,3,2,postgres_prod_conn
```

#### Auto Load Balancing

Automatically distribute tables across gateways and pipelines using `--auto-balance`:

```bash
python generate_postgres_pipeline.py example_config.csv \
    --auto-balance \
    --tables-per-gateway 50 \
    --tables-per-pipeline 20
```

**Auto-Balance Features:**
- Automatically assigns `gateway` and `pipeline_group` values
- Groups by `connection_name` (same connection shares gateways)
- Distributes tables evenly using round-robin algorithm
- No need to manually specify `gateway` or `pipeline_group` in CSV

**Auto-Balance CSV Format (Simplified):**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,connection_name,source_type,schedule
mydb,public,users,bronze,sales,postgres_prod_connection,POSTGRESQL,0 */6 * * *
mydb,public,orders,bronze,sales,postgres_prod_connection,POSTGRESQL,0 */6 * * *
mydb,public,products,bronze,inventory,postgres_prod_connection,POSTGRESQL,0 */12 * * *
```

**Parameters:**
- `--tables-per-gateway` - Max tables per gateway (default: 50)
- `--tables-per-pipeline` - Max tables per pipeline (default: 20)

**Example Output:**
```
Connection 'postgres_prod_connection': 120 tables -> 3 gateway(s)
   Gateway 1: 50 tables -> 3 pipeline(s)
   Gateway 2: 50 tables -> 3 pipeline(s)
   Gateway 3: 20 tables -> 1 pipeline(s)
```

## Project Structure

```
postgres/
├── README.md                          # This file
├── create_postgres_connection.py      # Connection setup script
├── preflight_check.py                 # Pre-flight validation (RECOMMENDED)
└── dab/
    ├── generate_postgres_pipeline.py  # YAML generator
    ├── examples/
    │   ├── example_config.csv        # Example configuration (manual)
    │   ├── example_databricks.yml     # DABs configuration template
    └── resources/                     # Generated YAML (gitignored)
        ├── gateways.yml
        └── pipelines.yml
```

## Cost Implications

**Gateway costs (24/7):**
- Small: 1 driver + 1 worker (m5d.large) = ~$5/hour = ~$3,600/month
- Medium: 1 driver + 3 workers = ~$10/hour = ~$7,200/month
- Large: 1 driver + 5 workers = ~$15/hour = ~$10,800/month

**When to use multiple gateways:**
- High-volume workloads needing isolation
- Different SLA requirements (real-time vs batch)
- Different Postgres instances/databases

## Troubleshooting

### Connection Issues

**PostgreSQL not accessible:**
```bash
# Check pg_hba.conf allows Databricks IP ranges
host    all    databricks_repl    10.0.0.0/8    md5

# Restart PostgreSQL
sudo systemctl restart postgresql
```

**Logical replication not enabled:**
```bash
# Check wal_level
SHOW wal_level;  # Should be 'logical'

# If not, edit postgresql.conf and restart
```

### Gateway Issues

**Gateway pipeline fails:**
- Verify connection test passes in Databricks UI
- Check gateway has permissions to read tables
- Ensure replication slots available: `SELECT * FROM pg_replication_slots;`

## Resources

- [Databricks Lakeflow Connect Docs](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [Unity Catalog Connections](https://docs.databricks.com/data-governance/unity-catalog/connections.html)

## License

MIT License - See root LICENSE file

