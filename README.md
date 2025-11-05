# Pipeline Splitter

A Databricks Asset Bundle (DAB) generator that intelligently creates and distributes data ingestion pipelines and gateways based on table metadata.

## Overview

Pipeline Splitter automates the creation of Databricks DLT (Delta Live Tables) pipelines and ingestion gateways for large-scale data migrations from external sources (SQL Server, MySQL, etc.) into Unity Catalog. Instead of manually defining hundreds or thousands of pipelines, this tool generates optimized YAML configurations based on your table inventory.

### Key Features

- **Intelligent Table Distribution**: Automatically distributes tables across pipelines based on size and priority
- **Gateway Management**: Creates ingestion gateways with optimal table allocation
- **DABS-Native**: Generates resources managed by Databricks Asset Bundles for consistent deployments
- **Priority Handling**: Dedicates separate pipelines for high-priority tables
- **Load Balancing**: Evenly distributes large tables to prevent pipeline bottlenecks
- **Flexible Input**: Accepts DataFrames from any source (Unity Catalog, CSV, APIs, etc.)
- **Schema Management**: Creates and references schemas via DABS for environment flexibility

## How It Works

The generator follows this allocation strategy:

1. **Priority Tables**: Tables with `priority_flag=1` get dedicated pipelines
2. **Large Tables**: Tables above the row count threshold are distributed evenly across pipelines
3. **Small Tables**: Remaining tables fill pipelines up to the table cap
4. **Gateways**: Pipelines are assigned to gateways (max tables per gateway)

### Example Allocation

For 500 tables from one server:
- 5 priority tables → 5 dedicated pipelines
- 495 normal tables, pipeline cap of 150 → ~4 shared pipelines
- 9 total pipelines → 1 gateway (under 1000 table limit)

## Project Structure

```
pipeline_splitter/
├── databricks.yml                           # Main DABS bundle configuration
├── gateway_pipeline_dab_generator.ipynb     # Generator notebook
├── README.md                                # This file
└── resources/
    ├── schemas/
    │   └── schemas.yml                      # Schema and catalog definitions
    ├── gateways/
    │   └── gateway_*.yml                    # Generated gateway configs
    └── pipelines/
        └── pipeline_*.yml                   # Generated pipeline configs
```

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Databricks CLI** installed and authenticated
3. **External Connections** configured in Databricks (SQL Server, MySQL, etc.)
4. **Table Inventory** with metadata about source tables

## Getting Started

### Step 1: Prepare Your Table Inventory

Create a DataFrame with the following structure:

| Column Name | Type | Description | Required |
|-------------|------|-------------|----------|
| `server_name` | String | Source server name | Yes |
| `connection_name` | String | Databricks connection name | Yes |
| `database_name` | String | Source database/catalog | Yes |
| `schema_name` | String | Source schema name | Yes |
| `table_name` | String | Source table name | Yes |
| `row_count` | Integer | Number of rows (0 if unknown) | Yes |
| `priority_flag` | Integer | 1 = high priority, 0 = normal | Yes |

**Example:**
```python
# From Unity Catalog
metadata_df = spark.table("catalog.schema.table_inventory")

# From CSV
metadata_df = spark.read.csv("/path/to/inventory.csv", header=True, inferSchema=True)
```

### Step 2: Configure Schema Resources

Edit `resources/schemas/schemas.yml` to define your target catalog and schema:

```yaml
resources:
  schemas:
    final_saas_db:
      name: final_saas_db
      catalog_name: poc2_test_catalog
      comment: "Schema for final SaaS data ingestion - managed by DABS"
      properties:
        managed_by: "databricks_asset_bundle"
```

### Step 3: Run the Generator

Open `gateway_pipeline_dab_generator.ipynb` in Databricks and execute:

```python
from databricks.sdk import WorkspaceClient

result = generate_pipeline_gateway_ymls(
    metadata_df=metadata_df,
    output_name="my_ingestion",
    destination_catalog_default="poc2_test_catalog",
    destination_schema_default="final_saas_db",
    source_type_default="SQLSERVER",
    pipeline_table_cap=150,           # Max tables per pipeline
    gateway_table_cap=1000,           # Max tables per gateway
    large_table_threshold=50_000_000, # Rows to consider "large"
    output_base_dir="resources",
    use_dabs_references=True,         # Use DABS schema references
    debug=True
)
```

This generates:
- `resources/gateways/gateway_my_ingestion.yml`
- `resources/pipelines/pipeline_my_ingestion.yml`

### Step 4: Deploy with DABS

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy -t dev

# Run pipelines (optional)
databricks bundle run -t dev
```

## Configuration Options

### Generator Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `metadata_df` | DataFrame | Required | Input DataFrame with table metadata |
| `output_name` | str | `"generated"` | Base name for output YAML files |
| `destination_catalog_default` | str | `"poc2_test_catalog"` | Target catalog name or DABS key |
| `destination_schema_default` | str | `"final_saas_db"` | Target schema name or DABS key |
| `source_type_default` | str | `"SQLSERVER"` | Source system type |
| `pipeline_table_cap` | int | `150` | Maximum tables per pipeline |
| `gateway_table_cap` | int | `1000` | Maximum tables per gateway |
| `large_table_threshold` | int | `50_000_000` | Row count threshold for large tables |
| `cdc_applier_timeout_seconds` | str | `"600"` | CDC applier timeout |
| `cluster_node_type_id` | str | `"m5d.large"` | Worker node type |
| `cluster_driver_node_type_id` | str | `"c5a.8xlarge"` | Driver node type |
| `cluster_num_workers` | int | `1` | Number of workers per cluster |
| `output_base_dir` | str | `"resources"` | Output directory for YAML files |
| `use_dabs_references` | bool | `True` | Use DABS schema references |
| `work_client` | WorkspaceClient | `None` | Optional pre-configured client |
| `debug` | bool | `False` | Enable debug output |

### DABS Bundle Configuration

Edit `databricks.yml` to customize deployment settings:

```yaml
bundle:
  name: final_saas_bundle

targets:
  dev:
    default: true
    mode: development
    presets:
      source_linked_deployment: true

  prod:
    mode: production
    workspace:
      host: https://your-workspace.cloud.databricks.com

include:
  - resources/*.yml
  - resources/*/*.yml
```

## Use Cases

### Scenario 1: Large-Scale Migration
Migrating 2,000 tables from multiple SQL Server instances:
```python
metadata_df = spark.table("migration.metadata.all_tables")

result = generate_pipeline_gateway_ymls(
    metadata_df=metadata_df,
    output_name="sql_migration",
    pipeline_table_cap=100,      # Smaller pipelines for manageability
    gateway_table_cap=500,       # More gateways for parallelism
    large_table_threshold=100_000_000
)
```

### Scenario 2: Incremental Updates
Adding new tables to existing ingestion:
```python
new_tables_df = spark.sql("""
    SELECT * FROM migration.metadata.all_tables
    WHERE added_date >= current_date()
""")

result = generate_pipeline_gateway_ymls(
    metadata_df=new_tables_df,
    output_name="incremental_20250105"
)
```

### Scenario 3: Environment-Specific Deployment
Using DABS references for multi-environment deployments:
```python
# Same code works in dev, staging, prod
result = generate_pipeline_gateway_ymls(
    metadata_df=metadata_df,
    destination_catalog_default="my_catalog",
    destination_schema_default="my_schema",
    use_dabs_references=True  # DABS handles environment differences
)
```

## Troubleshooting

### Connection Not Found Warning
```
⚠️ Warning: Connection 'my_connection' not found in workspace connections.
```
**Solution**: Create the connection in your Databricks workspace first:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.connections.create(name="my_connection", connection_type="SQLSERVER", ...)
```

### Bundle Validation Errors
```bash
databricks bundle validate
```
**Solution**: Check that:
- All referenced schemas exist in `resources/schemas/schemas.yml`
- Connection IDs are valid
- YAML syntax is correct

### Pipeline Deployment Failures
**Solution**:
- Verify catalog and schema exist in Unity Catalog
- Check that service principal/user has appropriate permissions
- Review Databricks workspace logs

## Advanced Topics

### Custom Schema Per Server
Modify the generator to route different servers to different schemas:
```python
# In the generator, customize the rows mapping
rows.append({
    ...
    "destination_schema": f"schema_{server_slug}",
    ...
})
```

### Dynamic Connection Resolution
Use metadata to specify different connections per server:
```python
# Add connection_name column per table in your inventory
metadata_df = spark.sql("""
    SELECT *,
        CASE
            WHEN server_name LIKE 'PROD%' THEN 'prod_connection'
            WHEN server_name LIKE 'DEV%' THEN 'dev_connection'
        END as connection_name
    FROM ...
""")
```

## Contributing

Suggestions for improvements:
1. Add support for other source types (PostgreSQL, Oracle, etc.)
2. Implement pipeline dependency management
3. Add monitoring and alerting configurations
4. Create Terraform/ARM templates for Azure resources

## License

MIT License - feel free to modify and distribute

## Support

For issues or questions:
- Check Databricks Asset Bundle documentation: https://docs.databricks.com/dev-tools/bundles/
- Review DLT ingestion gateway docs: https://docs.databricks.com/delta-live-tables/
- Open an issue in this repository
