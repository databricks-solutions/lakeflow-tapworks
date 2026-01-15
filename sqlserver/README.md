# SQL Server Connector

Automated pipeline generation for SQL Server to Databricks ingestion using Lakeflow Connect with gateway support.

## Quick Start

### Option 1: Interactive Notebook (Recommended)

1. Upload `pipeline_setup.ipynb` to your Databricks workspace
2. Configure parameters in the notebook widgets
3. Run all cells to generate and verify pipeline configuration
4. Deploy using Databricks Asset Bundles

### Option 2: Python Script

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Prepare your input CSV (see examples/tapworks/pipeline_config.csv)

# 3. Run the generator
python pipeline_generator.py

# 4. Deploy
cd examples/tapworks/deployment
databricks bundle deploy -t prod
```

## Input CSV Format

Your CSV should include these columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `source_database` | Yes | Source SQL Server database | `sales_db` |
| `source_schema` | Yes | Source schema | `dbo` |
| `source_table_name` | Yes | Table name | `customers` |
| `target_catalog` | Yes | Target Databricks catalog | `bronze` |
| `target_schema` | Yes | Target schema | `sales` |
| `target_table_name` | Yes | Target table name | `customers` |
| `priority_flag` | No | 1 for priority, 0 for normal | `0` |
| `gateway_catalog` | No | Gateway metadata catalog | `bronze_gw` |
| `gateway_schema` | No | Gateway metadata schema | `ingestion` |
| `connection_name` | No | Databricks connection name | `sql_conn` |
| `schedule` | No | Cron schedule | `*/15 * * * *` |
| `gateway_worker_type` | No | Worker node type | `m5d.large` |
| `gateway_driver_type` | No | Driver node type | `m5d.large` |

**Example CSV:**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,priority_flag,gateway_catalog,gateway_schema,connection_name,schedule,gateway_worker_type,gateway_driver_type
db1,dbo,products,tapworks_catalog,tapworks,products,0,tapworks_catalog,tapworks,tapworks_sql_connection,*/15 * * * *,,
db1,dbo,customers,tapworks_catalog,tapworks,customers,0,tapworks_catalog,tapworks,tapworks_sql_connection,0 */6 * * *,,
db1,dbo,orders,tapworks_catalog,tapworks,orders,1,tapworks_catalog,tapworks,tapworks_sql_connection,0 */7 * * *,m5d.large,m5d.large
```

## Key Features

- **Database Isolation** - Each source database gets its own gateway
- **Automatic Load Balancing** - Groups tables into optimized pipeline configurations (default: 1000 tables/pipeline)
- **Priority Handling** - Separates priority tables into dedicated pipelines
- **Per-Pipeline Schedules** - Configure individual schedules via CSV
- **Flexible Configuration** - Optional columns can be omitted (schedule is placed before optional columns for convenience)

## Generated Output

The tool generates a Databricks Asset Bundle structure:

```
examples/{example_name}/deployment/
├── databricks.yml           # Bundle configuration
└── resources/
    ├── gateways.yml         # Gateway definitions
    ├── pipelines.yml        # Pipeline definitions
    └── jobs.yml             # Scheduled jobs
```

## Load Balancing Strategy

1. **Database Separation** - Each `source_database` gets its own gateway
2. **Priority Separation** - Tables with `priority_flag=1` go to separate pipelines
3. **Load Balancing** - Tables grouped by `max_tables_per_group` (default 1000)
4. **Sequential Assignment** - Pipeline groups assigned globally sequential IDs

## Configuration

The `pipeline_generator.py` script accepts these parameters:

- `input_csv` - Path to input CSV file
- `gateway_catalog` - Catalog for gateway metadata
- `gateway_schema` - Schema for gateway metadata
- `project_name` - Project name prefix for resources
- `max_tables_per_group` - Maximum tables per pipeline (default: 1000)
- `default_connection_name` - Default connection name (default: "conn_1")
- `default_schedule` - Default cron schedule (default: "*/15 * * * *")
- `node_type_id` - Worker node type (default: "m5d.large")
- `driver_node_type_id` - Driver node type (default: "c5a.8xlarge")
- `output_dir` - Output directory for generated files

## Examples

See the `examples/` directory for sample configurations:

- `examples/tapworks/` - Basic example with Tapworks integration
- `examples/mixed/` - Multi-database example with different configurations

## Deployment

```bash
cd examples/{example_name}/deployment
databricks bundle validate -t prod
databricks bundle deploy -t prod
```

## Prerequisites

- Databricks workspace with Unity Catalog
- SQL Server connection configured in Databricks
- Databricks CLI installed and authenticated
- Python 3.8+ with required packages

## Project Structure

```
sqlserver/
├── README.md
├── requirements.txt
├── load_balancing/
│   ├── load_balancer.py           # Load balancing logic
│   └── examples/
│       └── tapworks_config.csv    # Example input
├── deployment/
│   └── connector_settings_generator.py  # YAML generation
├── examples/
│   ├── tapworks/                  # Example 1
│   │   ├── pipeline_config.csv
│   │   └── deployment/
│   └── mixed/                     # Example 2
│       ├── pipeline_config.csv
│       └── deployment/
├── pipeline_generator.py          # Unified workflow
└── pipeline_setup.ipynb           # Interactive notebook
```

## Support

For detailed information, see the comprehensive documentation in `_legacy/README.md`.
