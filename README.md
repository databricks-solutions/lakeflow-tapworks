# Lakehouse Tapworks

A Python automation tool for generating Databricks Asset Bundle (DAB) configurations for large-scale data ingestion pipelines. Lakehouse Tapworks simplifies the process of configuring bulk data ingestion from SQL Server databases to Databricks lakehouse by automatically generating gateway and pipeline YAML files.

## Overview

Lakehouse Tapworks reads a configuration dataframe containing table mappings and automatically generates:

- **Gateway YAML files**: Define continuous streaming gateways for connecting to external SQL Server databases
- **Pipeline YAML files**: Configure Delta Live Tables (DLT) pipelines for continuous data ingestion

This tool is ideal for DataOps teams managing multiple data ingestion pipelines and looking to standardize their Databricks Asset Bundle configurations.

## Features

- **Batch Configuration**: Process multiple tables at once from a simple CSV input
- **Automatic Connection Management**: Retrieves Databricks connection IDs automatically
- **Pipeline Grouping**: Logically group tables into separate ingestion pipelines
- **Flexible Destinations**: Each table can specify its own target catalog and schema
- **Project Namespacing**: Prefix all resources with project names to avoid naming clashes across multiple projects
- **DAB Integration**: Generates DAB-compatible YAML with proper resource references
- **Continuous Ingestion**: Configured for continuous streaming pipelines
- **Flexible Cluster Configuration**: Customizable node types for compute resources
- **Infrastructure as Code**: Version-controlled pipeline configurations

## Prerequisites

- Python 3.8+
- Access to a Databricks workspace
- Databricks CLI configured with authentication
- SQL Server database connections configured in Databricks

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd lakehouse-tapworks
```

2. Install required dependencies:
```bash
pip install databricks-sdk pyyaml pandas
```

3. Configure Databricks authentication (one of the following):
```bash
# Option 1: Environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"

# Option 2: Databricks CLI profile
databricks auth login
```

## Usage

### Basic Usage

1. Create a CSV configuration file with your table mappings (see [Configuration Format](#configuration-format))

2. Run the script:
```python
from generate_dab_yaml import generate_yaml_files
from databricks.sdk import WorkspaceClient
import pandas as pd

# Initialize Databricks client
w = WorkspaceClient()

# Load your configuration
df = pd.read_csv('your_config.csv')

# Generate YAML files
generate_yaml_files(
    df=df,
    gateway_catalog='your_catalog',    # For gateway storage metadata only
    gateway_schema='your_schema',      # For gateway storage metadata only
    workspace_client=w,
    project_name='my_project',         # Prefix for all resources to avoid naming clashes
    node_type_id='m5d.large',
    driver_node_type_id='c5a.8xlarge',
    output_gateway='resources/gateways/gateways.yml',
    output_pipeline='resources/pipelines/pipelines.yml'
)

# Note: Pipelines will use target_catalog and target_schema from the CSV for each table
```

3. Deploy using Databricks Asset Bundles:
```bash
databricks bundle deploy
```

## Configuration Format

Create a CSV file with the following columns:

| Column | Description | Example |
|--------|-------------|---------|
| `source_database` | Source database name | `sales_db` |
| `source_schema` | Source schema name | `dbo` |
| `source_table_name` | Source table name | `customers` |
| `target_catalog` | Databricks catalog | `bronze` |
| `target_schema` | Databricks schema | `sales` |
| `target_table_name` | Destination table name | `customers` |
| `pipeline_group` | Groups tables into pipelines | `sales_ingestion` |
| `gateway` | Gateway identifier | `sqlserver_gateway_1` |
| `connection_name` | Databricks connection name | `sql_server_prod` |
| `schedule` | Cron schedule (optional) | `0 0 * * *` |

### Example CSV

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,pipeline_group,gateway,connection_name,schedule
sales_db,dbo,customers,bronze,sales,customers,sales_ingestion,sqlserver_gateway_1,sql_server_prod,0 0 * * *
sales_db,dbo,orders,bronze,sales,orders,sales_ingestion,sqlserver_gateway_1,sql_server_prod,0 0 * * *
inventory_db,dbo,products,silver,inventory,products,inventory_ingestion,sqlserver_gateway_2,sql_server_prod,0 0 * * *
```

In this example:
- Sales tables (`customers`, `orders`) go to `bronze.sales`
- Inventory tables (`products`) go to `silver.inventory`
- Each table can specify its own destination catalog and schema

See `examples/example_config.csv` for a complete example.

## Generated YAML Structure

All resources are prefixed with the `project_name` to ensure uniqueness across multiple projects.

### Gateway YAML
Defines streaming gateways with cluster configurations:
```yaml
resources:
  pipelines:
    my_project_pipeline_my_project_gateway_1:
      name: my_project_gateway_1
      continuous: true
      clusters:
        - num_workers: 1
          node_type_id: m5d.large
          driver_node_type_id: c5a.8xlarge
      gateway_definition:
        connection_name: sql_server_prod
        gateway_storage_catalog: bronze
        gateway_storage_schema: ingestion
        gateway_storage_name: my_project_gateway_1
        source_type: SQLSERVER
```

### Pipeline YAML
Defines ingestion pipelines with table mappings:
```yaml
resources:
  pipelines:
    my_project_pipeline_ingestion_sales:
      name: my_project_ingestion_sales
      ingestion_definition:
        ingestion_gateway_id: ${resources.pipelines.my_project_pipeline_my_project_gateway_1.id}
        objects: [...]
```

## Project Structure

```
lakehouse-tapworks/
├── generate_dab_yaml.py           # Main script
├── examples/                       # Example configurations
│   ├── example_config.csv         # Sample input
│   ├── example_gateway_file.yml   # Sample gateway output
│   └── example_pipeline_file.yml  # Sample pipeline output
├── resources/                      # Generated YAML files (gitignored)
│   ├── gateways/
│   └── pipelines/
└── README.md                       # This file
```

## How It Works

1. **Load Configuration**: Reads CSV file with table mappings
2. **Connect to Databricks**: Initializes WorkspaceClient using your credentials
3. **Retrieve Connection IDs**: Automatically looks up Databricks connection IDs from connection names
4. **Generate Gateways**: Creates gateway YAML with cluster configurations (uses gateway_catalog/gateway_schema for gateway storage metadata)
5. **Generate Pipelines**: Creates pipeline YAML with table mappings grouped by `pipeline_group` (uses target_catalog/target_schema from each row in the config)
6. **Output Files**: Writes YAML files to specified paths

**Important**: The `gateway_catalog` and `gateway_schema` parameters are only used for gateway storage locations. Each pipeline table will be sent to its own `target_catalog` and `target_schema` as specified in the configuration CSV.

## Advanced Configuration

### Project Namespacing

The `project_name` parameter is crucial when managing multiple projects in the same Databricks workspace. It prefixes all resource names and keys to prevent naming conflicts:

```python
# Project A
generate_yaml_files(
    df=df_project_a,
    gateway_catalog='bronze',
    gateway_schema='ingestion',
    workspace_client=w,
    project_name='project_a',  # All resources prefixed with 'project_a_'
    # ...
)

# Project B
generate_yaml_files(
    df=df_project_b,
    gateway_catalog='bronze',
    gateway_schema='ingestion',
    workspace_client=w,
    project_name='project_b',  # All resources prefixed with 'project_b_'
    # ...
)
```

This generates distinct resources like:
- `project_a_pipeline_gateway_1` vs `project_b_pipeline_gateway_1`
- `project_a_ingestion_sales` vs `project_b_ingestion_sales`

### Custom Cluster Sizing

Adjust cluster node types based on your workload:

```python
generate_yaml_files(
    df=df,
    gateway_catalog='your_catalog',
    gateway_schema='your_schema',
    workspace_client=w,
    project_name='my_project',
    node_type_id='m5d.xlarge',         # Larger workers
    driver_node_type_id='c5a.16xlarge', # Larger driver
    # ...
)
```

### Multiple Pipeline Groups

Group tables logically to create separate pipelines:

- Group by source system
- Group by business domain
- Group by update frequency
- Group by data sensitivity level

## Limitations

- Currently supports SQL Server as the source database type
- Requires pre-configured Databricks connections
- Assumes continuous ingestion mode (not snapshot-based)

## Contributing

Contributions are welcome! Areas for improvement:

- Add support for additional source database types (PostgreSQL, MySQL, Oracle)
- Implement CLI argument parsing
- Add unit tests
- Enhance error handling and validation
- Add support for snapshot-based ingestion
- Create logging configuration

## License

[Add your license information here]

## Support

For issues or questions, please open an issue in the repository.

## Related Documentation

- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)
- [Databricks SDK for Python](https://docs.databricks.com/dev-tools/sdk-python.html)
