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
    target_catalog='your_catalog',
    target_schema='your_schema',
    worker_node_type='m5d.large',
    driver_node_type='c5a.8xlarge',
    gateway_output_path='resources/gateways/gateways.yml',
    pipeline_output_path='resources/pipelines/pipelines.yml',
    w=w
)
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
inventory_db,dbo,products,bronze,inventory,products,inventory_ingestion,sqlserver_gateway_2,sql_server_prod,0 0 * * *
```

See `examples/example_config.csv` for a complete example.

## Generated YAML Structure

### Gateway YAML
Defines streaming gateways with cluster configurations:
```yaml
resources:
  pipelines:
    gateway_name:
      name: gateway_name
      continuous: true
      cluster:
        - num_workers: 2
          node_type_id: m5d.large
          driver_node_type_id: c5a.8xlarge
          spark_conf: {...}
```

### Pipeline YAML
Defines ingestion pipelines with table mappings:
```yaml
resources:
  pipelines:
    pipeline_name:
      name: pipeline_name
      catalog: target_catalog
      target: target_schema
      ingestion_definition:
        connection_name: connection_name
        ingestion_gateway_id: ${resources.pipelines.gateway_name.id}
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
4. **Generate Gateways**: Creates gateway YAML with cluster configurations
5. **Generate Pipelines**: Creates pipeline YAML with table mappings grouped by `pipeline_group`
6. **Output Files**: Writes YAML files to specified paths

## Advanced Configuration

### Custom Cluster Sizing

Adjust cluster node types based on your workload:

```python
generate_yaml_files(
    df=df,
    target_catalog='your_catalog',
    target_schema='your_schema',
    worker_node_type='m5d.xlarge',      # Larger workers
    driver_node_type='c5a.16xlarge',    # Larger driver
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
