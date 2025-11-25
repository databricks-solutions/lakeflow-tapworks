# SQL Server Load Balancing Configuration Generator

This tool generates optimized pipeline configurations for SQL Server ingestion into Databricks, grouping tables efficiently across multiple pipelines and gateways.

## Overview

This is **Part 1** of a two-part process for SQL Server to Databricks ingestion:

1. **`generate_pipeline_config.py`** (this tool) - Groups tables into pipeline configurations
2. **`../deployment/generate_dab_yaml.py`** - Generates Databricks Asset Bundle YAML files from the configuration

## Features

- **Database Isolation**: Each source database gets its own gateway and set of pipelines
- **Load Balancing**: Automatically groups tables into pipeline groups (default: 1000 tables per group)
- **Priority Handling**: Separates priority tables (priority_flag=1) into dedicated pipeline groups
- **Configurable**: Customizable table limits, connection names, and cron schedules

## Installation

Install required dependencies:

```bash
cd sqlserver
pip install -r requirements.txt
```

## Usage

Edit the parameters in the `if __name__ == "__main__"` block at the bottom of the script:

```python
if __name__ == "__main__":
    # Example usage - modify these parameters as needed
    generate_pipeline_config(
        input_csv='examples/example_config.csv',
        output_csv='examples/output_config.csv',
        max_tables_per_group=1000,
        default_connection_name='conn_1',
        default_schedule='*/15 * * * *'
    )
```

Then run:

```bash
python generate_pipeline_config.py
```

### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `input_csv` | Yes | - | Input CSV file path with source table list |
| `output_csv` | Yes | - | Output CSV file path for pipeline configuration |
| `max_tables_per_group` | No | 1000 | Maximum tables per pipeline group |
| `default_connection_name` | No | conn_1 | Default Databricks connection name |
| `default_schedule` | No | */15 * * * * | Default cron schedule expression |

## Input CSV Format

The input CSV should contain the following columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `source_database` | Yes | Source SQL Server database name | `sales_db` |
| `source_schema` | Yes | Source schema name | `dbo` |
| `source_table_name` | Yes | Source table name | `customers` |
| `target_catalog` | Yes | Target Databricks catalog | `bronze` |
| `target_schema` | Yes | Target Databricks schema | `sales` |
| `target_table_name` | Yes | Target table name (can differ from source) | `customers` |
| `priority_flag` | No | 1 for priority tables, 0 for normal (default: 0) | `1` |

### Example Input CSV

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,priority_flag
sales_db,dbo,customers,bronze,sales,customers,0
sales_db,dbo,orders,bronze,sales,orders,1
inventory_db,dbo,products,bronze,inventory,products,0
```

## Output CSV Format

The output CSV will contain all input columns plus additional configuration columns:

| Column | Description | Example |
|--------|-------------|---------|
| `pipeline_group` | Numeric identifier for pipeline group | `1` |
| `gateway` | Gateway identifier (one per source_database) | `1` |
| `connection_name` | Databricks connection name | `conn_1` |
| `schedule` | Cron schedule expression | `*/15 * * * *` |

### Example Output CSV

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,pipeline_group,gateway,connection_name,schedule
sales_db,dbo,orders,bronze,sales,orders,1,1,conn_1,*/15 * * * *
sales_db,dbo,customers,bronze,sales,customers,2,1,conn_1,*/15 * * * *
inventory_db,dbo,products,bronze,inventory,products,3,2,conn_1,*/15 * * * *
```

## Grouping Logic

The script uses the following logic to group tables:

1. **Database-level Separation**: Each `source_database` gets its own `gateway` ID
2. **Priority Separation**: Within each database, priority tables (`priority_flag=1`) are placed in separate pipeline groups
3. **Load Balancing**: Tables are grouped into pipeline groups with a maximum of `max_tables_per_group` tables per group
4. **Sequential Assignment**: Pipeline groups are assigned sequential IDs globally across all databases

### Example Grouping

Given 2500 tables across 2 databases with a 1000 table limit:

- **Database 1** (1500 tables):
  - Gateway 1
  - Pipeline Group 1: 1000 non-priority tables
  - Pipeline Group 2: 500 non-priority tables

- **Database 2** (1000 tables, 100 priority):
  - Gateway 2
  - Pipeline Group 3: 100 priority tables
  - Pipeline Group 4: 900 non-priority tables

## Integration with Part 2: Generate DAB YAML

After generating the pipeline configuration, use it as input for the YAML generator:

```bash
# Step 1: Generate pipeline configuration
cd sqlserver/load_balancing
# Edit generate_pipeline_config.py to set your input/output paths
python generate_pipeline_config.py

# Step 2: Generate Databricks Asset Bundle YAML files
cd ../deployment
python generate_dab_yaml.py
```

The `generate_dab_yaml.py` script will:
1. Read the configuration CSV (with pipeline_group and gateway columns)
2. Create gateway YAML configurations (one per gateway)
3. Create pipeline YAML configurations (one per pipeline_group)
4. Output `resources/gateways.yml` and `resources/pipelines.yml`

### Modifying generate_dab_yaml.py for Custom Configuration

Edit the `__main__` section in `generate_dab_yaml.py` to point to your configuration:

```python
if __name__ == "__main__":
    # Load your generated config
    df = pd.read_csv('examples/my_config.csv')

    workspace_client = WorkspaceClient()

    generate_yaml_files(
        df=df,
        gateway_catalog='my_catalog',
        gateway_schema='my_schema',
        workspace_client=workspace_client,
        project_name='my_project',
        output_gateway='resources/gateways.yml',
        output_pipeline='resources/pipelines.yml'
    )
```

## Best Practices

1. **Start Conservative**: Begin with the default 1000 tables per group and adjust based on performance
2. **Use Priority Flags**: Mark time-sensitive or frequently updated tables with `priority_flag=1` to separate them
3. **Test with Small Sets**: Test your configuration with a small subset of tables before full deployment
4. **Monitor Performance**: After deployment, monitor pipeline execution times to optimize grouping
5. **Database Separation**: Keep databases with different SLAs or security requirements separate

## Troubleshooting

### "Missing required columns" Error

Ensure your input CSV contains all required columns:
- source_database
- source_schema
- source_table_name
- target_catalog
- target_schema
- target_table_name

### Too Many Pipeline Groups

If you're generating too many pipeline groups, consider:
- Increasing `max_tables_per_group` parameter
- Consolidating databases if appropriate
- Reducing the number of priority tables

### Memory Issues with Large CSVs

For very large CSV files (100k+ rows):
- Process databases in batches
- Increase the `max_tables_per_group` parameter to reduce the number of groups

## Example Workflow

Complete example from source tables to deployed pipelines:

```bash
# 1. Create your source table list (can be extracted from SQL Server)
# input_tables.csv should list all tables you want to ingest

# 2. Edit generate_pipeline_config.py to configure your parameters:
#    input_csv='input_tables.csv'
#    output_csv='../deployment/examples/production_config.csv'
#    max_tables_per_group=800
#    default_connection_name='sql_prod_connection'
#    default_schedule='0 */4 * * *'

# 3. Generate pipeline configuration
cd sqlserver/load_balancing
python generate_pipeline_config.py

# 4. Review the generated configuration
cat ../deployment/examples/production_config.csv

# 5. Generate YAML files for Databricks Asset Bundles
cd ../deployment
python generate_dab_yaml.py

# 6. Deploy using Databricks Asset Bundles CLI
databricks bundle deploy -t production
```

## Output Summary

After running the script, you'll see a summary like:

```
============================================================
SUMMARY
============================================================
Total tables processed: 2500
Total databases: 3
Total gateways: 3
Total pipeline groups: 8

Breakdown by database:
  sales_db:
    - Tables: 1200
    - Gateway: 1
    - Pipeline groups: 3
  inventory_db:
    - Tables: 800
    - Gateway: 2
    - Pipeline groups: 2
  finance_db:
    - Tables: 500
    - Gateway: 3
    - Pipeline groups: 3
============================================================
```

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [SQL Server to Databricks Migration Guide](https://docs.databricks.com/migration/index.html)

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the example files in `examples/`
3. Verify your input CSV format matches the requirements
