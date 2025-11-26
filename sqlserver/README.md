# SQL Server to Databricks Ingestion Pipeline Generator

This toolkit provides a complete solution for generating optimized SQL Server to Databricks ingestion pipelines using Databricks Asset Bundles.

## Overview

The toolkit consists of three main components:

1. **Load Balancing** (`load_balancing/`) - Groups tables into optimized pipeline configurations
2. **YAML Generation** (`deployment/`) - Creates Databricks Asset Bundle YAML files
3. **Unified Runner** (`run_pipeline_generation.py`) - Combines both steps into a single workflow

## Quick Start

### Option 1: Unified Workflow (Recommended)

Use the unified script to run the complete pipeline generation process:

```bash
cd sqlserver
python run_pipeline_generation.py
```

This single script will:
1. Load your source table list
2. Generate optimized pipeline configurations with load balancing
3. Create Databricks Asset Bundle YAML files
4. Output ready-to-deploy gateway and pipeline configurations

**Edit the script** to customize your parameters:

```python
if __name__ == "__main__":
    result_df = run_complete_pipeline_generation(
        input_csv='load_balancing/examples/example_config.csv',
        gateway_catalog='your_catalog',
        gateway_schema='your_schema',
        project_name='your_project',
        max_tables_per_group=1000,
        default_connection_name='your_connection',
        default_schedule='*/15 * * * *',
        output_gateway='deployment/resources/gateways.yml',
        output_pipeline='deployment/resources/pipelines.yml'
    )
```

### Option 2: Step-by-Step Workflow

Run each component separately for more control:

#### Step 1: Generate Pipeline Configuration

```bash
cd load_balancing
python generate_pipeline_config.py
```

This will group your tables into optimized pipeline groups based on:
- Source database isolation (each database gets its own gateway)
- Load balancing (max 1000 tables per group by default)
- Priority separation (priority tables get dedicated groups)

#### Step 2: Generate YAML Files

```bash
cd ../deployment
python generate_dab_yaml.py
```

This will create:
- `resources/gateways.yml` - Gateway configurations
- `resources/pipelines.yml` - Pipeline configurations

See individual component READMEs for detailed documentation:
- [Load Balancing Documentation](load_balancing/README.md)
- Deployment Documentation (see `deployment/generate_dab_yaml.py` docstrings)

## Architecture

### Two-Part Process

```
┌─────────────────────────────────────────────────────────────────┐
│                      INPUT: Source Table List                    │
│  (source_database, source_schema, source_table_name, etc.)      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PART 1: Load Balancing                        │
│  • Groups tables into pipeline_groups (max 1000/group)          │
│  • Assigns gateways per source_database                          │
│  • Separates priority tables                                     │
│  • Adds: pipeline_group, gateway, connection_name, schedule      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                 OUTPUT: Pipeline Configuration                   │
│     (all input columns + pipeline_group + gateway + ...)        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PART 2: YAML Generation                        │
│  • Creates gateway YAML (one per gateway)                        │
│  • Creates pipeline YAML (one per pipeline_group)                │
│  • Generates Databricks Asset Bundle resources                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     OUTPUT: YAML Files                           │
│              (gateways.yml + pipelines.yml)                      │
└─────────────────────────────────────────────────────────────────┘
```

## Input Requirements

Your input CSV should contain the following columns:

| Column | Required | Description | Example |
|--------|----------|-------------|---------|
| `source_database` | Yes | Source SQL Server database name | `sales_db` |
| `source_schema` | Yes | Source schema name | `dbo` |
| `source_table_name` | Yes | Source table name | `customers` |
| `target_catalog` | Yes | Target Databricks catalog | `bronze` |
| `target_schema` | Yes | Target Databricks schema | `sales` |
| `target_table_name` | Yes | Target table name | `customers` |
| `priority_flag` | No | 1 for priority, 0 for normal (default: 0) | `1` |

## Configuration Parameters

### Load Balancing Parameters

- **max_tables_per_group** (default: 1000) - Maximum tables per pipeline group
- **default_connection_name** (default: "conn_1") - Databricks connection name
- **default_schedule** (default: "*/15 * * * *") - Cron schedule for pipelines

### YAML Generation Parameters

- **gateway_catalog** - Catalog for gateway metadata storage
- **gateway_schema** - Schema for gateway metadata storage
- **project_name** - Prefix for all resource names (must be unique)
- **node_type_id** (default: "m5d.large") - Worker node type
- **driver_node_type_id** (default: "c5a.8xlarge") - Driver node type

## Output Files

### Intermediate Output

- `deployment/examples/generated_config.csv` - Pipeline configuration with groupings

### Final Output

- `deployment/resources/gateways.yml` - Gateway configurations for Databricks Asset Bundles
- `deployment/resources/pipelines.yml` - Pipeline configurations for Databricks Asset Bundles

## Deployment

After generating YAML files, deploy using Databricks Asset Bundles CLI:

```bash
cd deployment
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle deploy -t prod
```

## Example Workflow

Complete end-to-end example:

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Prepare your source table list (CSV)
# - Export from SQL Server metadata
# - Or manually create following the input format

# 3. Edit run_pipeline_generation.py with your parameters
# - input_csv path
# - gateway catalog/schema
# - project name
# - connection details

# 4. Run the unified generator
python run_pipeline_generation.py

# 5. Review generated files
cat deployment/resources/gateways.yml
cat deployment/resources/pipelines.yml

# 6. Deploy to Databricks
cd deployment
databricks bundle deploy -t dev
```

## Load Balancing Strategy

The load balancer automatically:

1. **Isolates databases** - Each source database gets its own gateway
2. **Groups tables** - Tables are grouped into pipeline groups (default: 1000 per group)
3. **Prioritizes tables** - Priority tables (priority_flag=1) get separate pipeline groups
4. **Optimizes resources** - Balances load across multiple pipelines for parallel execution

### Example Grouping

Given 2500 tables across 2 databases:

- **Database 1** (1500 tables)
  - Gateway 1
  - Pipeline Group 1: 1000 tables
  - Pipeline Group 2: 500 tables

- **Database 2** (1000 tables, 100 priority)
  - Gateway 2
  - Pipeline Group 3: 100 priority tables
  - Pipeline Group 4: 900 normal tables

## Best Practices

1. **Start Small** - Test with a subset of tables before full deployment
2. **Use Priority Flags** - Mark time-sensitive tables as priority
3. **Monitor Performance** - Adjust `max_tables_per_group` based on pipeline performance
4. **Unique Project Names** - Use descriptive, unique project names to avoid conflicts
5. **Version Control** - Commit generated YAML files to track changes

## Troubleshooting

### Import Errors

If you see import errors when running the unified script:
```bash
# Make sure you're in the sqlserver directory
cd sqlserver
python run_pipeline_generation.py
```

### Connection Issues

If Databricks connection fails:
```bash
# Configure Databricks CLI
databricks configure --token

# Verify connection
databricks workspace list
```

### Too Many Pipeline Groups

If generating too many groups:
- Increase `max_tables_per_group` parameter
- Consolidate databases if appropriate
- Review priority flag assignments

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [SQL Server Migration Guide](https://docs.databricks.com/migration/index.html)

## Project Structure

```
sqlserver/
├── README.md                           # This file
├── requirements.txt                    # Python dependencies
├── run_pipeline_generation.py          # Unified workflow script
├── load_balancing/
│   ├── README.md                       # Load balancing documentation
│   ├── generate_pipeline_config.py    # Load balancing logic
│   └── examples/
│       └── example_config.csv          # Example input
└── deployment/
    ├── generate_dab_yaml.py            # YAML generation logic
    ├── examples/
    │   ├── example_config.csv          # Example with groupings
    │   └── generated_config.csv        # Generated intermediate file
    └── resources/
        ├── gateways.yml                # Generated gateway config
        └── pipelines.yml               # Generated pipeline config
```
