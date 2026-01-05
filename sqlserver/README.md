# SQL Server to Databricks Ingestion Pipeline Generator

This toolkit provides a complete solution for generating optimized SQL Server to Databricks ingestion pipelines using Databricks Asset Bundles.

## Overview

The toolkit automates the creation of load-balanced, production-ready ingestion pipelines with intelligent table grouping and resource optimization.

### Components

1. **Load Balancing** (`load_balancing/generate_pipeline_config.py`) - Groups tables into optimized pipeline configurations
2. **YAML Generation** (`deployment/generate_dab_yaml.py`) - Creates Databricks Asset Bundle YAML files
3. **Unified Runner** (`run_pipeline_generation.py`) - Combines both steps into a single workflow

### Key Features

- **Database Isolation**: Each source database gets its own gateway and set of pipelines
- **Automatic Load Balancing**: Groups tables efficiently (default: 1000 tables per pipeline group)
- **Priority Handling**: Separates priority tables into dedicated pipeline groups
- **Fully Configurable**: Customizable table limits, connection names, schedules, and cluster configurations

## Quick Start

### Option 1: Unified Workflow (Recommended)

#### Option 1a: Python Script

Use the unified script to run the complete pipeline generation process:

```bash
cd sqlserver
python run_pipeline_generation.py
```

#### Option 1b: Databricks Notebook

Upload `run_pipeline_generation.ipynb` to your Databricks workspace and run it interactively. The notebook provides:
- Interactive widgets for configuration
- Step-by-step execution with visual feedback
- Built-in summary statistics and file verification

This single workflow will:
1. Load your source table list
2. Generate optimized pipeline configurations with load balancing
3. Create Databricks Asset Bundle YAML files
4. Output ready-to-deploy gateway and pipeline configurations

**Edit the script/notebook** to customize your parameters:

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
        output_dir='dab_project'  # Configurable output directory for DAB project
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

This will create a proper Databricks Asset Bundle structure:
```
dab_project/
├── databricks.yml           # Main bundle configuration
└── resources/
    ├── gateways.yml         # Gateway configurations
    └── pipelines.yml        # Pipeline configurations
```

### Option 3: Programmatic Usage

Import and use the methods directly in your own Python scripts:

```python
import pandas as pd
from databricks.sdk import WorkspaceClient
from load_balancing.generate_pipeline_config import generate_pipeline_config
from deployment.generate_dab_yaml import generate_yaml_files

# Load and transform data
input_df = pd.read_csv('your_tables.csv')

# Step 1: Generate pipeline configuration
config_df = generate_pipeline_config(
    df=input_df,
    max_tables_per_group=1000,
    default_connection_name='your_connection',
    default_schedule='*/15 * * * *'
)

# Optional: Save intermediate config
config_df.to_csv('pipeline_config.csv', index=False)

# Step 2: Generate YAML files in proper DAB structure
workspace_client = WorkspaceClient()
generate_yaml_files(
    df=config_df,
    gateway_catalog='your_catalog',
    gateway_schema='your_schema',
    workspace_client=workspace_client,
    project_name='your_project',
    output_dir='dab_project'  # Creates proper DAB directory structure
)
```

## Installation

Install required dependencies:

```bash
cd sqlserver
pip install -r requirements.txt
```

Required packages:
- `pandas>=2.0.0` - Data manipulation
- `pyyaml>=6.0` - YAML file generation
- `databricks-sdk>=0.20.0` - Databricks API integration

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

### Input CSV Format

Your input CSV should contain the following columns:

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

### Output CSV Format

After load balancing, the output CSV will contain all input columns plus these additional configuration columns:

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

## API Reference

### generate_pipeline_config()

```python
def generate_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_group: int = 1000,
    default_connection_name: str = "conn_1",
    default_schedule: str = "*/15 * * * *"
) -> pd.DataFrame:
```

**Description**: Groups source tables into optimized pipeline configurations with load balancing.

**Parameters**:
- `df` (pd.DataFrame): Input dataframe with source table information
- `max_tables_per_group` (int): Maximum tables per pipeline group
- `default_connection_name` (str): Default Databricks connection name
- `default_schedule` (str): Default cron schedule expression

**Returns**: DataFrame with pipeline_group, gateway, connection_name, and schedule columns added

**Location**: `load_balancing/generate_pipeline_config.py`

### generate_yaml_files()

```python
def generate_yaml_files(
    df: pd.DataFrame,
    gateway_catalog: str,
    gateway_schema: str,
    workspace_client: WorkspaceClient,
    project_name: str,
    node_type_id: str = 'm5d.large',
    driver_node_type_id: str = 'c5a.8xlarge',
    output_dir: str = 'dab_project'
) -> None:
```

**Description**: Generates Databricks Asset Bundle YAML files from pipeline configuration in a proper DAB structure.

**Parameters**:
- `df` (pd.DataFrame): Pipeline configuration dataframe (output from generate_pipeline_config)
- `gateway_catalog` (str): Catalog for gateway storage metadata
- `gateway_schema` (str): Schema for gateway storage metadata
- `workspace_client` (WorkspaceClient): Databricks workspace client instance
- `project_name` (str): Project name prefix for all resources
- `node_type_id` (str): Worker node type for clusters
- `driver_node_type_id` (str): Driver node type for clusters
- `output_dir` (str): Output directory for DAB project (creates `databricks.yml` and `resources/` subdirectory)

**Returns**: None (writes YAML files to disk in proper DAB structure)

**Location**: `deployment/generate_dab_yaml.py`

### run_complete_pipeline_generation()

```python
def run_complete_pipeline_generation(
    input_csv: str,
    gateway_catalog: str,
    gateway_schema: str,
    project_name: str,
    max_tables_per_group: int = 1000,
    default_connection_name: str = "conn_1",
    default_schedule: str = "*/15 * * * *",
    node_type_id: str = "m5d.large",
    driver_node_type_id: str = "c5a.8xlarge",
    output_dir: str = "dab_project"
) -> pd.DataFrame:
```

**Description**: Complete end-to-end pipeline generation combining both load balancing and YAML generation.

**Parameters**: Combination of all parameters from the two methods above

**Returns**: Pipeline configuration DataFrame

**Location**: `run_pipeline_generation.py`

## Output Files

### Intermediate Output

- `deployment/examples/generated_config.csv` - Pipeline configuration with groupings

### Final Output (Databricks Asset Bundle Structure)

The tool generates a proper Databricks Asset Bundle structure:

```
dab_project/                              # Configurable output directory
├── databricks.yml                        # Main bundle configuration (includes resources/*.yml)
└── resources/                            # Resources subdirectory
    ├── gateways.yml                      # Gateway configurations
    └── pipelines.yml                     # Pipeline configurations
```

The `databricks.yml` file uses the `include: ['resources/*.yml']` pattern to automatically include all resource definitions from the resources directory.

## Deployment

After generating the DAB project structure, deploy using Databricks Asset Bundles CLI:

```bash
cd dab_project  # Or your configured output directory
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

### Grouping Logic

The load balancer uses the following logic to intelligently group tables:

1. **Database-level Separation**: Each `source_database` gets its own `gateway` ID to ensure isolation and independent scaling
2. **Priority Separation**: Within each database, priority tables (`priority_flag=1`) are placed in separate pipeline groups for time-sensitive data
3. **Load Balancing**: Tables are grouped into pipeline groups with a maximum of `max_tables_per_group` tables per group
4. **Sequential Assignment**: Pipeline groups are assigned sequential IDs globally across all databases

### Example Grouping Scenarios

#### Scenario 1: Simple Database Split

Given 2500 tables across 2 databases with a 1000 table limit:

- **Database 1** (1500 tables)
  - Gateway 1
  - Pipeline Group 1: 1000 tables
  - Pipeline Group 2: 500 tables

- **Database 2** (1000 tables)
  - Gateway 2
  - Pipeline Group 3: 1000 tables

#### Scenario 2: Priority Table Handling

Given 2500 tables across 2 databases with 100 priority tables and a 1000 table limit:

- **Database 1** (1500 tables)
  - Gateway 1
  - Pipeline Group 1: 1000 non-priority tables
  - Pipeline Group 2: 500 non-priority tables

- **Database 2** (1000 tables, 100 priority)
  - Gateway 2
  - Pipeline Group 3: 100 priority tables (separate group)
  - Pipeline Group 4: 900 non-priority tables

### Output Summary Example

After running the load balancing script, you'll see a summary like:

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

## Best Practices

1. **Start Conservative**: Begin with the default 1000 tables per group and adjust based on performance metrics
2. **Use Priority Flags**: Mark time-sensitive or frequently updated tables with `priority_flag=1` to separate them into dedicated pipeline groups
3. **Test with Small Sets**: Test your configuration with a small subset of tables before full deployment
4. **Monitor Performance**: After deployment, monitor pipeline execution times and resource utilization to optimize grouping
5. **Database Separation**: Keep databases with different SLAs or security requirements separate
6. **Unique Project Names**: Use descriptive, unique project names to avoid resource naming conflicts
7. **Version Control**: Commit generated YAML files to Git to track changes and enable rollback
8. **Resource Planning**: Consider your Databricks cluster capacity when setting `max_tables_per_group`

## Troubleshooting

### "Missing required columns" Error

**Problem**: Error message about missing required columns in input CSV

**Solution**: Ensure your input CSV contains all required columns:
- `source_database`
- `source_schema`
- `source_table_name`
- `target_catalog`
- `target_schema`
- `target_table_name`

### Import Errors

**Problem**: Import errors when running the unified script

**Solution**: Make sure you're in the correct directory:
```bash
# Make sure you're in the sqlserver directory
cd sqlserver
python run_pipeline_generation.py
```

### Connection Issues

**Problem**: Databricks workspace client connection fails

**Solution**: Configure Databricks CLI authentication:
```bash
# Configure Databricks CLI with token authentication
databricks configure --token

# Verify connection works
databricks workspace list
```

### Too Many Pipeline Groups

**Problem**: Generating an excessive number of pipeline groups

**Solutions**:
- Increase `max_tables_per_group` parameter (e.g., from 1000 to 2000)
- Consolidate databases if they have similar characteristics
- Reduce the number of priority tables (only mark truly time-sensitive tables)
- Consider if all tables need to be ingested in the same project

### Memory Issues with Large CSVs

**Problem**: Out of memory errors when processing very large CSV files (100k+ rows)

**Solutions**:
- Process databases in batches by filtering the input CSV
- Increase the `max_tables_per_group` parameter to reduce the number of groups
- Use a machine with more memory for the generation process
- Consider splitting your ingestion project into multiple smaller projects

### Gateway Connection Lookup Fails

**Problem**: Error when looking up connection ID from connection name

**Solution**: Verify the connection exists in your Databricks workspace:
```bash
# List all available connections
databricks connections list

# Verify your connection name matches exactly
databricks connections get <connection_name>
```

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
