# Connector Development Guide

**Version:** 1.0
**Last Updated:** 2026-01-16
**Purpose:** Template and standards for implementing new Databricks Lakeflow Connect connectors

> **See also:** [Connector Architecture Overview](CONNECTOR_OVERVIEW.md) - For an overview of connector architecture and types

---

## Table of Contents

1. [Standard Directory Structure](#standard-directory-structure)
2. [Standard Function Names](#standard-function-names)
3. [Configuration Standards](#configuration-standards)
4. [Implementation Steps](#implementation-steps)
5. [Examples](#examples)
6. [Testing Guidelines](#testing-guidelines)
7. [Documentation Requirements](#documentation-requirements)
8. [Checklist for New Connectors](#checklist-for-new-connectors)

---

## Standard Directory Structure

### Required Structure

```
{connector_name}/
├── README.md                           # Connector-specific documentation
├── requirements.txt                    # Python dependencies
│
├── load_balancing/                     # STEP 1: Load balancing module
│   └── load_balancer.py               # Main load balancing logic
│
├── deployment/                         # STEP 2: YAML generation module
│   ├── connector_settings_generator.py # Main YAML generator
│   ├── setup_{connector}_connection.py    # Connection setup helper (optional)
│   └── create_{connector}_catalog.py      # Catalog creation helper (optional)
│
├── examples/                           # Example configurations and outputs
│   ├── README.md                      # Examples documentation
│   ├── {example1}/
│   │   ├── pipeline_config.csv        # Input CSV configuration
│   │   └── deployment/                # Generated DAB output
│   │       ├── databricks.yml
│   │       └── resources/
│   │           ├── pipelines.yml (or connector-specific naming)
│   │           ├── gateways.yml (SQL Server only)
│   │           └── jobs.yml
│   └── {example2}/
│       └── ...
│
├── pipeline_generator.py              # Unified runner (combines Steps 1+2)
└── pipeline_setup.ipynb               # Interactive notebook showing tool usage

```


---

## Standard Function Names

### 1. Load Balancing Module (`load_balancing/load_balancer.py`)

#### Function: `generate_pipeline_config()`

**Standard Signature:**
```python
def generate_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_group: int = 1000,              # Database only
    default_connection_name: str = "conn_1",
    default_schedule: str = "*/15 * * * *"
) -> pd.DataFrame:
    """
    Generate pipeline configuration with load balancing.

    Args:
        df: Input DataFrame with source table/object list
        max_tables_per_group: Maximum tables per pipeline (database only)
        default_connection_name: Default connection name
        default_schedule: Default cron schedule

    Returns:
        DataFrame with pipeline_group column added
    """
```

**Required Input Columns (Database):**
- `source_database` - Source database name
- `source_schema` - Schema name
- `source_table_name` - Table name
- `target_catalog` - Target Databricks catalog
- `target_schema` - Target Databricks schema
- `target_table_name` - Destination table name

**Required Input Columns (SaaS):**
- `source_schema` - Source schema (optional, e.g., "standard", "custom")
- `source_table_name` - Object/table name
- `target_catalog` - Target Databricks catalog
- `target_schema` - Target Databricks schema
- `target_table_name` - Destination table name
- `prefix` - Pipeline prefix (for grouping)
- `priority` - Priority level (e.g., "01", "02")

**Output Columns:**
- All input columns +
- `pipeline_group` - Pipeline group identifier (string or int)
- `gateway` - Gateway ID (database only)
- `connection_name` - Connection name
- `schedule` - Cron schedule

---

### 2. YAML Generation Module (`deployment/connector_settings_generator.py`)

#### Function: `generate_yaml_files()`

**Standard Signature (Database):**
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
    """
    Generate Databricks Asset Bundle YAML files for database connector.

    Creates proper DAB structure:
    - databricks.yml (root config)
    - resources/gateways.yml
    - resources/pipelines.yml
    """
```

**Standard Signature (SaaS):**
```python
def generate_yaml_files(
    df: pd.DataFrame,
    connection_name: str,
    output_path: str = "resources/{connector}_pipeline.yml"
) -> None:
    """
    Generate Databricks Asset Bundle YAML file for SaaS connector.

    Creates single YAML with:
    - Variables section
    - Pipeline resources
    - Job resources (scheduled)
    """
```

**Helper Function: `convert_cron_to_quartz()`**

```python
def convert_cron_to_quartz(cron_expression: str) -> str:
    """
    Convert standard 5-field cron to Quartz 6-field cron format.

    Standard cron: minute hour day month day-of-week
    Quartz cron:   second minute hour day month day-of-week

    Examples:
        */15 * * * *  → 0 */15 * * * ?
        0 */6 * * *   → 0 0 */6 * * ?
    """
```

---

### 3. Unified Runner (`pipeline_generator.py`)

#### Function: `run_complete_pipeline_generation()`

**Standard Signature (Database):**
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
    """
    Complete pipeline generation: load balancing + YAML generation.
    """
```

**Standard Signature (SaaS):**
```python
def run_complete_pipeline_generation(
    input_csv: str,
    default_connection_name: str = "{connector}_connection",
    default_schedule: str = "*/15 * * * *",
    output_yaml: str = "deployment/resources/{connector}_pipeline.yml",
    output_config: str = "deployment/examples/generated_config.csv"
) -> pd.DataFrame:
    """
    Complete pipeline generation: load balancing + YAML generation.
    """
```

---

## Configuration Standards

### 1. requirements.txt

**Minimum Required:**
```txt
pandas>=2.0.0
pyyaml>=6.0
databricks-sdk>=0.20.0
```

**Database Connector Additions:**
```txt
# No additional requirements
```

**SaaS Connector Additions:**
```txt
# Connector-specific SDK if needed
# simple-salesforce>=1.12.0  # Example for SFDC
```

---

### 2. Input CSV Schema

#### Database Connector CSV

**Minimum Required Columns:**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name
db1,dbo,customers,bronze,sales,customers
db1,dbo,orders,bronze,sales,orders
```

**With Optional Columns:**
```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,gateway_catalog,gateway_schema,connection_name,schedule,gateway_worker_type,gateway_driver_type
db1,dbo,customers,bronze,sales,customers,bronze_gw,ingestion,sql_conn,*/15 * * * *,,
db1,dbo,orders,bronze,sales,orders,bronze_gw,ingestion,sql_conn,*/15 * * * *,m5d.large,m5d.large
```

**Note:** Schedule column is placed after connection_name so users can omit trailing optional columns (gateway_worker_type, gateway_driver_type) without adding empty commas.

#### SaaS Connector CSV

**Minimum Required Columns:**
```csv
source_schema,source_table_name,target_catalog,target_schema,target_table_name,prefix,priority
standard,Account,bronze,sfdc,Account,sales,01
standard,Contact,bronze,sfdc,Contact,sales,01
```

**With Optional Columns:**
```csv
source_schema,source_table_name,target_catalog,target_schema,target_table_name,prefix,priority,connection_name,schedule,include_columns,exclude_columns
standard,Account,bronze,sfdc,Account,sales,01,sfdc_conn,*/15 * * * *,"Id,Name,Type",
standard,Contact,bronze,sfdc,Contact,sales,01,sfdc_conn,*/15 * * * *,,"CreatedDate,LastModifiedDate"
```

---

### 3. Output YAML Structure

#### Database Connector Output

**File: `dab_project/databricks.yml`**
```yaml
bundle:
  name: {project_name}

include:
  - resources/*.yml

targets:
  prod:
    mode: production
    workspace:
      host: ${workspace.host}
```

**File: `dab_project/resources/gateways.yml`**
```yaml
resources:
  pipelines:
    {project}_pipeline_{project}_gateway_1:
      name: {project}_gateway_1
      clusters:
        - node_type_id: m5d.large
          driver_node_type_id: c5a.8xlarge
          num_workers: 1
      gateway_definition:
        connection_name: {connection_name}
        connection_id: {connection_id}
        gateway_storage_catalog: {catalog}
        gateway_storage_schema: {schema}
        gateway_storage_name: {project}_gateway_1
        source_type: {SOURCE_TYPE}  # SQLSERVER, MYSQL, POSTGRESQL, etc.
      continuous: true
      catalog: {catalog}
```

**File: `dab_project/resources/pipelines.yml`**
```yaml
resources:
  pipelines:
    {project}_pipeline_ingestion_1:
      name: {project}_ingestion_1
      configuration:
        pipelines.cdcApplierFetchMetadataTimeoutSeconds: '600'
      ingestion_definition:
        ingestion_gateway_id: ${resources.pipelines.{project}_pipeline_{project}_gateway_1.id}
        objects:
          - table:
              source_catalog: {source_database}
              source_schema: {source_schema}
              source_table: {source_table}
              destination_catalog: {target_catalog}
              destination_schema: {target_schema}
```

#### SaaS Connector Output

**File: `deployment/resources/{connector}_pipeline.yml`**
```yaml
variables:
  dest_catalog:
    default: {target_catalog}
  dest_schema:
    default: {target_schema}
  {connector}_connection_name:
    default: {connection_name}

resources:
  pipelines:
    pipeline_{connector}_ingestion_{group}:
      name: {connector}_ingestion_pipeline_{group}
      catalog: ${var.dest_catalog}
      ingestion_definition:
        connection_name: ${var.{connector}_connection_name}
        objects:
          - table:
              source_schema: objects  # or specific schema
              source_table: {source_table}
              destination_catalog: ${var.dest_catalog}
              destination_schema: ${var.dest_schema}
              table_configuration:  # Optional
                include_columns:
                  - {column1}
                  - {column2}

  jobs:
    job_{connector}_pipeline_{group}:
      name: {connector}_pipeline_scheduler_{group}
      schedule:
        quartz_cron_expression: 0 */15 * * * ?
        timezone_id: UTC
      tasks:
        - task_key: run_{connector}_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.pipeline_{connector}_ingestion_{group}.id}
```

---

## Implementation Steps

### Step 1: Setup Project Structure

```bash
# Create connector directory
mkdir -p {connector_name}/{load_balancing,deployment,notebooks}
mkdir -p {connector_name}/load_balancing/examples
mkdir -p {connector_name}/deployment/{examples,resources}

# Create required files
touch {connector_name}/README.md
touch {connector_name}/requirements.txt
touch {connector_name}/load_balancing/generate_pipeline_config.py
touch {connector_name}/deployment/generate_dab_yaml.py
touch {connector_name}/run_pipeline_generation.py
```

### Step 2: Implement Load Balancing

**For Database Connectors:**
1. Read CSV with source tables
2. Group by `(prefix, subgroup)` combinations
3. Split groups exceeding max_tables_per_gateway into multiple gateways
4. Split gateways exceeding max_tables_per_pipeline into multiple pipelines
5. Assign `gateway` and `pipeline_group` IDs with format `prefix_subgroup_gw01_g01`

**For SaaS Connectors:**
1. Read CSV with source objects
2. Group by `(prefix, subgroup)` combinations
3. Split groups exceeding max_tables_per_pipeline into multiple pipelines
4. Generate `pipeline_group = {prefix}_{subgroup}_g01`
5. No gateway assignment needed

**Template:** See `sqlserver/load_balancing/load_balancer.py` (database) or `salesforce/load_balancing/load_balancer.py` (SaaS)

### Step 3: Implement YAML Generation

**For Database Connectors:**
1. Create `create_gateways()` function - generates gateway YAML
2. Create `create_pipelines()` function - generates pipeline YAML
3. Create `create_databricks_yml()` function - generates root config
4. Write all three files to `output_dir/`

**For SaaS Connectors:**
1. Group by `pipeline_group`
2. Create pipeline definitions with ingestion objects
3. Add scheduled jobs with Quartz cron
4. Write single YAML file

**Template:** See `sqlserver/deployment/connector_settings_generator.py` (database) or `salesforce/deployment/connector_settings_generator.py` (SaaS)

### Step 4: Create Unified Runner

Combine both steps into single workflow:

```python
def run_complete_pipeline_generation(...):
    # Step 1: Load input CSV
    input_df = pd.read_csv(input_csv)

    # Step 2: Generate pipeline config (load balancing)
    pipeline_config_df = generate_pipeline_config(df=input_df, ...)

    # Step 3: Initialize Databricks client (database only)
    workspace_client = WorkspaceClient()  # Database only

    # Step 4: Generate YAML files
    generate_yaml_files(df=pipeline_config_df, ...)

    return pipeline_config_df
```

### Step 5: Add Examples

Create example configurations:
- `load_balancing/examples/example_config.csv`
- `deployment/examples/example_config.csv`
- `deployment/examples/example_databricks.yml`
- `deployment/examples/example_pipeline.yml`

### Step 6: Documentation

Create comprehensive `README.md` covering:
- Overview and architecture
- Prerequisites
- Installation
- Usage (all options)
- Configuration
- Deployment
- Troubleshooting

---

## Examples

### Database Connector Example (SQL Server)

**Directory Structure:**
```
sqlserver/
├── README.md
├── requirements.txt
├── load_balancing/
│   ├── load_balancer.py
│   └── examples/
│       └── tapworks_config.csv
├── deployment/
│   └── connector_settings_generator.py
├── examples/
│   └── tapworks/
│       ├── pipeline_config.csv
│       └── deployment/
│           ├── databricks.yml
│           └── resources/
│               ├── gateways.yml
│               ├── pipelines.yml
│               └── jobs.yml
├── pipeline_generator.py
└── pipeline_setup.ipynb
```

**Key Characteristics:**
- Gateways: Yes (one per source database)
- Load balancing: Automatic (max 1000 tables/pipeline)
- Grouping: By database + priority
- Output: `dab_project/databricks.yml` + `resources/{gateways,pipelines}.yml`

**Example Usage:**
```python
run_complete_pipeline_generation(
    input_csv='tables.csv',
    gateway_catalog='bronze',
    gateway_schema='ingestion',
    project_name='sqlserver_project',
    max_tables_per_group=1000,
    output_dir='dab_project'
)
```

---

### SaaS Connector Example (Salesforce)

**Directory Structure:**
```
salesforce/
├── README.md
├── requirements.txt
├── load_balancing/
│   ├── load_balancer.py
│   └── examples/
│       └── salesforce_config.csv
├── deployment/
│   └── connector_settings_generator.py
├── examples/
│   └── sales/
│       ├── pipeline_config.csv
│       └── deployment/
│           ├── databricks.yml
│           └── resources/
│               ├── salesforce_pipeline.yml
│               └── jobs.yml
├── pipeline_generator.py
└── pipeline_setup.ipynb
```

**Key Characteristics:**
- Gateways: No (OAuth direct)
- Load balancing: Manual (prefix + priority)
- Grouping: User-defined via CSV
- Output: Single `sfdc_pipeline.yml`

**Example Usage:**
```python
run_complete_pipeline_generation(
    input_csv='objects.csv',
    default_connection_name='sfdc_connection',
    output_yaml='deployment/resources/sfdc_pipeline.yml'
)
```

---

## Testing Guidelines

### Unit Tests

Test each component independently:

```python
# Test load balancing
def test_generate_pipeline_config():
    df = pd.DataFrame({...})
    result = generate_pipeline_config(df, max_tables_per_group=10)
    assert 'pipeline_group' in result.columns
    assert result['pipeline_group'].nunique() > 0

# Test YAML generation
def test_generate_yaml_files():
    df = pd.DataFrame({...})
    generate_yaml_files(df, ...)
    assert os.path.exists('output.yml')
```

### Integration Tests

Test end-to-end workflow:

```python
def test_complete_pipeline_generation():
    result = run_complete_pipeline_generation(
        input_csv='test_input.csv',
        ...
    )
    assert os.path.exists('output_dir/databricks.yml')
    assert os.path.exists('output_dir/resources/gateways.yml')
    assert os.path.exists('output_dir/resources/pipelines.yml')
```

### Validation Tests

Validate generated YAML:

```bash
# Validate DAB bundle
databricks bundle validate -t dev

# Test deployment (dry run)
databricks bundle deploy -t dev --dry-run
```

---

## Documentation Requirements

### README.md Structure

Every connector must have a comprehensive README:

```markdown
# {Connector Name} Connector

## Overview
Brief description of connector purpose and capabilities

## Architecture
Diagram showing data flow

## Prerequisites
- Software requirements
- Credentials needed
- Access requirements

## Installation
Step-by-step setup

## Usage
### Option 1: Unified Workflow
### Option 2: Step-by-Step
### Option 3: Programmatic

## Configuration
### Input CSV Schema
### Configuration Options

## Deployment
Step-by-step deployment guide

## Troubleshooting
Common issues and solutions

## Examples
Real-world examples
```

---

## Checklist for New Connectors

Use this checklist when implementing a new connector:

### Structure
- [ ] Created `{connector}/load_balancing/load_balancer.py`
- [ ] Created `{connector}/deployment/connector_settings_generator.py`
- [ ] Created `{connector}/pipeline_generator.py`
- [ ] Created `{connector}/pipeline_setup.ipynb`
- [ ] Created `requirements.txt`
- [ ] Created example configurations

### Functions
- [ ] Implemented `generate_pipeline_config()` with standard signature
- [ ] Implemented `generate_yaml_files()` with standard signature
- [ ] Implemented `run_complete_pipeline_generation()`
- [ ] Implemented `convert_cron_to_quartz()` helper

### Configuration
- [ ] Defined input CSV schema
- [ ] Created example input CSV
- [ ] Created example output YAML
- [ ] Documented all configuration options

### Testing
- [ ] Unit tests for load balancing
- [ ] Unit tests for YAML generation
- [ ] Integration test for complete workflow
- [ ] Validated with `databricks bundle validate`
- [ ] Tested deployment to dev environment

### Documentation
- [ ] Created comprehensive README.md
- [ ] Documented prerequisites
- [ ] Provided usage examples
- [ ] Added troubleshooting section
- [ ] Included deployment instructions

---

## Contributing

When adding a new connector:

1. **Follow this guide** - Use templates and naming conventions
2. **Reference existing connectors** - SQL Server (database) or Salesforce (SaaS)
3. **Test thoroughly** - Unit, integration, and deployment tests
4. **Document completely** - README with all sections
5. **Submit PR** - Include summary of what was implemented

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-01-09 | Initial template based on SQL Server, Salesforce, and Google Analytics 4 implementations |

---

## Contact

For questions or clarifications on this guide, please open an issue in the repository.
