# Salesforce Examples

This directory contains example configurations and their generated outputs for Salesforce pipeline generation.

## Example Structure

Each example folder contains:
- `pipeline_config.csv` - Input configuration CSV
- `deployment/` - Generated Databricks Asset Bundle (DAB) files
  - `databricks.yml` - Root DAB configuration with variables
  - `resources/` - Resource definitions
    - `sfdc_pipeline.yml` - Pipeline and job definitions

## Available Examples

### tapworks
Real-world Salesforce configuration example:
- Multiple Salesforce objects grouped by business unit
- Prefix + priority grouping strategy
- Per-pipeline schedule configuration
- Includes column filtering (include_columns, exclude_columns)

**Input**: `tapworks/pipeline_config.csv`
**Generated**: `tapworks/deployment/`

### basic
Simple Salesforce example:
- Basic object ingestion
- Demonstrates prefix+priority grouping
- Default schedule for all pipelines

**Input**: `basic/pipeline_config.csv`
**Generated**: Not yet generated (run pipeline_generator.py to create)

## Running Examples

To generate DAB configurations from any example:

```bash
python pipeline_generator.py \
  --input-csv examples/tapworks/pipeline_config.csv \
  --project-name sfdc_project \
  --default-connection-name sfdc_connection \
  --workspace-host https://your-workspace.cloud.databricks.com \
  --output-dir examples/tapworks/deployment
```

Or use the notebook:
```python
from pipeline_generator import run_complete_pipeline_generation

result_df = run_complete_pipeline_generation(
    input_csv='examples/tapworks/pipeline_config.csv',
    project_name='sfdc_project',
    default_connection_name='sfdc_connection',
    workspace_host='https://your-workspace.cloud.databricks.com',
    output_dir='examples/tapworks/deployment'
)
```

## CSV Format

Salesforce CSV requires these columns:
- `source_database`, `source_schema`, `source_table_name`
- `target_catalog`, `target_schema`, `target_table_name`
- `prefix`, `priority` (for grouping)
- `connection_name` (optional)
- `schedule` (optional)
- `include_columns`, `exclude_columns` (optional)
