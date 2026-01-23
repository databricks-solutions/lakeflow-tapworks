# SQL Server Examples

This directory contains example configurations and their generated outputs for SQL Server pipeline generation.

## Example Structure

Each example folder contains:
- `pipeline_config.csv` - Input configuration CSV
- `deployment/` - Generated Databricks Asset Bundle (DAB) files
  - `databricks.yml` - Root DAB configuration
  - `resources/` - Resource definitions
    - `gateways.yml` - Gateway configurations
    - `pipelines.yml` - Pipeline definitions
    - `jobs.yml` - Scheduled job definitions

## Available Examples

### tapworks
Complete example with all gateway configuration options specified in CSV:
- Connection name, gateway catalog/schema
- Custom worker and driver node types
- Per-pipeline schedule configuration
- Priority flags for table ordering

**Input**: `tapworks/pipeline_config.csv`
**Generated**: `tapworks/deployment/`

### simple
Minimal example without gateway configuration columns in CSV:
- Uses default values for all gateway settings
- Demonstrates gateway configuration without cluster settings

**Input**: `simple/pipeline_config.csv`
**Generated**: `simple/deployment/`

### mixed
Example with partial gateway configuration:
- Some databases have specified configurations
- Other databases use defaults
- Demonstrates CSV fallback behavior

**Input**: `mixed/pipeline_config.csv`
**Generated**: Not yet generated (run pipeline_generator.py to create)

## Running Examples

To generate DAB configurations from any example:

```bash
python pipeline_generator.py \
  --input-csv examples/tapworks/pipeline_config.csv \
  --project-name my_project \
  --workspace-host https://your-workspace.cloud.databricks.com \
  --root-path /Users/your-email/.bundle/my_project \
  --output-dir examples/tapworks/deployment
```

Or use the notebook:
```python
from pipeline_generator import run_complete_pipeline_generation

result_df = run_complete_pipeline_generation(
    input_csv='examples/tapworks/pipeline_config.csv',
    project_name='my_project',
    workspace_host='https://your-workspace.cloud.databricks.com',
    root_path='/Users/your-email/.bundle/my_project',
    output_dir='examples/tapworks/deployment'
)
```
