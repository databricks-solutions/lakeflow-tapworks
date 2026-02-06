# Google Analytics 4 Examples

This directory contains example configurations and their generated outputs for GA4 pipeline generation.

## Example Structure

Each example folder contains:
- `pipeline_config.csv` - Input configuration CSV
- `deployment/` - Generated Databricks Asset Bundle (DAB) files
  - `resources/` - Resource definitions
    - `ga4_pipeline.yml` - Pipeline and job definitions with variables

## Available Examples

### basic
Basic GA4 configuration example:
- Multiple GA4 properties grouped by business unit
- Prefix + subgroup grouping strategy
- Standard GA4 tables (events, events_intraday, users)
- Per-pipeline schedule configuration

**Input**: `basic/pipeline_config.csv`
**Generated**: Not yet generated (run pipeline_generator.py to create)

## Running Examples

To generate DAB configurations from any example:

```bash
python pipeline_generator.py \
  --input-csv examples/basic/pipeline_config.csv \
  --project-name ga4_project \
  --workspace-host https://your-workspace.cloud.databricks.com \
  --output-dir examples/basic/deployment
```

Or use the notebook:
```python
from pipeline_generator import run_complete_pipeline_generation

result_df = run_complete_pipeline_generation(
    input_csv='examples/basic/pipeline_config.csv',
    project_name='ga4_project',
    workspace_host='https://your-workspace.cloud.databricks.com',
    output_dir='examples/basic/deployment'
)
```

## CSV Format

GA4 CSV requires these columns:
- `source_catalog` (GCP project ID)
- `source_schema` (GA4 property, e.g., "analytics_123456789")
- `tables` (comma-separated, e.g., "events,events_intraday,users")
- `target_catalog`, `target_schema`
- `prefix`, `subgroup` (for grouping)
- `schedule` (optional)

## Pipeline Grouping

Properties are grouped by unique (prefix, subgroup) combinations:
- `prefix="business_unit1", subgroup="01"` → pipeline `business_unit1_01`
- `prefix="business_unit1", subgroup="02"` → pipeline `business_unit1_02`
