# Job Pause Status Feature

## Overview

The pipeline generation toolkit now supports controlling the pause status of generated Databricks jobs. This allows you to create jobs in a paused or unpaused state.

## Usage

### Via Default Values

Set all jobs to be paused by default:

```python
from salesforce.pipeline_generator import run_complete_pipeline_generation

default_values = {
    'pause_status': 'PAUSED'  # or 'UNPAUSED'
}

result = run_complete_pipeline_generation(
    df=input_df,
    project_name='my_project',
    output_dir='output',
    targets=targets,
    default_values=default_values
)
```

### Via Override Config

Override pause status for all jobs:

```python
override_input_config = {
    'pause_status': 'UNPAUSED'
}

result = run_complete_pipeline_generation(
    df=input_df,
    project_name='my_project',
    output_dir='output',
    targets=targets,
    override_input_config=override_input_config
)
```

### In Input CSV

You can also specify pause_status per row in your input CSV:

```csv
source_table_name,target_catalog,target_schema,connection_name,prefix,priority,schedule,pause_status
Account,bronze,sfdc,sfdc_prod,sales,1,0 */6 * * *,PAUSED
Contact,bronze,sfdc,sfdc_prod,sales,1,0 */6 * * *,UNPAUSED
```

## Valid Values

- `PAUSED` - Job will be created in paused state (will not run on schedule)
- `UNPAUSED` - Job will be created in active state (will run on schedule)

**Note:** Values are case-insensitive and will be converted to uppercase in the YAML.

## Generated YAML

When pause_status is specified, the generated job YAML will include:

```yaml
resources:
  jobs:
    job_sfdc_sales_01:
      name: SFDC Pipeline Scheduler - sales_01
      schedule:
        quartz_cron_expression: 0 0 */6 ? * * *
        timezone_id: UTC
      pause_status: PAUSED  # <-- Added field
      tasks:
        - task_key: run_sfdc_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.pipeline_sfdc_sales_01.id}
```

## Use Cases

1. **Gradual Rollout**: Create all jobs paused, then manually unpause them one by one
2. **Testing**: Deploy pipelines without triggering automatic runs
3. **Maintenance Windows**: Temporarily pause specific jobs
4. **Environment Control**: Keep dev jobs paused by default, prod jobs unpaused

## Applies To

This feature works across all connector types:
- ✅ Salesforce (SFDC)
- ✅ SQL Server
- ✅ Google Analytics 4 (GA4)

## Implementation

The feature is implemented in the shared `create_jobs()` function in `utilities/config_utils.py`, making it automatically available to all connectors.
