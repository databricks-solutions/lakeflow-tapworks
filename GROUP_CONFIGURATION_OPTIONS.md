# Group-Based Configuration

> **Status:** Implemented in `feature/group_based_overrides` branch

This document describes per-group defaults and overrides, allowing different configuration values to be applied to different pipeline groups from a single CSV file.

## Overview

Both `default_values` and `override_config` support per-group configuration using a nested dict format. The matching is based on `prefix` (if specified in CSV) or `project_name` (fallback).

## Format

### Simple (Global) Format
```python
# Applied to all rows
default_values = {'schedule': '*/15 * * * *', 'pause_status': 'UNPAUSED'}
```

This is automatically normalized to:
```python
{'*': {'schedule': '*/15 * * * *', 'pause_status': 'UNPAUSED'}}
```

### Grouped Format
```python
default_values = {
    '*': {'schedule': '*/15 * * * *'},           # Global default
    'sales': {'schedule': '*/30 * * * *'},       # Matches prefix='sales'
    'sales_02': {'schedule': '0 * * * *'},       # Matches pipeline_group='sales_02' (most specific)
    'hr_project': {'schedule': '0 0 * * *'},     # Matches project_name='hr_project'
}

override_config = {
    '*': {'pause_status': 'UNPAUSED'},           # Global override
    'finance': {'pause_status': 'PAUSED'},       # Matches prefix='finance'
    'finance_01': {'pause_status': 'UNPAUSED'},  # Matches pipeline_group='finance_01'
}
```

## Matching Logic

### Match Key Resolution
For each row, the config key is matched in this order:
1. `pipeline_group` (`{prefix}_{subgroup}`) - most specific
2. `prefix` column
3. `project_name` column - fallback

### Precedence
More specific matches take precedence:
1. `pipeline_group` match (e.g., `sales_02`)
2. `prefix` match (e.g., `sales`)
3. `project_name` match
4. Global (`*`)

## Processing Flow

1. **Normalize configs** - Wrap flat dicts in `'*'` key
2. **Apply global defaults** - Ensures `project_name` exists for matching
3. **Determine match key** - Use `prefix` if exists, else `project_name`
4. **Apply group-specific defaults** - Only fills missing/empty values
5. **Apply global overrides** - Overwrites all matching rows
6. **Apply group-specific overrides** - Overwrites matching rows

## Example

### CSV Input
```csv
project_name,source_table_name,target_catalog,target_schema,target_table_name,connection_name
sales_project,Account,main,bronze,account,conn1
sales_project,Contact,main,bronze,contact,conn1
hr_project,Employee,main,bronze,employee,conn1
finance_project,Invoice,main,bronze,invoice,conn1
```

### Configuration
```python
default_values = {
    '*': {'schedule': '*/15 * * * *'},
    'sales_project': {'schedule': '*/30 * * * *'},
    'hr_project': {'schedule': '0 * * * *'},
}

override_config = {
    '*': {'pause_status': 'UNPAUSED'},
    'finance_project': {'pause_status': 'PAUSED'},
}
```

### Result

| project_name | schedule | pause_status |
|--------------|----------|--------------|
| sales_project | `*/30 * * * *` (group) | `UNPAUSED` (global) |
| sales_project | `*/30 * * * *` (group) | `UNPAUSED` (global) |
| hr_project | `0 * * * *` (group) | `UNPAUSED` (global) |
| finance_project | `*/15 * * * *` (global) | `PAUSED` (group) |

## Usage

```python
from core.runner import run_pipeline_generation

result = run_pipeline_generation(
    connector_name='salesforce',
    input_source='config.csv',
    output_dir='./output',
    targets={'dev': {'workspace_host': 'https://...'}},
    default_values={
        '*': {'schedule': '*/15 * * * *'},
        'sales_project': {'schedule': '*/30 * * * *'},
    },
    override_config={
        '*': {'pause_status': 'UNPAUSED'},
        'finance_project': {'pause_status': 'PAUSED'},
    },
)
```
