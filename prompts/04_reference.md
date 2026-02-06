# Reference Guide

Quick reference for connector development.

## CSV Column Reference

### Common Columns (All Connectors)

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `project_name` | No | `{connector}_ingestion` | DAB project name |
| `prefix` | No | `project_name` | Grouping prefix |
| `subgroup` | No | `01` | Sub-grouping within prefix |
| `schedule` | No | Connector-specific | Cron schedule (5-field) |
| `target_catalog` | Yes | - | Unity Catalog destination |
| `target_schema` | Yes | - | Schema destination |
| `target_table_name` | Yes | - | Table destination |
| `connection_name` | Yes | - | UC connection name |

### Database Connector Columns

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `source_database` | Yes | - | Source database name |
| `source_schema` | Yes | - | Source schema name |
| `source_table_name` | Yes | - | Source table name |
| `gateway_catalog` | No | `target_catalog` | Gateway storage catalog |
| `gateway_schema` | No | `target_schema` | Gateway storage schema |
| `gateway_driver_type` | No | Connector default | Gateway driver node type |
| `gateway_worker_type` | No | Connector default | Gateway worker node type |

### SaaS Connector Columns

Varies by connector. Common patterns:

| Column | Description |
|--------|-------------|
| `include_columns` | Columns to include (Salesforce) |
| `exclude_columns` | Columns to exclude (Salesforce) |
| `property_id` | GA4 property identifier |
| `scd_type` | SCD type if supported |

## Load Balancing Rules

### Grouping

Tables are grouped by `{prefix}_{subgroup}`:
- `prefix` defaults to `project_name` if empty
- `subgroup` defaults to `01` if all empty in a prefix
- Mixed subgroups (some explicit, some empty) in same prefix = error

### Splitting

Groups are split when they exceed capacity limits:

**Database connectors:**
```
prefix_subgroup (e.g., sales_01)
    ↓ split by max_tables_per_gateway (default: 250)
gateway (e.g., sales_01_gw01, sales_01_gw02)
    ↓ split by max_tables_per_pipeline (default: 250)
pipeline_group (e.g., sales_01_gw01_g01)
```

**SaaS connectors:**
```
prefix_subgroup (e.g., sales_01)
    ↓ split by max_tables_per_pipeline (default: 250)
pipeline_group (e.g., sales_01_g01, sales_01_g02)
```

## Resource Naming Convention

For `pipeline_group = "sales_01_g01"`:

| Resource | Name |
|----------|------|
| Pipeline display | `Ingestion - sales_01_g01` |
| Pipeline resource ID | `pipeline_sales_01_g01` |
| Job resource ID | `job_sales_01_g01` |
| Job display | `Pipeline Scheduler - sales_01_g01` |

## Configuration Priority

Values are applied in this order (later overrides earlier):

```
1. Built-in connector defaults (hardcoded)
2. CSV column values (per row)
3. default_values parameter (fills empty)
4. override_input_config parameter (overwrites all)
```

## Base Class Methods

### BaseConnector

| Method | Description |
|--------|-------------|
| `run_complete_pipeline_generation()` | Main entry point |
| `load_and_normalize_input()` | Normalize CSV data |
| `generate_pipeline_config()` | Load balancing (abstract) |
| `generate_yaml_files()` | YAML generation (abstract) |
| `_generate_resource_names()` | Consistent naming |
| `_split_groups_by_size()` | Split large groups |
| `_create_jobs()` | Generate job YAML |
| `_create_databricks_yml()` | Generate bundle config |
| `_write_yaml_file()` | Write YAML with retries |
| `_is_value_set()` | Check for non-empty value |
| `_validate_cron_expression()` | Validate cron format |

### DatabaseConnector (extends BaseConnector)

Adds two-level load balancing and gateway handling.

### SaaSConnector (extends BaseConnector)

| Method | Description |
|--------|-------------|
| `_create_pipelines()` | Pipeline YAML (abstract) |
| `generate_yaml_files()` | Implemented for SaaS |

## Error Types

| Exception | When Raised |
|-----------|-------------|
| `ConfigurationError` | Missing columns, empty input, invalid targets |
| `ValidationError` | Invalid cron, resource names, mixed subgroups |
| `YAMLGenerationError` | File write failures |

## Common Patterns

### Custom Validation

```python
def _apply_connector_specific_normalization(self, df):
    df = super()._apply_connector_specific_normalization(df)

    # Validate required combinations
    if 'mode' in df.columns:
        cdc_rows = df[df['mode'] == 'cdc']
        missing_cursor = cdc_rows[cdc_rows['cursor_field'].isna()]
        if not missing_cursor.empty:
            raise ValidationError("CDC mode requires cursor_field")

    return df
```

### Conditional Pipeline Properties

```python
def _create_pipelines(self, df, project_name):
    pipelines = {}
    for pipeline_group, group_df in df.groupby('pipeline_group'):
        config = {...}

        # Add optional properties
        first_row = group_df.iloc[0]
        if self._is_value_set(first_row.get('scd_type')):
            scd = self._validate_scd_type(first_row['scd_type'], pipeline_group)
            if scd:
                config['ingestion_definition']['scd_type'] = scd

        pipelines[...] = config
    return {'resources': {'pipelines': pipelines}}
```

### Column Filtering (Salesforce pattern)

```python
def _build_table_config(self, row):
    config = {
        'source_table': row['source_table_name'],
        'destination_table': row['target_table_name'],
    }

    if self._is_value_set(row.get('include_columns')):
        config['include_columns'] = [
            c.strip() for c in str(row['include_columns']).split(',')
        ]

    return config
```
