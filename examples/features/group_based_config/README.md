# Group-Based Configuration Examples

This folder contains examples demonstrating how to use group-based defaults and overrides to apply different configuration values to different pipeline groups from a single CSV file.

## Examples

| # | Folder | Description | Connector |
|---|--------|-------------|-----------|
| 1 | `01_global_defaults` | Simple flat dict applied to all rows | Salesforce |
| 2 | `02_prefix_based` | Different defaults per prefix | Salesforce |
| 3 | `03_pipeline_group_specific` | Granular control per subgroup | Salesforce |
| 4 | `04_group_overrides` | Overrides that overwrite values | Salesforce |
| 5 | `05_database_pipelines` | Prefix-based schedules | SQL Server |
| 6 | `06_database_gateways` | Subgroups with multiple gateways | SQL Server |
| 7 | `07_mixed_values` | Defaults only fill empty values | SQL Server |
| 8 | `08_connection_names` | Different connection per group | SQL Server |
| 9 | `09_gateway_driver_types` | Different compute config per group | SQL Server |

## Running the Examples

Open `example_notebook.ipynb` in Databricks to run all examples interactively.

## Matching Precedence

Config keys are matched in this order (most specific wins):

1. `pipeline_group` (prefix_subgroup) - e.g., `'sales_2'`
2. `prefix` - e.g., `'sales'`
3. `project_name` - e.g., `'my_project'`
4. `'*'` (global fallback)

## Configuration Formats

### Simple (Flat Dictionary)

Applies the same values to all rows:

```python
default_values = {
    'schedule': '0 */6 * * *',
    'pause_status': 'UNPAUSED',
}
```

### Group-Based (Nested Dictionary)

Applies different values per group:

```python
default_values = {
    '*': {'schedule': '0 */6 * * *'},        # Global fallback
    'sales': {'schedule': '*/15 * * * *'},   # All sales pipelines
    'sales_2': {'schedule': '*/30 * * * *'}, # Only sales_2 subgroup
    'hr': {'schedule': '0 0 * * *'},         # HR pipelines
}
```

## Defaults vs Overrides

| Parameter | Behavior |
|-----------|----------|
| `default_values` | Fill missing/empty values only |
| `override_config` | Overwrite all values (ignores CSV) |

### Example: Defaults

```python
# CSV has schedule for some rows, empty for others
# Defaults only fill the empty ones
default_values = {
    'sales': {'schedule': '*/15 * * * *'},
}
```

### Example: Overrides

```python
# Override ALL rows regardless of CSV values
# Useful for maintenance windows
override_config = {
    '*': {'pause_status': 'UNPAUSED'},
    'finance': {'pause_status': 'PAUSED'},  # Pause finance for audit
}
```

## Connector Support

Group-based configuration works with all connectors:

- **SaaS**: Salesforce, ServiceNow, Google Analytics, Workday Reports
- **Database**: SQL Server, PostgreSQL
