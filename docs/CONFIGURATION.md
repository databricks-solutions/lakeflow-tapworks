# Configuration Reference

This guide covers the input configuration format for Tapworks — what input types are supported, what columns are available, and how to provide them.

---

## Input Types

Tapworks accepts three input formats. All are passed via the `input_source` parameter.

### CSV File

A CSV file path (must end in `.csv`). All columns are loaded as strings.

```python
run_pipeline_generation(
    connector_name='sql_server',
    input_source='tables.csv',
    ...
)
```

### Delta Table

A Delta table name (any string that doesn't end in `.csv`). Requires a Spark session — either passed explicitly or auto-detected from a Databricks notebook environment.

```python
run_pipeline_generation(
    connector_name='sql_server',
    input_source='main.config.pipeline_tables',
    spark_session=spark,
    ...
)
```

### pandas DataFrame

A DataFrame passed directly. This is useful when building configs programmatically — for example, querying a metadata catalog, mapping columns automatically, or generating configs from another system's export.

```python
import pandas as pd

# Build config from any source
df = pd.DataFrame([
    {'source_database': 'db1', 'source_schema': 'dbo', 'source_table_name': 'customers', ...},
    {'source_database': 'db1', 'source_schema': 'dbo', 'source_table_name': 'orders', ...},
])

run_pipeline_generation(
    connector_name='sql_server',
    input_source=df,
    ...
)
```

Since any DataFrame works, you can generate configs dynamically:

```python
# Example: auto-map all tables from a source schema
source_tables = spark.sql("SHOW TABLES IN my_source_db.dbo").toPandas()

df = pd.DataFrame({
    'source_database': 'my_source_db',
    'source_schema': 'dbo',
    'source_table_name': source_tables['tableName'],
    'target_catalog': 'bronze',
    'target_schema': 'sales',
    'target_table_name': source_tables['tableName'],  # same name
    'connection_name': 'my_sql_connection',
    'pipeline_catalog': 'bronze',
    'pipeline_schema': 'sales',
})

run_pipeline_generation(
    connector_name='sql_server',
    input_source=df,
    ...
)
```

---

## Columns

Every connector requires a specific set of columns and supports additional optional ones. Columns can come from the input config directly, or be filled via `default_values` / `override_input_config` at generation time.

### Common Columns (All Connectors)

These columns are available across every connector.

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `project_name` | Yes* | — | DAB project/bundle name. Must be provided via input, `default_values`, or `override_input_config`. |
| `prefix` | No | Falls back to `project_name` | Grouping key for load balancing. Tables with the same prefix are grouped together before splitting. |
| `subgroup` | No | Empty (omitted from names) | Sub-grouping within a prefix. When used, all tables in a prefix must have explicit subgroups. |
| `schedule` | No | Connector-specific (see below) | Cron expression for the pipeline job. Standard 5-field or Quartz 6-7 field format. |
| `pause_status` | No | — | Job pause state: `PAUSED` or `UNPAUSED`. |
| `tags` | No | — | Databricks resource tags. JSON string or key=value format. |
| `scd_type` | No | — | Slowly Changing Dimension type: `SCD_TYPE_1` or `SCD_TYPE_2`. Supported by all connectors. |

*`project_name` has no built-in default — it must come from somewhere (input column, `default_values`, or `override_input_config`).

### Database Connector Columns

Used by **SQL Server** and **PostgreSQL**.

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `source_database` | Yes | — | Source database name |
| `source_schema` | Yes | — | Source schema (e.g., `dbo` for SQL Server, `public` for PostgreSQL) |
| `source_table_name` | Yes | — | Source table to ingest |
| `target_catalog` | Yes | — | Unity Catalog destination catalog |
| `target_schema` | Yes | — | Destination schema |
| `target_table_name` | Yes | — | Destination table name |
| `connection_name` | Yes | — | Databricks Unity Catalog connection name |
| `pipeline_catalog` | Yes | — | Catalog for the pipeline event log |
| `pipeline_schema` | Yes | — | Schema for the pipeline event log |
| `gateway_catalog` | No | Falls back to `target_catalog` | Gateway storage catalog |
| `gateway_schema` | No | Falls back to `target_schema` | Gateway storage schema |
| `gateway_worker_type` | No | — | Gateway worker node instance type |
| `gateway_driver_type` | No | — | Gateway driver node instance type |

### PostgreSQL-Specific Columns

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `slot_name` | Yes | — | PostgreSQL replication slot name |
| `publication_name` | Yes | — | PostgreSQL publication name |

### Salesforce Columns

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `source_database` | Yes | — | Always `Salesforce` |
| `source_schema` | Yes | — | Schema type: `standard` or `custom` |
| `source_table_name` | Yes | — | Salesforce object name (e.g., `Account`, `Contact`) |
| `target_catalog` | Yes | — | Unity Catalog destination catalog |
| `target_schema` | Yes | — | Destination schema |
| `target_table_name` | Yes | — | Destination table name |
| `connection_name` | Yes | — | Databricks connection name |
| `pipeline_catalog` | Yes | — | Catalog for the pipeline event log |
| `pipeline_schema` | Yes | — | Schema for the pipeline event log |
| `include_columns` | No | — | Comma-separated list of columns to include. Mutually exclusive with `exclude_columns`. |
| `exclude_columns` | No | — | Comma-separated list of columns to exclude. Mutually exclusive with `include_columns`. |
| `primary_keys` | No | — | Comma-separated primary key columns (supports composite keys) |

### Google Analytics 4 Columns

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `source_catalog` | Yes | — | GCP project ID |
| `source_schema` | Yes | — | GA4 property ID |
| `tables` | Yes | — | Comma-separated GA4 tables (e.g., `events,events_intraday,users`) |
| `target_catalog` | Yes | — | Unity Catalog destination catalog |
| `target_schema` | Yes | — | Destination schema |
| `connection_name` | Yes | — | Databricks connection name |
| `pipeline_catalog` | Yes | — | Catalog for the pipeline event log |
| `pipeline_schema` | Yes | — | Schema for the pipeline event log |

### ServiceNow Columns

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `source_database` | Yes | — | Always `SERVICENOW` |
| `source_schema` | Yes | — | Schema name (typically `default`) |
| `source_table_name` | Yes | — | ServiceNow table name |
| `target_catalog` | Yes | — | Unity Catalog destination catalog |
| `target_schema` | Yes | — | Destination schema |
| `target_table_name` | Yes | — | Destination table name |
| `connection_name` | Yes | — | Databricks connection name |
| `pipeline_catalog` | Yes | — | Catalog for the pipeline event log |
| `pipeline_schema` | Yes | — | Schema for the pipeline event log |
| `include_columns` | No | — | Comma-separated list of columns to include |
| `exclude_columns` | No | — | Comma-separated list of columns to exclude |

### Workday Reports Columns

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `source_url` | Yes | — | Workday report URL |
| `target_catalog` | Yes | — | Unity Catalog destination catalog |
| `target_schema` | Yes | — | Destination schema |
| `target_table_name` | Yes | — | Destination table name |
| `connection_name` | Yes | — | Databricks connection name |
| `primary_keys` | Yes | — | Comma-separated primary key columns (required for Workday) |
| `pipeline_catalog` | Yes | — | Catalog for the pipeline event log |
| `pipeline_schema` | Yes | — | Schema for the pipeline event log |
| `include_columns` | No | — | Comma-separated list of columns to include |
| `exclude_columns` | No | — | Comma-separated list of columns to exclude |

---

## Default Schedules

Each connector has a built-in default schedule used when no `schedule` column or `default_values` override is provided.

| Connector | Default Schedule |
|-----------|-----------------|
| Salesforce | `*/15 * * * *` (every 15 minutes) |
| SQL Server | `*/15 * * * *` (every 15 minutes) |
| PostgreSQL | `*/15 * * * *` (every 15 minutes) |
| ServiceNow | `*/15 * * * *` (every 15 minutes) |
| Google Analytics 4 | `0 */6 * * *` (every 6 hours) |
| Workday Reports | `0 */6 * * *` (every 6 hours) |

---

## Value Priority

When the same column has values from multiple sources, the last one wins:

```
1. Built-in connector defaults (hardcoded)
2. Input config values (CSV / Delta / DataFrame — per row)
3. default_values parameter (fills empty values only)
4. override_input_config parameter (overwrites everything)
```

For example, if a row has `schedule = '0 * * * *'` in the CSV but you pass `override_input_config = {'schedule': '*/30 * * * *'}`, the override wins.

Both `default_values` and `override_input_config` support [group-based configuration](./USAGE.md#group-based-format-per-pipeline-group) for applying different values to different pipeline groups.

---

## Naming Constraints

Column values used in Unity Catalog names (`target_catalog`, `target_schema`, `target_table_name`, `pipeline_catalog`, `pipeline_schema`, `gateway_catalog`, `gateway_schema`) must follow these rules:

- No periods (`.`), spaces, forward slashes (`/`), or control characters
- Maximum 255 characters
- Generated resource names (pipeline, job, gateway) must start with a letter and contain only letters, numbers, underscores, and hyphens

See [VALIDATIONS.md](./VALIDATIONS.md) for the full list of validation rules and error messages.

---

## Row Order

When load balancing splits a group into multiple pipelines or gateways, tables are assigned to chunks based on their row position in the input. See the [Row Order and Load Balancing](./USAGE.md#row-order-and-load-balancing) section in USAGE.md.
