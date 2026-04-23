# Validation Reference

Tapworks validates your configuration at every stage of the pipeline generation process. Errors are caught early — before any YAML is written — so you can fix problems in your input rather than debugging generated files.

---

## Stage 1: Input Loading

These checks run when reading your input source (CSV file, Delta table, or DataFrame).

### File not found

| | |
|---|---|
| **What it checks** | The CSV file path you provided actually exists on disk. |
| **Severity** | Error (`FileNotFoundError`) |
| **Example message** | `Input CSV file not found: config/tables.csv` |
| **How to fix** | Verify the file path is correct and the file exists. |

### Invalid input type

| | |
|---|---|
| **What it checks** | The input source is one of the supported types: a CSV file path (string ending in `.csv`), a Delta table name (string), or a pandas DataFrame. |
| **Severity** | Error (`ConfigurationError`) |
| **Example message** | `Invalid input_source type: <class 'list'>. Expected: CSV path (str), Delta table name (str), or pandas DataFrame` |
| **How to fix** | Pass a CSV file path, a Delta table name, or a pandas DataFrame. |

### Empty DataFrame

| | |
|---|---|
| **What it checks** | The input DataFrame has at least one row after loading. |
| **Severity** | Error (`ConfigurationError`) |
| **Example message** | `Input DataFrame is empty` |
| **How to fix** | Ensure your CSV or DataFrame contains data rows (not just headers). |

---

## Stage 2: Normalization

These checks run after defaults and overrides are applied but before input validation.

### Missing project_name

| | |
|---|---|
| **What it checks** | Every row has a `project_name` value. This is a required field with no default — it must come from your input config, `default_values`, or `override_config`. |
| **Severity** | Error (`ConfigurationError`) |
| **Example message** | `Missing required field: project_name. Provide via: input config column, default_values, or override_config` |
| **How to fix** | Add a `project_name` column to your input, or pass it via `default_values={'project_name': 'my_project'}`. |

### Mixed subgroup usage within a prefix

| | |
|---|---|
| **What it checks** | Within a given `prefix`, either all rows have a `subgroup` value or none do. You cannot mix explicit subgroups with empty subgroups in the same prefix. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Mixed subgroup usage in prefix 'sales': 3 table(s) have empty subgroups while others use ['crm', 'analytics']. When using subgroups, all tables in a prefix must have explicit subgroups.` |
| **How to fix** | Assign an explicit `subgroup` to every table in the prefix, or remove all subgroup values to let Tapworks auto-assign. |

---

## Stage 3: Input Validation

These checks run at the end of normalization, validating the fully resolved configuration.

### Required columns exist

| | |
|---|---|
| **What it checks** | All columns listed in the connector's `required_columns` are present in the DataFrame. |
| **Severity** | Error (`ConfigurationError`) |
| **Example message** | `Missing required columns: ['connection_name', 'target_catalog']. Required columns: source_table_name, target_catalog, target_schema, ...` |
| **How to fix** | Add the missing columns to your input CSV or provide them via `default_values` / `override_config`. |

### Required values non-empty

| | |
|---|---|
| **What it checks** | Every required column has a non-blank value in every row. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Required columns have empty values: 'connection_name' empty in rows [2, 5]; 'target_catalog' empty in rows [3]` |
| **How to fix** | Fill in the missing values in the indicated rows, or use `default_values` to provide a fallback. |

### Duplicate source rows

| | |
|---|---|
| **What it checks** | Whether multiple rows share the same `source_table_name` + `source_schema` + `connection_name` combination. |
| **Severity** | Warning (log only — does not stop generation) |
| **Example message** | `Duplicate source rows detected (2 unique combinations): columns ['source_table_name', 'source_schema', 'connection_name']. This may be intentional for multi-target scenarios.` |
| **How to fix** | If unintentional, remove the duplicate rows. If intentional (e.g., ingesting the same table to multiple targets), no action needed. |

### Invalid characters in target names

| | |
|---|---|
| **What it checks** | Values in `target_table_name`, `target_schema`, and `target_catalog` follow Unity Catalog naming rules. Periods (`.`), spaces, forward slashes (`/`), and control characters are not allowed. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Invalid characters in 'target_table_name': ['my.table', 'bad table']. Periods (.), spaces, forward slashes (/), and control characters are not allowed in Unity Catalog names.` |
| **How to fix** | Replace disallowed characters with underscores or other valid characters. |

### Target name too long

| | |
|---|---|
| **What it checks** | Values in `target_table_name`, `target_schema`, and `target_catalog` do not exceed 255 characters. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Name too long in 'target_table_name': ['very_long_name_that_exc...']. Unity Catalog names cannot exceed 255 characters.` |
| **How to fix** | Shorten the name to 255 characters or fewer. |

### Both include_columns and exclude_columns set

| | |
|---|---|
| **What it checks** | A row does not have values in both `include_columns` and `exclude_columns` at the same time. These are mutually exclusive. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Rows [1, 4] have both 'include_columns' and 'exclude_columns' set. These are mutually exclusive — use one or the other per row.` |
| **How to fix** | For each flagged row, keep only `include_columns` or `exclude_columns`, not both. |

### Invalid cron expression

| | |
|---|---|
| **What it checks** | The `schedule` column contains a valid cron expression with 5 fields (standard cron) or 6-7 fields (Quartz format). |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Invalid schedule cron expression '*/15 * *': expected 5-7 fields, got 3` |
| **How to fix** | Use a standard 5-field cron expression (e.g., `*/15 * * * *`) or a Quartz 6-7 field expression. |

### SCD Type 2 CDC recommendation

| | |
|---|---|
| **What it checks** | Whether any rows specify `SCD_TYPE_2` for their `scd_type`. If so, logs a recommendation to enable CDC on the source. |
| **Severity** | Info (log only — does not stop generation) |
| **Example message** | `3 row(s) use SCD_TYPE_2. Consider enabling CDC (Change Data Capture) on the source for optimal performance.` |
| **How to fix** | No action required. For best performance with SCD Type 2, enable Change Data Capture on your source database. |

---

## Stage 4: Post-Load-Balancing Validation

These checks run after load balancing assigns `pipeline_group` (and `gateway` for database connectors).

### Job-level group consistency

| | |
|---|---|
| **What it checks** | Within each `pipeline_group`, the `schedule`, `pause_status`, and `tags` values are the same for all rows. A single job is created per pipeline group, so these values cannot differ. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Pipeline group 'sales_01_p01' has conflicting schedule values: ['*/15 * * * *', '*/30 * * * *']. All tables in the same pipeline group must have the same schedule. Solutions: (1) Use the same schedule value for all tables, or (2) Use different 'subgroup' values to separate tables with different schedule values.` |
| **How to fix** | Either set the same value for all tables in the group, or use different `subgroup` values to separate tables that need different settings. |

### Pipeline consistency (SaaS connectors)

| | |
|---|---|
| **What it checks** | Within each `pipeline_group`, the `connection_name` and `tags` values are the same for all rows. SaaS pipelines use a single connection per pipeline. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Pipeline group 'sfdc_01_p01' has conflicting connection_name values: ['conn_a', 'conn_b']. All tables in the same pipeline group must have the same connection_name. Solutions: (1) Use the same connection_name value for all tables, or (2) Use different 'subgroup' values to separate tables with different connection_name values.` |
| **How to fix** | Use the same `connection_name` for all tables in the group, or split them into different subgroups. |

### Pipeline consistency (database connectors)

| | |
|---|---|
| **What it checks** | Within each `pipeline_group`, the `tags` values are the same for all rows. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Pipeline group 'sql_01_g01_p01' has conflicting tags values: ['{"team":"a"}', '{"team":"b"}']. All tables in the same pipeline group must have the same tags.` |
| **How to fix** | Use the same `tags` for all tables in the group, or split them into different subgroups. |

### Gateway consistency (database connectors only)

| | |
|---|---|
| **What it checks** | Within each `gateway`, the `gateway_catalog`, `gateway_schema`, `connection_name`, and `tags` values are the same for all rows. A gateway connects to one source via one connection. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Gateway 'sql_01_g01' has conflicting connection_name values: ['conn_a', 'conn_b']. All tables in the same gateway must have the same connection_name. Solutions: (1) Use the same connection_name value for all tables, or (2) Use different 'subgroup' values to separate tables with different connection_name values.` |
| **How to fix** | Use the same connection and gateway settings for all tables in the gateway, or split them into different subgroups. |

### Cross-project pipeline_group collision

| | |
|---|---|
| **What it checks** | The same `pipeline_group` value does not appear in multiple projects. Identical pipeline group names across projects would create resources with conflicting names in the workspace. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Pipeline group 'shared_01_p01' appears in multiple projects: 'project_a' and 'project_b'. Use distinct prefix or subgroup values per project to avoid resource name collisions in the workspace.` |
| **How to fix** | Use different `prefix` or `subgroup` values in each project so pipeline groups are unique. |

### Resource name format

| | |
|---|---|
| **What it checks** | Generated resource names (e.g., `pipeline_sales_01_p01`, `job_sales_01_p01`) start with a letter and contain only letters, numbers, underscores, and hyphens. |
| **Severity** | Error (`ValidationError`) |
| **Example message** | `Invalid pipeline name 'pipeline_123_invalid': must start with a letter` |
| **How to fix** | Ensure your `prefix` and `subgroup` values start with a letter and use only valid characters (letters, numbers, underscores, hyphens). |

---

## Stage 5: YAML Generation

These checks run when creating the `databricks.yml` bundle configuration.

### No targets defined

| | |
|---|---|
| **What it checks** | At least one target environment is provided in the `targets` dictionary. |
| **Severity** | Error (`ConfigurationError`) |
| **Example message** | `At least one target must be provided` |
| **How to fix** | Pass at least one target: `targets={'dev': {'workspace_host': 'https://your-workspace.databricks.com'}}` |

### Invalid default_target

| | |
|---|---|
| **What it checks** | The `default_target` value matches one of the keys in your `targets` dictionary. |
| **Severity** | Error (`ConfigurationError`) |
| **Example message** | `default_target 'staging' must be one of: ['dev', 'prod']` |
| **How to fix** | Set `default_target` to one of your defined target names, or add the missing target to the `targets` dictionary. |

### Missing workspace_host

| | |
|---|---|
| **What it checks** | Every target in the `targets` dictionary includes a `workspace_host` value. |
| **Severity** | Error (`ConfigurationError`) |
| **Example message** | `Target 'dev' must have 'workspace_host'` |
| **How to fix** | Add `workspace_host` to the target configuration: `{'dev': {'workspace_host': 'https://your-workspace.databricks.com'}}` |
