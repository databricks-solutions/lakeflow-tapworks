# Validation Reference

Two validation points catch all problems early, before YAML generation begins.

## 1. Input Validation — `_validate_config()`

Called at the end of `load_and_normalize_input()`, after defaults, overrides, and connector-specific normalization have been applied.

| # | Check | Severity | Error Type | Test(s) |
|---|-------|----------|------------|---------|
| 1 | **Required columns exist** — every column in `required_columns` must be present in the DataFrame | Error | `ConfigurationError` | `test_raises_error_for_missing_required_columns`, `test_error_message_lists_missing_columns`, `test_missing_required_columns_raises_configuration_error`, `test_missing_required_columns_error` |
| 2 | **Required values non-empty** — every row in every required column must have a non-blank value; reports which columns and row indices are empty | Error | `ValidationError` | `test_required_values_empty_raises_error`, `test_missing_primary_keys_raises_validation_error` |
| 3 | **Duplicate rows** — same `source_table_name` + `source_schema` + `connection_name` (only checked if those columns exist) | Warning | log only | `test_duplicate_rows_warns` |
| 4 | **Invalid characters in target names** — `target_table_name`, `target_schema`, `target_catalog` must match `^[a-zA-Z0-9_]*$` | Error | `ValidationError` | `test_invalid_chars_in_target_names` |
| 5 | **Mutually exclusive include/exclude** — a row cannot have both `include_columns` and `exclude_columns` set | Error | `ValidationError` | `test_include_and_exclude_mutually_exclusive` |
| 6 | **Cron format** — `schedule` values validated via `_validate_cron_expression()` (5–7 fields) | Error | `ValidationError` | `test_invalid_cron_raises_error` |
| 7 | **SCD Type 2 + CDC recommendation** — if `scd_type` is `SCD_TYPE_2`, log a recommendation to enable CDC | Info | log only | `test_scd_type_2_cdc_warning` |

Additional regression test: `test_project_name_via_default_values_works` — confirms that providing `project_name` via `default_values` does not trigger a false "missing column" error.

## 2. Post-Generation Validation — `_validate_generated_names()`

Called at the end of `generate_pipeline_config()`, after load balancing has assigned `pipeline_group` (and `gateway` for database connectors).

### Base checks (all connectors)

| # | Check | Severity | Error Type | Test(s) |
|---|-------|----------|------------|---------|
| 1 | **Job-level group consistency** — within each `pipeline_group`, `schedule`, `pause_status`, and `tags` must be uniform | Error | `ValidationError` | `test_group_consistency_schedule_mismatch`, `test_raises_error_conflicting_schedules_in_same_group`, `test_raises_error_shows_both_solutions`, `test_raises_error_conflicting_pause_status_in_same_group`, `test_pause_status_error_shows_both_solutions`, `test_raises_error_conflicting_job_tags` |
| 2 | **Resource name length/chars** — generated `pipeline_{pg}` and `job_{pg}` names validated via `_validate_resource_name()` (≤128 chars, valid chars) | Error | `ValidationError` | `test_resource_name_too_long` |
| 3 | **Cross-project name collisions** — same `pipeline_group` value in different projects would deploy resources with identical names to the workspace | Error | `ValidationError` | `test_name_collision_across_projects` |

### Database connector additions (`DatabaseConnector` override)

| # | Check | Severity | Error Type | Test(s) |
|---|-------|----------|------------|---------|
| 4 | **Gateway consistency** — within each `gateway`, `gateway_catalog`, `gateway_schema`, `connection_name`, and `tags` must be uniform | Error | `ValidationError` | `test_raises_error_conflicting_gateway_catalog`, `test_raises_error_conflicting_gateway_schema`, `test_raises_error_conflicting_gateway_connection_name`, `test_raises_error_conflicting_gateway_tags` |
| 5 | **Pipeline consistency (DB)** — within each `pipeline_group`, `tags` must be uniform | Error | `ValidationError` | `test_db_pipeline_tags_consistency` |

### SaaS connector additions (`SaaSConnector` override)

| # | Check | Severity | Error Type | Test(s) |
|---|-------|----------|------------|---------|
| 6 | **Pipeline consistency (SaaS)** — within each `pipeline_group`, `connection_name` and `tags` must be uniform | Error | `ValidationError` | `test_group_consistency_connection_name_mismatch`, `test_raises_error_conflicting_connection_names`, `test_raises_error_conflicting_pipeline_tags` |
