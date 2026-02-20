# TODO

## Testing

- [ ] Test what happens in Databricks if the same names/resource names are accidentally used in DAB templates (name collisions)
- [ ] Test what happens in Databricks when a table is moved from one pipeline to another (e.g., rebalancing after adding tables)
- [ ] Check if primary_keys is supported in table_configuration for GA4, SQL Server, and PostgreSQL connectors

## Flexible Config Sources

Currently only supports CSV files and Delta tables as input config. Add support for:

- [ ] JSON input (`--input-config config.json`)
- [ ] YAML input (`--input-config config.yaml`)
- [ ] Python function (`--input-config generator.py:build_config`)
- [ ] DataFrame (notebook only: `input_source=df`)

Start with simple `load_config()` function with type detection. Refactor to factory pattern only if user extensibility becomes a requirement.

## Config Validation and Derivation Redesign

Validation and transformation logic is interleaved and incomplete. Redesign into two clear validation points:

- [ ] Move validation out of `_process_input_config()` — it should only handle defaults and overrides
- [ ] Add `_validate_config(df, required_columns)` after all transformations are done
- [ ] Add `_validate_generated_names(df)` after `generate_pipeline_config()` assigns names
- [ ] Add column derivation in `_apply_connector_specific_normalization()` (e.g., fill `target_table_name` from `source_table_name` if empty)

New flow:
```
load_and_normalize_input()
  ├── _process_input_config()                        # defaults, overrides (no validation)
  ├── _apply_connector_specific_normalization()       # column derivation + connector transforms
  └── _validate_config(df, required_columns)          # all input validation in one place

generate_pipeline_config()
  ├── ... (existing splitting logic)
  └── _validate_generated_names(df)                   # collision detection
```

## Validations

### Input validation (`_validate_config`)

- [ ] Required fields: check every row has a non-empty value (not just column existence)
- [ ] Duplicate rows: same source_table + source_schema + connection_name
- [ ] Invalid characters in target_table_name, target_schema, target_catalog (spaces, special chars that produce invalid Delta table names)
- [ ] Connection name consistency: all rows in the same gateway/pipeline group should share the same connection_name — warn or error if not
- [ ] Mutually exclusive include_columns and exclude_columns on the same row
- [ ] Cron expression format validation (currently "99 99 * * *" passes through)
- [ ] Gateway config consistency: gateway_catalog, gateway_schema, gateway_worker_type should match within a gateway group
- [ ] SCD Type 2 + CDC recommendation: warn when scd_type is SCD_TYPE_2 that CDC should be enabled on source for full history

### Generated name validation (`_validate_generated_names`)

- [ ] Resource name collisions across pipeline groups
- [ ] Display name collisions across pipelines/gateways
- [ ] Resource name length limits (long prefix + subgroup + suffixes)
- [ ] max_tables_per_pipeline exceeds Databricks limit of 250

## Compact Naming Mode

For small deployments with no splitting, suffixes are noise. Add a `compact=True` flag that drops numbering when there's only one of each level:

- [ ] 1 subgroup, 1 pipeline: `sales_01_p01` → `sales`
- [ ] 2 subgroups, 1 pipeline each: `sales_01_p01`, `sales_02_p01` → `sales_01`, `sales_02`
- [ ] 1 subgroup, 3 pipelines: `sales_01_p01..p03` → `sales_p01..p03`
- [ ] 2 subgroups, split pipelines: unchanged

## Missing Connector Features

### Row filtering

Supported by Salesforce, ServiceNow, GA4, Workday per Databricks docs. Not implemented in any connector.

- [ ] Add row filtering support across SaaS connectors

### New connectors

- [ ] MySQL
- [ ] Confluence
- [ ] Dynamics 365
- [ ] Google Ads
- [ ] HubSpot
- [ ] Jira
- [ ] Meta Ads
- [ ] NetSuite
- [ ] SharePoint
- [ ] TikTok Ads
- [ ] Zendesk Support

## Minor

- [ ] Remove `prompts/examples/jira_walkthrough.md` (fictional example, 6 real connectors exist)
