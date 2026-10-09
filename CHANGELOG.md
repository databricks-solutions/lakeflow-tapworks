# Changelog

All notable changes to Tapworks. Versions follow [semantic versioning](https://semver.org/); see [docs/RELEASING.md](./docs/RELEASING.md).

Each release lists **Changes to generated output** separately. Those entries change the DAB files generated from an existing config and can rename or recreate deployed resources. Review them before upgrading.

## [Unreleased]

### Added
- **Oracle integrated CDC connector** (`oracle_integrated`), using Lakeflow Connect integrated CDC (Beta; requires workspace enablement). Pipelines connect directly through `connection_name` with `connector_type: CDC` on the `PREVIEW` channel; no gateway. Single-level load balancing (250 tables per pipeline), default schedule hourly. Optional `staging_catalog`/`staging_schema` columns set the staging location (`data_staging_options`) and default to the target catalog/schema, like the gateway columns. Logs a warning for lowercase Oracle identifiers. Example in `examples/connectors/oracle_integrated/`.
- **Oracle query-based connector** (`oracle_query_based`). Pipelines connect directly through `connection_name` with `connector_type: QUERY_BASED`; no gateway or staging. Optional `cursor_columns` (tables without one are read as full snapshots), `primary_keys`, `deletion_condition`, and `scd_type` (`SCD_TYPE_1`, `SCD_TYPE_2`, `APPEND_ONLY`). Default schedule hourly. Example in `examples/connectors/oracle_query_based/`.
- Compute for integrated CDC and query-based pipelines: serverless by default (`serverless: true`); optional `classic_compute=true` for classic compute (e.g. databases that only accept classic compute), with optional `pipeline_worker_type`/`pipeline_driver_type`.
- `IntegratedCDCConnector` and `QueryBasedConnector` base classes, ready for integrated CDC and query-based versions of other databases.

### Changed
- Database connectors restructured so each database can have standard, integrated CDC, and query-based connectors (see `docs/DATABASE_CONNECTORS_PLAN.md`). `DatabaseConnector` is now the shared base for all database connectors; the gateway logic moved to the new `StandardConnector`. Each database has a source class (`connectors/<database>/source.py`) for rules that apply in every mode.
- Database connector names are now `<database>_<mode>`: `sql_server_standard`, `postgresql_standard`. `sql_server` and `postgresql` still work as aliases, and `tapworks --list` shows them. Classes renamed to `SQLServerStandardConnector` / `PostgreSQLStandardConnector`; the old import paths (`tapworks.connectors.<database>.connector.SQLServerConnector` / `PostgreSQLConnector`) still work.

### Changes to generated output
- None.

## [0.2.0] - 2026-10-09

### Added
- Golden-file tests (`tests/test_golden_output.py`) that fail on any change to generated DAB output.
- Release and compatibility policy (`docs/RELEASING.md`) and the database connectors plan (`docs/DATABASE_CONNECTORS_PLAN.md`).

### Fixed
- Example CSV tests (`tests/test_example_csvs.py`) were always skipped because they looked for examples in an old location.
- `--max-tables-per-gateway` / `max_tables_per_gateway` is now applied by the CLI, `run_pipeline_generation()`, and the notebook runner. Previously it was ignored and the default of 250 always applied.

### Changes to generated output
- **Gateway limit now applied** (SQL Server, PostgreSQL). Affects only setups that set `max_tables_per_gateway` (CLI flag, settings file, or `run_pipeline_generation()` argument) to a value other than 250, **and** have a group (prefix/subgroup) with more tables than the smaller of that value and 250. Previously those groups were split at 250 tables per gateway; now they are split at the configured value. Tables move to different gateways and pipelines (for example, from `sales_g01p02` to `sales_g02p01`), and `bundle deploy` recreates the affected resources and re-ingests their tables. If affected, either stay on `v0.1.0`, remove the setting to keep the previous layout, or accept the new layout and review the recreate prompt in `bundle deploy`. All other setups are unchanged.

## [0.1.0] - 2026-10-09

Baseline release. Connectors: SQL Server and PostgreSQL (gateway CDC), Salesforce, Google Analytics 4, ServiceNow, Workday Reports.

### Known issues
- `--max-tables-per-gateway` / `max_tables_per_gateway` is ignored by the CLI, `run_pipeline_generation()`, and the notebook runner; the default of 250 always applies.

[Unreleased]: https://github.com/databricks-solutions/lakeflow-tapworks/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/databricks-solutions/lakeflow-tapworks/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/databricks-solutions/lakeflow-tapworks/releases/tag/v0.1.0
