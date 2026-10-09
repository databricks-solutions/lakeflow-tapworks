# Changelog

All notable changes to Tapworks. Versions follow [semantic versioning](https://semver.org/); see [docs/RELEASING.md](./docs/RELEASING.md).

Each release lists **Changes to generated output** separately. Those entries change the DAB files generated from an existing config and can rename or recreate deployed resources. Review them before upgrading.

## [Unreleased]

### Changed
- Database connector classes restructured to prepare for integrated CDC and query-based connectors (see `docs/DATABASE_CONNECTORS_PLAN.md`). `DatabaseConnector` is now the shared base for all database connectors, and the gateway logic moved to the new `GatewayConnector` (both in `core/database.py`). Internal only: registry names, CLI, API, and CSV columns are unchanged.

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
