# Changelog

All notable changes to Tapworks. Versions follow [semantic versioning](https://semver.org/); see [docs/RELEASING.md](./docs/RELEASING.md).

Each release lists **Changes to generated output** separately. Those entries change the DAB files generated from an existing config and can rename or recreate deployed resources. Review them before upgrading.

## [Unreleased]

### Added
- Golden-file tests (`tests/test_golden_output.py`) that fail on any change to generated DAB output.
- Release and compatibility policy (`docs/RELEASING.md`) and the database connectors plan (`docs/DATABASE_CONNECTORS_PLAN.md`).

### Fixed
- Example CSV tests (`tests/test_example_csvs.py`) were always skipped because they looked for examples in an old location.

### Changes to generated output
- None.

## [0.1.0] - 2026-10-09

Baseline release. Connectors: SQL Server and PostgreSQL (gateway CDC), Salesforce, Google Analytics 4, ServiceNow, Workday Reports.

### Known issues
- `--max-tables-per-gateway` / `max_tables_per_gateway` is ignored by the CLI, `run_pipeline_generation()`, and the notebook runner; the default of 250 always applies.

[Unreleased]: https://github.com/databricks-solutions/lakeflow-tapworks/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/databricks-solutions/lakeflow-tapworks/releases/tag/v0.1.0
