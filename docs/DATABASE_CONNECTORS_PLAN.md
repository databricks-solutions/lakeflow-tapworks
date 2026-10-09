# Plan: Integrated CDC and Query-Based Database Connectors

Plan for adding Oracle and query-based connectors, and restructuring database connectors so that integrated CDC versions of SQL Server and PostgreSQL can be added later.

> **Status:** Agreed design, not yet implemented. Open decisions are listed at the end.
> Release and compatibility rules: [RELEASING.md](./RELEASING.md).

## Goals

1. Add an **Oracle integrated CDC** connector.
2. Add **query-based** connectors with one class per source, starting with Oracle.
3. Structure database connectors so that **integrated CDC for SQL Server and PostgreSQL** can be added later as small subclasses. Integrated CDC is the way forward for database ingestion.
4. **No impact on existing users**: same registry names, CLI, API, and CSV columns, and byte-identical YAML for existing connectors.
5. Code that is easy to read and maintain: variation through inheritance, with no branching on connector type.

SaaS connectors are out of scope and are not changed.

## Lakeflow Connect ingestion modes for databases

| | Standard (gateway, current) | Integrated CDC | Query-based |
|---|---|---|---|
| Resources | gateway + ingestion pipeline | ingestion pipeline only | ingestion pipeline only |
| Pipeline connects via | `ingestion_gateway_id` | `connection_name` | `connection_name` |
| Pipeline-level extras | – | `connector_type: CDC`, `channel: PREVIEW` (required), staging location | – |
| Per-table config | `scd_type`, include/exclude columns | `scd_type`, `primary_keys`, `sequence_by` | `query_based_connector_config.cursor_columns` (required), `deletion_condition`, `primary_keys`, `scd_type` (+ `APPEND_ONLY`) |
| Load balancing | two levels (gateways, then pipelines) | one level (pipelines) | one level (pipelines) |
| Status | GA (SQL Server, PostgreSQL) | Beta, needs workspace enablement (Oracle) | GA on serverless; classic compute is Beta |
| Sources | SQL Server, PostgreSQL | Oracle (later SQL Server, PostgreSQL) | Oracle, SQL Server, PostgreSQL, MySQL, MariaDB, Teradata |

Oracle integrated CDC specifics:
- `source_catalog` is the Oracle **service name**; for multitenant databases it is the `CDB$ROOT` service name.
- Identifiers must match Oracle's stored case, which is usually uppercase for unquoted identifiers.
- The recommended maximum is 250 tables per pipeline, which matches the Tapworks default.
- Each update runs for about 30 minutes; the docs suggest a 60-minute schedule as a starting point.
- The Oracle source needs archive log mode, supplemental logging, and a replication user.

Docs:
- [Oracle integrated CDC overview](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/oracle-integrated-overview)
- [Create an Oracle integrated CDC pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/oracle-integrated-pipeline)
- [Configure Oracle for ingestion](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/oracle-integrated-setup)
- [Query-based connectors](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-overview)
- [Create a query-based pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-pipeline)

## Design

### Class hierarchy

Database connectors have two dimensions:

- **Mode** (standard, integrated CDC, query-based) decides the structure: resources, load balancing, and how a pipeline reaches the source. One abstract base class per mode.
- **Database** (SQL Server, PostgreSQL, Oracle, ...) adds rules that apply in every mode, such as Oracle's identifier case warning. One source class per database.

Each concrete connector combines one database with one mode, so every database can support any of the modes Lakeflow Connect offers for it.

```
BaseConnector                      (defaults/overrides, validation, jobs, databricks.yml,
│                                   _split_groups_by_size)
├── SaaSConnector                  (unchanged)
│
└── DatabaseConnector (abstract)   shared database logic
    │                                - pipeline-building flow, table entries, table configuration
    │                                - target_table_name fallback, SCD validation
    │                                - database grouping (_add_base_group) + single-level split
    │                                - pipelines.yml + jobs.yml writing
    │                                - pipeline consistency: connection_name, pipeline_catalog,
    │                                  pipeline_schema, tags
    │
    ├── StandardConnector (abstract)       + separate ingestion gateway, two-level split, gateways.yml
    ├── IntegratedCDCConnector (abstract)  + connection_name, connector_type: CDC, channel: PREVIEW,
    │                                        staging (staging_catalog/staging_schema)
    └── QueryBasedConnector (abstract)     + connection_name, connector_type: QUERY_BASED,
                                             cursor_columns, deletion_condition, APPEND_ONLY

Source classes (one per database, in connectors/<database>/source.py):
    SQLServerSource, PostgreSQLSource, OracleSource

Concrete connectors (source class first, then mode class):
    SQLServerStandardConnector(SQLServerSource, StandardConnector)          'sql_server_standard'
    PostgreSQLStandardConnector(PostgreSQLSource, StandardConnector)        'postgresql_standard'
    OracleIntegratedConnector(OracleSource, IntegratedCDCConnector)         'oracle_integrated'
    OracleQueryBasedConnector(OracleSource, QueryBasedConnector)            'oracle_query_based'
    later: SQLServerIntegratedConnector, PostgreSQLIntegratedConnector, *QueryBasedConnector, ...
```

Oracle has no standard connector because Lakeflow Connect only offers integrated CDC and query-based for Oracle.

### Hooks in `DatabaseConnector`

`DatabaseConnector._create_pipelines` implements the full flow: loop over pipeline groups, generate names, build table entries, set `name`/`catalog`/`schema`, build `ingestion_definition`, and add tags. Subclasses only fill in hooks:

| Hook | Purpose | Standard | Integrated CDC | Query-based |
|---|---|---|---|---|
| `_ingestion_source(group_df)` (abstract) | how the pipeline reaches the source | `ingestion_gateway_id` | `connection_name`, `connector_type: CDC` | `connection_name` |
| `_build_table_configuration(row)` | per-table options | base | base (+ `primary_keys`) | base + `query_based_connector_config` |
| `_build_pipeline(names, group_df)` | pipeline-level extras | – | + `channel`, staging | – |
| `_create_extra_resource_files(df, project_name)` | resource files beyond `pipelines.yml`/`jobs.yml` | `gateways.yml` | – | – |

`PostgreSQLStandardConnector` overrides `_build_pipeline` to add `source_configurations` (slot config), after `objects` as before.

### Rules

- **No branching on connector type.** All variation comes from abstract hooks, overrides, and class properties/attributes (`required_columns`, `default_values`, `supported_scd_types`, consistency fields). The only `if` statements are data-driven (for example, whether `include_columns` is set on a row).
- **One class per database per mode**, even when a database has no specific logic yet. This leaves room for database-specific behavior later without restructuring.
- **Database rules that apply in every mode** go in the source class (`connectors/<database>/source.py`). Source classes only override hooks and call `super()`; they contain no load-balancing or YAML logic. Mode-specific additions for one database (e.g. PostgreSQL slot config for the standard connector) stay in the concrete connector.
- **SaaS and database splitting are not shared.** Only the mechanical chunking (`BaseConnector._split_groups_by_size`) is shared. The grouping policy (group columns, levels, suffixes) determines resource names, which are a stability contract, so each family owns its own. `StandardConnector` and the single-level database split share `_add_base_group`.

### Registry names

Database connectors are named `<database>_<mode>`. The bare names of the existing connectors are kept as aliases (`ALIASES` in `core/registry.py`) so existing users see no change.

| Connector | Registry name | Aliases |
|---|---|---|
| SQL Server standard | `sql_server_standard` | `sql_server` |
| PostgreSQL standard | `postgresql_standard` | `postgresql` |
| Oracle integrated CDC | `oracle_integrated` | – |
| Oracle query-based | `oracle_query_based` | – |
| Future | `sql_server_integrated`, `postgresql_integrated`, `sql_server_query_based`, ... | – |

### File layout

```
src/tapworks/core/connectors.py          BaseConnector, DatabaseConnector, StandardConnector,
                                         IntegratedCDCConnector, QueryBasedConnector, SaaSConnector
src/tapworks/core/registry.py            CONNECTORS + ALIASES
src/tapworks/connectors/sql_server/      source.py, standard.py, connector.py (compat re-export)
src/tapworks/connectors/postgresql/      source.py, standard.py, connector.py (compat re-export)
src/tapworks/connectors/oracle/          source.py, integrated.py, query_based.py
examples/connectors/oracle_integrated/   basic/pipeline_config.csv, example_notebook.ipynb
examples/connectors/oracle_query_based/  basic/pipeline_config.csv, example_notebook.ipynb
```

## Backward compatibility

- `sql_server` and `postgresql` still work everywhere (CLI, `run_pipeline_generation()`, settings) as aliases; `tapworks --list` shows the canonical names and the aliases.
- `tapworks.connectors.sql_server.connector.SQLServerConnector` and `tapworks.connectors.postgresql.connector.PostgreSQLConnector` still import (re-exports of the standard connectors).
- CLI flags, `run_pipeline_generation()` parameters, CSV columns, and defaults for existing connectors are unchanged.
- Generated YAML for existing connectors is byte-identical, enforced by golden-file tests added **before** the refactor (see [RELEASING.md](./RELEASING.md)).
- Visible change: `tapworks --list` and `resolve_connector_name()` report the canonical names (`sql_server_standard`, `postgresql_standard`). Registry tests were updated for this; alias and old-import tests were added.

## Work order

Each step is its own commit; steps 2 and 3 can be one PR.

1. [x] **Release baseline**: tag current `main` as `v0.1.0` and create the GitHub Release (maintainer).
2. [x] **Safety net**: golden-file tests for all existing example CSVs and load-balancing cases; `CHANGELOG.md`.
3. [x] **Refactor, no output change**: `DatabaseConnector` and `StandardConnector`; SQL Server and PostgreSQL moved onto `StandardConnector`; PostgreSQL slot config via `_build_pipeline`. All existing tests and golden files unchanged. The single-level database split is deferred to step 5, its first user.
4. [x] **Gateway limit fix** (separate commit and changelog entry under "Changes to generated output"): `runner.py` inspects `generate_pipeline_config` instead of `run_complete_pipeline_generation`. See [RELEASING.md](./RELEASING.md#example-the-gateway-limit-fix).
5. [x] **Oracle integrated CDC**: single-level split and `connection_name` pipeline consistency in `DatabaseConnector` (`StandardConnector` keeps two levels and gateway-level `connection_name`); `IntegratedCDCConnector` and `OracleIntegratedConnector`, registry entry, example CSV and notebook, unit tests, golden files.
5b. [x] **Mode × database structure**: source classes (`connectors/<database>/source.py`), connectors renamed `<Database><Mode>Connector` in one module per mode, registry names `<database>_<mode>` with aliases for `sql_server`/`postgresql`, compat re-exports in `connector.py`. No output change.
6. [x] **Oracle query-based**: `QueryBasedConnector` and `OracleQueryBasedConnector`, registry entry, example CSV and notebook, unit tests, golden files.
7. [ ] **Docs**, per `AGENTS.md`: `README.md`, `docs/ARCHITECTURE.md`, `docs/CONFIGURATION.md`, `docs/USAGE.md`, `docs/VALIDATIONS.md`, `prompts/` (01, 02, 04, README).
8. [ ] **E2E in dogfood** (below).
9. [ ] **Release** `v0.3.0` with changelog. (`v0.2.0` shipped the gateway limit fix and golden-file tests.)

## E2E testing in dogfood

Workspace: `https://dogfood.staging.databricks.com/?o=6051921418418893`

1. Authenticate: `databricks auth login --host https://dogfood.staging.databricks.com --profile dogfood`.
2. Read-only checks:
   - an Oracle UC connection exists (`databricks connections list`),
   - destination catalogs/schemas are writable,
   - integrated CDC Beta is enabled in the workspace.
3. Generate bundles into the git-ignored `e2e/oracle/` and `e2e/oracle_query_based/`, then run `databricks bundle validate -t dev`.
4. With maintainer approval: `bundle deploy`, run the job, check that target tables fill, then `bundle destroy`. Record deployed resources in `e2e/TODO.md` as for the other connectors.

## Decisions

Decisions made for the Oracle integrated CDC connector (step 5). They can be revisited; changing a default or adding a column later is backward compatible as long as existing configs generate the same output.

| # | Decision | Rationale |
|---|---|---|
| 1 | **Default schedule: hourly (`0 * * * *`)** | Each integrated CDC update runs for about 30 minutes; the docs suggest 60 minutes as a starting point. |
| 2 | **Staging location: optional `staging_catalog`/`staging_schema` columns, falling back to `target_catalog`/`target_schema`; always emitted as `data_staging_options`.** | Same behavior as `gateway_catalog`/`gateway_schema` for gateway connectors, which also default to the target. Emitting it explicitly avoids the server default (pipeline catalog/schema), which differs from the other database connectors. Staging columns get the same UC naming and per-pipeline consistency checks. |
| 3 | **Lowercase identifier check: warning, not error** | Lowercase is valid for quoted Oracle identifiers, so it must not block generation. |
| 4 | **No error for mixed service names in one pipeline** | Not documented as invalid; an error could block valid configs. |
| 5 | **Include/exclude columns: inherited, emitted when set** | `include_columns`/`exclude_columns` are part of the generic `table_configuration` in the bundle schema; they are opt-in per row. Not yet verified against a live Oracle pipeline. |
| 6 | **`primary_keys` / `sequence_by`: not supported yet** | Autodetected by Lakeflow Connect; add when needed. |
| 7 | **SCD types: `SCD_TYPE_1`, `SCD_TYPE_2`** | As documented for Oracle. |
| 8 | **`channel: PREVIEW` and `connector_type: CDC` always set** | Both required for programmatic creation. Per the bundle schema, a database pipeline with `connection_name` and no `connector_type` defaults to query-based, so `CDC` must be explicit. |
| 9 | **Gateway limit fix: shipped in `v0.2.0`** | See [RELEASING.md](./RELEASING.md#example-the-gateway-limit-fix). |

Verified against the bundle schema from Databricks CLI v1.20.0 (`databricks bundle schema`): `connector_type` (`CDC`, `QUERY_BASED`), `channel`, `data_staging_options` (`catalog_name`, `schema_name`, `volume_name`), `table_configuration` (`include_columns`, `exclude_columns`, `primary_keys`, `sequence_by`, `scd_type`, `query_based_connector_config`).

### Query-based decisions (step 6)

| # | Decision | Rationale |
|---|---|---|
| Q1 | **`cursor_columns` required**, comma-separated | The documented incremental path. Snapshot mode for tables without a cursor is not supported yet. |
| Q2 | **`primary_keys` optional**, comma-separated | Emitted in `table_configuration` when set. |
| Q3 | **`deletion_condition` optional** | Soft deletes; emitted in `query_based_connector_config` when set. Hard-delete tracking (Beta) not supported yet. |
| Q4 | **`connector_type: QUERY_BASED` always set** | Explicit, though the bundle schema says it is the default for database pipelines with `connection_name`. |
| Q5 | **SCD types: `SCD_TYPE_1`, `SCD_TYPE_2`, `APPEND_ONLY`** | As documented; `APPEND_ONLY` is in the bundle schema enum. |
| Q6 | **No staging, no channel** | Query-based needs no staging volume and no preview channel. |
| Q7 | **Default schedule: hourly (`0 * * * *`)** | Matches the docs' job example and the integrated CDC connector. |
| Q8 | **Oracle only for now** | SQL Server, PostgreSQL, MySQL, ... query-based connectors are a source class plus a ~15-line module each. |

## Open decisions

1. **Oracle test source**: integrated CDC is being tested against `airnz_oracle_demo` in dogfood; query-based not yet tested end to end.
