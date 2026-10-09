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

| | Gateway CDC (current) | Integrated CDC | Query-based |
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

Mode (gateway, integrated CDC, query-based) is the main axis because it decides the structure: resources, splitting, and how the pipeline connects. The source (SQL Server, PostgreSQL, Oracle, ...) adds small details, so source classes are the leaves.

```
BaseConnector                      (unchanged: defaults/overrides, validation, jobs, databricks.yml,
│                                   _split_groups_by_size)
├── SaaSConnector                  (unchanged)
│
└── DatabaseConnector (abstract)   NEW: shared database logic
    │                                - pipeline-building flow, table entries, table configuration
    │                                - target_table_name fallback, SCD validation
    │                                - database grouping (_add_base_group) + single-level split
    │                                - pipelines.yml + jobs.yml writing
    │                                - pipeline consistency: connection_name, pipeline_catalog,
    │                                  pipeline_schema, tags
    │
    ├── GatewayConnector (abstract)        renamed from today's DatabaseConnector
    │   │                                    + gateways, two-level split, gateways.yml
    │   ├── SQLServerConnector             'sql_server'               (unchanged behavior)
    │   └── PostgreSQLConnector            'postgresql'               (+ slot source_configurations)
    │
    ├── IntegratedCDCConnector (abstract)  + connection_name, connector_type: CDC,
    │   │                                    channel: PREVIEW, staging
    │   └── OracleConnector                'oracle'
    │       (later: SQLServerIntegratedConnector, PostgreSQLIntegratedConnector)
    │
    └── QueryBasedConnector (abstract)     + connection_name, cursor_columns,
        │                                    deletion_condition, APPEND_ONLY
        └── OracleQueryBasedConnector      'oracle_query_based'
            (later: SQLServerQueryBasedConnector, PostgreSQLQueryBasedConnector, MySQL, ...)
```

### Hooks in `DatabaseConnector`

`DatabaseConnector._create_pipelines` implements the full flow: loop over pipeline groups, generate names, build table entries, set `name`/`catalog`/`schema`, build `ingestion_definition`, and add tags. Subclasses only fill in hooks:

| Hook | Purpose | Gateway | Integrated CDC | Query-based |
|---|---|---|---|---|
| `_ingestion_source(group_df)` (abstract) | how the pipeline reaches the source | `ingestion_gateway_id` | `connection_name`, `connector_type: CDC` | `connection_name` |
| `_build_table_configuration(row)` | per-table options | base | base (+ `primary_keys`) | base + `query_based_connector_config` |
| `_build_pipeline(pipeline_group, group_df)` | pipeline-level extras | – | + `channel`, staging | – |

`PostgreSQLConnector` overrides `_build_pipeline` to add `source_configurations` (slot config). This replaces today's second loop that patches the pipelines after they are built. Key order must stay the same (`source_configurations` after `objects`).

### Rules

- **No branching on connector type.** All variation comes from abstract hooks, overrides, and class properties/attributes (`required_columns`, `default_values`, `supported_scd_types`, consistency fields). The only `if` statements are data-driven (for example, whether `include_columns` is set on a row).
- **One class per source per mode**, even when a source has no specific logic yet. This leaves room for source-specific behavior later without restructuring.
- **Source-specific logic shared across modes** goes in a plain function in that source's module, for example Oracle's uppercase check, or PostgreSQL slot config once integrated PostgreSQL exists. No mixins.
- **SaaS and database splitting are not shared.** Only the mechanical chunking (`BaseConnector._split_groups_by_size`) is shared. The grouping policy (group columns, levels, suffixes) determines resource names, which are a stability contract, so each family owns its own. `GatewayConnector` and the single-level database split share `_add_base_group`.

### Registry names

| Connector | Registry name |
|---|---|
| SQL Server gateway (existing) | `sql_server` (unchanged) |
| PostgreSQL gateway (existing) | `postgresql` (unchanged) |
| Oracle integrated CDC | `oracle` |
| Oracle query-based | `oracle_query_based` |
| Future integrated CDC | `sql_server_integrated`, `postgresql_integrated` |
| Future query-based | `sql_server_query_based`, `postgresql_query_based`, ... |

Existing names keep their current meaning, because changing what `sql_server` generates would change existing deployments.

### File layout

```
src/tapworks/core/connectors.py        BaseConnector, SaaSConnector (database classes moved out)
src/tapworks/core/database.py          NEW: DatabaseConnector, GatewayConnector,
                                            IntegratedCDCConnector, QueryBasedConnector
src/tapworks/core/__init__.py          re-exports all base classes
src/tapworks/connectors/oracle/        NEW: OracleConnector, OracleQueryBasedConnector
src/tapworks/connectors/sql_server/    SQLServerConnector (parent → GatewayConnector)
src/tapworks/connectors/postgresql/    PostgreSQLConnector (parent → GatewayConnector)
examples/connectors/oracle/            NEW: basic/pipeline_config.csv, example_notebook.ipynb
examples/connectors/oracle_query_based/ NEW: basic/pipeline_config.csv, example_notebook.ipynb
```

Nothing in the repo imports `tapworks.core.connectors` directly, so moving classes is safe as long as `tapworks.core` re-exports them.

### Expected generated YAML

Oracle integrated CDC pipeline:

```yaml
resources:
  pipelines:
    pipeline_sales_p01:
      name: sales_p01
      channel: PREVIEW
      catalog: <pipeline_catalog>
      schema: <pipeline_schema>
      ingestion_definition:
        connection_name: <connection_name>
        connector_type: CDC
        objects:
          - table:
              source_catalog: ORCL          # service name
              source_schema: HR
              source_table: EMPLOYEES
              destination_catalog: main
              destination_schema: bronze
              destination_table: employees
              table_configuration:
                scd_type: SCD_TYPE_1
        # staging location: pending decision (see Open decisions)
```

Oracle query-based pipeline:

```yaml
resources:
  pipelines:
    pipeline_sales_p01:
      name: sales_p01
      catalog: <pipeline_catalog>
      schema: <pipeline_schema>
      ingestion_definition:
        connection_name: <connection_name>
        objects:
          - table:
              source_catalog: ORCL
              source_schema: HR
              source_table: EMPLOYEES
              destination_catalog: main
              destination_schema: bronze
              destination_table: employees
              table_configuration:
                query_based_connector_config:
                  cursor_columns: [UPDATED_AT]
```

Both produce `jobs.yml` and `databricks.yml` the same way existing connectors do, and no `gateways.yml`. Pipeline names use the single-level pattern (`{base_group}_p{NN}`).

## Backward compatibility

- Registry names, CLI flags, `run_pipeline_generation()` parameters, CSV columns, and defaults for existing connectors are unchanged.
- Existing tests pass without modification.
- Generated YAML for existing connectors is byte-identical, enforced by golden-file tests added **before** the refactor (see [RELEASING.md](./RELEASING.md)).
- Internal class names change (`DatabaseConnector` → `GatewayConnector`, and `DatabaseConnector` becomes the new shared base). This is acceptable because users select connectors by registry name and nobody builds connectors outside this repo.

## Work order

Each step is its own commit; steps 2 and 3 can be one PR.

1. [x] **Release baseline**: tag current `main` as `v0.1.0` and create the GitHub Release (maintainer).
2. [x] **Safety net**: golden-file tests for all existing example CSVs and load-balancing cases; `CHANGELOG.md`.
3. [ ] **Refactor, no output change**: `core/database.py` with the new hierarchy; SQL Server and PostgreSQL moved onto `GatewayConnector`; PostgreSQL slot config via `_build_pipeline`. All existing tests and golden files must be unchanged.
4. [x] **Gateway limit fix** (separate commit and changelog entry under "Changes to generated output"): `runner.py` inspects `generate_pipeline_config` instead of `run_complete_pipeline_generation`. See [RELEASING.md](./RELEASING.md#example-the-gateway-limit-fix).
5. [ ] **Oracle integrated CDC**: `IntegratedCDCConnector` and `OracleConnector`, registry entry, example CSV and notebook, unit tests, golden files.
6. [ ] **Oracle query-based**: `QueryBasedConnector` and `OracleQueryBasedConnector`, registry entry, example CSV and notebook, unit tests, golden files.
7. [ ] **Docs**, per `AGENTS.md`: `README.md`, `docs/ARCHITECTURE.md`, `docs/CONFIGURATION.md`, `docs/USAGE.md`, `docs/VALIDATIONS.md`, `prompts/` (01, 02, 04, README).
8. [ ] **E2E in dogfood** (below).
9. [ ] **Release** `v0.2.0` with changelog.

## E2E testing in dogfood

Workspace: `https://dogfood.staging.databricks.com/?o=6051921418418893`

1. Authenticate: `databricks auth login --host https://dogfood.staging.databricks.com --profile dogfood`.
2. Read-only checks:
   - an Oracle UC connection exists (`databricks connections list`),
   - destination catalogs/schemas are writable,
   - integrated CDC Beta is enabled in the workspace.
3. Generate bundles into the git-ignored `e2e/oracle/` and `e2e/oracle_query_based/`, then run `databricks bundle validate -t dev`.
4. With maintainer approval: `bundle deploy`, run the job, check that target tables fill, then `bundle destroy`. Record deployed resources in `e2e/TODO.md` as for the other connectors.

## Open decisions

1. **Default schedules** (AGENTS.md requires confirmation):
   - Integrated CDC: hourly `0 * * * *` per the docs, or `*/15 * * * *`?
   - Query-based: ?
2. **Staging location for integrated CDC**:
   - new optional `staging_catalog`/`staging_schema` columns falling back to `pipeline_catalog`/`pipeline_schema`,
   - required columns,
   - or omit (as in the docs' DAB example).

   Also confirm the exact DAB field name (`data_staging_options` in the REST example).
3. **Query-based columns**: `cursor_columns` and `primary_keys` as comma-separated strings (like `include_columns`)? Support `deletion_condition` now?
4. **Validations**:
   - warn when Oracle identifiers aren't uppercase,
   - error when a pipeline group mixes `source_database` (service name) values,
   - confirm `cursor_columns` as a required column for query-based.
5. **Query-based sources in this change**: Oracle only, or also SQL Server and PostgreSQL?
6. ~~**Gateway limit fix**: ship in this release or a later one?~~ Decided: fixed in this release (step 4).
7. **Oracle test source**: is an Oracle database reachable from dogfood with a UC connection? For CDC it also needs archive and supplemental logging.
8. **Include/exclude columns** for integrated CDC and query-based: not documented for these modes; confirm before emitting.
