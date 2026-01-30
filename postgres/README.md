## PostgreSQL (Lakeflow Connect)

This connector generates **gateway + ingestion** Lakeflow Connect pipelines for PostgreSQL using the shared OOP framework.

### 0) (Optional) Run via notebook

If you prefer an interactive setup (similar to `sqlserver/`), use:

- `postgres/pipeline_setup.ipynb`

### 1) Create a Unity Catalog connection (PostgreSQL)

Store credentials in a secret scope (example keys: `postgres_username`, `postgres_password`), then create a UC connection:

```bash
python postgres/create_postgres_connection.py <connection_name> <secret_scope> --host <hostname> --port 5432
```

Important: **UC POSTGRESQL connections don’t accept a `database` option**. Pick the database per table using `source_database` in the CSV.

### 2) Configure tables (CSV)

Use the same required columns as other database connectors:

```csv
source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name
postgres,public,customers,main,postgres_cloudsql_test,customers,postgres_cloudsql_test
```

Examples:
- `postgres/examples/basic/pipeline_config.csv`
- `postgres/examples/field_engg/pipeline_config.csv`

### 3) Generate DAB projects

```bash
python postgres/pipeline_generator.py \
  --input-csv postgres/examples/basic/pipeline_config.csv \
  --project-name postgres_cloudsql_test \
  --workspace-host https://<workspace-host> \
  --root-path '/Users/<you>/.bundle/${bundle.name}/${bundle.target}' \
  --output-dir output
```

### 4) Deploy

```bash
cd output/<project_name>
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

### CDC prerequisites (common failure causes)

- **Logical decoding enabled** (`wal_level=logical`, and Cloud SQL `cloudsql.logical_decoding=on`)
- **Replication privilege** for the DB user used by the connection
- **Replica identity**: tables without a usable PK (or with TOAST-able columns) may require `REPLICA IDENTITY FULL`

