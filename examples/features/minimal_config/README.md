# Minimal Config Example

Deploy Oracle integrated CDC and query-based pipelines from a config that only lists the tables to ingest. Everything else comes from `default_values` and `override_config`.

## Files

| File | Description |
|------|-------------|
| `tables.csv` | The minimal config: `source_schema`, `source_table_name` |
| `example_notebook.ipynb` | Creates the target schemas, generates both bundles, deploys them, runs each job once, and checks pipeline status |

## Where each setting comes from

| Setting | Source |
|---------|--------|
| `source_schema`, `source_table_name` | `tables.csv` |
| `source_database` (Oracle service name), `connection_name`, `target_catalog`, `pipeline_catalog` | `default_values`, shared by both bundles |
| `project_name`, `target_schema`, `pipeline_schema` | `default_values`, per bundle |
| `classic_compute` | `override_config` (forced for every row) |
| `target_table_name` | Derived from `source_table_name` |
| Schedule (hourly), staging location (target catalog/schema) | Connector defaults |

```python
shared_defaults = {
    'source_database': 'ORCLPDB1',
    'connection_name': 'my_oracle_connection',
    'target_catalog': 'main',
    'pipeline_catalog': 'main',
}

run_pipeline_generation(
    connector_name='oracle_integrated',          # or 'oracle_query_based'
    input_source='tables.csv',
    output_dir='deployment',
    targets=targets,
    default_values={**shared_defaults, 'project_name': 'tapworks_oracle_cdc',
                    'target_schema': 'tapworks_oracle_cdc', 'pipeline_schema': 'tapworks_oracle_cdc'},
    override_config={'classic_compute': 'true'},
)
```

## Notes

- Without `cursor_columns`, the query-based connector reads each table as a full snapshot. Add a `cursor_columns` column to `tables.csv` for incremental reads.
- Set `CLASSIC_COMPUTE = True` in the notebook if the Oracle database only accepts connections from classic compute; otherwise pipelines run on serverless.
- Integrated CDC is in Beta and must be enabled for the workspace.
