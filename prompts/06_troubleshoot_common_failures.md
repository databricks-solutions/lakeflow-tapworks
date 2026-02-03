# Step 6 — Troubleshoot common failures

> **Audience: Human** — Read this file as a workflow/playbook.
>
> **Audience: Agent** — Use the “Agent prompt” block below when debugging a specific failure with logs/error text.

## Goal
Diagnose common end-to-end failures in Lakeflow Connect ingestion and DAB deployment.

## Agent prompt (copy/paste into Cursor/Claude)

```
Given the full error message/log snippet, identify:
- which stage failed (CSV normalization, grouping, YAML generation, bundle validate/deploy, pipeline runtime)
- the most likely root cause
- the minimal fix, and where to apply it (CSV vs connector code vs UC connection settings)
Prefer repo-consistent fixes; do not add extra YAML fields not supported by DAB schemas.
```

## DAB validation/deploy failures

- **Unknown/unexpected YAML fields**
  - Symptom: `bundle validate` warnings/errors about schema/fields.
  - Fix: ensure generated `resources/*.yml` match supported DAB schemas and the connector-specific Lakeflow Connect spec. Avoid adding extra fields “just in case”.

- **Bad `root_path`**
  - Symptom: deploy succeeds but bundle paths don’t resolve, or workspace paths are wrong.
  - Fix: use `~/.bundle/${bundle.name}/${bundle.target}` style paths (see README examples).

## Unity Catalog / destination table failures

- **Table already owned by another pipeline**
  - Symptom: error indicating the table is already managed/owned by a different pipeline.
  - Fix: change destination schema/table names (use a dedicated schema per test run), or delete/retire the existing pipeline ownership carefully.

## Salesforce-specific ingestion failures

- **Required CDC/system columns missing when using `include_columns`**
  - Symptom: pipeline complains required columns (commonly `SystemModstamp`, `IsDeleted`) are missing from definition.
  - Fix: ensure those columns are included in `include_columns`, or avoid include-list filtering.

## PostgreSQL-specific ingestion failures (CDC)

- **CDC not enabled / logical decoding not enabled**
  - Symptom: errors mentioning `wal_level` not set to `logical`, or missing replication privileges.
  - Fix:
    - Enable logical decoding (e.g., Cloud SQL `cloudsql.logical_decoding=on`)
    - Ensure `wal_level=logical`
    - Grant `REPLICATION` privilege for the configured user
    - Full refresh the ingestion table after enabling CDC

- **Replica identity not configured**
  - Symptom: errors indicating missing PK / TOAST-able columns require FULL replica identity.
  - Fix: `ALTER TABLE <schema>.<table> REPLICA IDENTITY FULL;` (evaluate carefully for production).

## UC connection quirks (PostgreSQL)

- **UC `POSTGRESQL` connections don’t accept `database`**
  - Symptom: connection creation fails or host is incorrectly constructed as `host/db`.
  - Fix: keep connection host as hostname only, choose database per table using `source_database` in CSV.

