# Step 3 — Add connection helpers (optional)

> **Audience: Human** — Read this file as a workflow/playbook.
>
> **Audience: Agent** — Use the “Agent prompt” block below to implement helper scripts.

## Goal
Provide a small, connector-specific helper script to create required Unity Catalog connections, catalogs, or schemas.

## Agent prompt (copy/paste into Cursor/Claude)

```
If this connector has setup steps users frequently get wrong, add a helper script (idempotent if possible) under <connector>/.

Rules:
- Do not hardcode host/tokens; rely on standard Databricks auth or accept args
- Print actionable output (what it created, what secrets/keys are expected, links to README)
- Prefer safe re-runs
```

## When to add helpers

Add helpers when users regularly fail on setup steps or when setup is verbose.
Examples already in this repo:

- `postgres/create_postgres_connection.py`
- `google_analytics/create_ga4_connection.py`
- `google_analytics/create_ga4_catalog.py`

## Rules

- Prefer **idempotent** behavior (safe to re-run).
- Print clear guidance:
  - what will be created/updated
  - required secret scopes/keys (if applicable)
  - links to the connector README section
- Do not hardcode workspace host/tokens; accept via CLI args or rely on standard Databricks auth.

## Common UC nuances

- **PostgreSQL**: UC `POSTGRESQL` connection options do not accept a `database` field.
  - Choose database per table using `source_database` in the CSV.

## Acceptance criteria

- Script has a `--help` that shows usage examples.
- Script errors are actionable (missing args, auth issues, permission issues).

