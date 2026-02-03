# Step 7 — Refactor & cleanup (keep repo aligned)

> **Audience: Human** — Read this file as a workflow/playbook.
>
> **Audience: Agent** — Use the “Agent prompt” block below to propose safe cleanup changes after implementation.

## Goal
After implementing or updating connectors, ensure the repo stays consistent and free of redundant code.

## Agent prompt (copy/paste into Cursor/Claude)

```
Review the new/modified connector implementation and align it with repo conventions:
- keep run_complete_pipeline_generation() as the golden path
- remove duplicated shared logic
- ensure README + notebook + CLI commands are consistent
- ensure generated output directories are gitignored (output/ etc.)
Return a short checklist of concrete edits by file.
```

## Cleanup checklist

- Remove legacy scripts/paths that duplicate the OOP framework (prefer one “golden path”).
- Ensure README instructions match the current CLIs and output directories.
- Keep `.gitignore` focused on generated output (e.g., `output/`) rather than connector-specific legacy folders.
- Prefer connector-local examples in `<connector>/examples/` over root-level one-offs.
- Avoid committing generated DAB output unless the repo explicitly wants “checked-in deployments” under `examples/.../deployment/`.

## Alignment checklist for new connectors

- Connector class:
  - subclasses the correct base class (`DatabaseConnector` or `SaaSConnector`)
  - defines `connector_type`, `required_columns`, `default_values`
  - overrides `default_project_name` if needed
- CLI:
  - accepts consistent arguments (`--input-csv`, `--project-name`, `--workspace-host`, `--root-path`, `--output-dir`)
  - calls `run_complete_pipeline_generation(...)`
- Docs:
  - includes quickstart and troubleshooting
  - includes at least one example CSV

