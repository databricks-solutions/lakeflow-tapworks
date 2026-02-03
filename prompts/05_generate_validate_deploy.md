# Step 5 — Generate, validate, deploy (Databricks Asset Bundles)

> **Audience: Human** — Read this file as a workflow/playbook.
>
> **Audience: Agent** — Use the “Agent prompt” block below to output copy/paste commands for a specific connector.

## Goal
Provide a consistent, copy/paste workflow for generating and deploying a connector bundle to a Databricks workspace.

## Agent prompt (copy/paste into Cursor/Claude)

```
Given a connector name and an example CSV path, produce the exact command sequence to:
1) generate the bundle via <connector>/pipeline_generator.py
2) validate with databricks bundle validate
3) deploy with databricks bundle deploy
Include the expected output files under output/<project_name>/.
```

## Inputs you must have

- A Unity Catalog connection already created for the connector (if applicable)
- A `pipeline_config.csv` using the connector’s schema
- Databricks CLI authenticated to the target workspace

## Standard workflow

### Generate

```bash
python <connector>/pipeline_generator.py \
  --input-csv <connector>/examples/basic/pipeline_config.csv \
  --project-name <project_name> \
  --workspace-host https://<workspace-host> \
  --root-path '/Users/<you>/.bundle/${bundle.name}/${bundle.target}' \
  --output-dir output
```

### Validate

```bash
cd output/<project_name>
databricks bundle validate -t dev
```

### Deploy

```bash
databricks bundle deploy -t dev
```

### Run the pipelines (common)

For most Lakeflow Connect pipelines you’ll run a “start update”, optionally with full refresh:

```bash
# list pipelines (optional)
databricks pipelines list

# start update (example; pick pipeline id or name as appropriate)
databricks pipelines start-update --pipeline-id <pipeline-id> --full-refresh
```

## Verification checklist

- Pipelines reach `COMPLETED` (or stable `RUNNING` if continuous)
- Target UC tables exist in `target_catalog.target_schema`
- Query row counts for a small subset of tables

Use `prompts/templates/test_plan_template.md` to capture a repeatable test plan per connector.

