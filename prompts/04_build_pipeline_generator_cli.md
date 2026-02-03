# Step 4 — Build a pipeline generator CLI

> **Audience: Human** — Read this file as a workflow/playbook.
>
> **Audience: Agent** — Use the “Agent prompt” block below (and the templates) to generate the CLI + notebook entrypoints.

## Goal
Create a connector-specific CLI script that turns an input CSV into one or more DAB projects under an output directory.

## Agent prompt (copy/paste into Cursor/Claude)

```
Create <connector>/pipeline_generator.py as a thin CLI wrapper around connector.run_complete_pipeline_generation().
Also add <connector>/pipeline_setup.ipynb (recommended) to run generation interactively in Databricks.

Rules:
- Use utilities.load_input_csv
- Do not duplicate shared load-balancing logic
- Use repo-relative CSV paths in notebooks (e.g. examples/basic/pipeline_config.csv)
- Print next-step commands for bundle validate/deploy
```

## Required outputs
For a connector folder `myconnector/`, implement:

- `myconnector/pipeline_generator.py`
- `myconnector/pipeline_setup.ipynb` (recommended; mirrors `sqlserver/pipeline_setup.ipynb`)

## Required CLI behavior

- Inputs:
  - `--input-csv` path to CSV
  - `--project-name` default project identifier (used if CSV doesn’t specify it)
  - `--workspace-host` Databricks workspace host (for DAB target)
  - `--root-path` bundle root path (recommend: `~/.bundle/${bundle.name}/${bundle.target}`)
  - `--output-dir` output folder (recommend: `output`)
- Workflow:
  - Load CSV using `utilities.load_input_csv(...)`
  - Instantiate the connector class
  - Call `connector.run_complete_pipeline_generation(...)`
  - Print next-step commands to validate/deploy

## “Golden path” example (what the CLI should enable)

```bash
python myconnector/pipeline_generator.py \
  --input-csv myconnector/examples/basic/pipeline_config.csv \
  --project-name my_project \
  --workspace-host https://<workspace-host> \
  --root-path '/Users/<you>/.bundle/${bundle.name}/${bundle.target}' \
  --output-dir output

cd output/my_project
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Acceptance criteria

- Running the CLI generates:
  - `output/<project_name>/databricks.yml`
  - `output/<project_name>/resources/pipelines.yml`
  - `output/<project_name>/resources/jobs.yml`
  - and `resources/gateways.yml` for database connectors
- The bundle validates with `databricks bundle validate`.

## Notebook guidance (recommended)

Add `myconnector/pipeline_setup.ipynb` so users can run the generation interactively inside Databricks.

Requirements:
- Uses **repo-relative** CSV paths (so it works when the repo is a Databricks Repo), e.g.:
  - `INPUT_CSV = "examples/basic/pipeline_config.csv"` (or `examples/field_engg/pipeline_config.csv`)
- Output directory should be under the user’s workspace home, e.g.:
  - `OUTPUT_DIR = f"/Workspace/Users/{username}/lakehouse-tapworks/myconnector/examples/basic/deployment"`
- Uses `WorkspaceClient()` to auto-detect:
  - `username = w.current_user.me().user_name`
  - `WORKSPACE_HOST = w.config.host`
  - `ROOT_PATH = f"/Users/{username}/.bundle/${bundle.name}/${bundle.target}"` (database connectors)

Include at least these cells:
- “Basic run” cell that generates one deployment folder
- “Advanced” cells showing:
  - `override_input_config={...}` (force schedule / node types / etc. for all rows)
  - `default_values={...}` (fill missing CSV values only)

Template:
- `prompts/templates/pipeline_setup_notebook_template.md`

