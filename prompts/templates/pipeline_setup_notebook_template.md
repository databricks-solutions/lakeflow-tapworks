# Pipeline setup notebook template

> **Audience: Agent (template/scaffold)** — Use this to create `<connector>/pipeline_setup.ipynb`.
>
> This is not a step-by-step workflow; see `prompts/04_build_pipeline_generator_cli.md` for the human-facing instructions.

Create: `<connector>/pipeline_setup.ipynb`

Goal: a small Databricks notebook that runs `connector.run_complete_pipeline_generation(...)` interactively (mirrors `sqlserver/pipeline_setup.ipynb`).

## Notebook structure

### 1) Autoreload

```python
%load_ext autoreload
%autoreload 2
```

### 2) Install minimal deps

```python
%pip install pyyaml
```

### 3) Imports + workspace-derived paths

```python
from databricks.sdk import WorkspaceClient

from utilities import load_input_csv
from <connector>.connector import <ConnectorClass>

w = WorkspaceClient()
username = w.current_user.me().user_name
workspace_host = w.config.host

WORKSPACE_HOST = workspace_host

# For database connectors (gateways): keep ROOT_PATH and pass it in targets.
# For SaaS connectors: you can omit ROOT_PATH entirely and only pass workspace_host.
ROOT_PATH = f"/Users/{username}/.bundle/${{bundle.name}}/${{bundle.target}}"
```

### 4) Basic run (use repo-relative CSV path)

```python
INPUT_CSV = "examples/basic/pipeline_config.csv"
OUTPUT_DIR = f"/Workspace/Users/{username}/lakehouse-tapworks/<connector>/examples/basic/deployment"

df = load_input_csv(INPUT_CSV)
connector = <ConnectorClass>()

# Database connector example (includes ROOT_PATH)
result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir=OUTPUT_DIR,
    targets={
        "dev": {
            "workspace_host": WORKSPACE_HOST,
            "root_path": ROOT_PATH,
        }
    },
    max_tables_per_gateway=250,
    max_tables_per_pipeline=250,
)
```

If `<connector>` is a SaaS connector, use:

```python
result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir=OUTPUT_DIR,
    targets={"dev": {"workspace_host": WORKSPACE_HOST}},
    max_tables_per_pipeline=250,
)
```

### 5) Check generated files

```python
import os

files_to_check = [
    "databricks.yml",
    "resources/pipelines.yml",
    "resources/jobs.yml",
]

# Database connectors also produce gateways.yml
# files_to_check.append("resources/gateways.yml")

for file in files_to_check:
    full_path = os.path.join(OUTPUT_DIR, file)
    exists = "✓" if os.path.exists(full_path) else "✗"
    print(f"  {exists} {file}")
```

### 6) Advanced: overrides vs defaults

**Override** (force for all rows):

```python
result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir=OUTPUT_DIR + "_overrides",
    targets={"dev": {"workspace_host": WORKSPACE_HOST, "root_path": ROOT_PATH}},
    override_input_config={
        "schedule": "0 0 * * 0",
        "gateway_worker_type": "m6d.xlarge",
        "gateway_driver_type": "m6d.xlarge",
    },
    max_tables_per_gateway=250,
    max_tables_per_pipeline=250,
)
```

**Default values** (fill only missing/empty CSV values):

```python
result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir=OUTPUT_DIR + "_defaults",
    targets={"dev": {"workspace_host": WORKSPACE_HOST, "root_path": ROOT_PATH}},
    default_values={
        "schedule": "0 */12 * * *",
        "gateway_worker_type": "m7d.xlarge",
        "gateway_driver_type": "m7d.xlarge",
    },
    max_tables_per_gateway=250,
    max_tables_per_pipeline=250,
)
```

## Notes to the agent

- Keep `INPUT_CSV` **repo-relative** so the notebook works when this repo is attached as a Databricks Repo.
- Keep `OUTPUT_DIR` under `/Workspace/Users/{username}/...` so users can browse generated files from the workspace UI.
- Don’t duplicate load-balancing logic in the notebook—always call `run_complete_pipeline_generation(...)`.

