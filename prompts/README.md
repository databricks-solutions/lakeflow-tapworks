# Agentic coding prompts (OOP connector generators)

These prompts/templates are modeled after `lakeflow-community-connectors/prompts/` and are intended to instruct Claude/Cursor how to build and extend this repository’s **OOP-based connector + DAB generator** framework.

## How to use this folder (human vs agent)

This folder contains two different kinds of content:

- **Workflows (human-facing)**: the numbered `0x_*.md` files are step-by-step playbooks you read and follow. They explain sequencing, acceptance criteria, and what to verify.
- **Templates (agent/scaffold)**: files under `templates/` are copy/paste starter skeletons (code/CSV/notebook). They are intended to be turned into real repo files like `<connector>/connector.py`, `<connector>/pipeline_generator.py`, and `<connector>/pipeline_setup.ipynb`.

Convention used throughout:
- **“Audience: Human”** sections are meant to be read by the contributor.
- **“Audience: Agent”** sections are meant to be pasted into Cursor/Claude chat (or used as a scaffold) to generate actual files.

## Workflow

- **Step 1: Understand the framework** → `01_understand_oop_framework.md`
- **Step 2: Add or update a connector class** → `02_add_or_update_connector.md`
- **Step 3: Add connection creation helpers (optional)** → `03_add_connection_helpers.md`
- **Step 4: Build a pipeline generator CLI** → `04_build_pipeline_generator_cli.md`
- **Step 5: Generate / validate / deploy** → `05_generate_validate_deploy.md`
- **Step 6: Troubleshoot common failures** → `06_troubleshoot_common_failures.md`
- **Step 7: Cleanup / align with repo style** → `07_refactor_cleanup.md`

## Templates

See `templates/` for:
- Connector skeletons (database + SaaS)
- Pipeline generator CLI skeleton
- Pipeline setup notebook template (for interactive Databricks runs)
- Example `pipeline_config.csv` formats
- A standard test plan checklist

## Example workflows

- `examples/jira_new_connector_walkthrough.md`: end-to-end walkthrough for adding a new **SaaS** pipeline generator (Jira example), including how to encode source-specific constraints into CSV options.

