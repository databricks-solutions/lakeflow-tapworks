# Example workflow — add a new SaaS pipeline generator (Jira)

> **Audience: Human** — Read this end-to-end walkthrough.
>
> It references the workflow steps under `prompts/` and uses templates under `prompts/templates/` to scaffold code.

This walkthrough shows how to add a **new SaaS connector** to this repo using the prompt pack under `prompts/`.

We’ll use **Jira** as the example because it’s a common SaaS source with many objects (issues, projects, users, etc.) and some source-specific constraints (rate limits, incremental cursors, required fields) that are best handled explicitly.

## What you will build

You will add a new connector folder:

```text
jira/
  connector.py
  pipeline_generator.py
  pipeline_setup.ipynb
  README.md
  examples/basic/pipeline_config.csv
```

And it will generate one or more DAB projects:

```text
output/<project_name>/
  databricks.yml
  resources/
    pipelines.yml
    jobs.yml
```

## Step 1 — Understand the framework (required)

Follow `prompts/01_understand_oop_framework.md`.

You should internalize:
- The **golden entrypoint**: `BaseConnector.run_complete_pipeline_generation(...)`
- Grouping: **`prefix + subgroup` → `pipeline_group`**
- Auto-splitting when a group exceeds `max_tables_per_pipeline`

## Step 2 — Define the CSV contract for Jira

Start from the SaaS pattern in `prompts/02_add_or_update_connector.md`.

For Jira, a reasonable “table-based” contract treats each Jira entity as a “table”:
- `issue`, `project`, `user`, `issue_comment`, `issue_changelog`, etc.

### Minimal required columns
- `source_database` (e.g., `Jira`)
- `source_schema` (e.g., `cloud` or `server`, or instance label)
- `source_table_name` (e.g., `issue`)
- `target_catalog`, `target_schema`, `target_table_name`
- `connection_name`

### Optional (recommended) columns
These encode Jira-specific constraints so the generator can enforce them consistently:
- **Incremental/CDC**:
  - `mode`: `snapshot` or `cdc`
  - `cursor_field`: e.g. `updated`
  - `primary_keys`: e.g. `issue_id` (or `id`)
  - `lookback_days`: e.g. `7` (for initial/backfill windows)
- **API query scope**:
  - `jql`: e.g. `project = ABC ORDER BY updated ASC`
  - `projects`: optional comma-separated list to fan out per project (if you choose that design)
- **Pagination / rate limits**:
  - `page_size`: e.g. `100`
  - `max_requests_per_minute`: e.g. `300` (if supported by the ingestion definition/runtime)
- **Required fields when selecting columns**:
  - `include_columns` / `exclude_columns` (always ensure required ids + cursor fields stay included)

### Example CSV

```csv
project_name,source_database,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name,prefix,subgroup,schedule,mode,cursor_field,primary_keys,jql,page_size,lookback_days,include_columns
jira_it,Jira,cloud,issue,main,jira_bronze,issue,jira_connection,it,01,*/15 * * * *,cdc,updated,issue_id,"project in (ABC,DEF) ORDER BY updated ASC",100,7,"issue_id,key,updated,created,summary,status,project_id,assignee_id,reporter_id"
jira_it,Jira,cloud,project,main,jira_bronze,project,jira_connection,it,01,0 */6 * * *,snapshot,,,,"",100,,\"project_id,key,name,projectCategory\"
jira_sec,Jira,cloud,issue,main,jira_bronze,issue_security,jira_connection,sec,02,*/30 * * * *,cdc,updated,issue_id,\"labels = security ORDER BY updated ASC\",100,30,\"issue_id,key,updated,created,summary,status,project_id\"
```

Grouping outcome:
- `it_01` tables are one pipeline group (unless auto-split)
- `sec_02` is another pipeline group

## Step 3 — Encode Jira constraints explicitly (important)

Jira-specific constraints you should pass to Claude/Cursor (and encode in CSV defaults/validation):

- **Incremental ingestion requires cursor + id**
  - For Jira issues: cursor is typically `updated`, id is `id`/`issue_id`.
  - If `mode=cdc`, enforce `cursor_field` and `primary_keys` are present.
- **JQL is the real “filter language”**
  - Make `jql` an explicit CSV column so scope is reviewable and versioned.
- **Rate limits**
  - Jira Cloud enforces rate limits; your generator should include a `page_size` option and (if supported) a throttle setting.
- **Required fields**
  - If users specify `include_columns`, ensure id and cursor fields are still included (e.g., `issue_id`, `updated`).
- **Deletes**
  - Jira APIs don’t provide “delete streams” the same way some CDC sources do. If delete sync is required, document how you plan to model it (often: periodic snapshot, or tombstones via audit logs if available).

Tell the agent to implement these in three places:
1) **Defaults**: `default_values` includes safe defaults (`mode=snapshot`, `page_size=100`, `lookback_days=7`)
2) **Validation**: fail fast for invalid combinations (e.g., `mode=cdc` but missing cursor)
3) **YAML emission**: map CSV columns into the ingestion definition schema (objects, filters, incremental config)

## Step 4 — Implement `jira/connector.py` (SaaSConnector)

Use:
- `prompts/templates/saas_connector_template.md`
- Reference: `salesforce/connector.py`, `google_analytics/connector.py`

You’ll implement:
- `connector_type = "jira"`
- `required_columns` per the Jira CSV contract
- `default_values` including Jira options (`mode`, `page_size`, etc.)
- `_create_pipelines(...)` to map each row to an object entry
- `generate_yaml_files(...)` to write `pipelines.yml` and `jobs.yml`

**Critical decision:** confirm the Lakeflow Connect Jira ingestion-definition schema (what fields it expects for an “issue” object, how to pass JQL, etc.). Your generator must match that schema.

## Step 5 — Implement `jira/pipeline_generator.py` (CLI wrapper)

Follow `prompts/04_build_pipeline_generator_cli.md` and use `prompts/templates/pipeline_generator_cli_template.md`.

The CLI should:
- load CSV via `utilities.load_input_csv(...)`
- instantiate `JiraConnector`
- call `run_complete_pipeline_generation(...)`
- print next steps for validate/deploy

## Step 5b — Add `jira/pipeline_setup.ipynb` (recommended)

Add a notebook so users can run generation interactively inside Databricks (mirrors `sqlserver/pipeline_setup.ipynb`).

Use:
- `prompts/templates/pipeline_setup_notebook_template.md`

For Jira (SaaS connector), the notebook should:
- Use repo-relative CSV paths, e.g. `INPUT_CSV = "examples/basic/pipeline_config.csv"`
- Use `/Workspace/Users/{username}/...` output directories
- Use `targets={"dev": {"workspace_host": WORKSPACE_HOST}}` (no `root_path` needed for SaaS)

## Step 6 — Generate / validate / deploy

Follow `prompts/05_generate_validate_deploy.md`.

```bash
python jira/pipeline_generator.py \
  --input-csv jira/examples/basic/pipeline_config.csv \
  --project-name jira_it \
  --workspace-host https://<workspace-host> \
  --root-path '~/.bundle/${bundle.name}/${bundle.target}' \
  --output-dir output

cd output/jira_it
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

## Step 7 — Cleanup

Follow `prompts/07_refactor_cleanup.md`:
- Keep “golden path” as `run_complete_pipeline_generation(...)`
- Avoid connector-local re-implementations of shared logic

