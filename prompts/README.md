# Connector Development Prompts

Guides for adding new connectors to the Lakehouse Tapworks framework.

## How to Use

These prompts are designed to work with AI coding assistants (Claude, Cursor, etc.). You can:

1. **Point the AI at the whole folder**: "Read the prompts folder and help me create a new connector for Jira"
2. **Follow step by step**: Work through each numbered file in order
3. **Reference specific sections**: Jump to what you need

## Workflow

| Step | File | Description |
|------|------|-------------|
| 1 | `01_understand_framework.md` | Learn the architecture before coding |
| 2 | `02_add_connector.md` | Implement your connector class |
| 3 | `03_test_and_deploy.md` | Test locally and deploy to Databricks |
| 4 | `04_reference.md` | Quick reference for columns, methods, patterns |

## Quick Start

If you're familiar with the codebase, here's the minimal path:

1. **Create connector class** in `myconnector/connector.py`
   - Extend `DatabaseConnector` or `SaaSConnector`
   - Implement `connector_type`, `required_columns`, `default_values`
   - Implement `_create_pipelines()` (SaaS) or full YAML generation (Database)

2. **Register** in `core/registry.py`

3. **Add example** CSV in `myconnector/examples/basic/pipeline_config.csv`

4. **Test**: `python -m core.cli myconnector --input-config myconnector/examples/basic/pipeline_config.csv --output-dir output --targets '{"dev": {"workspace_host": "https://your-workspace.databricks.com"}}'`

## Examples

- `examples/jira_walkthrough.md` - End-to-end example for adding a Jira connector

## Reference Implementations

| Type | Connector | Notes |
|------|-----------|-------|
| Database | `sqlserver/` | Full gateway support |
| Database | `postgres/` | Similar to SQL Server |
| SaaS | `salesforce/` | Column filtering support |
| SaaS | `google_analytics/` | GA4 with property config |
| SaaS | `servicenow/` | Basic SaaS pattern |
| SaaS | `workday_reports/` | Report-based ingestion |
