# Databricks notebook source
"""
Unified Notebook Runner for Databricks Asset Bundle Pipeline Generation.

This notebook provides a single entry point for generating DAB pipelines
for any connector type. Edit the configuration section below and run all cells.

Supported connectors:
  - salesforce
  - sql_server
  - postgresql
  - google_analytics
  - servicenow
  - workday_reports
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # DAB Pipeline Generator
# MAGIC
# MAGIC Edit the configuration in the next cell, then run all cells to generate your DAB files.

# COMMAND ----------

# ============================================================================
# CONFIGURATION - Edit these values
# ============================================================================

# Connector to use (see supported connectors above)
connector_name = "salesforce"

# Input source: Delta table name (e.g., "catalog.schema.table") or CSV path
input_source = "main.pipeline_config.salesforce_tables"

# Output directory for generated DAB files
output_dir = "/Workspace/Users/your.email@company.com/dab_output"

# Target environments
targets = {
    "dev": {
        "workspace_host": "https://dev.cloud.databricks.com",
        "root_path": "/Shared/pipelines/dev",
    },
    "prod": {
        "workspace_host": "https://prod.cloud.databricks.com",
        "root_path": "/Shared/pipelines/prod",
    },
}

# Default values for optional columns (applied when column is missing or empty)
default_values = {
    "project_name": "my_ingestion_project",
    "schedule": "0 */6 * * *",  # Every 6 hours
}

# Override values (applied to ALL rows, overwriting any existing values)
override_config = None  # Set to dict like {"schedule": "0 0 * * *"} to override

# Pipeline limits
max_tables_per_pipeline = 250
max_tables_per_gateway = 250  # Only used for database connectors

# Optional: Save processed config to CSV
output_config = None  # Set to path like "/Workspace/.../processed_config.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Pipeline Generation
# MAGIC
# MAGIC The cells below execute the pipeline generation. No edits needed.

# COMMAND ----------

import sys
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
username = w.current_user.me().user_name

# Add src directory to path for tapworks imports
# Adjust this path if you cloned the repo to a different location
repo_path = f'/Workspace/Users/{username}/lakeflow-tapworks'
sys.path.insert(0, f'{repo_path}/src')

from tapworks.core.runner import run_pipeline_generation, list_connectors, get_connector_info

# COMMAND ----------

# Show available connectors (optional - for reference)
print("Available connectors:")
for name in list_connectors():
    print(f"  - {name}")

# COMMAND ----------

# Show connector info (optional - for reference)
info = get_connector_info(connector_name)
print(f"\nConnector: {info['name']}")
print(f"Required columns: {info['required_columns']}")
print(f"Default values: {info['default_values']}")

# COMMAND ----------

# Run pipeline generation
result_df = run_pipeline_generation(
    connector_name=connector_name,
    input_source=input_source,
    output_dir=output_dir,
    targets=targets,
    default_values=default_values,
    override_config=override_config,
    max_tables_per_pipeline=max_tables_per_pipeline,
    max_tables_per_gateway=max_tables_per_gateway
)

print(f"\nProcessed {len(result_df)} tables")
print(f"Pipeline groups: {result_df['pipeline_group'].nunique()}")
if 'gateway' in result_df.columns:
    print(f"Gateways: {result_df['gateway'].nunique()}")

# COMMAND ----------

# Display the result
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generated Files
# MAGIC
# MAGIC The following files were generated:

# COMMAND ----------

# List generated files
import os

print(f"\nGenerated files in: {output_dir}\n")
for root, dirs, files in os.walk(output_dir):
    level = root.replace(output_dir, '').count(os.sep)
    indent = '  ' * level
    print(f"{indent}{os.path.basename(root)}/")
    sub_indent = '  ' * (level + 1)
    for file in files:
        print(f"{sub_indent}{file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. Review the generated files in the output directory
# MAGIC 2. Deploy using Databricks CLI:
# MAGIC    ```bash
# MAGIC    cd <output_dir>/<project_name>
# MAGIC    databricks bundle validate -t dev
# MAGIC    databricks bundle deploy -t dev
# MAGIC    ```
