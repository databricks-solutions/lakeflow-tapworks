# Example: Adding a Jira Connector

This walkthrough shows how to add a new SaaS connector for Jira.

## What You'll Build

```
src/tapworks/connectors/jira/
├── __init__.py
└── connector.py

examples/connectors/jira/
├── basic/
│   └── pipeline_config.csv
└── example_notebook.ipynb
```

## Step 1: Understand Jira-Specific Requirements

Before implementing, identify what's unique about Jira:

- **Objects**: issues, projects, users, comments, changelogs
- **Filtering**: JQL (Jira Query Language) for scoping
- **Incremental**: `updated` field as cursor for CDC
- **Rate limits**: Jira Cloud enforces request limits
- **Required fields**: Always need `id`, `key`, and cursor field

## Step 2: Design the CSV Schema

Based on Jira's characteristics:

```csv
project_name,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name,prefix,subgroup,schedule,jql,primary_keys,cursor_field
jira_project,cloud,issue,main,bronze,issues,jira_conn,issues,01,*/15 * * * *,"project = ABC",id,updated
jira_project,cloud,project,main,bronze,projects,jira_conn,meta,01,0 */6 * * *,,,
jira_project,cloud,user,main,bronze,users,jira_conn,meta,01,0 */6 * * *,,,
```

**Design decisions:**
- `jql` column for Jira-specific filtering
- `cursor_field` for incremental sync (optional, defaults based on object type)
- `primary_keys` for deduplication
- Group issues separately from metadata (different update frequencies)

## Step 3: Implement the Connector

Create `src/tapworks/connectors/jira/connector.py`:

```python
import pandas as pd
from pathlib import Path
from typing import Dict

from tapworks.core import SaaSConnector
from tapworks.core.exceptions import ValidationError


class JiraConnector(SaaSConnector):

    # Default cursors for known object types
    OBJECT_CURSORS = {
        'issue': 'updated',
        'comment': 'updated',
        'changelog': 'created',
    }

    @property
    def connector_type(self) -> str:
        return "jira"

    @property
    def required_columns(self) -> list:
        return [
            "source_schema",       # e.g., 'cloud' or 'server'
            "source_table_name",   # e.g., 'issue', 'project'
            "target_catalog",
            "target_schema",
            "target_table_name",
            "connection_name",
        ]

    @property
    def default_values(self) -> dict:
        return {
            "schedule": "*/15 * * * *",
            "prefix": "",
            "subgroup": "01",
            "jql": "",
            "primary_keys": "id",
            "cursor_field": "",
        }

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply Jira-specific normalization and validation."""
        df = super()._apply_connector_specific_normalization(df)

        # Auto-set cursor_field based on object type if not specified
        for idx, row in df.iterrows():
            if not self._is_value_set(row.get('cursor_field')):
                object_type = str(row['source_table_name']).lower()
                if object_type in self.OBJECT_CURSORS:
                    df.at[idx, 'cursor_field'] = self.OBJECT_CURSORS[object_type]

        return df

    def _create_pipelines(self, df: pd.DataFrame, project_name: str) -> Dict:
        """Create Jira pipeline YAML configuration."""
        pipelines = {}

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            names = self._generate_resource_names(pipeline_group)

            # Build object definitions for this pipeline
            objects = []
            for _, row in group_df.iterrows():
                obj_config = {
                    "object": row["source_table_name"],
                    "destination_catalog": row["target_catalog"],
                    "destination_schema": row["target_schema"],
                    "destination_table": row["target_table_name"],
                }

                # Add JQL filter if specified
                if self._is_value_set(row.get('jql')):
                    obj_config["jql"] = row["jql"]

                # Add incremental config if cursor is set
                if self._is_value_set(row.get('cursor_field')):
                    obj_config["cursor_field"] = row["cursor_field"]

                if self._is_value_set(row.get('primary_keys')):
                    obj_config["primary_keys"] = [
                        k.strip() for k in str(row["primary_keys"]).split(',')
                    ]

                objects.append(obj_config)

            pipelines[names['pipeline_resource_name']] = {
                "name": names['pipeline_name'],
                "ingestion_definition": {
                    "connection_name": group_df.iloc[0]["connection_name"],
                    "objects": objects,
                }
            }

        return {"resources": {"pipelines": pipelines}}
```

## Step 4: Register the Connector

Add to `src/tapworks/core/registry.py`:

```python
CONNECTORS = {
    # ... existing ...
    'jira': ('tapworks.connectors.jira.connector', 'JiraConnector'),
}
```

## Step 5: Create Example Files

Create `src/tapworks/connectors/jira/__init__.py`:
```python
from .connector import JiraConnector

__all__ = ['JiraConnector']
```

Create `examples/connectors/jira/basic/pipeline_config.csv`:
```csv
project_name,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name,prefix,subgroup,schedule,jql,primary_keys,cursor_field
jira_ingestion,cloud,issue,main,jira_bronze,issues,jira_connection,issues,01,*/15 * * * *,"project in (PROJ1, PROJ2)",id,updated
jira_ingestion,cloud,project,main,jira_bronze,projects,jira_connection,meta,01,0 */6 * * *,,id,
jira_ingestion,cloud,user,main,jira_bronze,users,jira_connection,meta,01,0 */6 * * *,,accountId,
jira_ingestion,cloud,issue_comment,main,jira_bronze,comments,jira_connection,issues,01,*/15 * * * *,,id,updated
```

## Step 6: Test

```bash
# Install the package first
pip install -e .

# Verify registration
tapworks --list

# Generate DAB
tapworks jira \
  --input-config examples/connectors/jira/basic/pipeline_config.csv \
  --output-dir output \
  --targets '{"dev": {"workspace_host": "https://your-workspace.databricks.com"}}'

# Validate
cd output/jira_ingestion
databricks bundle validate -t dev
```

## Step 7: Create Notebook (Optional)

Create `examples/connectors/jira/example_notebook.ipynb` following the standard pattern:

```python
# Cell 1: Setup
%load_ext autoreload
%autoreload 2

# Cell 2: Install dependencies
%pip install pyyaml

# Cell 3: Configure
from databricks.sdk import WorkspaceClient
import sys
import os

w = WorkspaceClient()
username = w.current_user.me().user_name
WORKSPACE_HOST = w.config.host
ROOT_PATH = f'/Users/{username}/.bundle/${{bundle.name}}/${{bundle.target}}'

# Add src to path for imports
sys.path.append(os.path.abspath('../../../src'))

# Cell 4: Generate
from tapworks.core.runner import run_pipeline_generation

result_df = run_pipeline_generation(
    connector_name="jira",
    input_source='basic/pipeline_config.csv',
    output_dir='basic/deployment',
    targets={
        'dev': {
            'workspace_host': WORKSPACE_HOST,
            'root_path': ROOT_PATH,
        }
    },
    max_tables_per_pipeline=100,
)

print(f"Processed {len(result_df)} objects")
print(f"Pipelines: {result_df['pipeline_group'].nunique()}")
```

## Result

After completing these steps, you have a working Jira connector that:

- Groups issues separately from metadata (different prefixes)
- Supports JQL filtering for scoped ingestion
- Auto-detects cursor fields for known object types
- Integrates with the unified `run_pipeline_generation()` interface
- Works with both CLI and notebook entry points
