# Step 2: Add a New Connector

This guide covers adding a new connector to the framework.

## Choose Your Base Class

| Source Type | Base Class | Features |
|-------------|------------|----------|
| Database (network-isolated) | `DatabaseConnector` | Gateways + pipelines, two-level load balancing |
| SaaS (cloud-to-cloud) | `SaaSConnector` | Pipelines only, single-level load balancing |

## Files to Create

```
myconnector/
├── __init__.py
├── connector.py          # Connector class implementation
└── examples/
    └── basic/
        └── pipeline_config.csv
```

Plus updates to:
- `core/registry.py` - Register the connector

## Implementation Steps

### 1. Create the Connector Class

Create `myconnector/connector.py`:

```python
import pandas as pd
from pathlib import Path
from typing import Dict
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from core import SaaSConnector  # or DatabaseConnector


class MyConnector(SaaSConnector):

    @property
    def connector_type(self) -> str:
        return "myconnector"

    @property
    def required_columns(self) -> list:
        return [
            "source_schema",
            "source_table_name",
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
        }

    def _create_pipelines(self, df: pd.DataFrame, project_name: str) -> Dict:
        """Create pipeline YAML configuration."""
        pipelines = {}

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            names = self._generate_resource_names(pipeline_group)

            # Build table mappings for this pipeline
            tables = []
            for _, row in group_df.iterrows():
                tables.append({
                    "source_schema": row["source_schema"],
                    "source_table": row["source_table_name"],
                    "destination_catalog": row["target_catalog"],
                    "destination_schema": row["target_schema"],
                    "destination_table": row["target_table_name"],
                })

            pipelines[names['pipeline_resource_name']] = {
                "name": names['pipeline_name'],
                "ingestion_definition": {
                    "connection_name": group_df.iloc[0]["connection_name"],
                    "objects": tables,
                }
            }

        return {"resources": {"pipelines": pipelines}}
```

### 2. Register the Connector

Add to `core/registry.py`:

```python
CONNECTORS = {
    # ... existing connectors ...
    'myconnector': ('myconnector.connector', 'MyConnector'),
}
```

### 3. Create Example CSV

Create `myconnector/examples/basic/pipeline_config.csv`:

```csv
project_name,source_schema,source_table_name,target_catalog,target_schema,target_table_name,connection_name,prefix,subgroup,schedule
my_project,public,users,main,bronze,users,my_connection,data,01,*/15 * * * *
my_project,public,orders,main,bronze,orders,my_connection,data,01,*/15 * * * *
```

### 4. Create Example Notebook (Optional)

Create `myconnector/example_notebook.ipynb` following the pattern in existing connectors.

## Database Connector Specifics

If extending `DatabaseConnector`, you also need:

1. **Gateway columns** in required/default values:
   - `gateway_catalog`, `gateway_schema`
   - `gateway_driver_type`, `gateway_worker_type`
   - `source_database` (typically required)

2. **`_create_gateways()` method** to generate gateway YAML

3. **Override `generate_yaml_files()`** to include `gateways.yml`

See `sql_server/connector.py` for a complete example.

## Connector-Specific Normalization

Override `_apply_connector_specific_normalization()` for custom logic:

```python
def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
    df = super()._apply_connector_specific_normalization(df)

    # Add your custom normalization here
    # Example: validate required column combinations

    return df
```

## Testing Your Connector

```python
from myconnector.connector import MyConnector

connector = MyConnector()

# Check properties
assert connector.connector_type == "myconnector"
assert "connection_name" in connector.required_columns

# Test generation
result = connector.run_complete_pipeline_generation(
    df=test_df,
    output_dir="test_output",
    targets={"dev": {"workspace_host": "https://test.databricks.com"}},
)
```

## Validation Checklist

- [ ] Connector class implements all abstract properties
- [ ] Required columns match what your source needs
- [ ] Default values are sensible for your use case
- [ ] Generated YAML validates: `databricks bundle validate -t dev`
- [ ] Connector is registered and `python -m core.cli --list` shows it
- [ ] Example CSV demonstrates typical usage
