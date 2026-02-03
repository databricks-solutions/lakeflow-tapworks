## Database connector template (gateways + pipelines)

> **Audience: Agent (template/scaffold)** — Copy/adapt this skeleton into a real file at `<connector>/connector.py`.
>
> This is not a step-by-step workflow; see `prompts/02_add_or_update_connector.md` for the human-facing instructions.

### Create: `<connector>/connector.py`

Use `sqlserver/connector.py` and `postgres/connector.py` as reference implementations.

#### Skeleton

```python
import sys
import pandas as pd
from pathlib import Path
from typing import Dict

sys.path.insert(0, str(Path(__file__).parent.parent))
from core import DatabaseConnector


class MyDatabaseConnector(DatabaseConnector):
    @property
    def connector_type(self) -> str:
        return "mydatabase"

    @property
    def required_columns(self) -> list:
        return [
            "source_database",
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
            "gateway_catalog": None,
            "gateway_schema": None,
            "gateway_worker_type": None,
            "gateway_driver_type": None,
            "prefix": None,
            "subgroup": "01",
        }

    @property
    def default_project_name(self) -> str:
        return "mydatabase_ingestion"

    # Connector-specific YAML generation:
    # - implement _create_gateways(...) if your gateway schema differs
    # - implement _create_pipelines(...) if your ingestion schema differs
    # - implement generate_yaml_files(...) to write databricks.yml + resources/*.yml
```

### Acceptance checklist
- Uses `DatabaseConnector`
- Produces `resources/gateways.yml`, `resources/pipelines.yml`, `resources/jobs.yml`
- Does not add unsupported YAML fields

