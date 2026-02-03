## SaaS connector template (pipelines only)

> **Audience: Agent (template/scaffold)** — Copy/adapt this skeleton into a real file at `<connector>/connector.py`.
>
> This is not a step-by-step workflow; see `prompts/02_add_or_update_connector.md` for the human-facing instructions.

### Create: `<connector>/connector.py`

Use `salesforce/connector.py` and `google_analytics/connector.py` as reference implementations.

#### Skeleton

```python
import sys
import pandas as pd
from pathlib import Path
from typing import Dict

sys.path.insert(0, str(Path(__file__).parent.parent))
from core import SaaSConnector


class MySaaSConnector(SaaSConnector):
    @property
    def connector_type(self) -> str:
        return "mysaas"

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
            "prefix": None,
            "subgroup": "01",
        }

    @property
    def default_project_name(self) -> str:
        return "mysaas_ingestion"

    # Connector-specific YAML generation:
    # - implement _create_pipelines(...) if your ingestion schema differs
    # - implement generate_yaml_files(...) to write databricks.yml + resources/*.yml
```

### Acceptance checklist
- Uses `SaaSConnector`
- Produces `resources/pipelines.yml`, `resources/jobs.yml`
- Documents connector-specific configuration columns clearly (if any)

