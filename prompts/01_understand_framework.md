# Step 1: Understand the Framework

Before adding a new connector, understand how the existing framework works.

## Key Files to Read

1. **`ARCHITECTURE.md`** - High-level design and class hierarchy
2. **`core/connectors.py`** - Source of truth for base classes and shared logic
3. **`core/registry.py`** - Connector registration system

## Architecture Overview

```
BaseConnector (abstract)
├── DatabaseConnector (abstract) - Sources with gateways (SQL Server, PostgreSQL)
└── SaaSConnector (abstract) - Cloud sources without gateways (Salesforce, GA4)
```

## Entry Points

**Unified function (recommended):**
```python
from core import run_pipeline_generation

result = run_pipeline_generation(
    connector_name='salesforce',
    input_source='tables.csv',
    output_dir='output',
    targets={'dev': {'workspace_host': 'https://...'}},
)
```

**CLI:**
```bash
python -m core.cli salesforce --input-config tables.csv --output-dir output \
  --targets '{"dev": {"workspace_host": "https://your-workspace.databricks.com"}}'
```

## Core Workflow

```
Input CSV → Normalization → Load Balancing → YAML Generation
```

1. **Normalization** (`load_and_normalize_input`)
   - Validates required columns
   - Applies defaults: built-in → CSV values → default_values param → override_input_config
   - Sets `prefix = project_name` if empty
   - Sets `subgroup = '01'` if all empty (validates no mixed usage)

2. **Load Balancing** (`generate_pipeline_config`)
   - Database: Two-level splitting (gateways → pipelines)
   - SaaS: Single-level splitting (pipelines only)
   - Groups by `prefix_subgroup`, splits if > max_tables_per_pipeline

3. **YAML Generation** (`generate_yaml_files`)
   - Creates DAB project structure per `project_name`
   - Output: `databricks.yml` + `resources/{pipelines,jobs}.yml` (+ `gateways.yml` for databases)

## What You Need to Understand

Before proceeding, you should be able to answer:

- What's the difference between `DatabaseConnector` and `SaaSConnector`?
- How does `prefix + subgroup` become `pipeline_group`?
- What triggers auto-splitting into multiple pipelines?
- What files are generated and where?

## Reference Implementations

- **Database:** `sql_server/connector.py`, `postgresql/connector.py`
- **SaaS:** `salesforce/connector.py`, `google_analytics/connector.py`
