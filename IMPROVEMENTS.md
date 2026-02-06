# Suggested Improvements

This document outlines recommended improvements for the lakehouse-tapworks project, prioritized by impact and effort.

---

## High Priority

### 1. Eliminate Code Duplication (~40% of codebase)

| Duplication | Location | Fix |
|------------|----------|-----|
| `_split_groups_by_size()` | DatabaseConnector + SaaSConnector (identical 42 lines each) | Move to BaseConnector |
| Pipeline generators | 6 files, 90% identical code | Single shared script + connector registry |
| Gateway creation | SQLServer + PostgreSQL | Extract to DatabaseConnector base |

**Impact:** Reduces maintenance burden, ensures consistent behavior across connectors.

### 2. Fix Inconsistent Logging

Currently mixed usage of `print()` and `logger`:

```python
# SQLServer/Postgres use print():
print(f"\nCreating DAB for project: {project}")

# SaaS connectors use logger:
logger.info(f"Creating DAB for project: {project}")
```

**Fix:** Use `logger` everywhere for consistent output control, proper log levels, and easier debugging.

### 3. Add Proper Error Handling

Currently only 1 try-except block in the entire codebase. Missing validation for:
- File write failures
- Invalid cron expressions
- Databricks resource name format
- Duplicate pipeline/gateway names

**Recommended:** Create custom exception classes:

```python
class ConfigurationError(ValueError):
    """Raised when configuration is invalid"""
    pass

class ValidationError(ValueError):
    """Raised when data validation fails"""
    pass
```

### 4. Fix Import Path Hacks

Every connector uses sys.path manipulation:

```python
# Current (problematic):
sys.path.insert(0, str(Path(__file__).parent.parent))
from core import SaaSConnector

# Better approach:
from lakehouse_tapworks.core import SaaSConnector  # proper package import
```

**Fix:** Add proper `__init__.py` files to all directories and use relative imports.

---

## Medium Priority

### 5. Consolidate Null-Checking Logic

Currently 3+ different patterns for checking empty values:

```python
# Pattern 1
if pd.notna(schedule) and schedule and str(schedule).strip():

# Pattern 2
mask = df['col'].isna() | (df['col'].astype(str).str.strip() == '')

# Pattern 3
if 'col' in item and pd.notna(item['col']) and item['col'].strip():
```

**Fix:** Create a unified utility method:

```python
def _is_value_set(value) -> bool:
    """Check if value is meaningfully set (not None/NaN/empty)."""
    if value is None or pd.isna(value):
        return False
    return bool(str(value).strip())
```

### 6. Extract Magic Numbers to Constants

Currently scattered throughout the codebase:

```python
max_tables_per_gateway=250
max_tables_per_pipeline=250
timeout=600
```

**Fix:** Centralize configuration:

```python
class Config:
    DEFAULT_MAX_TABLES_PER_PIPELINE = 250
    DEFAULT_MAX_TABLES_PER_GATEWAY = 250
    ***REMOVED***
```

### 7. Add Column Schema Definitions

84 hardcoded column name strings across the codebase make refactoring error-prone.

**Fix:** Use dataclasses for schema definition:

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class SourceConfig:
    source_database: str
    source_schema: str
    source_table_name: str
    target_catalog: str
    target_schema: str
    target_table_name: str
    connection_name: str
    project_name: Optional[str] = None
    prefix: Optional[str] = None
    subgroup: Optional[str] = None
```

---

## Lower Priority

### 8. Validate Databricks Resource Names

No validation that generated names follow Databricks naming conventions.

**Fix:**

```python
import re

def _validate_resource_name(self, name: str) -> None:
    """Validate Databricks resource naming rules."""
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', name):
        raise ValidationError(f"Invalid resource name: {name}")
    if len(name) > 128:
        raise ValidationError(f"Resource name too long: {name}")
```

### 9. Expand Test Coverage

Current tests cover happy paths. Missing tests for:
- Error handling paths
- File I/O failures
- Invalid cron expressions
- Resource name validation
- Edge cases (empty groups, single-row dataframes)

### 10. Add Type Hints Throughout

Many methods lack complete type annotations. Adding them improves:
- IDE support and autocomplete
- Static analysis with mypy
- Documentation clarity

---

## Quick Wins (Implemented)

These improvements have been implemented:

- [x] Move `_split_groups_by_size()` to BaseConnector
- [x] Replace `print()` with `logger` in SQLServer/PostgreSQL connectors
- [x] Add `__init__.py` to all connector directories
- [x] Create `_is_value_set()` utility method in BaseConnector
- [x] Extract magic numbers to constants (`DEFAULT_MAX_TABLES_PER_PIPELINE`, `DEFAULT_MAX_TABLES_PER_GATEWAY`, `DEFAULT_TIMEOUT_SECONDS`)
- [x] Add proper error handling with custom exceptions (ConfigurationError, ValidationError, YAMLGenerationError)
- [x] Add cron expression validation
- [x] Add Databricks resource name validation
- [x] Add YAML file write retry logic

---

## Implementation Notes

### Unified Pipeline Generator (Future)

Instead of 6 nearly-identical pipeline generator scripts, create a unified architecture with shared entry points for CLI and notebooks.

#### File Structure

```
├── core/
│   ├── connectors.py          # Existing connector classes
│   ├── registry.py            # NEW: Connector registry
│   └── runner.py              # NEW: Shared execution logic
├── cli.py                     # CLI entry point
├── notebook_runner.py         # Notebook entry point
```

#### Connector Registry (`core/registry.py`)

```python
CONNECTORS = {
    'salesforce': 'salesforce.connector.SalesforceConnector',
    'sqlserver': 'sqlserver.connector.SQLServerConnector',
    'postgres': 'postgres.connector.PostgreSQLConnector',
    'google_analytics': 'google_analytics.connector.GoogleAnalyticsConnector',
    'servicenow': 'servicenow.connector.ServiceNowConnector',
    'workday_reports': 'workday_reports.connector.WorkdayReportsConnector',
}

def get_connector(name: str):
    """Get connector instance by name."""
    import importlib
    module_path, class_name = CONNECTORS[name].rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)()

def list_connectors() -> list:
    return list(CONNECTORS.keys())
```

#### Shared Runner (`core/runner.py`)

```python
def run_pipeline_generation(
    connector_name: str,
    input_source: str,          # CSV path or Delta table
    output_dir: str,
    targets: dict,
    default_values: dict = None,
    override_config: dict = None,
    max_tables_per_pipeline: int = 250,
    output_config: str = None,
):
    """
    Core logic shared by CLI and notebook.
    Returns the result DataFrame.
    """
    from core.registry import get_connector

    connector = get_connector(connector_name)

    # Load input (CSV or Delta)
    if input_source.endswith('.csv'):
        df = pd.read_csv(input_source)
    else:
        # Assume Delta table path
        df = spark.read.table(input_source).toPandas()

    return connector.run_complete_pipeline_generation(
        df=df,
        output_dir=output_dir,
        targets=targets,
        default_values=default_values,
        override_input_config=override_config,
        max_tables_per_pipeline=max_tables_per_pipeline,
        output_config=output_config,
    )
```

#### CLI Entry Point (`cli.py`)

```python
import argparse
from core.runner import run_pipeline_generation
from core.registry import list_connectors

def main():
    parser = argparse.ArgumentParser(description='Generate Databricks Asset Bundles')
    parser.add_argument('--connector', choices=list_connectors(), required=True)
    parser.add_argument('--input', required=True, help='CSV file or Delta table')
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--targets-file', required=True, help='YAML file with targets')
    parser.add_argument('--max-tables', type=int, default=250)

    args = parser.parse_args()
    targets = yaml.safe_load(open(args.targets_file))

    run_pipeline_generation(
        connector_name=args.connector,
        input_source=args.input,
        output_dir=args.output_dir,
        targets=targets,
        max_tables_per_pipeline=args.max_tables,
    )

if __name__ == '__main__':
    main()
```

#### Notebook Entry Point (`notebook_runner.py`)

```python
# Databricks notebook source

# COMMAND ----------
# Configuration - edit these values
connector_name = "salesforce"  # salesforce, sqlserver, postgres, workday_reports, etc.
input_table = "catalog.schema.pipeline_config"  # Delta table or CSV path
output_path = "/Workspace/Users/you/dab_output"
max_tables_per_pipeline = 250

targets = {
    "dev": {
        "workspace_host": "https://dev.cloud.databricks.com",
        "root_path": "/Shared/pipelines/dev",
    },
    "prod": {
        "workspace_host": "https://prod.cloud.databricks.com",
        "root_path": "/Shared/pipelines/prod",
    }
}

# Optional overrides
default_values = {
    "schedule": "0 */6 * * *",
}
override_config = None  # Set to dict to override all rows

# COMMAND ----------
from core.runner import run_pipeline_generation

result_df = run_pipeline_generation(
    connector_name=connector_name,
    input_source=input_table,
    output_dir=output_path,
    targets=targets,
    default_values=default_values,
    override_config=override_config,
    max_tables_per_pipeline=max_tables_per_pipeline,
)

display(result_df)

# COMMAND ----------
# Show generated files
import os
for root, dirs, files in os.walk(output_path):
    for f in files:
        print(os.path.join(root, f))
```

**Key insight:** Both CLI and notebook call the same `run_pipeline_generation()` function - they just get parameters differently (argparse vs direct assignment).

---

## Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Code duplication | ~40% | <10% |
| Test coverage | ~60% | >80% |
| Error handling blocks | 1 | 10+ |
| Type hint coverage | ~30% | >90% |
| Logging consistency | Mixed | 100% logger |
