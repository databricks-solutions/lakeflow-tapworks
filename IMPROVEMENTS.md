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
    DEFAULT_TIMEOUT_SECONDS = 600
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

---

## Implementation Notes

### Unified Pipeline Generator (Future)

Instead of 6 nearly-identical pipeline generator scripts, create a single entry point:

```python
# pipeline_generator.py (shared)
from connector_registry import get_connector

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--connector', choices=['salesforce', 'sqlserver', ...])
    # ... common arguments

    args = parser.parse_args()
    connector = get_connector(args.connector)
    connector.run_complete_pipeline_generation(...)
```

### Connector Registry Pattern

```python
# connector_registry.py
CONNECTORS = {
    'salesforce': 'salesforce.connector.SalesforceConnector',
    'sqlserver': 'sqlserver.connector.SQLServerConnector',
    'postgres': 'postgres.connector.PostgreSQLConnector',
    'google_analytics': 'google_analytics.connector.GoogleAnalyticsConnector',
    'servicenow': 'servicenow.connector.ServiceNowConnector',
    'workday_reports': 'workday_reports.connector.WorkdayReportsConnector',
}

def get_connector(name: str):
    """Get connector class by name."""
    module_path, class_name = CONNECTORS[name].rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)()
```

---

## Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Code duplication | ~40% | <10% |
| Test coverage | ~60% | >80% |
| Error handling blocks | 1 | 10+ |
| Type hint coverage | ~30% | >90% |
| Logging consistency | Mixed | 100% logger |
