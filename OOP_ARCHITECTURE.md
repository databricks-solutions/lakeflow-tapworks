# OOP Architecture Guide

This document describes the Object-Oriented Programming (OOP) architecture introduced to the Lakehouse Tapworks project.

## Overview

The OOP architecture provides a cleaner, more maintainable way to implement connectors for different data sources. Instead of scattered functions, each connector is now a class that encapsulates all connector-specific logic while sharing common functionality through inheritance.

## Benefits

1. **Simplified Interface**: One method call (`run_complete_pipeline_generation()`) handles everything
2. **Consistent API**: All connectors use the same interface
3. **Easy to Extend**: Add new connectors by subclassing `DatabaseConnector` or `SaaSConnector`
4. **Better Organization**: Connector-specific logic is encapsulated in one place
5. **Improved Testability**: Easy to mock connectors for testing
6. **Code Reuse**: Shared functionality in base classes eliminates duplication

## Class Hierarchy

```
BaseConnector (ABC)
├── DatabaseConnector (ABC)
│   └── SQLServerConnector
│       └── Future: MySQLConnector, PostgreSQLConnector, OracleConnector
└── SaaSConnector (ABC)
    ├── SalesforceConnector
    ├── GoogleAnalyticsConnector
    └── Future: ServiceNowConnector, WorkdayConnector
```

## Core Classes

### BaseConnector (Abstract)

The root base class that defines the common interface for all connectors.

**Abstract Properties:**
- `connector_type` - Connector identifier ('sqlserver', 'salesforce', etc.)
- `required_columns` - List of required CSV columns
- `default_values` - Dictionary of default values for optional columns

**Concrete Methods:**
- `load_and_normalize_input()` - Loads and normalizes CSV input
- `run_complete_pipeline_generation()` - Main entry point for pipeline generation

**Abstract Methods:**
- `generate_pipeline_config()` - Implements load balancing logic
- `generate_yaml_files()` - Generates DAB YAML files

### DatabaseConnector (Abstract)

Base class for database connectors with gateway support.

**Features:**
- Two-level load balancing (gateways + pipelines)
- Gateway configuration handling
- Implements `generate_pipeline_config()` using `generate_database_pipeline_config()`

**Subclass Requirements:**
- Implement `connector_type`, `required_columns`, `default_values`
- Implement `generate_yaml_files()` to call connector-specific YAML generator

### SaaSConnector (Abstract)

Base class for SaaS connectors without gateway support.

**Features:**
- Single-level load balancing (pipelines only)
- Simpler YAML structure
- Implements `generate_pipeline_config()` using `generate_saas_pipeline_config()`

**Subclass Requirements:**
- Implement `connector_type`, `required_columns`, `default_values`
- Implement `generate_yaml_files()` to call connector-specific YAML generator

## Creating a New Connector

### Step 1: Choose Base Class

- Use `DatabaseConnector` if your source requires gateways (databases)
- Use `SaaSConnector` if your source doesn't require gateways (SaaS applications)

### Step 2: Create Connector Class

```python
# For a new SaaS connector
from utilities.connectors import SaaSConnector

class MyServiceConnector(SaaSConnector):
    @property
    def connector_type(self) -> str:
        return 'myservice'  # Used in naming and defaults

    @property
    def required_columns(self) -> list:
        return [
            'source_table',
            'target_catalog',
            'target_schema',
            'target_table_name',
            'connection_name'
        ]

    @property
    def default_values(self) -> dict:
        return {
            'schedule': '0 */6 * * *'
        }

    def generate_yaml_files(self, df, output_dir, targets):
        # Import your connector-specific YAML generator
        from deployment.connector_settings_generator import generate_yaml_files
        generate_yaml_files(df=df, output_dir=output_dir, targets=targets)
```

### Step 3: Use Your Connector

```python
from utilities import load_input_csv
from myservice.connector import MyServiceConnector

# Create connector instance
connector = MyServiceConnector()

# Load input data
df = load_input_csv('input.csv')

# Generate pipelines
result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='output',
    targets={'dev': {'workspace_host': 'https://...'}},
    default_values={'project_name': 'my_project'}
)
```

## Usage Examples

### SQL Server (Database Connector)

```python
from sqlserver.connector import SQLServerConnector
from utilities import load_input_csv

connector = SQLServerConnector()
df = load_input_csv('sqlserver_tables.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='output/sqlserver',
    targets={
        'dev': {
            'workspace_host': 'https://dev.databricks.com',
            'root_path': '/Users/me/.bundle/${bundle.name}/${bundle.target}'
        }
    },
    default_values={
        'project_name': 'sales_db_ingestion',
        'gateway_worker_type': 'm5d.large'
    },
    max_tables_per_gateway=250,
    max_tables_per_pipeline=250
)
```

### Salesforce (SaaS Connector)

```python
from salesforce.connector import SalesforceConnector
from utilities import load_input_csv

connector = SalesforceConnector()
df = load_input_csv('salesforce_objects.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='output/salesforce',
    targets={'prod': {'workspace_host': 'https://prod.databricks.com'}},
    default_values={'project_name': 'sfdc_ingestion'},
    max_tables_per_pipeline=250
)
```

### Google Analytics 4 (SaaS Connector)

```python
from google_analytics.connector import GoogleAnalyticsConnector
from utilities import load_input_csv

connector = GoogleAnalyticsConnector()
df = load_input_csv('ga4_properties.csv')

result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='output/ga4',
    targets={'dev': {'workspace_host': 'https://dev.databricks.com'}},
    default_values={'project_name': 'ga4_ingestion'},
    max_tables_per_pipeline=250
)
```

## Configuration Hierarchy

The OOP architecture maintains the same configuration hierarchy as the functional approach:

```
1. Built-in defaults (hardcoded in connector class)
   ↓
2. CSV column values (per row)
   ↓
3. default_values parameter (overrides empty CSV values)
   ↓
4. override_input_config parameter (overrides everything)
```

## Command-Line Interface

Each connector has an OOP-based CLI script:

**SQL Server:**
```bash
python sqlserver/pipeline_generator_oop.py \
  --input-csv input.csv \
  --project-name my_project \
  --workspace-host https://workspace.databricks.com \
  --root-path '/Users/me/.bundle/${bundle.name}/${bundle.target}'
```

**Salesforce:**
```bash
python salesforce/pipeline_generator_oop.py \
  --input-csv input.csv \
  --project-name sfdc_ingestion \
  --workspace-host https://workspace.databricks.com
```

**Google Analytics:**
```bash
python google_analytics/pipeline_generator_oop.py \
  --input-csv input.csv \
  --project-name ga4_ingestion \
  --workspace-host https://workspace.databricks.com
```

## Backward Compatibility

The OOP architecture is **fully backward compatible**:

- Original functional API (`run_complete_pipeline_generation()` function) still works
- Original CLI scripts (`pipeline_generator.py`) still work
- New OOP approach is available alongside functional approach
- Shared utilities remain the same

You can gradually migrate to OOP or use both approaches side-by-side.

## Migration Guide

### From Functional to OOP

**Before (Functional):**
```python
from sqlserver.pipeline_generator import run_complete_pipeline_generation
from utilities import load_input_csv

df = load_input_csv('input.csv')
result = run_complete_pipeline_generation(
    df=df,
    output_dir='output',
    targets={'dev': {'workspace_host': '...', 'root_path': '...'}},
    default_values={'project_name': 'my_project'}
)
```

**After (OOP):**
```python
from sqlserver.connector import SQLServerConnector
from utilities import load_input_csv

connector = SQLServerConnector()
df = load_input_csv('input.csv')
result = connector.run_complete_pipeline_generation(
    df=df,
    output_dir='output',
    targets={'dev': {'workspace_host': '...', 'root_path': '...'}},
    default_values={'project_name': 'my_project'}
)
```

**Key Changes:**
1. Import connector class instead of function
2. Create connector instance
3. Call method on connector instead of standalone function
4. Everything else stays the same!

## Testing with OOP

The OOP architecture makes testing easier:

```python
import unittest
from unittest.mock import Mock, patch
from sqlserver.connector import SQLServerConnector

class TestSQLServerConnector(unittest.TestCase):
    def setUp(self):
        self.connector = SQLServerConnector()

    def test_connector_type(self):
        self.assertEqual(self.connector.connector_type, 'sqlserver')

    def test_required_columns(self):
        required = self.connector.required_columns
        self.assertIn('source_database', required)
        self.assertIn('connection_name', required)

    @patch('sqlserver.connector.generate_database_pipeline_config')
    def test_generate_pipeline_config(self, mock_generate):
        # Mock the shared function
        mock_generate.return_value = Mock()

        # Call connector method
        result = self.connector.generate_pipeline_config(df=Mock())

        # Verify shared function was called
        mock_generate.assert_called_once()
```

## Advanced: Custom Normalization

Override `_apply_connector_specific_normalization()` for custom logic:

```python
class MyConnector(SaaSConnector):
    # ... implement required properties ...

    def _apply_connector_specific_normalization(self, df):
        # Call parent normalization first
        df = super()._apply_connector_specific_normalization(df)

        # Add custom normalization
        if 'custom_column' in df.columns:
            df['custom_column'] = df['custom_column'].str.upper()

        return df
```

## Future Enhancements

Possible future improvements to the OOP architecture:

1. **Plugin System**: Auto-discover connectors in a plugins directory
2. **Connector Registry**: Register connectors and lookup by type
3. **Validation Framework**: Abstract validation logic into base classes
4. **Connector Metadata**: Add metadata properties (version, supported features, etc.)
5. **Event Hooks**: Add hooks for pre/post processing steps
6. **Async Support**: Add async versions of methods for better performance

## Questions?

For questions or suggestions about the OOP architecture:
- Review examples in `examples/oop_usage_example.py`
- Check existing connector implementations
- Open an issue with questions or suggestions
