# Tapworks Architecture

Technical reference for developers working on the Lakehouse Tapworks codebase.

## Class Hierarchy

```
BaseConnector (abstract)
├── DatabaseConnector (abstract)
│   ├── SQLServerConnector
│   └── PostgreSQLConnector
└── SaaSConnector (abstract)
    ├── SalesforceConnector
    ├── GoogleAnalyticsConnector
    ├── ServiceNowConnector
    └── WorkdayReportsConnector
```

**Location:** `core/connectors.py`

## Entry Points

### Unified CLI

```bash
# List available connectors
python cli.py --list

# Show connector info
python cli.py salesforce --info

# Generate pipelines
python cli.py salesforce --input-config tables.csv --output-dir output \
  --targets '{"dev": {"workspace_host": "https://your-workspace.databricks.com"}}' 
```

### Programmatic

```python
from core import run_pipeline_generation

result = run_pipeline_generation(
    connector_name='salesforce',
    input_source='tables.csv',  # CSV, Delta table, or DataFrame
    output_dir='output',
    targets={'dev': {'workspace_host': 'https://...'}},
    default_values={'project_name': 'my_project'},
)
```

### Direct Connector Usage

```python
from sql_server.connector import SQLServerConnector

connector = SQLServerConnector()
result = connector.run_complete_pipeline_generation(
    df=input_df,
    output_dir='output',
    targets={'dev': {'workspace_host': '...', 'root_path': '...'}},
    default_values={'project_name': 'my_project'},
    override_input_config={'schedule': None},  # pause all jobs
    max_tables_per_gateway=250,
    max_tables_per_pipeline=250
)
```

## Core Flow

```
Input CSV → Normalization → Load Balancing → YAML Generation
```

### 1. Normalization

**Method:** `load_and_normalize_input()`

Applies configuration in this order (later overrides earlier):

```
1. Built-in defaults (hardcoded in connector)
2. CSV column values (per row)
3. default_values parameter (fills empty CSV values)
4. override_input_config parameter (overwrites everything)
```

Also sets:
- `prefix = project_name` if empty
- `subgroup = '01'` if all empty within a prefix

**Subgroup validation:** If any table in a prefix has an explicit subgroup, all tables in that prefix must have explicit subgroups. This prevents accidental grouping of tables that should be isolated.

#### Group-Based Configuration

Both `default_values` and `override_input_config` support group-based matching via nested dictionaries:

```python
default_values = {
    '*': {'schedule': '0 */6 * * *'},        # Global fallback
    'sales': {'schedule': '*/15 * * * *'},   # All sales pipelines
    'sales_2': {'schedule': '*/30 * * * *'}, # Only sales_2 subgroup
}
```

**Method:** `_apply_group_based_value()`

**Matching precedence** (most specific wins):
1. `pipeline_group` (prefix_subgroup) - e.g., `'sales_2'`
2. `prefix` - e.g., `'sales'`
3. `project_name` - e.g., `'my_project'`
4. `'*'` (global fallback)

### 2. Load Balancing

**Method:** `generate_pipeline_config()`

**Database connectors** - Two-level splitting:

```
Tables grouped by prefix_subgroup
    ↓ split by max_tables_per_gateway (default: 250)
Gateway groups (e.g., sales_01_gw01, sales_01_gw02)
    ↓ split by max_tables_per_pipeline (default: 250)
Pipeline groups (e.g., sales_01_gw01_g01, sales_01_gw01_g02)
```

**SaaS connectors** - Single-level splitting:

```
Tables grouped by prefix_subgroup
    ↓ split by max_tables_per_pipeline (default: 250)
Pipeline groups (e.g., sales_01_g01, sales_01_g02)
```

**Algorithm:** `_split_groups_by_size()` iterates through each group, splits into chunks if size > max, assigns sequential suffixes (`_gw01`, `_g01`, etc.).

### 3. YAML Generation

**Method:** `generate_yaml_files()`

**Database connector output:**
```
project_name/
├── databricks.yml           # bundle config + targets
└── resources/
    ├── gateways.yml         # connection, storage, cluster specs
    ├── pipelines.yml        # table mappings referencing gateway
    └── jobs.yml             # cron schedules triggering pipelines
```

**SaaS connector output:**
```
project_name/
├── databricks.yml
└── resources/
    ├── pipelines.yml
    └── jobs.yml
```


## Core Classes

### BaseConnector (Abstract)

The root base class that defines the common interface for all connectors.

**Abstract Properties:**
- `connector_type` - Connector identifier ('sql_server', 'salesforce', etc.)
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

### SaaSConnector (Abstract)

Base class for SaaS connectors without gateway support.

**Features:**
- Single-level load balancing (pipelines only)
- Simpler YAML structure
- Implements `generate_pipeline_config()` using `generate_saas_pipeline_config()`


## Adding a New Connector

### Step 1: Choose Base Class

- `DatabaseConnector` - source requires gateways (databases with network isolation)
- `SaaSConnector` - no gateways needed (cloud-to-cloud)

### Step 2: Create Connector Class

```python
from core import DatabaseConnector  # or SaaSConnector

class MyConnector(DatabaseConnector):
    @property
    def connector_type(self) -> str:
        return 'myservice'

    @property
    def required_columns(self) -> list:
        return [
            'source_schema', 'source_table_name',
            'target_catalog', 'target_schema', 'target_table_name',
            'connection_name'
        ]

    @property
    def default_values(self) -> dict:
        return {
            'project_name': 'myservice_ingestion',
            'prefix': '',
            'subgroup': '01',
            'schedule': '*/15 * * * *'
        }

    def generate_yaml_files(self, df, output_dir, targets):
        for project_name in df['project_name'].unique():
            project_df = df[df['project_name'] == project_name]
            project_dir = Path(output_dir) / project_name
            resources_dir = project_dir / 'resources'
            resources_dir.mkdir(parents=True, exist_ok=True)

            # Use helper methods from base class
            gateways = self._create_gateways(project_df, project_name)  # DB only
            pipelines = self._create_pipelines(project_df)
            jobs = self._create_jobs(project_df)
            databricks_yml = self._create_databricks_yml(project_name, targets)

            # Write YAML files
            self._write_yaml(resources_dir / 'gateways.yml', gateways)
            self._write_yaml(resources_dir / 'pipelines.yml', pipelines)
            self._write_yaml(resources_dir / 'jobs.yml', jobs)
            self._write_yaml(project_dir / 'databricks.yml', databricks_yml)
```

### Step 3: Register the Connector

Add to `core/registry.py`:

```python
CONNECTORS = {
    # ... existing connectors ...
    'myservice': ('myservice.connector', 'MyServiceConnector'),
}
```

### Step 4: Optional - Custom Normalization

```python
def _apply_connector_specific_normalization(self, df):
    df = super()._apply_connector_specific_normalization(df)
    # Add connector-specific logic
    return df
```

## Testing

The OOP architecture makes testing easier:

```python
import pytest
from sql_server.connector import SQLServerConnector

class TestSQLServerConnector:
    def setup_method(self):
        self.connector = SQLServerConnector()

    def test_connector_type(self):
        assert self.connector.connector_type == 'sql_server'

    def test_required_columns(self):
        required = self.connector.required_columns
        assert 'source_database' in required
        assert 'connection_name' in required
```

## Resource Naming Convention

**Method:** `_generate_resource_names()`

For `pipeline_group = "sales_01_gw01_g01"`:

| Resource | Name |
|----------|------|
| Pipeline display | `Ingestion - sales_01_gw01_g01` |
| Pipeline resource ID | `pipeline_sales_01_gw01_g01` |
| Job resource ID | `job_sales_01_gw01_g01` |
| Job display | `Pipeline Scheduler - sales_01_gw01_g01` |
