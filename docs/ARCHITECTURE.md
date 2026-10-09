# Tapworks Architecture

Technical reference for developers working on the Lakehouse Tapworks codebase.

## Class Hierarchy

```
BaseConnector (abstract)
├── DatabaseConnector (abstract)
│   ├── GatewayConnector (abstract)
│   │   ├── SQLServerConnector
│   │   └── PostgreSQLConnector
│   └── IntegratedCDCConnector (abstract)
│       └── OracleConnector
└── SaaSConnector (abstract)
    ├── SalesforceConnector
    ├── GoogleAnalyticsConnector
    ├── ServiceNowConnector
    └── WorkdayReportsConnector
```

**Location:** `src/tapworks/core/connectors.py` (`BaseConnector`, `SaaSConnector`) and `src/tapworks/core/database.py` (`DatabaseConnector`, `GatewayConnector`, `IntegratedCDCConnector`)

## Entry Points

### Unified CLI

```bash
# List available connectors
tapworks --list

# Show connector info
tapworks salesforce --info

# Generate pipelines
tapworks salesforce --input-config tables.csv --output-dir output \
  --targets '{"dev": {"workspace_host": "https://your-workspace.databricks.com"}}' 
```

### Programmatic

```python
from tapworks.core import run_pipeline_generation

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
from tapworks.connectors.sql_server.connector import SQLServerConnector

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
- `subgroup` left empty if not specified (no `_01` infix in names)

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

**Method:** `_get_group_mask()`

**Matching precedence** (most specific wins):
1. `pipeline_group` (prefix_subgroup) - e.g., `'sales_2'`
2. `prefix` - e.g., `'sales'`
3. `project_name` - e.g., `'my_project'`
4. `'*'` (global fallback)

### 2. Load Balancing

**Method:** `generate_pipeline_config()`

**Database connectors** - Two-level splitting:

```
Tables grouped by prefix (or prefix_subgroup if subgroup is explicit)
    ↓ split by max_tables_per_gateway (default: 250)
Gateway groups (e.g., sales_g01, sales_g02)
    ↓ split by max_tables_per_pipeline (default: 250)
Pipeline groups (e.g., sales_g01p01, sales_g01p02)
```

**SaaS connectors** - Single-level splitting:

```
Tables grouped by prefix (or prefix_subgroup if subgroup is explicit)
    ↓ split by max_tables_per_pipeline (default: 250)
Pipeline groups (e.g., sales_p01, sales_p02)
```

**Algorithm:** `_split_groups_by_size()` iterates through each group, splits into chunks if size > max, assigns sequential suffixes (`_g01`, `_p01`, etc.).

**Row order matters for load balancing.** When a group exceeds the max size and gets split into chunks, tables are assigned to chunks based on their row position in the input (using positional indexing). This means:
- The first 250 rows (by input order) go to chunk 1, the next 250 to chunk 2, etc.
- Rows for the same prefix do **not** need to be contiguous — they are collected regardless of position, but their relative order determines chunk assignment.
- **Adding new tables in the middle of existing rows can shift tables between chunks**, which changes which pipeline they belong to. In DABs, a pipeline name change causes the old pipeline to be removed and recreated — resulting in data loss.
- **Always append new tables to the end** of their prefix group in the config to avoid shifting existing table assignments.

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

**Concrete Properties:**
- `supported_scd_types` - List of supported SCD types (default: `[]`, override in subclass)

**Concrete Methods:**
- `load_and_normalize_input()` - Loads and normalizes CSV input
- `run_complete_pipeline_generation()` - Main entry point for pipeline generation

**Abstract Methods:**
- `generate_pipeline_config()` - Implements load balancing logic
- `generate_yaml_files()` - Generates DAB YAML files

### DatabaseConnector (Abstract)

Base class for all database connectors. Implements the pipeline-building flow shared by every database ingestion mode.

**Features:**
- `_create_pipelines()` builds each pipeline via `_build_pipeline()`, which builds table entries via `_build_table_entry()` and `_build_table_configuration()` (include/exclude columns, SCD type)
- `generate_yaml_files()` writes `databricks.yml`, `pipelines.yml`, `jobs.yml`, plus any files from `_create_extra_resource_files()`
- `target_table_name` defaults to `source_table_name`
- Single-level load balancing (`generate_pipeline_config()`: pipelines only)
- Pipeline consistency validation (`connection_name`, `pipeline_catalog`, `pipeline_schema`, `tags`)

**Abstract Methods:**
- `_ingestion_source()` - `ingestion_definition` fields that tell a pipeline how to reach the source

**Extension points:**
- `_build_table_configuration()` - Per-table options
- `_build_pipeline()` - Pipeline-level additions (e.g., PostgreSQL `source_configurations`)
- `_create_extra_resource_files()` - Additional resource files

### GatewayConnector (Abstract)

Base class for database connectors that ingest through a gateway.

**Features:**
- Two-level load balancing (gateways + pipelines)
- Gateway configuration handling (`gateway_catalog`/`gateway_schema` default to the target catalog/schema)
- Writes `gateways.yml`; pipelines reference their gateway via `ingestion_gateway_id`
- Gateway consistency validation (`gateway_catalog`, `gateway_schema`, `connection_name`, `tags`); `connection_name` is checked per gateway instead of per pipeline

### IntegratedCDCConnector (Abstract)

Base class for database connectors that use integrated CDC: each pipeline reads changes directly through its `connection_name`, with no gateway.

**Features:**
- Single-level load balancing (inherited from `DatabaseConnector`)
- Pipelines set `connection_name` and `connector_type: CDC`, on the `PREVIEW` channel

### SaaSConnector (Abstract)

Base class for SaaS connectors without gateway support.

**Features:**
- Single-level load balancing (pipelines only)
- Simpler YAML structure
- Implements `generate_pipeline_config()` directly with single-level splitting


## Adding a New Connector

### Step 1: Choose Base Class

- `GatewayConnector` - database source ingested through a gateway
- `IntegratedCDCConnector` - database source ingested with integrated CDC (no gateway)
- `SaaSConnector` - no gateways needed (cloud-to-cloud)

### Step 2: Create Connector Class

```python
from tapworks.core import GatewayConnector  # or SaaSConnector

class MyConnector(GatewayConnector):
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
            'subgroup': '',
            'schedule': '*/15 * * * *'
        }
```

`GatewayConnector` already implements load balancing, gateway/pipeline/job YAML, and file writing. Override `_build_pipeline()` or `_build_table_configuration()` only for source-specific additions (see `PostgreSQLConnector`).

### Step 3: Register the Connector

Add to `src/tapworks/core/registry.py`:

```python
CONNECTORS = {
    # ... existing connectors ...
    'myservice': 'tapworks.connectors.myservice.connector.MyServiceConnector',
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

The architecture makes testing straightforward:

```python
import pytest
from tapworks.connectors.sql_server.connector import SQLServerConnector

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

For `pipeline_group = "sales_g01p01"`:

| Resource | Name |
|----------|------|
| Pipeline display | `sales_g01p01` |
| Pipeline resource ID | `pipeline_sales_g01p01` |
| Job resource ID | `job_sales_g01p01` |
| Job display | `sales_g01p01` |
