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
- `subgroup = '01'` if empty

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

## Resource Relationships

```
Gateway (Database connectors only)
├── Holds connection config + cluster specs
├── Storage location (catalog.schema)
└── Multiple pipelines can share one gateway

Pipeline
├── References gateway (DB) or connection directly (SaaS)
├── Contains table/object mappings
└── Column filters (SaaS only)

Job
├── 1:1 relationship with pipeline
├── Cron schedule (converted to Quartz format)
└── Can be paused via override_input_config
```

## Entry Point

**Method:** `run_complete_pipeline_generation()`

```python
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

## Connector Comparison

| Feature | DatabaseConnector | SaaSConnector |
|---------|-------------------|---------------|
| Load balancing levels | 2 (gateway + pipeline) | 1 (pipeline only) |
| Max gateway size | 250 (configurable) | N/A |
| Max pipeline size | 250 (configurable) | 250 (configurable) |
| YAML files | 4 (includes gateways.yml) | 3 |
| Examples | SQL Server, PostgreSQL | Salesforce, GA4, ServiceNow, Workday |

## Adding a New Connector

### Step 1: Choose Base Class

- `DatabaseConnector` - source requires gateways (databases with network isolation)
- `SaaSConnector` - no gateways needed (cloud-to-cloud)

### Step 2: Implement Required Properties

```python
from core.connectors import DatabaseConnector  # or SaaSConnector

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
```

### Step 3: Implement YAML Generation

```python
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

### Step 4: Optional - Custom Normalization

```python
def _apply_connector_specific_normalization(self, df):
    df = super()._apply_connector_specific_normalization(df)
    # Add connector-specific logic
    return df
```

## Utilities

**Location:** `utilities/utilities.py`

- `load_input_csv(path)` - Load CSV into DataFrame
- `convert_cron_to_quartz(cron)` - Convert 5-field cron to 6-field Quartz format

## Resource Naming Convention

**Method:** `_generate_resource_names()`

For `pipeline_group = "sales_01_gw01_g01"`:

| Resource | Name |
|----------|------|
| Pipeline display | `Ingestion - sales_01_gw01_g01` |
| Pipeline resource ID | `pipeline_sales_01_gw01_g01` |
| Job resource ID | `job_sales_01_gw01_g01` |
| Job display | `Pipeline Scheduler - sales_01_gw01_g01` |

## File Locations

| Component | Path |
|-----------|------|
| Base classes | `core/connectors.py` |
| Utilities | `utilities/utilities.py` |
| SQL Server | `sqlserver/connector.py` |
| PostgreSQL | `postgres/connector.py` |
| Salesforce | `salesforce/connector.py` |
| Google Analytics | `google_analytics/connector.py` |
| ServiceNow | `servicenow/connector.py` |
| Workday | `workday_reports/connector.py` |
