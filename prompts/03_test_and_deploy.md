# Step 3: Test and Deploy

After creating your connector, test it locally and deploy to Databricks.

## Local Testing

### 1. Generate DAB Files

```bash
# Using CLI
python -m core.cli myconnector \
  --input-config myconnector/examples/basic/pipeline_config.csv \
  --output-dir output \
  --workspace-host "https://your-workspace.databricks.com"

# Or programmatically
python -c "
from core import run_pipeline_generation

run_pipeline_generation(
    connector_name='myconnector',
    input_source='myconnector/examples/basic/pipeline_config.csv',
    output_dir='output',
    targets={'dev': {'workspace_host': 'https://your-workspace.databricks.com'}},
)
"
```

### 2. Verify Output Structure

**SaaS connector output:**
```
output/my_project/
├── databricks.yml
└── resources/
    ├── pipelines.yml
    └── jobs.yml
```

**Database connector output:**
```
output/my_project/
├── databricks.yml
└── resources/
    ├── gateways.yml
    ├── pipelines.yml
    └── jobs.yml
```

### 3. Validate Bundle

```bash
cd output/my_project
databricks bundle validate -t dev
```

Common validation errors:
- **Missing connection**: Create the UC connection first
- **Invalid cron**: Check schedule format (5-field standard cron)
- **Resource name issues**: Names must start with letter, use only `[a-zA-Z0-9_-]`

## Deploying to Databricks

### 1. Deploy the Bundle

```bash
cd output/my_project
databricks bundle deploy -t dev
```

### 2. Run a Pipeline Manually

```bash
# List deployed pipelines
databricks pipelines list

# Start a pipeline
databricks pipelines start <pipeline-id>
```

### 3. Monitor Execution

Check the Databricks UI:
- **Pipelines** tab for ingestion status
- **Jobs** tab for scheduled runs
- **Unity Catalog** for created tables

## Running Tests

### Unit Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_yaml_generation.py

# Run with verbose output
pytest tests/ -v
```

### Integration Tests

Test against a real Databricks workspace:

```python
from core import run_pipeline_generation

# Generate with real workspace
result = run_pipeline_generation(
    connector_name='myconnector',
    input_source='myconnector/examples/basic/pipeline_config.csv',
    output_dir='/Workspace/Users/you@company.com/test_deployment',
    targets={
        'dev': {
            'workspace_host': 'https://your-workspace.databricks.com',
            'root_path': '/Users/you@company.com/.bundle/${bundle.name}/${bundle.target}'
        }
    },
)

print(f"Generated {len(result)} table configs")
print(f"Pipelines: {result['pipeline_group'].nunique()}")
```

## Troubleshooting

### Common Issues

**1. Import errors**
```
ModuleNotFoundError: No module named 'core'
```
Run from the repository root, not from inside a connector folder.

**2. Missing columns**
```
ConfigurationError: Missing required columns: ['connection_name']
```
Add missing columns to your CSV or check `required_columns` property.

**3. Bundle validation fails**
```
Error: connection 'my_connection' not found
```
Create the UC connection before deploying:
```sql
CREATE CONNECTION my_connection TYPE mytype ...
```

**4. Pipeline fails at runtime**
- Check source connectivity (firewall, credentials)
- Verify column mappings match source schema
- Check Unity Catalog permissions

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

from core import run_pipeline_generation
# ... run generation
```

## Using Override Parameters

### Pause all jobs (for testing)

```python
run_pipeline_generation(
    connector_name='myconnector',
    input_source='config.csv',
    output_dir='output',
    targets={'dev': {'workspace_host': '...'}},
    override_input_config={'schedule': None},  # No scheduled jobs
)
```

### Force specific values

```python
run_pipeline_generation(
    connector_name='myconnector',
    input_source='config.csv',
    output_dir='output',
    targets={'dev': {'workspace_host': '...'}},
    override_input_config={
        'target_catalog': 'test_catalog',  # All tables to test catalog
        'schedule': '0 0 * * *',           # Daily schedule for all
    },
)
```

### Provide defaults for missing values

```python
run_pipeline_generation(
    connector_name='myconnector',
    input_source='config.csv',
    output_dir='output',
    targets={'dev': {'workspace_host': '...'}},
    default_values={
        'schedule': '*/30 * * * *',  # Default if not in CSV
        'subgroup': '01',
    },
)
```
