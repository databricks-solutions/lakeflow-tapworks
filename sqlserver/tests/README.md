# SQL Server Pipeline Generation Tests

This directory contains comprehensive unit tests for the SQL Server to Databricks pipeline generation toolkit.

## Test Coverage

The test suite covers three main scenarios based on real usage patterns:

### 1. Simple Config (`TestSimpleConfig`)
Tests pipeline generation when the input CSV **does not have gateway configuration columns**.

**Test Data**: Matches `simple_tapworks_config.csv`
- CSV has only required columns (source/target info)
- All gateway configs should use default values
- Tests:
  - Default connection name is applied to all databases
  - Default node types are applied to all gateways
  - Gateway catalog/schema fall back to target catalog/schema
  - YAML includes cluster configuration

### 2. Mixed Config (`TestMixedConfig`)
Tests pipeline generation when the input CSV **has gateway config columns but some rows have empty/null values**.

**Test Data**: Matches `mixed_tapworks_config.csv`
- db1: All gateway configs specified
- db2: All gateway configs are empty/null (should use defaults)
- db3: Connection/catalog/schema specified, node types empty (should use defaults for node types)
- Tests:
  - Empty values are replaced with defaults (not null)
  - Non-empty values from CSV are preserved
  - YAML has no null values for any gateway
  - Proper handling of NaN values from pandas

### 3. Full Config (`TestFullConfig`)
Tests pipeline generation when the input CSV **has all values specified with different configs per database**.

**Test Data**: Matches `tapworks_config.csv`
- db1: Fully specified with m5d.large workers, c5a.8xlarge driver
- db2: Fully specified with c5a.xlarge workers, c5a.2xlarge driver
- db3: Connection/catalog/schema specified, node types None
- Tests:
  - Each database can have different gateway configurations
  - Serverless gateways (None node types) don't include cluster config
  - Gateways with specified node types include cluster config

### 4. Integration Tests (`TestCompleteIntegration`)
End-to-end tests for the complete pipeline generation workflow.

- Tests:
  - Complete workflow from CSV to YAML files
  - Priority tables are separated into different pipeline groups
  - Tables are split when exceeding max_tables_per_group
  - All expected files are created (databricks.yml, gateways.yml, pipelines.yml)
  - YAML files have valid structure and content

## Running the Tests

### Prerequisites
```bash
pip install pandas pyyaml
```

### Run All Tests
```bash
cd /path/to/lakehouse-tapworks/sqlserver
python -m pytest tests/test_pipeline_generation.py -v
```

Or using unittest:
```bash
cd /path/to/lakehouse-tapworks/sqlserver
python -m unittest tests.test_pipeline_generation -v
```

### Run Specific Test Classes
```bash
# Test simple config only
python -m unittest tests.test_pipeline_generation.TestSimpleConfig -v

# Test mixed config only
python -m unittest tests.test_pipeline_generation.TestMixedConfig -v

# Test full config only
python -m unittest tests.test_pipeline_generation.TestFullConfig -v

# Test integration only
python -m unittest tests.test_pipeline_generation.TestCompleteIntegration -v
```

### Run Specific Test Methods
```bash
# Test that empty values use defaults
python -m unittest tests.test_pipeline_generation.TestMixedConfig.test_empty_values_use_defaults -v

# Test gateway without cluster configuration
python -m unittest tests.test_pipeline_generation.TestFullConfig.test_gateway_without_cluster_config -v
```

## Test Output

Each test will show:
- ✓ Pass: Test passed successfully
- ✗ Fail: Test failed with assertion error
- E Error: Test encountered an exception

Example output:
```
test_different_configs_per_database (tests.test_pipeline_generation.TestFullConfig) ... ok
test_empty_values_use_defaults (tests.test_pipeline_generation.TestMixedConfig) ... ok
test_generate_pipeline_config_uses_defaults (tests.test_pipeline_generation.TestSimpleConfig) ... ok
test_gateway_without_cluster_config (tests.test_pipeline_generation.TestFullConfig) ... ok
test_yaml_no_null_values (tests.test_pipeline_generation.TestMixedConfig) ... ok

----------------------------------------------------------------------
Ran 5 tests in 0.234s

OK
```

## Key Test Assertions

### 1. No Null Values in YAML
The most critical test ensures that empty CSV cells don't result in `null` values in the YAML output:

```python
# From TestMixedConfig.test_yaml_no_null_values
for pipeline_name, config in gateway_yaml['resources']['pipelines'].items():
    self.assertIsNotNone(config['gateway_definition']['connection_name'])
    self.assertIsNotNone(config['gateway_definition']['gateway_storage_catalog'])
    self.assertIsNotNone(config['gateway_definition']['gateway_storage_schema'])
```

### 2. Defaults Applied Correctly
Tests verify that default values are used when columns are missing or empty:

```python
# Empty value should use default
self.assertEqual(db2_rows['connection_name'].iloc[0], self.default_connection_name)
self.assertEqual(db2_rows['gateway_worker_type'].iloc[0], self.default_gateway_worker_type)
```

### 3. CSV Values Preserved
Tests verify that non-empty CSV values are preserved and not overwritten:

```python
# CSV value should be preserved
self.assertEqual(db1_rows['connection_name'].iloc[0], 'sqlserver_conn_1')
self.assertEqual(db1_rows['gateway_worker_type'].iloc[0], 'm5d.large')
```

### 4. Serverless Configuration
Tests verify that gateways with None node types don't include cluster config:

```python
# Serverless gateway should not have cluster configuration
gateway_3 = gateway_yaml['resources']['pipelines']['test_project_pipeline_test_project_gateway_3']
self.assertNotIn('clusters', gateway_3)
```

## Continuous Integration

These tests should be run:
- Before committing changes to pipeline generation code
- In CI/CD pipelines before deployment
- After any changes to CSV handling or YAML generation logic

## Troubleshooting

### ImportError: No module named 'pandas'
```bash
pip install pandas pyyaml
```

### Tests fail with "NaN values in YAML"
This indicates a regression in the NaN handling logic. Check that `pd.isna()` is used as the first condition when checking for empty values:

```python
if pd.isna(value) or value == '' or value is None:
    # Use default
```

### Tests fail with "Columns don't exist"
Ensure that the column existence flags are captured before the groupby loop:

```python
has_connection_name = 'connection_name' in df.columns
# ... capture all flags BEFORE the loop
for source_db, db_group in df.groupby('source_database'):
    # Use has_connection_name flag here
```
