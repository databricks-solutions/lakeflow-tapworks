# Salesforce Connector Tests

This directory contains unit tests for the Salesforce connector components.

## Test Files

- `test_read_input_config.py` - Tests for the `read_input_config()` function

## Running Tests

### Install Dependencies

```bash
pip install pytest pandas
```

### Run All Tests

From the salesforce directory:

```bash
cd salesforce
pytest tests/ -v
```

### Run Specific Test File

```bash
pytest tests/test_read_input_config.py -v
```

### Run Specific Test

```bash
pytest tests/test_read_input_config.py::TestReadInputConfig::test_missing_required_columns -v
```

### Run with Coverage

```bash
pip install pytest-cov
pytest tests/ --cov=load_balancing --cov-report=html
```

## Test Coverage

### `test_read_input_config.py`

Tests for the `read_input_config()` function covering:

1. **Valid Input**
   - Valid DataFrame with all columns
   - Valid DataFrame with only required columns
   - Large DataFrame (performance test)

2. **Missing Columns**
   - Missing single required column
   - Missing multiple required columns
   - Missing all optional columns (should add with defaults)

3. **Empty/Invalid Data**
   - Empty DataFrame
   - NaN values in optional columns
   - Empty strings in optional columns
   - Whitespace-only values
   - Mixed valid and invalid values

4. **Edge Cases**
   - Different data types in columns
   - Special characters in values
   - DataFrame not modified in place
   - Custom default values

## Test Scenarios

### Scenario 1: Missing Required Columns
```python
def test_missing_required_columns(self):
    # DataFrame missing source_table_name
    # Should raise ValueError
```

### Scenario 2: Missing Optional Columns
```python
def test_missing_all_optional_columns(self):
    # DataFrame with only required columns
    # Should add optional columns with defaults
```

### Scenario 3: NaN Values
```python
def test_nan_values_in_optional_columns(self):
    # DataFrame with NaN in connection_name, schedule
    # Should replace NaN with defaults
```

### Scenario 4: Empty Strings
```python
def test_empty_string_values_in_optional_columns(self):
    # DataFrame with empty strings
    # Should replace empty strings with defaults
```

### Scenario 5: Mixed Valid/Invalid
```python
def test_mixed_valid_and_empty_values(self):
    # DataFrame with mix of valid, NaN, and empty values
    # Should handle each appropriately
```

## Expected Behavior

### Required Columns
- Must be present in input DataFrame
- Missing required columns raise `ValueError`
- Required columns: `source_database`, `source_schema`, `source_table_name`, `target_catalog`, `target_schema`, `target_table_name`, `prefix`, `priority`

### Optional Columns
- Added with defaults if missing
- NaN values replaced with defaults
- Empty strings replaced with defaults
- Whitespace-only strings replaced with defaults
- Optional columns: `connection_name`, `schedule`, `include_columns`, `exclude_columns`

### Output
- All required columns present
- All optional columns present with valid values
- No NaN values
- No empty strings in columns with defaults
- Original DataFrame not modified

## Adding New Tests

When adding new tests:

1. Follow the naming convention: `test_<description>`
2. Use descriptive docstrings
3. Test one scenario per test function
4. Use pytest fixtures for common test data
5. Update this README with new test scenarios
