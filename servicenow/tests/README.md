# ServiceNow Connector Tests

This directory contains tests for the ServiceNow connector.

## Test Coverage

### Unit Tests
- Connector initialization and configuration
- CSV input validation
- Pipeline grouping logic
- YAML generation

### Integration Tests
- End-to-end pipeline generation
- DAB structure validation
- Example CSV processing

## Running Tests

### Prerequisites
```bash
pip install -r ../requirements.txt
pip install pytest
```

### Run All Tests
```bash
# From the servicenow directory
pytest tests/

# With verbose output
pytest tests/ -v

# With coverage
pytest tests/ --cov=. --cov-report=html
```

### Run Specific Tests
```bash
# Test connector initialization
pytest tests/test_connector.py::test_connector_initialization

# Test pipeline generation
pytest tests/test_pipeline_generation.py
```

## Test Data

Test data files are located in the `tests/data/` directory:
- `test_config.csv` - Minimal test configuration
- `test_config_full.csv` - Complete test configuration with all features

## Adding New Tests

When adding new features to the ServiceNow connector:

1. Create test cases in appropriate test file
2. Add test data to `tests/data/` if needed
3. Update this README with new test descriptions
4. Ensure all tests pass before committing

## Test Structure

```
tests/
├── __init__.py
├── README.md
├── test_connector.py           # Connector class tests
├── test_pipeline_generation.py # Pipeline generation tests
├── test_yaml_generation.py     # YAML output tests
└── data/
    ├── test_config.csv
    └── test_config_full.csv
```

## CI/CD Integration

Tests should be run automatically in CI/CD pipelines:
- On pull requests
- Before merging to main branch
- Before releases

## Known Issues

None at this time.

## Future Test Improvements

- [ ] Add performance tests for large configurations (1000+ tables)
- [ ] Add validation tests for ServiceNow connection schema
- [ ] Add integration tests with actual Databricks workspace
- [ ] Add tests for error handling and edge cases
