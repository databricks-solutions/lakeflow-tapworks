"""
Tests for process_input_config function in Salesforce load_balancer module.

Tests cover various scenarios:
- Missing required columns
- Missing optional columns
- Empty DataFrames
- NaN values in columns
- Empty string values in columns
- Mixed valid and invalid data
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utilities import process_input_config


# Default required and optional columns for Salesforce connector
REQUIRED_COLUMNS = [
    'source_database', 'source_schema', 'source_table_name',
    'target_catalog', 'target_schema', 'target_table_name',
    'prefix', 'subgroup', 'connection_name'
]

DEFAULT_VALUES = {
    'schedule': '*/15 * * * *',
    'include_columns': '',
    'exclude_columns': ''
}


class TestProcessInputConfig:
    """Test suite for process_input_config function"""

    def test_valid_dataframe_with_all_columns(self):
        """Test with a valid DataFrame containing all required and optional columns"""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'custom'],
            'source_table_name': ['Account', 'MyCustom__c'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sfdc', 'sfdc'],
            'target_table_name': ['Account', 'MyCustom'],
            'prefix': ['sales', 'sales'],
            'subgroup': ['01', '02'],
            'connection_name': ['sfdc_conn', 'sfdc_conn'],
            'schedule': ['*/15 * * * *', '*/30 * * * *'],
            'include_columns': ['Id,Name', ''],
            'exclude_columns': ['', 'CreatedDate']
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

        # Verify all columns present
        assert 'source_database' in result.columns
        assert 'connection_name' in result.columns
        assert 'schedule' in result.columns
        assert 'include_columns' in result.columns
        assert 'exclude_columns' in result.columns

        # Verify values preserved
        assert result['source_table_name'].tolist() == ['Account', 'MyCustom__c']
        assert result['connection_name'].tolist() == ['sfdc_conn', 'sfdc_conn']
        assert result['schedule'].tolist() == ['*/15 * * * *', '*/30 * * * *']

        # Verify row count
        assert len(result) == 2

    def test_missing_required_columns(self):
        """Test that missing required columns raise ValueError"""
        # Missing source_table_name
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['Account'],
            'prefix': ['sales'],
            'subgroup': ['01']
        })

        with pytest.raises(ValueError, match="Missing required columns"):
            process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

    def test_missing_multiple_required_columns(self):
        """Test that multiple missing required columns are reported"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'target_catalog': ['bronze']
        })

        with pytest.raises(ValueError, match="Missing required columns"):
            process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

    def test_missing_all_optional_columns(self):
        """Test that missing optional columns are added with defaults"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['Account'],
            'prefix': ['sales'],
            'subgroup': ['01'],
            'connection_name': ['my_conn']  # Now required
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values={
                'schedule': '0 */6 * * *',
                'include_columns': '',
                'exclude_columns': ''
            }
        )

        # Verify optional columns added with defaults
        assert 'schedule' in result.columns
        assert 'include_columns' in result.columns
        assert 'exclude_columns' in result.columns

        # connection_name should be preserved from input
        assert result['connection_name'].iloc[0] == 'my_conn'
        # Optional columns should have defaults
        assert result['schedule'].iloc[0] == '0 */6 * * *'
        assert result['include_columns'].iloc[0] == ''
        assert result['exclude_columns'].iloc[0] == ''

    def test_nan_values_in_optional_columns(self):
        """Test that NaN values in optional columns are replaced with defaults"""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sfdc', 'sfdc'],
            'target_table_name': ['Account', 'Contact'],
            'prefix': ['sales', 'sales'],
            'subgroup': ['01', '01'],
            'connection_name': ['conn1', 'custom_conn'],  # Required, must have valid values
            'schedule': ['*/15 * * * *', None],  # NaN in optional
            'include_columns': [None, 'Id,Name'],  # NaN in optional
            'exclude_columns': ['SystemModstamp', None]  # NaN in optional
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values={
                'schedule': '0 */12 * * *',
                'include_columns': '',
                'exclude_columns': ''
            }
        )

        # Required columns should be preserved
        assert result['connection_name'].iloc[0] == 'conn1'
        assert result['connection_name'].iloc[1] == 'custom_conn'

        # Optional columns with NaN should get defaults
        assert result['schedule'].iloc[1] == '0 */12 * * *'  # NaN replaced with default
        assert result['include_columns'].iloc[0] == ''  # NaN replaced with default
        assert result['exclude_columns'].iloc[1] == ''  # NaN replaced with default

        # Non-NaN optional values should be preserved
        assert result['schedule'].iloc[0] == '*/15 * * * *'
        assert result['include_columns'].iloc[1] == 'Id,Name'
        assert result['exclude_columns'].iloc[0] == 'SystemModstamp'

    def test_empty_string_values_in_optional_columns(self):
        """Test that empty strings in optional columns are replaced with defaults"""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sfdc', 'sfdc'],
            'target_table_name': ['Account', 'Contact'],
            'prefix': ['sales', 'sales'],
            'subgroup': ['01', '01'],
            'connection_name': ['conn1', 'my_conn'],  # Required, must have valid values
            'schedule': ['*/15 * * * *', ''],  # Empty string in optional
            'include_columns': ['', ''],  # Empty strings (default is also empty)
            'exclude_columns': ['', '']  # Empty strings (default is also empty)
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values={
                'schedule': '0 */6 * * *',
                'include_columns': '',
                'exclude_columns': ''
            }
        )

        # Required columns should be preserved
        assert result['connection_name'].iloc[0] == 'conn1'
        assert result['connection_name'].iloc[1] == 'my_conn'

        # Empty strings in schedule should be replaced with default
        assert result['schedule'].iloc[1] == '0 */6 * * *'

        # Non-empty schedule should be preserved
        assert result['schedule'].iloc[0] == '*/15 * * * *'

        # Empty strings for include/exclude should remain empty (since default is also empty)
        assert result['include_columns'].iloc[0] == ''
        assert result['exclude_columns'].iloc[1] == ''

    def test_whitespace_only_values_in_optional_columns(self):
        """Test that whitespace-only strings are treated as empty and replaced"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['Account'],
            'prefix': ['sales'],
            'subgroup': ['01'],
            'connection_name': ['my_conn'],  # Required, must have valid value
            'schedule': ['\t\n'],  # Whitespace only in optional
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values={
                'schedule': '0 */6 * * *',
                'include_columns': '',
                'exclude_columns': ''
            }
        )

        # Required columns should be preserved
        assert result['connection_name'].iloc[0] == 'my_conn'
        # Whitespace-only in optional column should be replaced with default
        assert result['schedule'].iloc[0] == '0 */6 * * *'

    def test_empty_dataframe(self):
        """Test that empty DataFrame raises ValueError"""
        df = pd.DataFrame({
            'source_database': [],
            'source_schema': [],
            'source_table_name': [],
            'target_catalog': [],
            'target_schema': [],
            'target_table_name': [],
            'prefix': [],
            'subgroup': []
        })

        with pytest.raises(ValueError, match="Input DataFrame is empty"):
            process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

    def test_mixed_valid_and_empty_values(self):
        """Test DataFrame with mix of valid values, NaN, and empty strings in optional columns"""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'custom', 'standard'],
            'source_table_name': ['Account', 'MyCustom__c', 'Contact'],
            'target_catalog': ['bronze', 'bronze', 'bronze'],
            'target_schema': ['sfdc', 'sfdc', 'sfdc'],
            'target_table_name': ['Account', 'MyCustom', 'Contact'],
            'prefix': ['sales', 'marketing', 'sales'],
            'subgroup': ['01', '02', '01'],
            'connection_name': ['conn1', 'conn2', 'conn3'],  # Required, all valid
            'schedule': [None, '*/30 * * * *', ''],  # NaN, valid, empty
            'include_columns': ['Id,Name', '', None],
            'exclude_columns': [None, 'CreatedDate', '']
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

        # Row 0: Valid connection, NaN schedule
        assert result['connection_name'].iloc[0] == 'conn1'
        assert result['schedule'].iloc[0] == '*/15 * * * *'  # NaN replaced with default
        assert result['include_columns'].iloc[0] == 'Id,Name'
        assert result['exclude_columns'].iloc[0] == ''  # NaN replaced with default

        # Row 1: Valid connection, valid schedule
        assert result['connection_name'].iloc[1] == 'conn2'
        assert result['schedule'].iloc[1] == '*/30 * * * *'
        assert result['include_columns'].iloc[1] == ''  # Empty stays empty
        assert result['exclude_columns'].iloc[1] == 'CreatedDate'

        # Row 2: Valid connection, empty schedule
        assert result['connection_name'].iloc[2] == 'conn3'
        assert result['schedule'].iloc[2] == '*/15 * * * *'  # Empty replaced with default
        assert result['include_columns'].iloc[2] == ''  # NaN replaced with default
        assert result['exclude_columns'].iloc[2] == ''  # Empty stays empty

    def test_all_required_columns_present_different_types(self):
        """Test that function works with different data types in columns"""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'custom'],
            'source_table_name': ['Account', 'MyCustom__c'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sfdc', 'sfdc'],
            'target_table_name': ['Account', 'MyCustom'],
            'prefix': ['sales', 'marketing'],
            'subgroup': [1, 2],  # integers instead of strings
            'connection_name': ['conn1', 'conn2']
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

        # Should handle different types
        assert len(result) == 2
        assert 'subgroup' in result.columns

    def test_custom_defaults_applied_correctly(self):
        """Test that custom default values are applied correctly"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['Account'],
            'prefix': ['sales'],
            'subgroup': ['01'],
            'connection_name': ['my_custom_connection']
        })

        custom_schedule = '0 0 * * *'  # Daily at midnight

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values={
                'schedule': custom_schedule,
                'include_columns': '',
                'exclude_columns': ''
            }
        )

        # connection_name should be preserved from DataFrame
        assert result['connection_name'].iloc[0] == 'my_custom_connection'
        # Custom schedule should be applied
        assert result['schedule'].iloc[0] == custom_schedule

    def test_dataframe_not_modified_in_place(self):
        """Test that original DataFrame is not modified"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['Account'],
            'prefix': ['sales'],
            'subgroup': ['01'],
            'connection_name': ['my_conn']
        })

        original_columns = list(df.columns)
        original_len = len(df)

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

        # Original DataFrame should be unchanged
        assert list(df.columns) == original_columns
        assert len(df) == original_len
        assert 'schedule' not in df.columns  # Optional column shouldn't be in original

        # Result should have new optional columns
        assert 'schedule' in result.columns
        assert 'include_columns' in result.columns

    def test_large_dataframe(self):
        """Test with a larger DataFrame to ensure performance"""
        num_rows = 100
        df = pd.DataFrame({
            'source_database': ['Salesforce'] * num_rows,
            'source_schema': ['standard'] * num_rows,
            'source_table_name': [f'Table{i}' for i in range(num_rows)],
            'target_catalog': ['bronze'] * num_rows,
            'target_schema': ['sfdc'] * num_rows,
            'target_table_name': [f'Table{i}' for i in range(num_rows)],
            'prefix': ['sales'] * num_rows,
            'subgroup': ['01'] * num_rows,
            'connection_name': ['sfdc_connection'] * num_rows
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

        assert len(result) == num_rows
        assert all(result['connection_name'] == 'sfdc_connection')
        assert all(result['schedule'] == '*/15 * * * *')

    def test_special_characters_in_values(self):
        """Test that special characters in values are preserved"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['custom__c'],
            'source_table_name': ['My_Custom__c'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['My_Custom'],
            'prefix': ['sales-team-1'],
            'subgroup': ['01'],
            'connection_name': ['conn@special!'],
            'include_columns': ['Id,Name__c,Custom_Field__c']
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES
        )

        assert result['source_schema'].iloc[0] == 'custom__c'
        assert result['source_table_name'].iloc[0] == 'My_Custom__c'
        assert result['prefix'].iloc[0] == 'sales-team-1'
        assert result['connection_name'].iloc[0] == 'conn@special!'
        assert result['include_columns'].iloc[0] == 'Id,Name__c,Custom_Field__c'

    def test_override_single_column(self):
        """Test that override_input_config overrides a single column for all rows"""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'custom'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sfdc', 'sfdc'],
            'target_table_name': ['Account', 'Contact'],
            'prefix': ['sales', 'sales'],
            'subgroup': ['01', '02'],
            'connection_name': ['conn1', 'conn2'],
            'schedule': ['*/15 * * * *', '*/30 * * * *']
        })

        override_config = {'schedule': '0 */6 * * *'}

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES,
            override_input_config=override_config
        )

        # Schedule should be overridden for all rows
        assert result['schedule'].iloc[0] == '0 */6 * * *'
        assert result['schedule'].iloc[1] == '0 */6 * * *'

        # Other columns should remain unchanged
        assert result['connection_name'].iloc[0] == 'conn1'
        assert result['connection_name'].iloc[1] == 'conn2'

    def test_override_multiple_columns(self):
        """Test that override_input_config can override multiple columns"""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'custom', 'standard'],
            'source_table_name': ['Account', 'Contact', 'Lead'],
            'target_catalog': ['bronze', 'silver', 'bronze'],
            'target_schema': ['sfdc', 'sfdc', 'sfdc'],
            'target_table_name': ['Account', 'Contact', 'Lead'],
            'prefix': ['sales', 'marketing', 'sales'],
            'subgroup': ['01', '02', '01'],
            'connection_name': ['conn1', 'conn2', 'conn3'],
            'schedule': ['*/15 * * * *', '*/30 * * * *', '*/45 * * * *']
        })

        override_config = {
            'schedule': '0 0 * * *',
            'target_catalog': 'gold'
        }

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES,
            override_input_config=override_config
        )

        # Both schedule and target_catalog should be overridden for all rows
        assert all(result['schedule'] == '0 0 * * *')
        assert all(result['target_catalog'] == 'gold')

        # Other columns should remain unchanged
        assert result['connection_name'].tolist() == ['conn1', 'conn2', 'conn3']
        assert result['prefix'].tolist() == ['sales', 'marketing', 'sales']

    def test_override_replaces_existing_values(self):
        """Test that override replaces existing non-empty values"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['Account'],
            'prefix': ['sales'],
            'subgroup': ['01'],
            'connection_name': ['original_conn'],
            'schedule': ['*/15 * * * *']
        })

        override_config = {
            'connection_name': 'override_conn',
            'schedule': '0 */12 * * *'
        }

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES,
            override_input_config=override_config
        )

        # Original values should be replaced
        assert result['connection_name'].iloc[0] == 'override_conn'
        assert result['schedule'].iloc[0] == '0 */12 * * *'

    def test_override_with_none(self):
        """Test that None override_input_config doesn't affect processing"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['Account'],
            'prefix': ['sales'],
            'subgroup': ['01'],
            'connection_name': ['my_conn'],
            'schedule': ['*/15 * * * *']
        })

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES,
            override_input_config=None
        )

        # Values should remain unchanged
        assert result['connection_name'].iloc[0] == 'my_conn'
        assert result['schedule'].iloc[0] == '*/15 * * * *'

    def test_override_with_missing_column_adds_column(self):
        """Test that override can add a new column not in original DataFrame"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['bronze'],
            'target_schema': ['sfdc'],
            'target_table_name': ['Account'],
            'prefix': ['sales'],
            'subgroup': ['01'],
            'connection_name': ['my_conn']
        })

        override_config = {
            'schedule': '0 0 * * *'  # schedule not in original, will be added by optional
        }

        result = process_input_config(
            df,
            required_columns=REQUIRED_COLUMNS,
            default_values=DEFAULT_VALUES,
            override_input_config=override_config
        )

        # Schedule should be added and overridden
        assert 'schedule' in result.columns
        assert result['schedule'].iloc[0] == '0 0 * * *'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
