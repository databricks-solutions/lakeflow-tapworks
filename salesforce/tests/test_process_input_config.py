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

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from load_balancing.load_balancer import process_input_config


# Default required and optional columns for Salesforce connector
REQUIRED_COLUMNS = [
    'source_database', 'source_schema', 'source_table_name',
    'target_catalog', 'target_schema', 'target_table_name',
    'prefix', 'priority', 'connection_name'
]

OPTIONAL_COLUMNS = {
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
            'priority': ['01', '02'],
            'connection_name': ['sfdc_conn', 'sfdc_conn'],
            'schedule': ['*/15 * * * *', '*/30 * * * *'],
            'include_columns': ['Id,Name', ''],
            'exclude_columns': ['', 'CreatedDate']
        })

        result = read_input_config(df, default_connection_name='default_conn')

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
            'priority': ['01']
        })

        with pytest.raises(ValueError, match="Missing required columns"):
            read_input_config(df)

    def test_missing_multiple_required_columns(self):
        """Test that multiple missing required columns are reported"""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'target_catalog': ['bronze']
        })

        with pytest.raises(ValueError, match="Missing required columns"):
            read_input_config(df)

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
            'priority': ['01']
        })

        result = read_input_config(
            df,
            default_connection_name='my_conn',
            default_schedule='0 */6 * * *'
        )

        # Verify optional columns added with defaults
        assert 'connection_name' in result.columns
        assert 'schedule' in result.columns
        assert 'include_columns' in result.columns
        assert 'exclude_columns' in result.columns

        assert result['connection_name'].iloc[0] == 'my_conn'
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
            'priority': ['01', '01'],
            'connection_name': [None, 'custom_conn'],  # NaN
            'schedule': ['*/15 * * * *', None],  # NaN
            'include_columns': [None, 'Id,Name'],  # NaN
            'exclude_columns': ['SystemModstamp', None]  # NaN
        })

        result = read_input_config(
            df,
            default_connection_name='default_conn',
            default_schedule='0 */12 * * *'
        )

        # First row should have defaults where NaN
        assert result['connection_name'].iloc[0] == 'default_conn'
        assert result['include_columns'].iloc[0] == ''
        assert result['exclude_columns'].iloc[1] == ''

        # Second row should preserve non-NaN values
        assert result['connection_name'].iloc[1] == 'custom_conn'
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
            'priority': ['01', '01'],
            'connection_name': ['', 'my_conn'],  # Empty string
            'schedule': ['*/15 * * * *', ''],  # Empty string
            'include_columns': ['', ''],  # Empty strings
            'exclude_columns': ['', '']  # Empty strings
        })

        result = read_input_config(
            df,
            default_connection_name='default_conn',
            default_schedule='0 */6 * * *'
        )

        # Empty strings should be replaced with defaults
        assert result['connection_name'].iloc[0] == 'default_conn'
        assert result['schedule'].iloc[1] == '0 */6 * * *'

        # Non-empty values should be preserved
        assert result['connection_name'].iloc[1] == 'my_conn'
        assert result['schedule'].iloc[0] == '*/15 * * * *'

        # Empty strings for include/exclude should remain empty
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
            'priority': ['01'],
            'connection_name': ['   '],  # Whitespace only
            'schedule': ['\t\n'],  # Whitespace only
        })

        result = read_input_config(
            df,
            default_connection_name='default_conn',
            default_schedule='0 */6 * * *'
        )

        # Whitespace-only should be replaced with defaults
        assert result['connection_name'].iloc[0] == 'default_conn'
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
            'priority': []
        })

        with pytest.raises(ValueError, match="Input DataFrame is empty"):
            read_input_config(df)

    def test_mixed_valid_and_empty_values(self):
        """Test DataFrame with mix of valid values, NaN, and empty strings"""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'custom', 'standard'],
            'source_table_name': ['Account', 'MyCustom__c', 'Contact'],
            'target_catalog': ['bronze', 'bronze', 'bronze'],
            'target_schema': ['sfdc', 'sfdc', 'sfdc'],
            'target_table_name': ['Account', 'MyCustom', 'Contact'],
            'prefix': ['sales', 'marketing', 'sales'],
            'priority': ['01', '02', '01'],
            'connection_name': ['conn1', '', None],  # Valid, empty, NaN
            'schedule': [None, '*/30 * * * *', ''],  # NaN, valid, empty
            'include_columns': ['Id,Name', '', None],
            'exclude_columns': [None, 'CreatedDate', '']
        })

        result = read_input_config(
            df,
            default_connection_name='default_conn',
            default_schedule='*/15 * * * *'
        )

        # Row 0: Valid connection, NaN schedule
        assert result['connection_name'].iloc[0] == 'conn1'
        assert result['schedule'].iloc[0] == '*/15 * * * *'  # Default
        assert result['include_columns'].iloc[0] == 'Id,Name'
        assert result['exclude_columns'].iloc[0] == ''  # Default for NaN

        # Row 1: Empty connection, valid schedule
        assert result['connection_name'].iloc[1] == 'default_conn'  # Default
        assert result['schedule'].iloc[1] == '*/30 * * * *'
        assert result['include_columns'].iloc[1] == ''  # Empty stays empty
        assert result['exclude_columns'].iloc[1] == 'CreatedDate'

        # Row 2: NaN connection, empty schedule
        assert result['connection_name'].iloc[2] == 'default_conn'  # Default
        assert result['schedule'].iloc[2] == '*/15 * * * *'  # Default
        assert result['include_columns'].iloc[2] == ''  # Default for NaN
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
            'priority': [1, 2]  # integers instead of strings
        })

        result = read_input_config(df)

        # Should handle different types
        assert len(result) == 2
        assert 'priority' in result.columns

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
            'priority': ['01']
        })

        custom_conn = 'my_custom_connection'
        custom_schedule = '0 0 * * *'  # Daily at midnight

        result = read_input_config(
            df,
            default_connection_name=custom_conn,
            default_schedule=custom_schedule
        )

        assert result['connection_name'].iloc[0] == custom_conn
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
            'priority': ['01']
        })

        original_columns = list(df.columns)
        original_len = len(df)

        result = read_input_config(df)

        # Original DataFrame should be unchanged
        assert list(df.columns) == original_columns
        assert len(df) == original_len
        assert 'connection_name' not in df.columns

        # Result should have new columns
        assert 'connection_name' in result.columns
        assert 'schedule' in result.columns

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
            'priority': ['01'] * num_rows
        })

        result = read_input_config(df)

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
            'priority': ['01'],
            'connection_name': ['conn@special!'],
            'include_columns': ['Id,Name__c,Custom_Field__c']
        })

        result = read_input_config(df)

        assert result['source_schema'].iloc[0] == 'custom__c'
        assert result['source_table_name'].iloc[0] == 'My_Custom__c'
        assert result['prefix'].iloc[0] == 'sales-team-1'
        assert result['connection_name'].iloc[0] == 'conn@special!'
        assert result['include_columns'].iloc[0] == 'Id,Name__c,Custom_Field__c'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
