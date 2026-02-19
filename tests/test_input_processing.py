"""
Tests for input processing functionality.

Tests the _process_input_config and load_and_normalize_input methods
which handle default values, overrides, and validation.
"""

import pytest
import pandas as pd
import numpy as np
from tapworks.core import ConfigurationError


class TestProcessInputConfig:
    """Tests for the _process_input_config method."""

    def test_returns_copy_of_dataframe(self, salesforce_connector, sample_salesforce_df):
        """Should return a copy, not modify the original."""
        original_df = sample_salesforce_df.copy()

        result = salesforce_connector._process_input_config(
            df=sample_salesforce_df,
            required_columns=salesforce_connector.required_columns
        )

        # Original should be unchanged
        pd.testing.assert_frame_equal(sample_salesforce_df, original_df)

    def test_raises_error_for_empty_dataframe(self, salesforce_connector):
        """Should raise ConfigurationError for empty DataFrame."""
        empty_df = pd.DataFrame()

        with pytest.raises(ConfigurationError, match="Input DataFrame is empty"):
            salesforce_connector._process_input_config(
                df=empty_df,
                required_columns=salesforce_connector.required_columns
            )

    def test_raises_error_for_missing_required_columns(self, salesforce_connector):
        """Should raise ConfigurationError when required columns are missing."""
        incomplete_df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            # Missing: source_table_name, target_catalog, etc.
        })

        with pytest.raises(ConfigurationError, match="Missing required columns"):
            salesforce_connector._process_input_config(
                df=incomplete_df,
                required_columns=salesforce_connector.required_columns
            )

    def test_error_message_lists_missing_columns(self, salesforce_connector):
        """Error message should list which columns are missing."""
        incomplete_df = pd.DataFrame({
            'source_database': ['Salesforce'],
        })

        with pytest.raises(ConfigurationError) as exc_info:
            salesforce_connector._process_input_config(
                df=incomplete_df,
                required_columns=salesforce_connector.required_columns
            )

        # Check that missing columns are mentioned
        assert 'source_table_name' in str(exc_info.value)
        assert 'connection_name' in str(exc_info.value)


class TestDefaultValues:
    """Tests for default value application."""

    def test_adds_missing_columns_with_defaults(self, salesforce_connector, sample_salesforce_df):
        """Should add columns that don't exist with default values."""
        default_values = {'new_column': 'default_value'}

        result = salesforce_connector._process_input_config(
            df=sample_salesforce_df,
            required_columns=salesforce_connector.required_columns,
            default_values=default_values
        )

        assert 'new_column' in result.columns
        assert all(result['new_column'] == 'default_value')

    def test_fills_nan_values_with_defaults(self, salesforce_connector, sample_salesforce_df):
        """Should fill NaN values with defaults."""
        df = sample_salesforce_df.copy()
        df['schedule'] = [np.nan, '*/30 * * * *', np.nan]

        default_values = {'schedule': '*/15 * * * *'}

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values=default_values
        )

        assert result.loc[0, 'schedule'] == '*/15 * * * *'
        assert result.loc[1, 'schedule'] == '*/30 * * * *'
        assert result.loc[2, 'schedule'] == '*/15 * * * *'

    def test_fills_empty_strings_with_defaults(self, salesforce_connector, sample_salesforce_df):
        """Should fill empty string values with defaults."""
        df = sample_salesforce_df.copy()
        df['schedule'] = ['', '*/30 * * * *', '  ']  # Empty and whitespace

        default_values = {'schedule': '*/15 * * * *'}

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values=default_values
        )

        assert result.loc[0, 'schedule'] == '*/15 * * * *'
        assert result.loc[1, 'schedule'] == '*/30 * * * *'
        assert result.loc[2, 'schedule'] == '*/15 * * * *'

    def test_none_default_does_not_fill_nan(self, salesforce_connector, sample_salesforce_df):
        """When default is None, NaN values should remain."""
        df = sample_salesforce_df.copy()
        df['optional_col'] = [np.nan, 'value', np.nan]

        default_values = {'optional_col': None}

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values=default_values
        )

        assert pd.isna(result.loc[0, 'optional_col'])
        assert result.loc[1, 'optional_col'] == 'value'

    def test_preserves_existing_values(self, salesforce_connector, sample_salesforce_df):
        """Should not overwrite existing non-empty values."""
        df = sample_salesforce_df.copy()
        df['schedule'] = ['custom_schedule', 'another_schedule', 'third_schedule']

        default_values = {'schedule': '*/15 * * * *'}

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values=default_values
        )

        assert result.loc[0, 'schedule'] == 'custom_schedule'
        assert result.loc[1, 'schedule'] == 'another_schedule'
        assert result.loc[2, 'schedule'] == 'third_schedule'


class TestOverrideInputConfig:
    """Tests for override_input_config functionality."""

    def test_override_replaces_all_values(self, salesforce_connector, sample_salesforce_df):
        """Override should replace values for all rows."""
        df = sample_salesforce_df.copy()
        df['schedule'] = ['schedule1', 'schedule2', 'schedule3']

        override = {'schedule': '0 0 * * *'}

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            override_input_config=override
        )

        assert all(result['schedule'] == '0 0 * * *')

    def test_override_with_none_sets_all_to_none(self, salesforce_connector, sample_salesforce_df):
        """Override with None should set all values to None."""
        df = sample_salesforce_df.copy()
        df['schedule'] = ['schedule1', 'schedule2', 'schedule3']

        override = {'schedule': None}

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            override_input_config=override
        )

        assert all(result['schedule'].isna())

    def test_override_creates_new_column(self, salesforce_connector, sample_salesforce_df):
        """Override should create column if it doesn't exist."""
        override = {'new_override_col': 'override_value'}

        result = salesforce_connector._process_input_config(
            df=sample_salesforce_df,
            required_columns=salesforce_connector.required_columns,
            override_input_config=override
        )

        assert 'new_override_col' in result.columns
        assert all(result['new_override_col'] == 'override_value')

    def test_override_takes_precedence_over_defaults(self, salesforce_connector, sample_salesforce_df):
        """Override should take precedence over default values."""
        default_values = {'schedule': 'default_schedule'}
        override = {'schedule': 'override_schedule'}

        result = salesforce_connector._process_input_config(
            df=sample_salesforce_df,
            required_columns=salesforce_connector.required_columns,
            default_values=default_values,
            override_input_config=override
        )

        assert all(result['schedule'] == 'override_schedule')


class TestLoadAndNormalizeInput:
    """Tests for the full load_and_normalize_input workflow."""

    def test_applies_connector_defaults(self, salesforce_connector, sample_salesforce_df):
        """Should apply connector's default_values."""
        result = salesforce_connector.load_and_normalize_input(
            df=sample_salesforce_df,
            default_values={'project_name': 'test_project'}
        )

        # Connector default for schedule is '*/15 * * * *'
        assert 'schedule' in result.columns

    def test_user_defaults_override_connector_defaults(self, salesforce_connector, sample_salesforce_df):
        """User-provided defaults should override connector defaults."""
        user_defaults = {'project_name': 'test_project', 'schedule': 'user_schedule'}

        result = salesforce_connector.load_and_normalize_input(
            df=sample_salesforce_df,
            default_values=user_defaults
        )

        assert all(result['schedule'] == 'user_schedule')

    def test_project_name_required(self, salesforce_connector):
        """Should raise error when project_name is missing."""
        df_without_project = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
        })

        with pytest.raises(ConfigurationError, match="project_name"):
            salesforce_connector.load_and_normalize_input(df=df_without_project)

    def test_prefix_defaults_to_project_name(self, salesforce_connector, sample_salesforce_df):
        """Prefix should default to project_name if not provided."""
        result = salesforce_connector.load_and_normalize_input(
            df=sample_salesforce_df,
            default_values={'project_name': 'my_project'}
        )

        assert 'prefix' in result.columns
        assert all(result['prefix'] == 'my_project')

    def test_subgroup_defaults_to_01(self, salesforce_connector, sample_salesforce_df):
        """Subgroup should default to '01' if not provided."""
        result = salesforce_connector.load_and_normalize_input(
            df=sample_salesforce_df,
            default_values={'project_name': 'test_project'}
        )

        assert 'subgroup' in result.columns
        assert all(result['subgroup'] == '01')

    def test_existing_prefix_preserved(self, salesforce_connector):
        """Should preserve existing prefix values."""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
            'project_name': ['test_project'],
            'prefix': ['custom_prefix'],
        })

        result = salesforce_connector.load_and_normalize_input(df=df)

        assert result.loc[0, 'prefix'] == 'custom_prefix'

    def test_empty_prefix_filled_with_project_name(self, salesforce_connector):
        """Empty prefix should be filled with project_name."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['sfdc_conn', 'sfdc_conn'],
            'prefix': ['', '  '],  # Empty and whitespace
        })

        result = salesforce_connector.load_and_normalize_input(
            df=df,
            default_values={'project_name': 'test_project'}
        )

        assert result.loc[0, 'prefix'] == 'test_project'
        assert result.loc[1, 'prefix'] == 'test_project'


class TestDatabaseConnectorNormalization:
    """Tests specific to DatabaseConnector normalization."""

    def test_gateway_catalog_defaults_to_target_catalog(self, sqlserver_connector, sample_sqlserver_df):
        """gateway_catalog should default to target_catalog."""
        df = sample_sqlserver_df.copy()
        df['gateway_catalog'] = [np.nan, np.nan, np.nan]

        result = sqlserver_connector.load_and_normalize_input(
            df=df,
            default_values={'project_name': 'test_project'}
        )

        assert all(result['gateway_catalog'] == result['target_catalog'])

    def test_gateway_schema_defaults_to_target_schema(self, sqlserver_connector, sample_sqlserver_df):
        """gateway_schema should default to target_schema."""
        df = sample_sqlserver_df.copy()
        df['gateway_schema'] = [np.nan, np.nan, np.nan]

        result = sqlserver_connector.load_and_normalize_input(
            df=df,
            default_values={'project_name': 'test_project'}
        )

        assert all(result['gateway_schema'] == result['target_schema'])

    def test_existing_gateway_catalog_preserved(self, sqlserver_connector, sample_sqlserver_df):
        """Existing gateway_catalog values should be preserved."""
        df = sample_sqlserver_df.copy()
        df['gateway_catalog'] = ['custom_catalog', np.nan, 'another_catalog']

        result = sqlserver_connector.load_and_normalize_input(
            df=df,
            default_values={'project_name': 'test_project'}
        )

        assert result.loc[0, 'gateway_catalog'] == 'custom_catalog'
        assert result.loc[1, 'gateway_catalog'] == 'main'  # Defaults to target
        assert result.loc[2, 'gateway_catalog'] == 'another_catalog'


class TestScdTypeValidation:
    """Tests for SCD type validation."""

    def test_valid_scd_type_1(self, salesforce_connector):
        """SCD_TYPE_1 should be accepted."""
        result = salesforce_connector._validate_scd_type('SCD_TYPE_1', 'test_table')
        assert result == 'SCD_TYPE_1'

    def test_valid_scd_type_2(self, salesforce_connector):
        """SCD_TYPE_2 should be accepted."""
        result = salesforce_connector._validate_scd_type('SCD_TYPE_2', 'test_table')
        assert result == 'SCD_TYPE_2'

    def test_case_insensitive(self, salesforce_connector):
        """SCD type validation should be case insensitive."""
        result = salesforce_connector._validate_scd_type('scd_type_1', 'test_table')
        assert result == 'SCD_TYPE_1'

        result = salesforce_connector._validate_scd_type('Scd_Type_2', 'test_table')
        assert result == 'SCD_TYPE_2'

    def test_empty_string_returns_none(self, salesforce_connector):
        """Empty string should return None."""
        result = salesforce_connector._validate_scd_type('', 'test_table')
        assert result is None

        result = salesforce_connector._validate_scd_type('  ', 'test_table')
        assert result is None

    def test_none_returns_none(self, salesforce_connector):
        """None should return None."""
        result = salesforce_connector._validate_scd_type(None, 'test_table')
        assert result is None

    def test_nan_returns_none(self, salesforce_connector):
        """NaN should return None."""
        result = salesforce_connector._validate_scd_type(np.nan, 'test_table')
        assert result is None

    def test_invalid_scd_type_returns_none(self, salesforce_connector):
        """Invalid SCD type should return None."""
        result = salesforce_connector._validate_scd_type('SCD_TYPE_3', 'test_table')
        assert result is None

        result = salesforce_connector._validate_scd_type('INVALID', 'test_table')
        assert result is None


class TestSubgroupValidation:
    """Tests for subgroup validation - mixed usage within a prefix."""

    def test_all_empty_subgroups_default_to_01(self, salesforce_connector):
        """All empty subgroups should default to '01'."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['sfdc_conn', 'sfdc_conn'],
            'prefix': ['sales', 'sales'],
            'subgroup': ['', ''],
            'project_name': ['test_project', 'test_project'],
        })

        result = salesforce_connector.load_and_normalize_input(df=df)

        assert all(result['subgroup'] == '01')

    def test_all_explicit_subgroups_preserved(self, salesforce_connector):
        """All explicit subgroups should be preserved."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['sfdc_conn', 'sfdc_conn'],
            'prefix': ['sales', 'sales'],
            'subgroup': ['01', '02'],
            'project_name': ['test_project', 'test_project'],
        })

        result = salesforce_connector.load_and_normalize_input(df=df)

        assert result.loc[0, 'subgroup'] == '01'
        assert result.loc[1, 'subgroup'] == '02'

    def test_mixed_subgroups_in_same_prefix_raises_error(self, salesforce_connector):
        """Mixed subgroups (some empty, some defined) in same prefix should raise error."""
        from tapworks.core import ValidationError

        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard', 'standard'],
            'source_table_name': ['Account', 'Contact', 'Opportunity'],
            'target_catalog': ['main', 'main', 'main'],
            'target_schema': ['salesforce', 'salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact', 'opportunity'],
            'connection_name': ['sfdc_conn', 'sfdc_conn', 'sfdc_conn'],
            'prefix': ['sales', 'sales', 'sales'],
            'subgroup': ['01', '', ''],  # Mixed: one explicit, two empty
            'project_name': ['test_project', 'test_project', 'test_project'],
        })

        with pytest.raises(ValidationError, match="Mixed subgroup usage"):
            salesforce_connector.load_and_normalize_input(df=df)

    def test_mixed_subgroups_different_prefixes_allowed(self, salesforce_connector):
        """Different prefixes can have different subgroup strategies."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard', 'standard'],
            'source_table_name': ['Account', 'Contact', 'Opportunity'],
            'target_catalog': ['main', 'main', 'main'],
            'target_schema': ['salesforce', 'salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact', 'opportunity'],
            'connection_name': ['sfdc_conn', 'sfdc_conn', 'sfdc_conn'],
            'prefix': ['sales', 'sales', 'marketing'],  # Different prefixes
            'subgroup': ['01', '02', ''],  # sales: all explicit, marketing: empty
            'project_name': ['test_project', 'test_project', 'test_project'],
        })

        result = salesforce_connector.load_and_normalize_input(df=df)

        # sales prefix: explicit subgroups preserved
        assert result.loc[0, 'subgroup'] == '01'
        assert result.loc[1, 'subgroup'] == '02'
        # marketing prefix: empty defaults to '01'
        assert result.loc[2, 'subgroup'] == '01'

    def test_error_message_includes_prefix_and_subgroups(self, salesforce_connector):
        """Error message should include the prefix and defined subgroups."""
        from tapworks.core import ValidationError

        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['sfdc_conn', 'sfdc_conn'],
            'prefix': ['sales', 'sales'],
            'subgroup': ['01', ''],
            'project_name': ['test_project', 'test_project'],
        })

        with pytest.raises(ValidationError) as exc_info:
            salesforce_connector.load_and_normalize_input(df=df)

        error_message = str(exc_info.value)
        assert 'sales' in error_message
        assert '01' in error_message

    def test_whitespace_subgroup_treated_as_empty(self, salesforce_connector):
        """Whitespace-only subgroups should be treated as empty."""
        from tapworks.core import ValidationError

        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['sfdc_conn', 'sfdc_conn'],
            'prefix': ['sales', 'sales'],
            'subgroup': ['01', '   '],  # Whitespace treated as empty
            'project_name': ['test_project', 'test_project'],
        })

        with pytest.raises(ValidationError, match="Mixed subgroup usage"):
            salesforce_connector.load_and_normalize_input(df=df)
