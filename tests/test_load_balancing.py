"""
Tests for load balancing functionality.

Tests the generate_pipeline_config methods for both SaaS and Database connectors,
including gateway assignment and pipeline grouping.
"""

import pytest
import pandas as pd


class TestSaaSConnectorLoadBalancing:
    """Tests for SaaS connector single-level load balancing."""

    def test_assigns_pipeline_group(self, salesforce_connector, sample_salesforce_df):
        """Should assign pipeline_group column."""
        df = salesforce_connector.load_and_normalize_input(
            df=sample_salesforce_df,
            default_values={'project_name': 'test'}
        )

        result = salesforce_connector.generate_pipeline_config(df=df)

        assert 'pipeline_group' in result.columns
        assert all(result['pipeline_group'].notna())

    def test_pipeline_group_format(self, salesforce_connector, sample_salesforce_df):
        """Pipeline group should be prefix_subgroup format."""
        df = salesforce_connector.load_and_normalize_input(
            df=sample_salesforce_df,
            default_values={'project_name': 'myproject', 'prefix': 'sales', 'subgroup': '01'}
        )

        result = salesforce_connector.generate_pipeline_config(df=df)

        assert all(result['pipeline_group'] == 'sales_01')

    def test_respects_max_tables_per_pipeline(self, salesforce_connector):
        """Should split groups that exceed max_tables_per_pipeline."""
        # Create df with 10 rows, all same group
        df = pd.DataFrame({
            'source_database': ['Salesforce'] * 10,
            'source_schema': ['standard'] * 10,
            'source_table_name': [f'Table_{i}' for i in range(10)],
            'target_catalog': ['main'] * 10,
            'target_schema': ['salesforce'] * 10,
            'target_table_name': [f'table_{i}' for i in range(10)],
            'connection_name': ['conn'] * 10,
            'project_name': ['project'] * 10,
            'prefix': ['test'] * 10,
            'subgroup': ['01'] * 10,
        })

        df = salesforce_connector.load_and_normalize_input(df=df)
        result = salesforce_connector.generate_pipeline_config(
            df=df,
            max_tables_per_pipeline=3
        )

        # Should be split into 4 groups (10 / 3 = 4 groups)
        unique_groups = result['pipeline_group'].unique()
        assert len(unique_groups) == 4

    def test_split_group_naming(self, salesforce_connector):
        """Split groups should have sequential suffixes."""
        df = pd.DataFrame({
            'source_database': ['Salesforce'] * 10,
            'source_schema': ['standard'] * 10,
            'source_table_name': [f'Table_{i}' for i in range(10)],
            'target_catalog': ['main'] * 10,
            'target_schema': ['salesforce'] * 10,
            'target_table_name': [f'table_{i}' for i in range(10)],
            'connection_name': ['conn'] * 10,
            'project_name': ['project'] * 10,
            'prefix': ['test'] * 10,
            'subgroup': ['01'] * 10,
        })

        df = salesforce_connector.load_and_normalize_input(df=df)
        result = salesforce_connector.generate_pipeline_config(
            df=df,
            max_tables_per_pipeline=3
        )

        unique_groups = sorted(result['pipeline_group'].unique())
        assert 'test_01_g01' in unique_groups
        assert 'test_01_g02' in unique_groups
        assert 'test_01_g03' in unique_groups
        assert 'test_01_g04' in unique_groups

    def test_small_group_no_split(self, salesforce_connector, sample_salesforce_df):
        """Groups smaller than max should not be split."""
        df = salesforce_connector.load_and_normalize_input(
            df=sample_salesforce_df,
            default_values={'project_name': 'test', 'prefix': 'test', 'subgroup': '01'}
        )

        result = salesforce_connector.generate_pipeline_config(
            df=df,
            max_tables_per_pipeline=250  # Much larger than our 3 rows
        )

        unique_groups = result['pipeline_group'].unique()
        assert len(unique_groups) == 1
        assert unique_groups[0] == 'test_01'

    def test_multiple_groups_handled_separately(self, salesforce_connector, multi_group_df):
        """Each prefix_subgroup combination should be handled independently."""
        df = salesforce_connector.load_and_normalize_input(df=multi_group_df)
        result = salesforce_connector.generate_pipeline_config(
            df=df,
            max_tables_per_pipeline=250
        )

        unique_groups = result['pipeline_group'].unique()
        # Should have: sales_01, sales_02, hr_01, hr_02, finance_01
        assert len(unique_groups) == 5

    def test_subgroup_zero_padded(self, salesforce_connector):
        """Subgroup should be zero-padded to 2 digits."""
        df = pd.DataFrame({
            'source_database': ['Salesforce'],
            'source_schema': ['standard'],
            'source_table_name': ['Table'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['table'],
            'connection_name': ['conn'],
            'project_name': ['test'],
            'prefix': ['test'],
            'subgroup': ['1'],  # Single digit
        })

        df = salesforce_connector.load_and_normalize_input(df=df)
        result = salesforce_connector.generate_pipeline_config(df=df)

        assert result.loc[0, 'pipeline_group'] == 'test_01'


class TestDatabaseConnectorLoadBalancing:
    """Tests for Database connector two-level load balancing."""

    def test_assigns_gateway_and_pipeline_group(self, sqlserver_connector, sample_sqlserver_df):
        """Should assign both gateway and pipeline_group columns."""
        df = sqlserver_connector.load_and_normalize_input(
            df=sample_sqlserver_df,
            default_values={'project_name': 'test'}
        )

        result = sqlserver_connector.generate_pipeline_config(df=df)

        assert 'gateway' in result.columns
        assert 'pipeline_group' in result.columns
        assert all(result['gateway'].notna())
        assert all(result['pipeline_group'].notna())

    def test_respects_max_tables_per_gateway(self, sqlserver_connector, large_df_for_load_balancing):
        """Should split into multiple gateways when exceeding limit."""
        df = sqlserver_connector.load_and_normalize_input(df=large_df_for_load_balancing)

        result = sqlserver_connector.generate_pipeline_config(
            df=df,
            max_tables_per_gateway=250,
            max_tables_per_pipeline=250
        )

        # 600 rows with max 250 per gateway = 3 gateways
        unique_gateways = result['gateway'].unique()
        assert len(unique_gateways) == 3

        # Each gateway should have at most 250 tables
        for gateway in unique_gateways:
            gateway_count = len(result[result['gateway'] == gateway])
            assert gateway_count <= 250

    def test_respects_max_tables_per_pipeline(self, sqlserver_connector, large_df_for_load_balancing):
        """Should split gateways into pipelines when exceeding limit."""
        df = sqlserver_connector.load_and_normalize_input(df=large_df_for_load_balancing)

        result = sqlserver_connector.generate_pipeline_config(
            df=df,
            max_tables_per_gateway=300,
            max_tables_per_pipeline=100
        )

        # Each pipeline should have at most 100 tables
        for pipeline_group in result['pipeline_group'].unique():
            pipeline_count = len(result[result['pipeline_group'] == pipeline_group])
            assert pipeline_count <= 100

    def test_gateway_naming_format(self, sqlserver_connector, large_df_for_load_balancing):
        """Gateway names should follow correct format when split."""
        df = sqlserver_connector.load_and_normalize_input(df=large_df_for_load_balancing)

        result = sqlserver_connector.generate_pipeline_config(
            df=df,
            max_tables_per_gateway=250,
            max_tables_per_pipeline=250
        )

        unique_gateways = sorted(result['gateway'].unique())
        # Should have suffixes like _gw01, _gw02, _gw03
        assert 'test_01_gw01' in unique_gateways
        assert 'test_01_gw02' in unique_gateways
        assert 'test_01_gw03' in unique_gateways

    def test_pipeline_naming_within_gateway(self, sqlserver_connector):
        """Pipeline names should include gateway and pipeline suffix when split."""
        # Create 100 rows - will need multiple pipelines
        df = pd.DataFrame({
            'source_database': ['DB'] * 100,
            'source_schema': ['dbo'] * 100,
            'source_table_name': [f'Table_{i}' for i in range(100)],
            'target_catalog': ['main'] * 100,
            'target_schema': ['bronze'] * 100,
            'target_table_name': [f'table_{i}' for i in range(100)],
            'connection_name': ['conn'] * 100,
            'project_name': ['project'] * 100,
            'prefix': ['test'] * 100,
            'subgroup': ['01'] * 100,
        })

        df = sqlserver_connector.load_and_normalize_input(df=df)
        result = sqlserver_connector.generate_pipeline_config(
            df=df,
            max_tables_per_gateway=100,  # All in one gateway
            max_tables_per_pipeline=30   # Split into multiple pipelines
        )

        unique_pipelines = sorted(result['pipeline_group'].unique())
        # Should have 4 pipelines (100 / 30 = 4)
        assert len(unique_pipelines) == 4

    def test_two_level_split(self, sqlserver_connector):
        """Should handle both gateway and pipeline splits correctly."""
        # 500 rows - should split into gateways, then into pipelines
        num_rows = 500
        df = pd.DataFrame({
            'source_database': ['DB'] * num_rows,
            'source_schema': ['dbo'] * num_rows,
            'source_table_name': [f'Table_{i}' for i in range(num_rows)],
            'target_catalog': ['main'] * num_rows,
            'target_schema': ['bronze'] * num_rows,
            'target_table_name': [f'table_{i}' for i in range(num_rows)],
            'connection_name': ['conn'] * num_rows,
            'project_name': ['project'] * num_rows,
            'prefix': ['test'] * num_rows,
            'subgroup': ['01'] * num_rows,
        })

        df = sqlserver_connector.load_and_normalize_input(df=df)
        result = sqlserver_connector.generate_pipeline_config(
            df=df,
            max_tables_per_gateway=200,  # 3 gateways
            max_tables_per_pipeline=75   # Multiple pipelines per gateway
        )

        # Verify gateway split
        unique_gateways = result['gateway'].unique()
        assert len(unique_gateways) == 3

        # Verify pipeline split within each gateway
        for gateway in unique_gateways:
            gateway_df = result[result['gateway'] == gateway]
            for pipeline in gateway_df['pipeline_group'].unique():
                pipeline_count = len(result[result['pipeline_group'] == pipeline])
                assert pipeline_count <= 75

    def test_small_dataset_no_split(self, sqlserver_connector, sample_sqlserver_df):
        """Small datasets should not be split."""
        df = sqlserver_connector.load_and_normalize_input(
            df=sample_sqlserver_df,
            default_values={'project_name': 'test', 'prefix': 'test', 'subgroup': '01'}
        )

        result = sqlserver_connector.generate_pipeline_config(
            df=df,
            max_tables_per_gateway=250,
            max_tables_per_pipeline=250
        )

        assert len(result['gateway'].unique()) == 1
        assert len(result['pipeline_group'].unique()) == 1
        assert result['gateway'].iloc[0] == 'test_01'
        assert result['pipeline_group'].iloc[0] == 'test_01'


class TestResourceNaming:
    """Tests for resource name generation."""

    def test_generate_resource_names_structure(self, salesforce_connector):
        """Should return dictionary with all required keys."""
        result = salesforce_connector._generate_resource_names('test_01')

        assert 'pipeline_name' in result
        assert 'pipeline_resource_name' in result
        assert 'job_name' in result
        assert 'job_display_name' in result
        assert 'task_key' in result

    def test_pipeline_name_format(self, salesforce_connector):
        """Pipeline name should match pipeline_group."""
        result = salesforce_connector._generate_resource_names('sales_01')

        assert result['pipeline_name'] == 'sales_01'

    def test_pipeline_resource_name_format(self, salesforce_connector):
        """Pipeline resource name should be valid identifier."""
        result = salesforce_connector._generate_resource_names('sales_01')

        assert result['pipeline_resource_name'] == 'pipeline_sales_01'

    def test_job_name_format(self, salesforce_connector):
        """Job name should be valid identifier."""
        result = salesforce_connector._generate_resource_names('sales_01')

        assert result['job_name'] == 'job_sales_01'

    def test_job_display_name_format(self, salesforce_connector):
        """Job display name should be pipeline_group_scheduler."""
        result = salesforce_connector._generate_resource_names('sales_01')

        assert result['job_display_name'] == 'sales_01_scheduler'


class TestSplitGroupsBySize:
    """Tests for the _split_groups_by_size helper method."""

    def test_no_split_when_under_limit(self, salesforce_connector):
        """Groups under limit should not be split."""
        df = pd.DataFrame({
            'group_col': ['A', 'A', 'A', 'B', 'B'],
            'data': [1, 2, 3, 4, 5]
        })

        result = salesforce_connector._split_groups_by_size(
            df=df,
            group_column='group_col',
            max_size=10,
            output_column='result',
            suffix='g'
        )

        assert list(result[result['group_col'] == 'A']['result'].unique()) == ['A']
        assert list(result[result['group_col'] == 'B']['result'].unique()) == ['B']

    def test_split_when_over_limit(self, salesforce_connector):
        """Groups over limit should be split."""
        df = pd.DataFrame({
            'group_col': ['A'] * 10,
            'data': list(range(10))
        })

        result = salesforce_connector._split_groups_by_size(
            df=df,
            group_column='group_col',
            max_size=3,
            output_column='result',
            suffix='g'
        )

        unique_results = result['result'].unique()
        assert len(unique_results) == 4  # ceil(10/3) = 4

    def test_split_suffix_format(self, salesforce_connector):
        """Split groups should have correct suffix format."""
        df = pd.DataFrame({
            'group_col': ['A'] * 10,
            'data': list(range(10))
        })

        result = salesforce_connector._split_groups_by_size(
            df=df,
            group_column='group_col',
            max_size=3,
            output_column='result',
            suffix='chunk'
        )

        unique_results = sorted(result['result'].unique())
        assert 'A_chunk01' in unique_results
        assert 'A_chunk02' in unique_results

    def test_exact_split_boundary(self, salesforce_connector):
        """Exact multiples should not create extra group."""
        df = pd.DataFrame({
            'group_col': ['A'] * 9,
            'data': list(range(9))
        })

        result = salesforce_connector._split_groups_by_size(
            df=df,
            group_column='group_col',
            max_size=3,
            output_column='result',
            suffix='g'
        )

        unique_results = result['result'].unique()
        assert len(unique_results) == 3  # 9/3 = exactly 3

    def test_preserves_row_order(self, salesforce_connector):
        """Split should preserve original row order within groups."""
        df = pd.DataFrame({
            'group_col': ['A'] * 6,
            'data': [1, 2, 3, 4, 5, 6]
        })

        result = salesforce_connector._split_groups_by_size(
            df=df,
            group_column='group_col',
            max_size=2,
            output_column='result',
            suffix='g'
        )

        # First chunk should have rows with data 1, 2
        chunk1 = result[result['result'] == 'A_g01']
        assert list(chunk1['data']) == [1, 2]

        # Second chunk should have rows with data 3, 4
        chunk2 = result[result['result'] == 'A_g02']
        assert list(chunk2['data']) == [3, 4]
