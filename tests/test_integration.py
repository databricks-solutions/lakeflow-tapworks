"""
Integration tests for end-to-end pipeline generation.

Tests the complete workflow from input CSV to generated DAB files.
"""

import pytest
import pandas as pd
import yaml
import os
from pathlib import Path
from tapworks.core import ConfigurationError


class TestSalesforceEndToEnd:
    """End-to-end tests for Salesforce connector."""

    def test_complete_pipeline_generation(
        self,
        salesforce_connector,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should generate complete DAB structure."""
        result_df = salesforce_connector.run_complete_pipeline_generation(
            df=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'sfdc_test', 'schedule': '*/15 * * * *'}
        )

        # Check result dataframe has required columns
        assert 'pipeline_group' in result_df.columns
        assert len(result_df) == len(sample_salesforce_df)

        # Check output files exist
        project_dir = temp_output_dir / 'sfdc_test'
        assert project_dir.exists()
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()
        assert (project_dir / 'resources' / 'jobs.yml').exists()

    def test_databricks_yml_content(
        self,
        salesforce_connector,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """databricks.yml should have correct content."""
        salesforce_connector.run_complete_pipeline_generation(
            df=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'sfdc_test'}
        )

        with open(temp_output_dir / 'sfdc_test' / 'databricks.yml') as f:
            content = yaml.safe_load(f)

        assert content['bundle']['name'] == 'sfdc_test'
        assert 'dev' in content['targets']
        assert content['targets']['dev']['workspace']['host'] == sample_targets_minimal['dev']['workspace_host']

    def test_pipelines_yml_content(
        self,
        salesforce_connector,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """pipelines.yml should have correct content."""
        salesforce_connector.run_complete_pipeline_generation(
            df=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'sfdc_test'}
        )

        with open(temp_output_dir / 'sfdc_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        assert 'resources' in content
        assert 'pipelines' in content['resources']
        # Should have one pipeline (all 3 tables in same group)
        assert len(content['resources']['pipelines']) >= 1

    def test_jobs_yml_content(
        self,
        salesforce_connector,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """jobs.yml should have correct content."""
        salesforce_connector.run_complete_pipeline_generation(
            df=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'sfdc_test', 'schedule': '*/15 * * * *'}
        )

        with open(temp_output_dir / 'sfdc_test' / 'resources' / 'jobs.yml') as f:
            content = yaml.safe_load(f)

        assert 'resources' in content
        assert 'jobs' in content['resources']
        # Should have jobs with Quartz cron format
        for job_name, job_config in content['resources']['jobs'].items():
            assert 'schedule' in job_config
            assert 'quartz_cron_expression' in job_config['schedule']


class TestSQLServerEndToEnd:
    """End-to-end tests for SQL Server connector."""

    def test_complete_pipeline_generation_with_gateways(
        self,
        sqlserver_connector,
        sample_sqlserver_df_with_gateway,
        sample_targets,
        temp_output_dir
    ):
        """Should generate complete DAB structure with gateways."""
        result_df = sqlserver_connector.run_complete_pipeline_generation(
            df=sample_sqlserver_df_with_gateway,
            output_dir=str(temp_output_dir),
            targets=sample_targets,
            default_values={'project_name': 'sqlserver_test', 'schedule': '*/15 * * * *'}
        )

        # Check result dataframe has required columns
        assert 'pipeline_group' in result_df.columns
        assert 'gateway' in result_df.columns

        # Check output files exist
        project_dir = temp_output_dir / 'sqlserver_test'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'gateways.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()
        assert (project_dir / 'resources' / 'jobs.yml').exists()

    def test_gateways_yml_content(
        self,
        sqlserver_connector,
        sample_sqlserver_df_with_gateway,
        sample_targets,
        temp_output_dir
    ):
        """gateways.yml should have correct content."""
        sqlserver_connector.run_complete_pipeline_generation(
            df=sample_sqlserver_df_with_gateway,
            output_dir=str(temp_output_dir),
            targets=sample_targets,
            default_values={'project_name': 'sqlserver_test'}
        )

        with open(temp_output_dir / 'sqlserver_test' / 'resources' / 'gateways.yml') as f:
            content = yaml.safe_load(f)

        assert 'resources' in content
        assert 'pipelines' in content['resources']  # Gateways are a type of pipeline

        # Check gateway structure
        for gateway_name, gateway_config in content['resources']['pipelines'].items():
            assert 'gateway_definition' in gateway_config
            assert 'connection_name' in gateway_config['gateway_definition']
            assert gateway_config['continuous'] is True


class TestMultipleProjects:
    """Tests for multiple project_name handling."""

    def test_creates_separate_dab_per_project(
        self,
        salesforce_connector,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should create separate DAB folders for each project_name."""
        df = pd.DataFrame({
            'source_database': ['Salesforce'] * 4,
            'source_schema': ['standard'] * 4,
            'source_table_name': ['Account', 'Contact', 'Lead', 'Opportunity'],
            'target_catalog': ['main'] * 4,
            'target_schema': ['salesforce'] * 4,
            'target_table_name': ['account', 'contact', 'lead', 'opportunity'],
            'connection_name': ['sfdc_conn'] * 4,
            'project_name': ['project_a', 'project_a', 'project_b', 'project_b'],
        })

        salesforce_connector.run_complete_pipeline_generation(
            df=df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'schedule': '*/15 * * * *'}
        )

        # Check both project folders exist
        assert (temp_output_dir / 'project_a').exists()
        assert (temp_output_dir / 'project_b').exists()

        # Check each has full structure
        for project in ['project_a', 'project_b']:
            project_dir = temp_output_dir / project
            assert (project_dir / 'databricks.yml').exists()
            assert (project_dir / 'resources' / 'pipelines.yml').exists()


class TestLoadBalancingIntegration:
    """Integration tests for load balancing with file output."""

    def test_large_dataset_splits_correctly(
        self,
        salesforce_connector,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Large dataset should be split into multiple pipelines."""
        # Create 300 tables
        df = pd.DataFrame({
            'source_database': ['Salesforce'] * 300,
            'source_schema': ['standard'] * 300,
            'source_table_name': [f'Table_{i}' for i in range(300)],
            'target_catalog': ['main'] * 300,
            'target_schema': ['salesforce'] * 300,
            'target_table_name': [f'table_{i}' for i in range(300)],
            'connection_name': ['sfdc_conn'] * 300,
            'project_name': ['large_project'] * 300,
            'prefix': ['bulk'] * 300,
            'subgroup': ['01'] * 300,
        })

        result_df = salesforce_connector.run_complete_pipeline_generation(
            df=df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'schedule': '*/15 * * * *'},
            max_tables_per_pipeline=100
        )

        # Should have 3 pipelines (300 / 100)
        assert result_df['pipeline_group'].nunique() == 3

        # Check pipelines.yml has 3 pipelines
        with open(temp_output_dir / 'large_project' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        assert len(content['resources']['pipelines']) == 3


class TestOverrideIntegration:
    """Integration tests for override functionality."""

    def test_override_applied_to_all_rows(
        self,
        salesforce_connector,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Override should be applied to all rows in output."""
        result_df = salesforce_connector.run_complete_pipeline_generation(
            df=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'test'},
            override_input_config={'schedule': '0 0 * * *'}
        )

        # All rows should have the overridden schedule
        assert all(result_df['schedule'] == '0 0 * * *')


class TestConfigSaving:
    """Tests for intermediate configuration saving."""

    def test_saves_generated_config_csv(
        self,
        salesforce_connector,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should save intermediate configuration to CSV."""
        output_config = str(temp_output_dir / 'config.csv')

        salesforce_connector.run_complete_pipeline_generation(
            df=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            output_config=output_config,
            default_values={'project_name': 'test'}
        )

        assert os.path.exists(output_config)

        # Load and verify
        saved_df = pd.read_csv(output_config)
        assert 'pipeline_group' in saved_df.columns
        assert len(saved_df) == len(sample_salesforce_df)


class TestTargetEnvironments:
    """Tests for multiple target environment support."""

    def test_multiple_targets_in_databricks_yml(
        self,
        salesforce_connector,
        sample_salesforce_df,
        sample_targets,
        temp_output_dir
    ):
        """databricks.yml should include all target environments."""
        salesforce_connector.run_complete_pipeline_generation(
            df=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets,
            default_values={'project_name': 'multi_env'}
        )

        with open(temp_output_dir / 'multi_env' / 'databricks.yml') as f:
            content = yaml.safe_load(f)

        assert 'dev' in content['targets']
        assert 'prod' in content['targets']
        assert content['targets']['dev']['mode'] == 'development'
        assert content['targets']['prod']['mode'] == 'production'


class TestWorkdayReportsEndToEnd:
    """End-to-end tests for Workday Reports connector."""

    def test_complete_pipeline_generation(
        self,
        workday_connector,
        sample_workday_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should generate complete DAB structure for Workday reports."""
        result_df = workday_connector.run_complete_pipeline_generation(
            df=sample_workday_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'workday_test', 'schedule': '0 */6 * * *'}
        )

        # Check output files exist
        project_dir = temp_output_dir / 'workday_test'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()
        assert (project_dir / 'resources' / 'jobs.yml').exists()

    def test_report_structure_in_pipelines_yml(
        self,
        workday_connector,
        sample_workday_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Workday should use 'report' key instead of 'table'."""
        workday_connector.run_complete_pipeline_generation(
            df=sample_workday_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'workday_test'}
        )

        with open(temp_output_dir / 'workday_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        # Check that objects use 'report' key
        for pipeline_name, pipeline_config in content['resources']['pipelines'].items():
            for obj in pipeline_config['ingestion_definition']['objects']:
                assert 'report' in obj
                assert 'source_url' in obj['report']


class TestGA4EndToEnd:
    """End-to-end tests for Google Analytics 4 connector."""

    def test_complete_pipeline_generation(
        self,
        ga4_connector,
        sample_ga4_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should generate complete DAB structure for GA4."""
        result_df = ga4_connector.run_complete_pipeline_generation(
            df=sample_ga4_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'ga4_test', 'schedule': '0 */6 * * *'}
        )

        # Check output files exist
        project_dir = temp_output_dir / 'ga4_test'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()


class TestErrorHandling:
    """Tests for error handling in end-to-end scenarios."""

    def test_missing_required_columns_error(
        self,
        salesforce_connector,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should raise ConfigurationError for missing required columns."""
        incomplete_df = pd.DataFrame({
            'source_database': ['Salesforce'],
            # Missing many required columns
        })

        with pytest.raises(ConfigurationError, match="Missing required columns"):
            salesforce_connector.run_complete_pipeline_generation(
                df=incomplete_df,
                output_dir=str(temp_output_dir),
                targets=sample_targets_minimal
            )

    def test_empty_dataframe_error(
        self,
        salesforce_connector,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should raise ConfigurationError for empty DataFrame."""
        empty_df = pd.DataFrame()

        with pytest.raises(ConfigurationError, match="empty"):
            salesforce_connector.run_complete_pipeline_generation(
                df=empty_df,
                output_dir=str(temp_output_dir),
                targets=sample_targets_minimal
            )

    def test_missing_targets_error(
        self,
        salesforce_connector,
        sample_salesforce_df,
        temp_output_dir
    ):
        """Should raise ConfigurationError when targets is empty."""
        with pytest.raises(ConfigurationError, match="target"):
            salesforce_connector.run_complete_pipeline_generation(
                df=sample_salesforce_df,
                output_dir=str(temp_output_dir),
                targets={},
                default_values={'project_name': 'test_project'}
            )
