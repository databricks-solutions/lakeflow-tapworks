"""
Tests for individual connector implementations.

Tests ServiceNow, Workday Reports, Google Analytics, and PostgreSQL connectors
with end-to-end pipeline generation and YAML output validation.
"""

import pytest
import yaml
from tapworks.core import ValidationError


class TestServiceNowConnector:
    """Tests for ServiceNow connector."""

    def test_end_to_end(self, servicenow_connector, sample_servicenow_df, sample_targets_minimal, temp_output_dir):
        result = servicenow_connector.run_complete_pipeline_generation(
            df=sample_servicenow_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'snow_test'},
        )

        assert 'pipeline_group' in result.columns
        assert len(result) == 3

        project_dir = temp_output_dir / 'snow_test'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()
        assert (project_dir / 'resources' / 'jobs.yml').exists()

    def test_pipeline_yaml_content(self, servicenow_connector, sample_servicenow_df, sample_targets_minimal, temp_output_dir):
        servicenow_connector.run_complete_pipeline_generation(
            df=sample_servicenow_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'snow_test'},
        )

        with open(temp_output_dir / 'snow_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        pipelines = content['resources']['pipelines']
        assert len(pipelines) == 1

        pipeline = list(pipelines.values())[0]
        assert pipeline['catalog'] == 'main'
        assert pipeline['schema'] == 'servicenow'
        assert 'connection_name' in pipeline['ingestion_definition']
        assert len(pipeline['ingestion_definition']['objects']) == 3

    def test_table_entry_structure(self, servicenow_connector, sample_servicenow_df, sample_targets_minimal, temp_output_dir):
        servicenow_connector.run_complete_pipeline_generation(
            df=sample_servicenow_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'snow_test'},
        )

        with open(temp_output_dir / 'snow_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        pipeline = list(content['resources']['pipelines'].values())[0]
        table = pipeline['ingestion_definition']['objects'][0]['table']
        assert 'source_schema' in table
        assert 'source_table' in table
        assert 'destination_catalog' in table
        assert 'destination_schema' in table
        assert 'destination_table' in table

    def test_connector_type(self, servicenow_connector):
        assert servicenow_connector.connector_type == 'servicenow'

    def test_required_columns(self, servicenow_connector):
        assert 'connection_name' in servicenow_connector.required_columns
        assert 'source_table_name' in servicenow_connector.required_columns


class TestWorkdayReportsConnector:
    """Tests for Workday Reports connector."""

    def test_end_to_end(self, workday_connector, sample_workday_df, sample_targets_minimal, temp_output_dir):
        result = workday_connector.run_complete_pipeline_generation(
            df=sample_workday_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'wd_test'},
        )

        assert 'pipeline_group' in result.columns
        assert len(result) == 2

        project_dir = temp_output_dir / 'wd_test'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()
        assert (project_dir / 'resources' / 'jobs.yml').exists()

    def test_pipeline_yaml_has_reports(self, workday_connector, sample_workday_df, sample_targets_minimal, temp_output_dir):
        workday_connector.run_complete_pipeline_generation(
            df=sample_workday_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'wd_test'},
        )

        with open(temp_output_dir / 'wd_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        pipeline = list(content['resources']['pipelines'].values())[0]
        objects = pipeline['ingestion_definition']['objects']
        assert len(objects) == 2

        # Workday uses 'report' not 'table'
        report = objects[0]['report']
        assert 'source_url' in report
        assert 'destination_table' in report

    def test_primary_keys_in_output(self, workday_connector, sample_workday_df, sample_targets_minimal, temp_output_dir):
        workday_connector.run_complete_pipeline_generation(
            df=sample_workday_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'wd_test'},
        )

        with open(temp_output_dir / 'wd_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        pipeline = list(content['resources']['pipelines'].values())[0]
        report = pipeline['ingestion_definition']['objects'][0]['report']
        assert 'table_configuration' in report
        assert 'primary_keys' in report['table_configuration']

    def test_missing_primary_keys_raises_validation_error(self, workday_connector, sample_targets_minimal, temp_output_dir):
        import pandas as pd
        df = pd.DataFrame({
            'source_url': ['https://wd2.workday.com/report1'],
            'target_catalog': ['main'],
            'target_schema': ['workday'],
            'target_table_name': ['employees'],
            'connection_name': ['wd_conn'],
            'primary_keys': [''],
        })

        with pytest.raises(ValidationError, match="Missing required primary_keys"):
            workday_connector.run_complete_pipeline_generation(
                df=df,
                output_dir=str(temp_output_dir),
                targets=sample_targets_minimal,
                default_values={'project_name': 'wd_test'},
            )

    def test_connector_type(self, workday_connector):
        assert workday_connector.connector_type == 'workday_reports'


class TestGoogleAnalyticsConnector:
    """Tests for Google Analytics 4 connector."""

    def test_end_to_end(self, ga4_connector, sample_ga4_df, sample_targets_minimal, temp_output_dir):
        result = ga4_connector.run_complete_pipeline_generation(
            df=sample_ga4_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'ga4_test'},
        )

        assert 'pipeline_group' in result.columns
        assert len(result) == 2

        project_dir = temp_output_dir / 'ga4_test'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()

    def test_tables_expanded_in_output(self, ga4_connector, sample_ga4_df, sample_targets_minimal, temp_output_dir):
        ga4_connector.run_complete_pipeline_generation(
            df=sample_ga4_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'ga4_test'},
        )

        with open(temp_output_dir / 'ga4_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        pipeline = list(content['resources']['pipelines'].values())[0]
        objects = pipeline['ingestion_definition']['objects']
        # 2 properties with 2 tables each = 4 table entries
        assert len(objects) == 4

    def test_destination_table_format(self, ga4_connector, sample_ga4_df, sample_targets_minimal, temp_output_dir):
        ga4_connector.run_complete_pipeline_generation(
            df=sample_ga4_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'ga4_test'},
        )

        with open(temp_output_dir / 'ga4_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        pipeline = list(content['resources']['pipelines'].values())[0]
        table = pipeline['ingestion_definition']['objects'][0]['table']
        # GA4 destination_table format is {source_schema}_{table}
        assert 'analytics_' in table['destination_table']

    def test_connector_type(self, ga4_connector):
        assert ga4_connector.connector_type == 'ga4'


class TestPostgreSQLConnector:
    """Tests for PostgreSQL connector."""

    def test_end_to_end(self, postgres_connector, sample_postgresql_df, sample_targets_minimal, temp_output_dir):
        result = postgres_connector.run_complete_pipeline_generation(
            df=sample_postgresql_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'pg_test', 'schedule': '*/15 * * * *'},
        )

        assert 'pipeline_group' in result.columns
        assert 'gateway' in result.columns
        assert len(result) == 3

        project_dir = temp_output_dir / 'pg_test'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'gateways.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()
        assert (project_dir / 'resources' / 'jobs.yml').exists()

    def test_gateway_yaml_content(self, postgres_connector, sample_postgresql_df, sample_targets_minimal, temp_output_dir):
        postgres_connector.run_complete_pipeline_generation(
            df=sample_postgresql_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'pg_test'},
        )

        with open(temp_output_dir / 'pg_test' / 'resources' / 'gateways.yml') as f:
            content = yaml.safe_load(f)

        gateways = content['resources']['pipelines']
        assert len(gateways) == 1

        gateway = list(gateways.values())[0]
        assert 'gateway_definition' in gateway
        assert gateway['gateway_definition']['connection_name'] == 'pg_conn'
        assert gateway['continuous'] is True

    def test_pipeline_references_gateway(self, postgres_connector, sample_postgresql_df, sample_targets_minimal, temp_output_dir):
        postgres_connector.run_complete_pipeline_generation(
            df=sample_postgresql_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'pg_test'},
        )

        with open(temp_output_dir / 'pg_test' / 'resources' / 'pipelines.yml') as f:
            content = yaml.safe_load(f)

        pipeline = list(content['resources']['pipelines'].values())[0]
        gateway_ref = pipeline['ingestion_definition']['ingestion_gateway_id']
        assert '${resources.pipelines.gateway_' in gateway_ref

    def test_connector_type(self, postgres_connector):
        assert postgres_connector.connector_type == 'postgresql'
