"""
Tests for individual connector implementations.

Tests ServiceNow, Workday Reports, Google Analytics, PostgreSQL, and Oracle connectors
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

        with pytest.raises(ValidationError, match="primary_keys.*empty"):
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


class TestOracleConnector:
    """Tests for Oracle integrated CDC connector."""

    def _generate(self, connector, df, targets, output_dir, **kwargs):
        connector.run_complete_pipeline_generation(
            df=df,
            output_dir=str(output_dir),
            targets=targets,
            default_values={'project_name': 'oracle_test'},
            **kwargs,
        )
        with open(output_dir / 'oracle_test' / 'resources' / 'pipelines.yml') as f:
            return yaml.safe_load(f)['resources']['pipelines']

    def test_end_to_end_without_gateways(self, oracle_connector, sample_oracle_df, sample_targets_minimal, temp_output_dir):
        result = oracle_connector.run_complete_pipeline_generation(
            df=sample_oracle_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'oracle_test'},
        )

        assert 'pipeline_group' in result.columns
        assert 'gateway' not in result.columns
        assert len(result) == 3

        project_dir = temp_output_dir / 'oracle_test'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()
        assert (project_dir / 'resources' / 'jobs.yml').exists()
        assert not (project_dir / 'resources' / 'gateways.yml').exists()

    def test_pipeline_uses_integrated_cdc(self, oracle_connector, sample_oracle_df, sample_targets_minimal, temp_output_dir):
        pipelines = self._generate(oracle_connector, sample_oracle_df, sample_targets_minimal, temp_output_dir)

        pipeline = pipelines['pipeline_oracle_test_p01']
        assert pipeline['channel'] == 'PREVIEW'
        assert pipeline['catalog'] == 'main'
        assert pipeline['schema'] == 'bronze'

        ingestion = pipeline['ingestion_definition']
        assert ingestion['connection_name'] == 'oracle_conn'
        assert ingestion['connector_type'] == 'CDC'
        assert 'ingestion_gateway_id' not in ingestion

        table = ingestion['objects'][0]['table']
        assert table['source_catalog'] == 'ORCLPDB1'
        assert table['source_schema'] == 'HR'
        assert table['source_table'] == 'EMPLOYEES'

    def test_splits_into_pipelines_only(self, oracle_connector, large_df_for_load_balancing):
        df = oracle_connector.load_and_normalize_input(large_df_for_load_balancing)
        result = oracle_connector.generate_pipeline_config(df, max_tables_per_pipeline=250)

        assert sorted(result['pipeline_group'].unique()) == ['test_01_p01', 'test_01_p02', 'test_01_p03']

    def test_conflicting_connection_names_in_pipeline(self, oracle_connector, sample_oracle_df):
        df = sample_oracle_df.copy()
        df.loc[0, 'connection_name'] = 'other_conn'
        df = oracle_connector.load_and_normalize_input(df, default_values={'project_name': 'oracle_test'})

        with pytest.raises(ValidationError, match='connection_name'):
            oracle_connector.generate_pipeline_config(df)

    def test_staging_defaults_to_target(self, oracle_connector, sample_oracle_df, sample_targets_minimal, temp_output_dir):
        pipelines = self._generate(oracle_connector, sample_oracle_df, sample_targets_minimal, temp_output_dir)

        staging = pipelines['pipeline_oracle_test_p01']['ingestion_definition']['data_staging_options']
        assert staging == {'catalog_name': 'main', 'schema_name': 'bronze'}

    def test_staging_from_columns(self, oracle_connector, sample_oracle_df, sample_targets_minimal, temp_output_dir):
        df = sample_oracle_df.copy()
        df['staging_catalog'] = 'staging_cat'
        df['staging_schema'] = 'staging_sch'
        pipelines = self._generate(oracle_connector, df, sample_targets_minimal, temp_output_dir)

        staging = pipelines['pipeline_oracle_test_p01']['ingestion_definition']['data_staging_options']
        assert staging == {'catalog_name': 'staging_cat', 'schema_name': 'staging_sch'}

    def test_conflicting_staging_in_pipeline(self, oracle_connector, sample_oracle_df):
        df = sample_oracle_df.copy()
        df['staging_schema'] = ['staging_a', 'staging_b', 'staging_a']
        df = oracle_connector.load_and_normalize_input(df, default_values={'project_name': 'oracle_test'})

        with pytest.raises(ValidationError, match='staging_schema'):
            oracle_connector.generate_pipeline_config(df)

    def test_invalid_staging_name(self, oracle_connector, sample_oracle_df):
        df = sample_oracle_df.copy()
        df['staging_schema'] = 'bad.name'

        with pytest.raises(ValidationError, match="Invalid characters in 'staging_schema'"):
            oracle_connector.load_and_normalize_input(df, default_values={'project_name': 'oracle_test'})

    def test_scd_type(self, oracle_connector, sample_oracle_df, sample_targets_minimal, temp_output_dir):
        df = sample_oracle_df.copy()
        df['scd_type'] = 'SCD_TYPE_2'
        pipelines = self._generate(oracle_connector, df, sample_targets_minimal, temp_output_dir)

        table = pipelines['pipeline_oracle_test_p01']['ingestion_definition']['objects'][0]['table']
        assert table['table_configuration']['scd_type'] == 'SCD_TYPE_2'

    def test_default_schedule_is_hourly(self, oracle_connector, sample_oracle_df):
        df = oracle_connector.load_and_normalize_input(sample_oracle_df, default_values={'project_name': 'oracle_test'})
        assert set(df['schedule']) == {'0 * * * *'}

    def test_warns_on_lowercase_identifiers(self, oracle_connector, sample_oracle_df, caplog):
        df = sample_oracle_df.copy()
        df.loc[0, 'source_table_name'] = 'employees'

        with caplog.at_level('WARNING'):
            oracle_connector.load_and_normalize_input(df, default_values={'project_name': 'oracle_test'})

        assert "source_table_name has values with lowercase letters: ['employees']" in caplog.text

    def test_no_warning_for_uppercase_identifiers(self, oracle_connector, sample_oracle_df, caplog):
        with caplog.at_level('WARNING'):
            oracle_connector.load_and_normalize_input(sample_oracle_df, default_values={'project_name': 'oracle_test'})

        assert 'lowercase' not in caplog.text

    def test_connector_type(self, oracle_connector):
        assert oracle_connector.connector_type == 'oracle_integrated'


class TestOracleQueryBasedConnector:
    """Tests for Oracle query-based connector."""

    def _generate(self, connector, df, targets, output_dir):
        connector.run_complete_pipeline_generation(
            df=df,
            output_dir=str(output_dir),
            targets=targets,
            default_values={'project_name': 'oracle_qb_test'},
        )
        with open(output_dir / 'oracle_qb_test' / 'resources' / 'pipelines.yml') as f:
            return yaml.safe_load(f)['resources']['pipelines']

    def test_end_to_end_without_gateways(self, oracle_query_based_connector, sample_oracle_query_based_df, sample_targets_minimal, temp_output_dir):
        result = oracle_query_based_connector.run_complete_pipeline_generation(
            df=sample_oracle_query_based_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'oracle_qb_test'},
        )

        assert 'gateway' not in result.columns
        project_dir = temp_output_dir / 'oracle_qb_test'
        assert (project_dir / 'resources' / 'pipelines.yml').exists()
        assert (project_dir / 'resources' / 'jobs.yml').exists()
        assert not (project_dir / 'resources' / 'gateways.yml').exists()

    def test_pipeline_uses_query_based(self, oracle_query_based_connector, sample_oracle_query_based_df, sample_targets_minimal, temp_output_dir):
        pipelines = self._generate(oracle_query_based_connector, sample_oracle_query_based_df, sample_targets_minimal, temp_output_dir)

        pipeline = pipelines['pipeline_oracle_qb_test_p01']
        assert 'channel' not in pipeline
        ingestion = pipeline['ingestion_definition']
        assert ingestion['connection_name'] == 'oracle_conn'
        assert ingestion['connector_type'] == 'QUERY_BASED'
        assert 'data_staging_options' not in ingestion
        assert 'ingestion_gateway_id' not in ingestion

        table_config = ingestion['objects'][0]['table']['table_configuration']
        assert table_config['query_based_connector_config'] == {'cursor_columns': ['UPDATED_AT']}

    def test_optional_table_options(self, oracle_query_based_connector, sample_oracle_query_based_df, sample_targets_minimal, temp_output_dir):
        df = sample_oracle_query_based_df.copy()
        df['cursor_columns'] = 'UPDATED_AT, ROW_ID'
        df['primary_keys'] = 'ID, REGION'
        df['deletion_condition'] = 'IS_DELETED = 1'
        df['scd_type'] = 'APPEND_ONLY'
        pipelines = self._generate(oracle_query_based_connector, df, sample_targets_minimal, temp_output_dir)

        table_config = pipelines['pipeline_oracle_qb_test_p01']['ingestion_definition']['objects'][0]['table']['table_configuration']
        assert table_config['scd_type'] == 'APPEND_ONLY'
        assert table_config['primary_keys'] == ['ID', 'REGION']
        assert table_config['query_based_connector_config'] == {
            'cursor_columns': ['UPDATED_AT', 'ROW_ID'],
            'deletion_condition': 'IS_DELETED = 1',
        }

    def test_cursor_columns_required(self, oracle_query_based_connector, sample_oracle_df):
        with pytest.raises(ValidationError, match='cursor_columns'):
            oracle_query_based_connector.load_and_normalize_input(sample_oracle_df, default_values={'project_name': 'oracle_qb_test'})

    def test_splits_into_pipelines_only(self, oracle_query_based_connector, large_df_for_load_balancing):
        df = large_df_for_load_balancing.copy()
        df['cursor_columns'] = 'UPDATED_AT'
        df = oracle_query_based_connector.load_and_normalize_input(df)
        result = oracle_query_based_connector.generate_pipeline_config(df, max_tables_per_pipeline=250)

        assert sorted(result['pipeline_group'].unique()) == ['test_01_p01', 'test_01_p02', 'test_01_p03']

    def test_shares_oracle_source_rules(self, oracle_query_based_connector, sample_oracle_query_based_df, caplog):
        df = sample_oracle_query_based_df.copy()
        df.loc[0, 'source_table_name'] = 'employees'

        with caplog.at_level('WARNING'):
            oracle_query_based_connector.load_and_normalize_input(df, default_values={'project_name': 'oracle_qb_test'})

        assert "source_table_name has values with lowercase letters: ['employees']" in caplog.text

    def test_default_schedule_is_hourly(self, oracle_query_based_connector, sample_oracle_query_based_df):
        df = oracle_query_based_connector.load_and_normalize_input(sample_oracle_query_based_df, default_values={'project_name': 'oracle_qb_test'})
        assert set(df['schedule']) == {'0 * * * *'}

    def test_connector_type(self, oracle_query_based_connector):
        assert oracle_query_based_connector.connector_type == 'oracle_query_based'
