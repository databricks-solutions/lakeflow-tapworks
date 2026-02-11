"""
Tests for YAML generation functionality.

Tests the _create_databricks_yml, _create_jobs, _create_pipelines, and
_create_gateways methods that generate DAB YAML structures.
"""

import pytest
import pandas as pd
import numpy as np
from core import ConfigurationError


class TestCreateDatabricksYml:
    """Tests for _create_databricks_yml method."""

    def test_basic_structure(self, salesforce_connector, sample_targets):
        """Should create correct basic structure."""
        result = salesforce_connector._create_databricks_yml(
            project_name='test_project',
            targets=sample_targets,
            default_target='dev'
        )

        assert 'bundle' in result
        assert 'include' in result
        assert 'targets' in result

    def test_bundle_name(self, salesforce_connector, sample_targets):
        """Bundle name should match project_name."""
        result = salesforce_connector._create_databricks_yml(
            project_name='my_project',
            targets=sample_targets,
            default_target='dev'
        )

        assert result['bundle']['name'] == 'my_project'

    def test_includes_resources(self, salesforce_connector, sample_targets):
        """Should include resources/*.yml."""
        result = salesforce_connector._create_databricks_yml(
            project_name='test',
            targets=sample_targets,
            default_target='dev'
        )

        assert 'resources/*.yml' in result['include']

    def test_all_targets_included(self, salesforce_connector, sample_targets):
        """All provided targets should be included."""
        result = salesforce_connector._create_databricks_yml(
            project_name='test',
            targets=sample_targets,
            default_target='dev'
        )

        assert 'dev' in result['targets']
        assert 'prod' in result['targets']

    def test_target_workspace_host(self, salesforce_connector, sample_targets):
        """Each target should have workspace host."""
        result = salesforce_connector._create_databricks_yml(
            project_name='test',
            targets=sample_targets,
            default_target='dev'
        )

        assert result['targets']['dev']['workspace']['host'] == sample_targets['dev']['workspace_host']
        assert result['targets']['prod']['workspace']['host'] == sample_targets['prod']['workspace_host']

    def test_target_root_path(self, salesforce_connector, sample_targets):
        """Root path should be included when provided."""
        result = salesforce_connector._create_databricks_yml(
            project_name='test',
            targets=sample_targets,
            default_target='dev'
        )

        assert result['targets']['dev']['workspace']['root_path'] == sample_targets['dev']['root_path']

    def test_default_target_marked(self, salesforce_connector, sample_targets):
        """Default target should be marked with default: true."""
        result = salesforce_connector._create_databricks_yml(
            project_name='test',
            targets=sample_targets,
            default_target='dev'
        )

        assert result['targets']['dev'].get('default') is True
        assert 'default' not in result['targets']['prod']

    def test_mode_auto_detected(self, salesforce_connector, sample_targets):
        """Mode should be auto-detected based on target name."""
        result = salesforce_connector._create_databricks_yml(
            project_name='test',
            targets=sample_targets,
            default_target='dev'
        )

        assert result['targets']['dev']['mode'] == 'development'
        assert result['targets']['prod']['mode'] == 'production'

    def test_explicit_mode_preserved(self, salesforce_connector):
        """Explicit mode in config should be preserved."""
        targets = {
            'staging': {
                'workspace_host': 'https://staging.databricks.com',
                'mode': 'production'
            }
        }

        result = salesforce_connector._create_databricks_yml(
            project_name='test',
            targets=targets,
            default_target='staging'
        )

        assert result['targets']['staging']['mode'] == 'production'

    def test_raises_error_empty_targets(self, salesforce_connector):
        """Should raise error when targets is empty."""
        with pytest.raises(ConfigurationError, match="At least one target"):
            salesforce_connector._create_databricks_yml(
                project_name='test',
                targets={},
                default_target='dev'
            )

    def test_raises_error_invalid_default_target(self, salesforce_connector, sample_targets):
        """Should raise error when default_target not in targets."""
        with pytest.raises(ConfigurationError, match="default_target"):
            salesforce_connector._create_databricks_yml(
                project_name='test',
                targets=sample_targets,
                default_target='nonexistent'
            )

    def test_raises_error_missing_workspace_host(self, salesforce_connector):
        """Should raise error when workspace_host is missing."""
        targets = {'dev': {'root_path': '/some/path'}}

        with pytest.raises(ConfigurationError, match="workspace_host"):
            salesforce_connector._create_databricks_yml(
                project_name='test',
                targets=targets,
                default_target='dev'
            )


class TestCreateJobs:
    """Tests for _create_jobs method."""

    def test_basic_structure(self, salesforce_connector):
        """Should create correct basic structure."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'schedule': ['*/15 * * * *'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        assert 'resources' in result
        assert 'jobs' in result['resources']

    def test_job_created_for_each_pipeline_group(self, salesforce_connector):
        """Should create one job per pipeline_group."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01', 'group_02', 'group_02'],
            'schedule': ['*/15 * * * *', '*/15 * * * *', '*/30 * * * *', '*/30 * * * *'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        assert len(result['resources']['jobs']) == 2
        assert 'job_group_01' in result['resources']['jobs']
        assert 'job_group_02' in result['resources']['jobs']

    def test_job_schedule_converted_to_quartz(self, salesforce_connector):
        """Schedule should be converted to Quartz cron format."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'schedule': ['*/15 * * * *'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        job = result['resources']['jobs']['job_test_01']
        # Quartz format: second minute hour day month dow
        assert job['schedule']['quartz_cron_expression'] == '0 */15 * * * ?'

    def test_job_references_pipeline(self, salesforce_connector):
        """Job task should reference correct pipeline."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'schedule': ['*/15 * * * *'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        job = result['resources']['jobs']['job_test_01']
        task = job['tasks'][0]
        assert 'pipeline_id' in task['pipeline_task']
        assert 'pipeline_test_01' in task['pipeline_task']['pipeline_id']

    def test_job_has_timezone(self, salesforce_connector):
        """Job should have UTC timezone."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'schedule': ['*/15 * * * *'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        job = result['resources']['jobs']['job_test_01']
        assert job['schedule']['timezone_id'] == 'UTC'

    def test_no_job_when_schedule_empty(self, salesforce_connector):
        """Should not create job when schedule is empty."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01', 'test_02'],
            'schedule': ['*/15 * * * *', ''],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        assert 'job_test_01' in result['resources']['jobs']
        assert 'job_test_02' not in result['resources']['jobs']

    def test_no_job_when_schedule_nan(self, salesforce_connector):
        """Should not create job when schedule is NaN."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01', 'test_02'],
            'schedule': ['*/15 * * * *', np.nan],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        assert 'job_test_01' in result['resources']['jobs']
        assert 'job_test_02' not in result['resources']['jobs']

    def test_pause_status_included_when_specified(self, salesforce_connector):
        """Pause status should be included when specified."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'schedule': ['*/15 * * * *'],
            'pause_status': ['PAUSED'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        job = result['resources']['jobs']['job_test_01']
        assert job['pause_status'] == 'PAUSED'

    def test_pause_status_uppercase(self, salesforce_connector):
        """Pause status should be uppercased."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'schedule': ['*/15 * * * *'],
            'pause_status': ['paused'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        job = result['resources']['jobs']['job_test_01']
        assert job['pause_status'] == 'PAUSED'

    def test_job_tags_included_when_specified(self, salesforce_connector):
        """Job tags should be included when specified."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'schedule': ['*/15 * * * *'],
            'tags': ['team=field-eng,demo=true'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        job = result['resources']['jobs']['job_test_01']
        assert job['tags'] == {'team': 'field-eng', 'demo': 'true'}


class TestSalesforceCreatePipelines:
    """Tests for Salesforce _create_pipelines method."""

    def test_basic_structure(self, salesforce_connector):
        """Should create correct basic structure."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        assert 'resources' in result
        assert 'pipelines' in result['resources']

    def test_pipeline_has_name_and_catalog_schema(self, salesforce_connector):
        """Pipeline should have name, catalog, and schema."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        assert pipeline['name'] == 'Ingestion - test_01'
        assert pipeline['catalog'] == 'main'
        assert pipeline['schema'] == 'salesforce'

    def test_pipeline_has_connection_name(self, salesforce_connector):
        """Pipeline should have connection_name in ingestion_definition."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        assert pipeline['ingestion_definition']['connection_name'] == 'sfdc_conn'

    def test_salesforce_source_schema_is_objects(self, salesforce_connector):
        """Salesforce source_schema should always be 'objects'."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        table_obj = pipeline['ingestion_definition']['objects'][0]
        assert table_obj['table']['source_schema'] == 'objects'

    def test_multiple_tables_in_pipeline(self, salesforce_connector):
        """Pipeline should include all tables in the group."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01', 'test_01'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['sfdc_conn', 'sfdc_conn'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        objects = pipeline['ingestion_definition']['objects']
        assert len(objects) == 2

    def test_include_columns_added_to_table_config(self, salesforce_connector):
        """Include columns should be added to table_configuration."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
            'include_columns': ['Id,Name,Industry'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        table_obj = pipeline['ingestion_definition']['objects'][0]
        assert 'table_configuration' in table_obj['table']
        assert table_obj['table']['table_configuration']['include_columns'] == ['Id', 'Name', 'Industry']

    def test_exclude_columns_added_to_table_config(self, salesforce_connector):
        """Exclude columns should be added to table_configuration."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
            'exclude_columns': ['SystemModstamp,LastModifiedDate'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        table_obj = pipeline['ingestion_definition']['objects'][0]
        assert 'table_configuration' in table_obj['table']
        assert table_obj['table']['table_configuration']['exclude_columns'] == ['SystemModstamp', 'LastModifiedDate']

    def test_primary_keys_added_to_table_config(self, salesforce_connector):
        """Primary keys should be added to table_configuration (supports composite keys)."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
            'primary_keys': ['Id,AccountId'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        table_obj = pipeline['ingestion_definition']['objects'][0]
        assert 'table_configuration' in table_obj['table']
        assert table_obj['table']['table_configuration']['primary_keys'] == ['Id', 'AccountId']

    def test_primary_keys_empty_not_included(self, salesforce_connector):
        """Empty primary_keys should not create table_configuration."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
            'primary_keys': ['   '],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        table_obj = pipeline['ingestion_definition']['objects'][0]
        assert 'table_configuration' not in table_obj['table']

    def test_pipeline_tags_included_when_specified(self, salesforce_connector):
        """Pipeline tags should be included when specified."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'source_table_name': ['Account'],
            'target_catalog': ['main'],
            'target_schema': ['salesforce'],
            'target_table_name': ['account'],
            'connection_name': ['sfdc_conn'],
            'tags': ['team=field-eng,demo=true'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        assert pipeline['tags'] == {'team': 'field-eng', 'demo': 'true'}


class TestDatabaseConnectorCreateGateways:
    """Tests for database connector _create_gateways method."""

    def test_basic_structure(self, sqlserver_connector):
        """Should create correct basic structure."""
        df = pd.DataFrame({
            'gateway': ['test_01'],
            'connection_name': ['conn'],
            'gateway_catalog': ['main'],
            'gateway_schema': ['gateway'],
        })

        result = sqlserver_connector._create_gateways(df, 'project')

        assert 'resources' in result
        assert 'pipelines' in result['resources']  # Gateways are type of pipeline

    def test_gateway_has_connection_name(self, sqlserver_connector):
        """Gateway should have connection_name in gateway_definition."""
        df = pd.DataFrame({
            'gateway': ['test_01'],
            'connection_name': ['sqlserver_conn'],
            'gateway_catalog': ['main'],
            'gateway_schema': ['gateway'],
        })

        result = sqlserver_connector._create_gateways(df, 'project')

        # Gateway pipeline name format: project_pipeline_project_gateway_<gateway_id>
        gateway_key = 'project_pipeline_project_gateway_test_01'
        gateway = result['resources']['pipelines'][gateway_key]
        assert gateway['gateway_definition']['connection_name'] == 'sqlserver_conn'

    def test_gateway_has_storage_config(self, sqlserver_connector):
        """Gateway should have storage catalog and schema."""
        df = pd.DataFrame({
            'gateway': ['test_01'],
            'connection_name': ['conn'],
            'gateway_catalog': ['main'],
            'gateway_schema': ['gateway_storage'],
        })

        result = sqlserver_connector._create_gateways(df, 'project')

        gateway_key = 'project_pipeline_project_gateway_test_01'
        gateway = result['resources']['pipelines'][gateway_key]
        assert gateway['gateway_definition']['gateway_storage_catalog'] == 'main'
        assert gateway['gateway_definition']['gateway_storage_schema'] == 'gateway_storage'

    def test_gateway_continuous_true(self, sqlserver_connector):
        """Gateway should have continuous: true."""
        df = pd.DataFrame({
            'gateway': ['test_01'],
            'connection_name': ['conn'],
            'gateway_catalog': ['main'],
            'gateway_schema': ['gateway'],
        })

        result = sqlserver_connector._create_gateways(df, 'project')

        gateway_key = 'project_pipeline_project_gateway_test_01'
        gateway = result['resources']['pipelines'][gateway_key]
        assert gateway['continuous'] is True

    def test_cluster_config_when_worker_type_provided(self, sqlserver_connector):
        """Should include cluster config when worker_type is provided."""
        df = pd.DataFrame({
            'gateway': ['test_01'],
            'connection_name': ['conn'],
            'gateway_catalog': ['main'],
            'gateway_schema': ['gateway'],
            'gateway_worker_type': ['m5d.large'],
        })

        result = sqlserver_connector._create_gateways(df, 'project')

        gateway_key = 'project_pipeline_project_gateway_test_01'
        gateway = result['resources']['pipelines'][gateway_key]
        assert 'clusters' in gateway
        assert gateway['clusters'][0]['node_type_id'] == 'm5d.large'

    def test_cluster_config_with_driver_type(self, sqlserver_connector):
        """Should include driver_node_type_id when provided."""
        df = pd.DataFrame({
            'gateway': ['test_01'],
            'connection_name': ['conn'],
            'gateway_catalog': ['main'],
            'gateway_schema': ['gateway'],
            'gateway_worker_type': ['m5d.large'],
            'gateway_driver_type': ['c5a.8xlarge'],
        })

        result = sqlserver_connector._create_gateways(df, 'project')

        gateway_key = 'project_pipeline_project_gateway_test_01'
        gateway = result['resources']['pipelines'][gateway_key]
        assert gateway['clusters'][0]['driver_node_type_id'] == 'c5a.8xlarge'

    def test_no_cluster_config_when_no_node_types(self, sqlserver_connector):
        """Should not include cluster config when no node types provided."""
        df = pd.DataFrame({
            'gateway': ['test_01'],
            'connection_name': ['conn'],
            'gateway_catalog': ['main'],
            'gateway_schema': ['gateway'],
            'gateway_worker_type': [None],
            'gateway_driver_type': [None],
        })

        result = sqlserver_connector._create_gateways(df, 'project')

        gateway_key = 'project_pipeline_project_gateway_test_01'
        gateway = result['resources']['pipelines'][gateway_key]
        assert 'clusters' not in gateway

    def test_gateway_tags_included_when_specified(self, sqlserver_connector):
        """Gateway tags should be included when specified."""
        df = pd.DataFrame({
            'gateway': ['test_01'],
            'connection_name': ['conn'],
            'gateway_catalog': ['main'],
            'gateway_schema': ['gateway'],
            'tags': ['team=field-eng,demo=true'],
        })

        result = sqlserver_connector._create_gateways(df, 'project')

        gateway_key = 'project_pipeline_project_gateway_test_01'
        gateway = result['resources']['pipelines'][gateway_key]
        assert gateway['tags'] == {'team': 'field-eng', 'demo': 'true'}


class TestDatabaseConnectorCreatePipelines:
    """Tests for database connector _create_pipelines method."""

    def test_pipeline_references_gateway(self, sqlserver_connector):
        """Pipeline should reference gateway id."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'gateway': ['test_01'],
            'source_database': ['SourceDB'],
            'source_schema': ['dbo'],
            'source_table_name': ['Users'],
            'target_catalog': ['main'],
            'target_schema': ['bronze'],
            'target_table_name': ['users'],
        })

        result = sqlserver_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        assert 'ingestion_gateway_id' in pipeline['ingestion_definition']
        assert 'project_gateway_test_01' in pipeline['ingestion_definition']['ingestion_gateway_id']

    def test_table_has_source_catalog_as_database(self, sqlserver_connector):
        """source_catalog should be the source_database for SQL Server."""
        df = pd.DataFrame({
            'pipeline_group': ['test_01'],
            'gateway': ['test_01'],
            'source_database': ['MyDatabase'],
            'source_schema': ['dbo'],
            'source_table_name': ['Users'],
            'target_catalog': ['main'],
            'target_schema': ['bronze'],
            'target_table_name': ['users'],
        })

        result = sqlserver_connector._create_pipelines(df, 'project')

        pipeline = result['resources']['pipelines']['pipeline_test_01']
        table_obj = pipeline['ingestion_definition']['objects'][0]
        assert table_obj['table']['source_catalog'] == 'MyDatabase'


class TestCronToQuartzConversion:
    """Tests for cron to Quartz format conversion."""

    def test_every_15_minutes(self, salesforce_connector):
        """*/15 * * * * should convert correctly."""
        from core import convert_cron_to_quartz
        result = convert_cron_to_quartz('*/15 * * * *')
        assert result == '0 */15 * * * ?'

    def test_every_6_hours(self, salesforce_connector):
        """0 */6 * * * should convert correctly."""
        from core import convert_cron_to_quartz
        result = convert_cron_to_quartz('0 */6 * * *')
        assert result == '0 0 */6 * * ?'

    def test_daily_at_midnight(self, salesforce_connector):
        """0 0 * * * should convert correctly."""
        from core import convert_cron_to_quartz
        result = convert_cron_to_quartz('0 0 * * *')
        assert result == '0 0 0 * * ?'

    def test_specific_day_of_week(self, salesforce_connector):
        """0 9 * * 1 should convert correctly (Monday at 9am)."""
        from core import convert_cron_to_quartz
        result = convert_cron_to_quartz('0 9 * * 1')
        # When DOW is specified, day-of-month should be ?
        assert result == '0 0 9 ? * 1'

    def test_already_quartz_format_unchanged(self, salesforce_connector):
        """Already 6-field format should be returned unchanged."""
        from core import convert_cron_to_quartz
        result = convert_cron_to_quartz('0 0 9 * * ?')
        assert result == '0 0 9 * * ?'
