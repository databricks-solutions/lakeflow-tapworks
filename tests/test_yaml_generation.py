"""
Tests for YAML generation functionality.

Tests the _create_databricks_yml, _create_jobs, _create_pipelines, and
_create_gateways methods that generate DAB YAML structures.
"""

import pytest
import pandas as pd
import numpy as np
from tapworks.core import ConfigurationError, ValidationError


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

    def test_raises_error_conflicting_schedules_in_same_group(self, salesforce_connector):
        """Should raise ValidationError when tables in same pipeline_group have different schedules."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/30 * * * *', '0 0 * * *'],
        })

        with pytest.raises(ValidationError, match="conflicting schedule"):
            salesforce_connector._create_jobs(df, 'project')

    def test_raises_error_shows_both_solutions(self, salesforce_connector):
        """Error message should mention both solutions: same schedule or different subgroups."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/30 * * * *'],
        })

        with pytest.raises(ValidationError) as exc_info:
            salesforce_connector._create_jobs(df, 'project')

        error_msg = str(exc_info.value)
        assert "same schedule" in error_msg
        assert "subgroup" in error_msg

    def test_allows_same_schedule_in_same_group(self, salesforce_connector):
        """Should allow multiple tables with same schedule in same pipeline_group."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/15 * * * *', '*/15 * * * *'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        assert 'job_group_01' in result['resources']['jobs']

    def test_allows_different_schedules_in_different_groups(self, salesforce_connector):
        """Should allow different schedules when in different pipeline_groups."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01', 'group_02', 'group_02'],
            'schedule': ['*/15 * * * *', '*/15 * * * *', '*/30 * * * *', '*/30 * * * *'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        assert len(result['resources']['jobs']) == 2
        assert 'job_group_01' in result['resources']['jobs']
        assert 'job_group_02' in result['resources']['jobs']

    def test_ignores_empty_schedules_in_conflict_check(self, salesforce_connector):
        """Should ignore empty/NaN schedules when checking for conflicts."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '', np.nan],
        })

        # Should not raise error - empty schedules are ignored
        result = salesforce_connector._create_jobs(df, 'project')
        assert 'job_group_01' in result['resources']['jobs']

    def test_raises_error_conflicting_pause_status_in_same_group(self, salesforce_connector):
        """Should raise ValidationError when tables in same pipeline_group have different pause_status."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/15 * * * *'],
            'pause_status': ['PAUSED', 'UNPAUSED'],
        })

        with pytest.raises(ValidationError, match="conflicting pause_status"):
            salesforce_connector._create_jobs(df, 'project')

    def test_pause_status_error_shows_both_solutions(self, salesforce_connector):
        """Pause status error message should mention both solutions."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/15 * * * *'],
            'pause_status': ['PAUSED', 'UNPAUSED'],
        })

        with pytest.raises(ValidationError) as exc_info:
            salesforce_connector._create_jobs(df, 'project')

        error_msg = str(exc_info.value)
        assert "same pause_status" in error_msg
        assert "subgroup" in error_msg

    def test_allows_same_pause_status_in_same_group(self, salesforce_connector):
        """Should allow multiple tables with same pause_status in same pipeline_group."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/15 * * * *'],
            'pause_status': ['PAUSED', 'PAUSED'],
        })

        result = salesforce_connector._create_jobs(df, 'project')

        job = result['resources']['jobs']['job_group_01']
        assert job['pause_status'] == 'PAUSED'

    def test_ignores_empty_pause_status_in_conflict_check(self, salesforce_connector):
        """Should ignore empty/NaN pause_status when checking for conflicts."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/15 * * * *', '*/15 * * * *'],
            'pause_status': ['PAUSED', '', np.nan],
        })

        # Should not raise error - empty pause_status values are ignored
        result = salesforce_connector._create_jobs(df, 'project')
        job = result['resources']['jobs']['job_group_01']
        assert job['pause_status'] == 'PAUSED'


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
        from tapworks.core import convert_cron_to_quartz
        result = convert_cron_to_quartz('*/15 * * * *')
        assert result == '0 */15 * * * ?'

    def test_every_6_hours(self, salesforce_connector):
        """0 */6 * * * should convert correctly."""
        from tapworks.core import convert_cron_to_quartz
        result = convert_cron_to_quartz('0 */6 * * *')
        assert result == '0 0 */6 * * ?'

    def test_daily_at_midnight(self, salesforce_connector):
        """0 0 * * * should convert correctly."""
        from tapworks.core import convert_cron_to_quartz
        result = convert_cron_to_quartz('0 0 * * *')
        assert result == '0 0 0 * * ?'

    def test_specific_day_of_week(self, salesforce_connector):
        """0 9 * * 1 should convert correctly (Monday at 9am)."""
        from tapworks.core import convert_cron_to_quartz
        result = convert_cron_to_quartz('0 9 * * 1')
        # When DOW is specified, day-of-month should be ?
        assert result == '0 0 9 ? * 1'

    def test_already_quartz_format_unchanged(self, salesforce_connector):
        """Already 6-field format should be returned unchanged."""
        from tapworks.core import convert_cron_to_quartz
        result = convert_cron_to_quartz('0 0 9 * * ?')
        assert result == '0 0 9 * * ?'


class TestSaaSPipelineValidation:
    """Tests for SaaS connector pipeline validation (connection_name, tags)."""

    def test_raises_error_conflicting_connection_names(self, salesforce_connector, tmp_path, sample_targets):
        """Should raise ValidationError when tables in same pipeline_group have different connection_names."""
        df = pd.DataFrame({
            'project_name': ['project', 'project', 'project'],
            'pipeline_group': ['group_01', 'group_01', 'group_01'],
            'target_catalog': ['bronze', 'bronze', 'bronze'],
            'target_schema': ['sales', 'sales', 'sales'],
            'target_table_name': ['table1', 'table2', 'table3'],
            'source_table_name': ['Account', 'Contact', 'Opportunity'],
            'connection_name': ['prod_sf', 'dev_sf', 'prod_sf'],
        })

        with pytest.raises(ValidationError, match="conflicting connection_name"):
            salesforce_connector.generate_yaml_files(df, str(tmp_path), sample_targets)

    def test_allows_same_connection_name(self, salesforce_connector):
        """Should allow same connection_name for all tables in pipeline_group."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sales', 'sales'],
            'target_table_name': ['table1', 'table2'],
            'source_table_name': ['Account', 'Contact'],
            'connection_name': ['prod_sf', 'prod_sf'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')
        assert 'pipeline_group_01' in result['resources']['pipelines']

    def test_raises_error_conflicting_pipeline_tags(self, salesforce_connector, tmp_path, sample_targets):
        """Should raise ValidationError when tables in same pipeline_group have different tags."""
        df = pd.DataFrame({
            'project_name': ['project', 'project'],
            'pipeline_group': ['group_01', 'group_01'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sales', 'sales'],
            'target_table_name': ['table1', 'table2'],
            'source_table_name': ['Account', 'Contact'],
            'connection_name': ['prod_sf', 'prod_sf'],
            'tags': ['env=prod,team=sales', 'env=dev,team=marketing'],
        })

        with pytest.raises(ValidationError, match="conflicting tags"):
            salesforce_connector.generate_yaml_files(df, str(tmp_path), sample_targets)

    def test_allows_same_tags(self, salesforce_connector):
        """Should allow same tags for all tables in pipeline_group."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sales', 'sales'],
            'target_table_name': ['table1', 'table2'],
            'source_table_name': ['Account', 'Contact'],
            'connection_name': ['prod_sf', 'prod_sf'],
            'tags': ['env=prod,team=sales', 'env=prod,team=sales'],
        })

        result = salesforce_connector._create_pipelines(df, 'project')
        assert 'pipeline_group_01' in result['resources']['pipelines']


class TestJobTagsValidation:
    """Tests for job-level tags validation."""

    def test_raises_error_conflicting_job_tags(self, salesforce_connector):
        """Should raise ValidationError when tables in same pipeline_group have different job tags."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/15 * * * *'],
            'tags': ['env=prod', 'env=dev'],
        })

        with pytest.raises(ValidationError, match="conflicting tags"):
            salesforce_connector._create_jobs(df, 'project')

    def test_allows_same_job_tags(self, salesforce_connector):
        """Should allow same tags for all tables in same pipeline_group."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/15 * * * *'],
            'tags': ['env=prod', 'env=prod'],
        })

        result = salesforce_connector._create_jobs(df, 'project')
        assert 'job_group_01' in result['resources']['jobs']

    def test_ignores_empty_tags_in_conflict_check(self, salesforce_connector):
        """Should ignore empty/NaN tags when checking for conflicts."""
        df = pd.DataFrame({
            'pipeline_group': ['group_01', 'group_01', 'group_01'],
            'schedule': ['*/15 * * * *', '*/15 * * * *', '*/15 * * * *'],
            'tags': ['env=prod', '', np.nan],
        })

        result = salesforce_connector._create_jobs(df, 'project')
        assert 'job_group_01' in result['resources']['jobs']


class TestGatewayValidation:
    """Tests for gateway-level validation (gateway_catalog, gateway_schema, connection_name, tags)."""

    def test_raises_error_conflicting_gateway_catalog(self, sqlserver_connector):
        """Should raise ValidationError when tables in same gateway have different gateway_catalog."""
        df = pd.DataFrame({
            'gateway': ['g01', 'g01'],
            'pipeline_group': ['group_01', 'group_01'],
            'source_database': ['db1', 'db2'],
            'source_schema': ['dbo', 'dbo'],
            'source_table_name': ['table1', 'table2'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sales', 'sales'],
            'target_table_name': ['table1', 'table2'],
            'connection_name': ['prod_sql', 'prod_sql'],
            'gateway_catalog': ['bronze', 'silver'],
            'gateway_schema': ['ingestion', 'ingestion'],
        })

        with pytest.raises(ValidationError, match="conflicting gateway_catalog"):
            sqlserver_connector._create_gateways(df, 'project')

    def test_raises_error_conflicting_gateway_schema(self, sqlserver_connector):
        """Should raise ValidationError when tables in same gateway have different gateway_schema."""
        df = pd.DataFrame({
            'gateway': ['g01', 'g01'],
            'pipeline_group': ['group_01', 'group_01'],
            'source_database': ['db1', 'db2'],
            'source_schema': ['dbo', 'dbo'],
            'source_table_name': ['table1', 'table2'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sales', 'sales'],
            'target_table_name': ['table1', 'table2'],
            'connection_name': ['prod_sql', 'prod_sql'],
            'gateway_catalog': ['bronze', 'bronze'],
            'gateway_schema': ['ingestion', 'staging'],
        })

        with pytest.raises(ValidationError, match="conflicting gateway_schema"):
            sqlserver_connector._create_gateways(df, 'project')

    def test_raises_error_conflicting_gateway_connection_name(self, sqlserver_connector):
        """Should raise ValidationError when tables in same gateway have different connection_name."""
        df = pd.DataFrame({
            'gateway': ['g01', 'g01'],
            'pipeline_group': ['group_01', 'group_01'],
            'source_database': ['db1', 'db2'],
            'source_schema': ['dbo', 'dbo'],
            'source_table_name': ['table1', 'table2'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sales', 'sales'],
            'target_table_name': ['table1', 'table2'],
            'connection_name': ['prod_sql', 'dev_sql'],
            'gateway_catalog': ['bronze', 'bronze'],
            'gateway_schema': ['ingestion', 'ingestion'],
        })

        with pytest.raises(ValidationError, match="conflicting connection_name"):
            sqlserver_connector._create_gateways(df, 'project')

    def test_raises_error_conflicting_gateway_tags(self, sqlserver_connector):
        """Should raise ValidationError when tables in same gateway have different tags."""
        df = pd.DataFrame({
            'gateway': ['g01', 'g01'],
            'pipeline_group': ['group_01', 'group_01'],
            'source_database': ['db1', 'db2'],
            'source_schema': ['dbo', 'dbo'],
            'source_table_name': ['table1', 'table2'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sales', 'sales'],
            'target_table_name': ['table1', 'table2'],
            'connection_name': ['prod_sql', 'prod_sql'],
            'gateway_catalog': ['bronze', 'bronze'],
            'gateway_schema': ['ingestion', 'ingestion'],
            'tags': ['env=prod', 'env=dev'],
        })

        with pytest.raises(ValidationError, match="conflicting tags"):
            sqlserver_connector._create_gateways(df, 'project')

    def test_allows_consistent_gateway_fields(self, sqlserver_connector):
        """Should allow same values for all gateway fields in same gateway."""
        df = pd.DataFrame({
            'gateway': ['g01', 'g01'],
            'pipeline_group': ['group_01', 'group_01'],
            'source_database': ['db1', 'db2'],
            'source_schema': ['dbo', 'dbo'],
            'source_table_name': ['table1', 'table2'],
            'target_catalog': ['bronze', 'bronze'],
            'target_schema': ['sales', 'sales'],
            'target_table_name': ['table1', 'table2'],
            'connection_name': ['prod_sql', 'prod_sql'],
            'gateway_catalog': ['bronze', 'bronze'],
            'gateway_schema': ['ingestion', 'ingestion'],
            'tags': ['env=prod', 'env=prod'],
        })

        result = sqlserver_connector._create_gateways(df, 'project')
        assert 'project_pipeline_project_gateway_g01' in result['resources']['pipelines']


class TestGroupBasedConfiguration:
    """Tests for group-based defaults and overrides."""

    def test_flat_defaults_applied_globally(self, salesforce_connector):
        """Flat default dict should apply to all rows."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['conn', 'conn'],
            'project_name': ['project1', 'project2'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values={'schedule': '*/15 * * * *'},
        )

        assert all(result['schedule'] == '*/15 * * * *')

    def test_grouped_defaults_applied_per_group(self, salesforce_connector):
        """Grouped defaults should apply to matching rows only."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard', 'standard'],
            'source_table_name': ['Account', 'Contact', 'Lead'],
            'target_catalog': ['main', 'main', 'main'],
            'target_schema': ['salesforce', 'salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact', 'lead'],
            'connection_name': ['conn', 'conn', 'conn'],
            'project_name': ['sales_project', 'sales_project', 'hr_project'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values={
                '*': {'schedule': '*/15 * * * *'},
                'sales_project': {'schedule': '*/30 * * * *'},
            },
        )

        assert result.loc[0, 'schedule'] == '*/30 * * * *'
        assert result.loc[1, 'schedule'] == '*/30 * * * *'
        assert result.loc[2, 'schedule'] == '*/15 * * * *'

    def test_grouped_overrides_applied_per_group(self, salesforce_connector):
        """Grouped overrides should apply to matching rows only."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard', 'standard'],
            'source_table_name': ['Account', 'Contact', 'Lead'],
            'target_catalog': ['main', 'main', 'main'],
            'target_schema': ['salesforce', 'salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact', 'lead'],
            'connection_name': ['conn', 'conn', 'conn'],
            'project_name': ['sales_project', 'sales_project', 'hr_project'],
            'pause_status': ['UNPAUSED', 'UNPAUSED', 'UNPAUSED'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            override_input_config={
                '*': {'pause_status': 'UNPAUSED'},
                'hr_project': {'pause_status': 'PAUSED'},
            },
        )

        assert result.loc[0, 'pause_status'] == 'UNPAUSED'
        assert result.loc[1, 'pause_status'] == 'UNPAUSED'
        assert result.loc[2, 'pause_status'] == 'PAUSED'

    def test_prefix_takes_priority_over_project_name(self, salesforce_connector):
        """When prefix exists, it should be used for matching instead of project_name."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['conn', 'conn'],
            'project_name': ['my_project', 'my_project'],
            'prefix': ['sales', 'hr'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values={
                '*': {'schedule': '*/15 * * * *'},
                'sales': {'schedule': '*/30 * * * *'},
                'hr': {'schedule': '0 * * * *'},
            },
        )

        assert result.loc[0, 'schedule'] == '*/30 * * * *'
        assert result.loc[1, 'schedule'] == '0 * * * *'

    def test_defaults_only_fill_missing_values(self, salesforce_connector):
        """Defaults should only fill missing/empty values, not overwrite existing."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['conn', 'conn'],
            'project_name': ['sales_project', 'sales_project'],
            'schedule': ['0 0 * * *', ''],  # First has value, second is empty
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values={
                'sales_project': {'schedule': '*/30 * * * *'},
            },
        )

        assert result.loc[0, 'schedule'] == '0 0 * * *'  # Unchanged
        assert result.loc[1, 'schedule'] == '*/30 * * * *'  # Filled with default

    def test_overrides_overwrite_all_values(self, salesforce_connector):
        """Overrides should overwrite all values, not just missing ones."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['conn', 'conn'],
            'project_name': ['sales_project', 'sales_project'],
            'schedule': ['0 0 * * *', '*/15 * * * *'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            override_input_config={
                'sales_project': {'schedule': '*/30 * * * *'},
            },
        )

        assert result.loc[0, 'schedule'] == '*/30 * * * *'
        assert result.loc[1, 'schedule'] == '*/30 * * * *'

    def test_group_specific_wins_over_global(self, salesforce_connector):
        """Group-specific config should take precedence over global."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['conn', 'conn'],
            'project_name': ['sales_project', 'other_project'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values={
                '*': {'schedule': '*/15 * * * *', 'pause_status': 'UNPAUSED'},
                'sales_project': {'schedule': '*/30 * * * *'},
            },
        )

        # sales_project: group schedule, global pause_status
        assert result.loc[0, 'schedule'] == '*/30 * * * *'
        assert result.loc[0, 'pause_status'] == 'UNPAUSED'

        # other_project: global schedule, global pause_status
        assert result.loc[1, 'schedule'] == '*/15 * * * *'
        assert result.loc[1, 'pause_status'] == 'UNPAUSED'

    def test_combined_defaults_and_overrides(self, salesforce_connector):
        """Both defaults and overrides should work together correctly."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard', 'standard'],
            'source_table_name': ['Account', 'Contact', 'Lead'],
            'target_catalog': ['main', 'main', 'main'],
            'target_schema': ['salesforce', 'salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact', 'lead'],
            'connection_name': ['conn', 'conn', 'conn'],
            'project_name': ['sales', 'hr', 'finance'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values={
                '*': {'schedule': '*/15 * * * *'},
                'sales': {'schedule': '*/30 * * * *'},
            },
            override_input_config={
                '*': {'pause_status': 'UNPAUSED'},
                'finance': {'pause_status': 'PAUSED'},
            },
        )

        # sales: group default schedule, global override pause_status
        assert result.loc[0, 'schedule'] == '*/30 * * * *'
        assert result.loc[0, 'pause_status'] == 'UNPAUSED'

        # hr: global default schedule, global override pause_status
        assert result.loc[1, 'schedule'] == '*/15 * * * *'
        assert result.loc[1, 'pause_status'] == 'UNPAUSED'

        # finance: global default schedule, group override pause_status
        assert result.loc[2, 'schedule'] == '*/15 * * * *'
        assert result.loc[2, 'pause_status'] == 'PAUSED'

    def test_pipeline_group_matching(self, salesforce_connector):
        """Should match on pipeline_group (prefix_subgroup) for granular control."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard', 'standard'],
            'source_table_name': ['Account', 'Contact', 'Lead'],
            'target_catalog': ['main', 'main', 'main'],
            'target_schema': ['salesforce', 'salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact', 'lead'],
            'connection_name': ['conn', 'conn', 'conn'],
            'project_name': ['project', 'project', 'project'],
            'prefix': ['sales', 'sales', 'sales'],
            'subgroup': ['01', '02', '02'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values={
                '*': {'schedule': '*/15 * * * *'},
                'sales': {'schedule': '*/30 * * * *'},
                'sales_02': {'schedule': '0 * * * *'},
            },
        )

        # sales_01: matches 'sales' prefix
        assert result.loc[0, 'schedule'] == '*/30 * * * *'
        # sales_02: matches 'sales_02' pipeline_group (more specific)
        assert result.loc[1, 'schedule'] == '0 * * * *'
        assert result.loc[2, 'schedule'] == '0 * * * *'

    def test_pipeline_group_precedence_over_prefix(self, salesforce_connector):
        """pipeline_group match should take precedence over prefix match."""
        df = pd.DataFrame({
            'source_database': ['Salesforce', 'Salesforce'],
            'source_schema': ['standard', 'standard'],
            'source_table_name': ['Account', 'Contact'],
            'target_catalog': ['main', 'main'],
            'target_schema': ['salesforce', 'salesforce'],
            'target_table_name': ['account', 'contact'],
            'connection_name': ['conn', 'conn'],
            'project_name': ['project', 'project'],
            'prefix': ['hr', 'hr'],
            'subgroup': ['01', '01'],
        })

        result = salesforce_connector._process_input_config(
            df=df,
            required_columns=salesforce_connector.required_columns,
            default_values={
                '*': {'schedule': '*/15 * * * *'},
                'hr': {'schedule': '*/30 * * * *'},
                'hr_01': {'schedule': '0 0 * * *'},
            },
        )

        # Both should match hr_01 (pipeline_group) not hr (prefix)
        assert result.loc[0, 'schedule'] == '0 0 * * *'
        assert result.loc[1, 'schedule'] == '0 0 * * *'
