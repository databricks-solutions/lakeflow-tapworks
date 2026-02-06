"""
Tests for the unified entry point (registry and runner).

Tests the connector registry, runner module, and CLI functionality.
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path

from core.registry import (
    get_connector,
    get_connector_class,
    list_connectors,
    list_aliases,
    get_connector_info,
    resolve_connector_name,
    CONNECTORS,
    CONNECTOR_ALIASES,
)
from core.runner import run_pipeline_generation, _load_input
from core import ConfigurationError


class TestConnectorRegistry:
    """Tests for connector registry."""

    def test_list_connectors_returns_all(self):
        """Should return all registered connectors."""
        connectors = list_connectors()
        assert 'salesforce' in connectors
        assert 'sqlserver' in connectors
        assert 'postgres' in connectors
        assert 'google_analytics' in connectors
        assert 'servicenow' in connectors
        assert 'workday_reports' in connectors

    def test_list_connectors_is_sorted(self):
        """Connectors should be returned in sorted order."""
        connectors = list_connectors()
        assert connectors == sorted(connectors)

    def test_list_aliases_returns_dict(self):
        """Should return aliases dictionary."""
        aliases = list_aliases()
        assert isinstance(aliases, dict)
        assert 'sf' in aliases
        assert aliases['sf'] == 'salesforce'

    def test_resolve_canonical_name(self):
        """Should return canonical name unchanged."""
        assert resolve_connector_name('salesforce') == 'salesforce'
        assert resolve_connector_name('sqlserver') == 'sqlserver'

    def test_resolve_alias(self):
        """Should resolve aliases to canonical names."""
        assert resolve_connector_name('sf') == 'salesforce'
        assert resolve_connector_name('pg') == 'postgres'
        assert resolve_connector_name('ga4') == 'google_analytics'

    def test_resolve_case_insensitive(self):
        """Should be case insensitive."""
        assert resolve_connector_name('SALESFORCE') == 'salesforce'
        assert resolve_connector_name('SF') == 'salesforce'
        assert resolve_connector_name('Postgres') == 'postgres'

    def test_resolve_unknown_raises_error(self):
        """Should raise ValueError for unknown connector."""
        with pytest.raises(ValueError, match="Unknown connector"):
            resolve_connector_name('unknown_connector')

    def test_get_connector_returns_instance(self):
        """Should return instantiated connector."""
        connector = get_connector('salesforce')
        assert connector is not None
        assert connector.connector_type == 'salesforce'

    def test_get_connector_with_alias(self):
        """Should work with aliases."""
        connector = get_connector('sf')
        assert connector.connector_type == 'salesforce'

    def test_get_connector_class_returns_class(self):
        """Should return class, not instance."""
        cls = get_connector_class('salesforce')
        assert isinstance(cls, type)
        # Instantiate to verify it's the right class
        instance = cls()
        assert instance.connector_type == 'salesforce'

    def test_get_connector_info(self):
        """Should return connector info dict."""
        info = get_connector_info('salesforce')
        assert info['name'] == 'salesforce'
        assert info['type'] == 'salesforce'
        assert 'required_columns' in info
        assert 'default_values' in info
        assert 'default_project_name' in info
        assert 'supported_scd_types' in info


class TestLoadInput:
    """Tests for input loading."""

    def test_load_csv_file(self, sample_salesforce_df, temp_output_dir):
        """Should load CSV file."""
        csv_path = temp_output_dir / 'test.csv'
        sample_salesforce_df.to_csv(csv_path, index=False)

        result = _load_input(str(csv_path))
        assert len(result) == len(sample_salesforce_df)

    def test_load_dataframe_returns_copy(self, sample_salesforce_df):
        """Should return a copy of DataFrame."""
        result = _load_input(sample_salesforce_df)
        assert len(result) == len(sample_salesforce_df)
        # Verify it's a copy
        result['new_col'] = 'test'
        assert 'new_col' not in sample_salesforce_df.columns

    def test_load_csv_not_found_raises_error(self):
        """Should raise FileNotFoundError for missing CSV."""
        with pytest.raises(FileNotFoundError):
            _load_input('/nonexistent/path.csv')

    def test_load_delta_without_spark_raises_error(self):
        """Should raise ConfigurationError for Delta without Spark."""
        with pytest.raises(ConfigurationError, match="Spark session"):
            _load_input('catalog.schema.table')

    def test_load_invalid_type_raises_error(self):
        """Should raise ConfigurationError for invalid input type."""
        with pytest.raises(ConfigurationError, match="Invalid input_source type"):
            _load_input(12345)


class TestRunPipelineGeneration:
    """Tests for run_pipeline_generation function."""

    def test_run_with_dataframe(
        self,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should run pipeline generation with DataFrame input."""
        result = run_pipeline_generation(
            connector_name='salesforce',
            input_source=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'test_project'},
        )

        assert 'pipeline_group' in result.columns
        assert len(result) == len(sample_salesforce_df)

    def test_run_with_csv_file(
        self,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should run pipeline generation with CSV file input."""
        csv_path = temp_output_dir / 'input.csv'
        sample_salesforce_df.to_csv(csv_path, index=False)

        output_path = temp_output_dir / 'output'
        result = run_pipeline_generation(
            connector_name='salesforce',
            input_source=str(csv_path),
            output_dir=str(output_path),
            targets=sample_targets_minimal,
            default_values={'project_name': 'test_project'},
        )

        assert len(result) == len(sample_salesforce_df)

    def test_run_with_alias(
        self,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should work with connector alias."""
        result = run_pipeline_generation(
            connector_name='sf',  # alias
            input_source=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'test_project'},
        )

        assert len(result) == len(sample_salesforce_df)

    def test_run_creates_output_files(
        self,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should create DAB output files."""
        run_pipeline_generation(
            connector_name='salesforce',
            input_source=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'test_project'},
        )

        project_dir = temp_output_dir / 'test_project'
        assert (project_dir / 'databricks.yml').exists()
        assert (project_dir / 'resources' / 'pipelines.yml').exists()

    def test_run_empty_targets_raises_error(
        self,
        sample_salesforce_df,
        temp_output_dir
    ):
        """Should raise ConfigurationError for empty targets."""
        with pytest.raises(ConfigurationError, match="target"):
            run_pipeline_generation(
                connector_name='salesforce',
                input_source=sample_salesforce_df,
                output_dir=str(temp_output_dir),
                targets={},
            )

    def test_run_unknown_connector_raises_error(
        self,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should raise ValueError for unknown connector."""
        with pytest.raises(ValueError, match="Unknown connector"):
            run_pipeline_generation(
                connector_name='unknown',
                input_source=sample_salesforce_df,
                output_dir=str(temp_output_dir),
                targets=sample_targets_minimal,
            )

    def test_run_with_override_config(
        self,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should apply override config."""
        result = run_pipeline_generation(
            connector_name='salesforce',
            input_source=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'test_project'},
            override_config={'schedule': '0 0 * * *'},
        )

        assert all(result['schedule'] == '0 0 * * *')

    def test_run_with_output_config(
        self,
        sample_salesforce_df,
        sample_targets_minimal,
        temp_output_dir
    ):
        """Should save output config when specified."""
        output_config = temp_output_dir / 'config_out.csv'

        run_pipeline_generation(
            connector_name='salesforce',
            input_source=sample_salesforce_df,
            output_dir=str(temp_output_dir),
            targets=sample_targets_minimal,
            default_values={'project_name': 'test_project'},
            output_config=str(output_config),
        )

        assert output_config.exists()


class TestDatabaseConnectorRunner:
    """Tests for database connector through runner."""

    def test_run_sqlserver(
        self,
        sample_sqlserver_df_with_gateway,
        sample_targets,
        temp_output_dir
    ):
        """Should run SQL Server connector."""
        result = run_pipeline_generation(
            connector_name='sqlserver',
            input_source=sample_sqlserver_df_with_gateway,
            output_dir=str(temp_output_dir),
            targets=sample_targets,
            default_values={'project_name': 'sql_test'},
        )

        assert 'gateway' in result.columns
        assert 'pipeline_group' in result.columns

    def test_run_sqlserver_with_alias(
        self,
        sample_sqlserver_df_with_gateway,
        sample_targets,
        temp_output_dir
    ):
        """Should work with SQL Server aliases."""
        for alias in ['sql', 'mssql']:
            result = run_pipeline_generation(
                connector_name=alias,
                input_source=sample_sqlserver_df_with_gateway,
                output_dir=str(temp_output_dir / alias),
                targets=sample_targets,
                default_values={'project_name': f'{alias}_test'},
            )
            assert len(result) > 0


class TestAllConnectorsLoad:
    """Test that all registered connectors can be loaded."""

    @pytest.mark.parametrize('connector_name', list(CONNECTORS.keys()))
    def test_connector_loads(self, connector_name):
        """Each registered connector should load successfully."""
        connector = get_connector(connector_name)
        assert connector is not None
        assert connector.connector_type is not None
        assert connector.required_columns is not None

    @pytest.mark.parametrize('alias,canonical', list(CONNECTOR_ALIASES.items()))
    def test_alias_resolves(self, alias, canonical):
        """Each alias should resolve to its canonical name."""
        resolved = resolve_connector_name(alias)
        assert resolved == canonical
