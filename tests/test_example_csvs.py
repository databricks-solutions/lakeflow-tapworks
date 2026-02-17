"""
Integration tests for example CSV files.

These tests verify that all example CSV files in the repository
can be successfully processed by their respective connectors.
"""

import pytest
from pathlib import Path

from core.runner import run_pipeline_generation


# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Map connector directories to connector names
CONNECTOR_DIR_MAP = {
    'salesforce': 'salesforce',
    'sql_server': 'sql_server',
    'postgresql': 'postgresql',
    'google_analytics': 'google_analytics',
    'servicenow': 'servicenow',
    'workday_reports': 'workday_reports',
}


def discover_example_csvs():
    """Discover all example CSV files in the project."""
    examples = []
    for connector_dir, connector_name in CONNECTOR_DIR_MAP.items():
        examples_path = PROJECT_ROOT / connector_dir / 'examples'
        if examples_path.exists():
            for csv_file in examples_path.rglob('*.csv'):
                examples.append((connector_name, csv_file))
    return examples


# Discover examples at module load time for parametrization
EXAMPLE_CSVS = discover_example_csvs()


class TestExampleCSVs:
    """Integration tests for example CSV files."""

    @pytest.fixture
    def targets(self):
        """Minimal targets configuration for testing."""
        return {
            'dev': {
                'workspace_host': 'https://dev-workspace.cloud.databricks.com'
            }
        }

    @pytest.mark.parametrize(
        'connector_name,csv_path',
        EXAMPLE_CSVS,
        ids=[f"{name}:{path.parent.name}/{path.name}" for name, path in EXAMPLE_CSVS]
    )
    def test_example_csv_generates_successfully(
        self,
        connector_name,
        csv_path,
        targets,
        tmp_path
    ):
        """Each example CSV should generate valid YAML without errors."""
        output_dir = tmp_path / 'output'
        output_dir.mkdir()

        # Run pipeline generation - should not raise any errors
        result = run_pipeline_generation(
            connector_name=connector_name,
            input_source=str(csv_path),
            output_dir=str(output_dir),
            targets=targets,
        )

        # Verify we got results
        assert result is not None
        assert len(result) > 0

        # Verify output directory was created with expected structure
        project_dirs = list(output_dir.iterdir())
        assert len(project_dirs) > 0, "No project directory created"

        # Verify databricks.yml exists
        project_dir = project_dirs[0]
        assert (project_dir / 'databricks.yml').exists(), "databricks.yml not created"

        # Verify resources directory exists with YAML files
        resources_dir = project_dir / 'resources'
        assert resources_dir.exists(), "resources directory not created"

        yaml_files = list(resources_dir.glob('*.yml'))
        assert len(yaml_files) > 0, "No YAML files created in resources"

    @pytest.mark.parametrize(
        'connector_name,csv_path',
        EXAMPLE_CSVS,
        ids=[f"{name}:{path.parent.name}/{path.name}" for name, path in EXAMPLE_CSVS]
    )
    def test_example_csv_has_required_columns(self, connector_name, csv_path):
        """Each example CSV should have all required columns for its connector."""
        import pandas as pd
        from core.registry import get_connector

        connector = get_connector(connector_name)
        df = pd.read_csv(csv_path)

        missing_columns = [
            col for col in connector.required_columns
            if col not in df.columns
        ]

        assert not missing_columns, (
            f"Example CSV {csv_path.name} missing required columns: {missing_columns}"
        )
