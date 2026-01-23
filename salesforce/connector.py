"""
Salesforce connector implementation using OOP architecture.

This module provides the SalesforceConnector class which implements the
SaaSConnector interface for Salesforce data sources.
"""

import sys
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from utilities.connectors import SaaSConnector


class SalesforceConnector(SaaSConnector):
    """
    Salesforce connector for Databricks Lakeflow Connect pipelines.

    Implements SaaS connector pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - OAuth connection management
    - Column filtering support (include/exclude)

    Required CSV columns:
    - source_database: Always 'Salesforce'
    - source_schema: Schema type ('standard' or 'custom')
    - source_table_name: Salesforce object name (e.g., 'Account', 'Contact')
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for Salesforce

    Optional CSV columns:
    - project_name: Project identifier (default: 'salesforce_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - schedule: Cron schedule (e.g., '0 0 * * *', default: '*/15 * * * *')
    - include_columns: Comma-separated list of columns to include (default: '')
    - exclude_columns: Comma-separated list of columns to exclude (default: '')
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'salesforce'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for Salesforce input CSV.

        All these columns must be present and non-empty in the input.
        """
        return [
            'source_database',
            'source_schema',
            'source_table_name',
            'target_catalog',
            'target_schema',
            'target_table_name',
            'connection_name'
        ]

    @property
    def default_values(self) -> dict:
        """
        Return default values for optional Salesforce columns.

        These values are used when columns are missing or empty.
        """
        return {
            'schedule': '*/15 * * * *',
            'include_columns': '',
            'exclude_columns': ''
        }

    @property
    def default_project_name(self) -> str:
        """Return default project name for Salesforce connector."""
        return 'sfdc_ingestion'

    def generate_yaml_files(self, df, output_dir, targets):
        """
        Generate YAML files for Salesforce connector without gateways.

        Creates a DAB structure:
        - databricks.yml (root configuration)
        - resources/pipelines.yml (pipeline definitions)
        - resources/jobs.yml (scheduled jobs)

        Args:
            df: DataFrame with pipeline configuration
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments
        """
        # Import Salesforce specific YAML generator
        from deployment.connector_settings_generator import generate_yaml_files

        generate_yaml_files(df=df, output_dir=output_dir, targets=targets)
