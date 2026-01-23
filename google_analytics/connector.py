"""
Google Analytics 4 connector implementation using OOP architecture.

This module provides the GoogleAnalyticsConnector class which implements the
SaaSConnector interface for Google Analytics 4 data sources.
"""

import sys
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from utilities.connectors import SaaSConnector


class GoogleAnalyticsConnector(SaaSConnector):
    """
    Google Analytics 4 connector for Databricks Lakeflow Connect pipelines.

    Implements SaaS connector pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - BigQuery integration
    - Multiple tables per property support

    Required CSV columns:
    - source_catalog: GCP project ID (e.g., 'my-gcp-project')
    - source_schema: GA4 property ID (e.g., 'analytics_123456789')
    - tables: Comma-separated list of tables (e.g., 'events,events_intraday,users')
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - connection_name: Databricks connection name for BigQuery

    Optional CSV columns:
    - project_name: Project identifier (default: 'ga4_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - schedule: Cron schedule (e.g., '0 */6 * * *', default: '0 */6 * * *')
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return 'ga4'

    @property
    def required_columns(self) -> list:
        """
        Return required columns for GA4 input CSV.

        All these columns must be present and non-empty in the input.
        """
        return [
            'source_catalog',
            'source_schema',
            'tables',
            'target_catalog',
            'target_schema',
            'connection_name'
        ]

    @property
    def default_values(self) -> dict:
        """
        Return default values for optional GA4 columns.

        These values are used when columns are missing or empty.
        """
        return {
            'schedule': '0 */6 * * *'  # Every 6 hours by default
        }

    @property
    def default_project_name(self) -> str:
        """Return default project name for GA4 connector."""
        return 'ga4_ingestion'

    def generate_yaml_files(self, df, output_dir, targets):
        """
        Generate YAML files for GA4 connector without gateways.

        Creates a DAB structure:
        - databricks.yml (root configuration)
        - resources/pipelines.yml (pipeline definitions)
        - resources/jobs.yml (scheduled jobs)

        Args:
            df: DataFrame with pipeline configuration
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments
        """
        # Import GA4 specific YAML generator
        from deployment.connector_settings_generator import generate_yaml_files

        generate_yaml_files(df=df, output_dir=output_dir, targets=targets)
