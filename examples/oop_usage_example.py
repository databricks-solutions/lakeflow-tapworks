"""
Example: Using the OOP connector architecture for pipeline generation.

This script demonstrates how to use the new OOP-based connector classes
for generating Databricks Lakeflow Connect pipelines.

Key benefits of OOP architecture:
1. Simplified interface - connector handles all complexity
2. Consistent API across all connectors
3. Easy to extend - add new connectors by subclassing
4. Better code organization - connector-specific logic is encapsulated
5. Improved testability - mock connectors for testing
"""

import sys
from pathlib import Path
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utilities import load_input_csv
from sqlserver.connector import SQLServerConnector
from salesforce.connector import SalesforceConnector
from google_analytics.connector import GoogleAnalyticsConnector


def example_sql_server():
    """Example: Generate SQL Server pipelines using OOP."""
    print("\n" + "="*80)
    print("Example 1: SQL Server Connector")
    print("="*80 + "\n")

    # Sample data
    data = {
        'source_database': ['salesdb'] * 3,
        'source_schema': ['dbo'] * 3,
        'source_table_name': ['customers', 'orders', 'products'],
        'target_catalog': ['bronze'] * 3,
        'target_schema': ['sales'] * 3,
        'target_table_name': ['customers', 'orders', 'products'],
        'connection_name': ['sql_server_prod'] * 3
    }
    df = pd.DataFrame(data)

    # Create connector
    connector = SQLServerConnector()

    # Generate pipelines
    result = connector.run_complete_pipeline_generation(
        df=df,
        output_dir='output/sqlserver_example',
        targets={
            'dev': {
                'workspace_host': 'https://dev.databricks.com',
                'root_path': '/Users/me/.bundle/${bundle.name}/${bundle.target}'
            }
        },
        default_values={'project_name': 'my_sql_project'},
        max_tables_per_gateway=250,
        max_tables_per_pipeline=250
    )

    print(f"\n✓ Generated configuration for {len(result)} tables")
    print(f"✓ Created {result['pipeline_group'].nunique()} pipelines")


def example_salesforce():
    """Example: Generate Salesforce pipelines using OOP."""
    print("\n" + "="*80)
    print("Example 2: Salesforce Connector")
    print("="*80 + "\n")

    # Sample data
    data = {
        'source_database': ['Salesforce'] * 3,
        'source_schema': ['standard'] * 3,
        'source_table_name': ['Account', 'Contact', 'Opportunity'],
        'target_catalog': ['bronze'] * 3,
        'target_schema': ['salesforce'] * 3,
        'target_table_name': ['accounts', 'contacts', 'opportunities'],
        'connection_name': ['salesforce_prod'] * 3
    }
    df = pd.DataFrame(data)

    # Create connector
    connector = SalesforceConnector()

    # Generate pipelines
    result = connector.run_complete_pipeline_generation(
        df=df,
        output_dir='output/salesforce_example',
        targets={
            'prod': {
                'workspace_host': 'https://prod.databricks.com'
            }
        },
        default_values={'project_name': 'sfdc_ingestion'},
        max_tables_per_pipeline=250
    )

    print(f"\n✓ Generated configuration for {len(result)} objects")
    print(f"✓ Created {result['pipeline_group'].nunique()} pipelines")


def example_google_analytics():
    """Example: Generate GA4 pipelines using OOP."""
    print("\n" + "="*80)
    print("Example 3: Google Analytics 4 Connector")
    print("="*80 + "\n")

    # Sample data
    data = {
        'source_catalog': ['my-gcp-project'] * 2,
        'source_schema': ['analytics_123456789', 'analytics_987654321'],
        'tables': ['events,events_intraday,users', 'events'],
        'target_catalog': ['bronze'] * 2,
        'target_schema': ['ga4'] * 2,
        'connection_name': ['ga4_bigquery'] * 2
    }
    df = pd.DataFrame(data)

    # Create connector
    connector = GoogleAnalyticsConnector()

    # Generate pipelines
    result = connector.run_complete_pipeline_generation(
        df=df,
        output_dir='output/ga4_example',
        targets={
            'dev': {
                'workspace_host': 'https://dev.databricks.com'
            },
            'prod': {
                'workspace_host': 'https://prod.databricks.com'
            }
        },
        default_values={'project_name': 'ga4_ingestion'},
        max_tables_per_pipeline=250
    )

    print(f"\n✓ Generated configuration for {len(result)} properties")
    print(f"✓ Created {result['pipeline_group'].nunique()} pipelines")


def example_multiple_connectors():
    """Example: Using multiple connectors in the same script."""
    print("\n" + "="*80)
    print("Example 4: Multiple Connectors")
    print("="*80 + "\n")

    # Define targets once, use for all connectors
    targets = {
        'dev': {'workspace_host': 'https://dev.databricks.com'},
        'prod': {'workspace_host': 'https://prod.databricks.com'}
    }

    # Dictionary of connector instances
    connectors = {
        'sqlserver': SQLServerConnector(),
        'salesforce': SalesforceConnector(),
        'ga4': GoogleAnalyticsConnector()
    }

    print("Available connectors:")
    for name, connector in connectors.items():
        print(f"  - {name}: {connector.connector_type}")
        print(f"    Required columns: {', '.join(connector.required_columns[:3])}...")
        print(f"    Default project: {connector.default_project_name}")

    print("\n✓ All connectors use the same interface!")
    print("✓ Easy to add new connectors by subclassing BaseConnector")


def example_custom_connector():
    """Example: How to create a custom connector."""
    print("\n" + "="*80)
    print("Example 5: Creating a Custom Connector")
    print("="*80 + "\n")

    example_code = """
    from utilities.connectors import SaaSConnector

    class MySaaSConnector(SaaSConnector):
        @property
        def connector_type(self) -> str:
            return 'my_saas'

        @property
        def required_columns(self) -> list:
            return [
                'source_table',
                'target_catalog',
                'target_schema',
                'connection_name'
            ]

        @property
        def default_values(self) -> dict:
            return {
                'schedule': '0 */6 * * *'
            }

        def generate_yaml_files(self, df, output_dir, targets):
            from deployment.connector_settings_generator import generate_yaml_files
            generate_yaml_files(df=df, output_dir=output_dir, targets=targets)

    # Use it just like any other connector
    connector = MySaaSConnector()
    result = connector.run_complete_pipeline_generation(...)
    """

    print("To create a custom connector:")
    print("1. Subclass SaaSConnector or DatabaseConnector")
    print("2. Implement required properties (connector_type, required_columns, default_values)")
    print("3. Implement generate_yaml_files() method")
    print("4. Use it like any other connector!")
    print("\nExample code:")
    print(example_code)


if __name__ == "__main__":
    print("\n" + "="*80)
    print("OOP Connector Architecture Examples")
    print("="*80)

    # Run examples (commented out to avoid actual file generation)
    # Uncomment the ones you want to run

    # example_sql_server()
    # example_salesforce()
    # example_google_analytics()
    example_multiple_connectors()
    example_custom_connector()

    print("\n" + "="*80)
    print("Examples Complete!")
    print("="*80 + "\n")
    print("To run actual pipeline generation, uncomment the examples above")
    print("or use the connector-specific pipeline_generator_oop.py scripts.")
