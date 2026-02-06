"""
Shared pytest fixtures for lakehouse-tapworks tests.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# =============================================================================
# Sample DataFrames
# =============================================================================

@pytest.fixture
def sample_salesforce_df():
    """Sample Salesforce input dataframe with minimal required columns."""
    return pd.DataFrame({
        'source_database': ['Salesforce', 'Salesforce', 'Salesforce'],
        'source_schema': ['standard', 'standard', 'custom'],
        'source_table_name': ['Account', 'Contact', 'CustomObject__c'],
        'target_catalog': ['main', 'main', 'main'],
        'target_schema': ['salesforce', 'salesforce', 'salesforce'],
        'target_table_name': ['account', 'contact', 'custom_object'],
        'connection_name': ['sfdc_conn', 'sfdc_conn', 'sfdc_conn'],
    })


@pytest.fixture
def sample_salesforce_df_with_options():
    """Sample Salesforce input with optional columns."""
    return pd.DataFrame({
        'source_database': ['Salesforce', 'Salesforce'],
        'source_schema': ['standard', 'standard'],
        'source_table_name': ['Account', 'Contact'],
        'target_catalog': ['main', 'main'],
        'target_schema': ['salesforce', 'salesforce'],
        'target_table_name': ['account', 'contact'],
        'connection_name': ['sfdc_conn', 'sfdc_conn'],
        'project_name': ['my_project', 'my_project'],
        'prefix': ['sales', 'sales'],
        'subgroup': ['01', '01'],
        'schedule': ['*/15 * * * *', '*/30 * * * *'],
        'include_columns': ['Id,Name', ''],
        'exclude_columns': ['', 'SystemModstamp'],
    })


@pytest.fixture
def sample_sqlserver_df():
    """Sample SQL Server input dataframe with minimal required columns."""
    return pd.DataFrame({
        'source_database': ['SourceDB', 'SourceDB', 'SourceDB'],
        'source_schema': ['dbo', 'dbo', 'sales'],
        'source_table_name': ['Users', 'Orders', 'Products'],
        'target_catalog': ['main', 'main', 'main'],
        'target_schema': ['bronze', 'bronze', 'bronze'],
        'target_table_name': ['users', 'orders', 'products'],
        'connection_name': ['sqlserver_conn', 'sqlserver_conn', 'sqlserver_conn'],
    })


@pytest.fixture
def sample_sqlserver_df_with_gateway():
    """Sample SQL Server input with gateway configuration."""
    return pd.DataFrame({
        'source_database': ['SourceDB'] * 5,
        'source_schema': ['dbo'] * 5,
        'source_table_name': ['Table1', 'Table2', 'Table3', 'Table4', 'Table5'],
        'target_catalog': ['main'] * 5,
        'target_schema': ['bronze'] * 5,
        'target_table_name': ['table1', 'table2', 'table3', 'table4', 'table5'],
        'connection_name': ['sqlserver_conn'] * 5,
        'gateway_catalog': ['main'] * 5,
        'gateway_schema': ['gateway'] * 5,
        'gateway_worker_type': ['m5d.large'] * 5,
        'gateway_driver_type': ['c5a.8xlarge'] * 5,
    })


@pytest.fixture
def sample_ga4_df():
    """Sample Google Analytics 4 input dataframe."""
    return pd.DataFrame({
        'source_catalog': ['my-gcp-project', 'my-gcp-project'],
        'source_schema': ['analytics_123456789', 'analytics_987654321'],
        'tables': ['events,users', 'events,events_intraday'],
        'target_catalog': ['main', 'main'],
        'target_schema': ['ga4', 'ga4'],
        'connection_name': ['ga4_conn', 'ga4_conn'],
    })


@pytest.fixture
def sample_workday_df():
    """Sample Workday Reports input dataframe."""
    return pd.DataFrame({
        'source_url': [
            'https://wd2.workday.com/ccx/service/report1',
            'https://wd2.workday.com/ccx/service/report2',
        ],
        'target_catalog': ['main', 'main'],
        'target_schema': ['workday', 'workday'],
        'target_table_name': ['employees', 'departments'],
        'connection_name': ['workday_conn', 'workday_conn'],
        'primary_keys': ['Employee_ID', 'Department_ID'],
    })


@pytest.fixture
def large_df_for_load_balancing():
    """Large dataframe for testing load balancing splits."""
    num_rows = 600
    return pd.DataFrame({
        'source_database': ['SourceDB'] * num_rows,
        'source_schema': ['dbo'] * num_rows,
        'source_table_name': [f'Table_{i}' for i in range(num_rows)],
        'target_catalog': ['main'] * num_rows,
        'target_schema': ['bronze'] * num_rows,
        'target_table_name': [f'table_{i}' for i in range(num_rows)],
        'connection_name': ['conn'] * num_rows,
        'project_name': ['test_project'] * num_rows,
        'prefix': ['test'] * num_rows,
        'subgroup': ['01'] * num_rows,
    })


@pytest.fixture
def multi_group_df():
    """Dataframe with multiple prefix/subgroup combinations."""
    return pd.DataFrame({
        'source_database': ['DB'] * 9,
        'source_schema': ['dbo'] * 9,
        'source_table_name': [f'Table_{i}' for i in range(9)],
        'target_catalog': ['main'] * 9,
        'target_schema': ['bronze'] * 9,
        'target_table_name': [f'table_{i}' for i in range(9)],
        'connection_name': ['conn'] * 9,
        'project_name': ['project'] * 9,
        'prefix': ['sales', 'sales', 'sales', 'hr', 'hr', 'hr', 'finance', 'finance', 'finance'],
        'subgroup': ['01', '01', '02', '01', '01', '02', '01', '01', '01'],
    })


# =============================================================================
# Configuration Fixtures
# =============================================================================

@pytest.fixture
def sample_targets():
    """Sample targets configuration."""
    return {
        'dev': {
            'workspace_host': 'https://dev-workspace.cloud.databricks.com',
            'root_path': '/Users/user/.bundle/${bundle.name}/${bundle.target}'
        },
        'prod': {
            'workspace_host': 'https://prod-workspace.cloud.databricks.com',
            'root_path': '/Users/user/.bundle/${bundle.name}/${bundle.target}'
        }
    }


@pytest.fixture
def sample_targets_minimal():
    """Minimal targets configuration (dev only)."""
    return {
        'dev': {
            'workspace_host': 'https://dev-workspace.cloud.databricks.com'
        }
    }


@pytest.fixture
def sample_default_values():
    """Sample default values for testing."""
    return {
        'project_name': 'test_project',
        'schedule': '*/15 * * * *',
        'prefix': 'default_prefix',
    }


@pytest.fixture
def sample_override_config():
    """Sample override configuration."""
    return {
        'schedule': '0 0 * * *',
        'project_name': 'overridden_project',
    }


# =============================================================================
# Temporary Directory Fixtures
# =============================================================================

@pytest.fixture
def temp_output_dir(tmp_path):
    """Temporary directory for output files."""
    output_dir = tmp_path / "dab_output"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def temp_csv_file(tmp_path, sample_salesforce_df):
    """Create a temporary CSV file for testing."""
    csv_path = tmp_path / "test_input.csv"
    sample_salesforce_df.to_csv(csv_path, index=False)
    return csv_path


# =============================================================================
# Connector Fixtures
# =============================================================================

@pytest.fixture
def salesforce_connector():
    """SalesforceConnector instance."""
    from salesforce.connector import SalesforceConnector
    return SalesforceConnector()


@pytest.fixture
def sqlserver_connector():
    """SQLServerConnector instance."""
    from sqlserver.connector import SQLServerConnector
    return SQLServerConnector()


@pytest.fixture
def postgres_connector():
    """PostgreSQLConnector instance."""
    from postgres.connector import PostgreSQLConnector
    return PostgreSQLConnector()


@pytest.fixture
def ga4_connector():
    """GoogleAnalyticsConnector instance."""
    from google_analytics.connector import GoogleAnalyticsConnector
    return GoogleAnalyticsConnector()


@pytest.fixture
def servicenow_connector():
    """ServiceNowConnector instance."""
    from servicenow.connector import ServiceNowConnector
    return ServiceNowConnector()


@pytest.fixture
def workday_connector():
    """WorkdayReportsConnector instance."""
    from workday_reports.connector import WorkdayReportsConnector
    return WorkdayReportsConnector()
