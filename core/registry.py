"""
Connector registry for unified pipeline generation.

This module provides a registry of all available connectors and
utility functions to instantiate them by name.
"""

import importlib
from typing import Dict, Type

# Registry mapping connector names to their module paths
CONNECTORS: Dict[str, str] = {
    'salesforce': 'salesforce.connector.SalesforceConnector',
    'sql_server': 'sql_server.connector.SQLServerConnector',
    'postgresql': 'postgresql.connector.PostgreSQLConnector',
    'google_analytics': 'google_analytics.connector.GoogleAnalyticsConnector',
    'servicenow': 'servicenow.connector.ServiceNowConnector',
    'workday_reports': 'workday_reports.connector.WorkdayReportsConnector',
}


def resolve_connector_name(name: str) -> str:
    """
    Resolve a connector name to the canonical name.

    Args:
        name: Connector name (case-insensitive)

    Returns:
        Canonical connector name

    Raises:
        ValueError: If connector name is not found
    """
    name_lower = name.lower()

    # Check if it's a canonical name
    if name_lower in CONNECTORS:
        return name_lower

    # Not found
    raise ValueError(
        f"Unknown connector: '{name}'. "
        f"Available connectors: {', '.join(sorted(CONNECTORS.keys()))}"
    )


def get_connector(name: str):
    """
    Get a connector instance by name.

    Args:
        name: Connector name (case-insensitive)

    Returns:
        Instantiated connector object

    Raises:
        ValueError: If connector name is not found
        ImportError: If connector module cannot be imported
    """
    canonical_name = resolve_connector_name(name)
    module_path = CONNECTORS[canonical_name]

    # Split into module and class name
    module_name, class_name = module_path.rsplit('.', 1)

    # Import module and get class
    module = importlib.import_module(module_name)
    connector_class = getattr(module, class_name)

    return connector_class()


def get_connector_class(name: str) -> Type:
    """
    Get a connector class (not instantiated) by name.

    Args:
        name: Connector name (case-insensitive)

    Returns:
        Connector class

    Raises:
        ValueError: If connector name is not found
        ImportError: If connector module cannot be imported
    """
    canonical_name = resolve_connector_name(name)
    module_path = CONNECTORS[canonical_name]

    module_name, class_name = module_path.rsplit('.', 1)
    module = importlib.import_module(module_name)

    return getattr(module, class_name)


def list_connectors() -> list:
    """
    List all available connector names.

    Returns:
        List of canonical connector names
    """
    return sorted(CONNECTORS.keys())


def get_connector_info(name: str) -> dict:
    """
    Get information about a connector.

    Args:
        name: Connector name

    Returns:
        Dictionary with connector information
    """
    canonical_name = resolve_connector_name(name)
    connector = get_connector(canonical_name)

    return {
        'name': canonical_name,
        'type': connector.connector_type,
        'required_columns': connector.required_columns,
        'default_values': connector.default_values,
        'default_project_name': connector.default_project_name,
        'supported_scd_types': connector.supported_scd_types,
    }
