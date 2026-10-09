"""
Backward-compatible import path for the SQL Server standard connector.

Use SQLServerStandardConnector from tapworks.connectors.sql_server.standard instead.
"""

from .standard import SQLServerStandardConnector

# Previous class name, kept so existing imports keep working
SQLServerConnector = SQLServerStandardConnector

__all__ = ["SQLServerStandardConnector", "SQLServerConnector"]
