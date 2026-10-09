"""
Backward-compatible import path for the PostgreSQL standard connector.

Use PostgreSQLStandardConnector from tapworks.connectors.postgresql.standard instead.
"""

from .standard import PostgreSQLStandardConnector

# Previous class name, kept so existing imports keep working
PostgreSQLConnector = PostgreSQLStandardConnector

__all__ = ["PostgreSQLStandardConnector", "PostgreSQLConnector"]
