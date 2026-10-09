"""Oracle connectors for Databricks Lakeflow Connect."""

from .integrated import OracleIntegratedConnector
from .query_based import OracleQueryBasedConnector

__all__ = ["OracleIntegratedConnector", "OracleQueryBasedConnector"]
