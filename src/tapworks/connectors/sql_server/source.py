"""
SQL Server source rules.

SQLServerSource holds rules that apply to SQL Server in every ingestion mode
(standard, integrated, query-based). Connector classes combine it with a
mode base class, e.g. SQLServerStandardConnector(SQLServerSource, StandardConnector).
"""


class SQLServerSource:
    """
    SQL Server rules shared by all SQL Server connectors.

    Override hooks here (calling super()) for SQL Server-specific behavior.
    None are needed yet.
    """
