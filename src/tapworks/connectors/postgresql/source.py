"""
PostgreSQL source rules.

PostgreSQLSource holds rules that apply to PostgreSQL in every ingestion mode
(standard, integrated, query-based). Connector classes combine it with a
mode base class, e.g. PostgreSQLStandardConnector(PostgreSQLSource, StandardConnector).
"""


class PostgreSQLSource:
    """
    PostgreSQL rules shared by all PostgreSQL connectors.

    Override hooks here (calling super()) for PostgreSQL-specific behavior.
    None are needed yet.
    """
