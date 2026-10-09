"""
Oracle source rules.

OracleSource holds rules that apply to Oracle in every ingestion mode
(integrated, query-based). Connector classes combine it with a mode base
class, e.g. OracleIntegratedConnector(OracleSource, IntegratedCDCConnector).
"""

import logging
import pandas as pd

# Configure module logger
logger = logging.getLogger(__name__)

# Columns holding Oracle identifiers, whose case must match how Oracle stores them
IDENTIFIER_COLUMNS = ['source_database', 'source_schema', 'source_table_name']


def warn_on_lowercase_identifiers(df: pd.DataFrame) -> None:
    """
    Log a warning for Oracle identifiers that contain lowercase letters.

    Oracle stores unquoted identifiers in uppercase, and Lakeflow Connect requires
    the case to match. Lowercase is only correct for quoted identifiers, so this
    warns rather than fails.
    """
    for column in IDENTIFIER_COLUMNS:
        if column not in df.columns:
            continue
        values = df[column].dropna().astype(str)
        lowercase = sorted(set(values[values != values.str.upper()]))
        if lowercase:
            logger.warning(
                f"{column} has values with lowercase letters: {lowercase[:5]}"
                f"{' ...' if len(lowercase) > 5 else ''}. Oracle stores unquoted identifiers "
                f"in uppercase; the case must match how Oracle stores the identifier."
            )


class OracleSource:
    """
    Oracle rules shared by all Oracle connectors.

    - source_database is the Oracle service name (CDB$ROOT service name for
      multitenant databases)
    - Identifier case must match how Oracle stores it (warns on lowercase)
    """

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """Warn about identifiers whose case may not match Oracle."""
        df = super()._apply_connector_specific_normalization(df)
        warn_on_lowercase_identifiers(df)
        return df
