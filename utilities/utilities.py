"""
Utility functions for lakehouse-tapworks connectors.

This module provides only true utility functions that are not part of
the connector class hierarchy. All configuration processing, YAML generation,
and resource naming logic is implemented in the core.BaseConnector class.
"""

import pandas as pd
from pathlib import Path


def load_input_csv(
    input_csv: str,
    required_columns_hint: str = None
) -> pd.DataFrame:
    """
    Load and validate input CSV configuration file.

    Args:
        input_csv (str): Path to input CSV file
        required_columns_hint (str): Optional hint text for error message
            describing required columns for the specific connector

    Returns:
        pd.DataFrame: Loaded configuration dataframe

    Raises:
        FileNotFoundError: If input file does not exist
        ValueError: If CSV is empty or cannot be parsed

    Example Usage:
        >>> df = load_input_csv('config.csv')
        >>> # With custom hint:
        >>> df = load_input_csv(
        ...     'config.csv',
        ...     required_columns_hint="source_database, source_schema, source_table_name"
        ... )
    """
    input_path = Path(input_csv)

    if not input_path.exists():
        error_msg = f"Input file not found: {input_csv}\n\n"
        if required_columns_hint:
            error_msg += f"Please create an input CSV with required columns:\n{required_columns_hint}"
        else:
            error_msg += "Please create an input CSV with the required columns for your connector."
        raise FileNotFoundError(error_msg)

    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        raise ValueError(f"Failed to parse CSV file: {e}")

    if df.empty:
        raise ValueError(f"Input CSV is empty: {input_csv}")

    print(f"✓ Loaded {len(df)} rows from {input_csv}")

    return df


def convert_cron_to_quartz(cron_expression: str) -> str:
    """
    Convert standard 5-field cron to Quartz 6-field cron format.

    Standard cron: minute hour day month day-of-week
    Quartz cron:   second minute hour day month day-of-week

    In Quartz cron, you must use ? for either day-of-month OR day-of-week (not both can be *).
    - If day-of-week is *, use ? for day-of-week
    - If day-of-week is specified (not *), use ? for day-of-month

    Args:
        cron_expression (str): Standard 5-field cron expression
            Example: "*/15 * * * *" or "0 9 * * 1"

    Returns:
        str: Quartz 6-field cron expression
            Example: "0 */15 * * * ?" or "0 0 9 ? * 1"

    Examples:
        >>> convert_cron_to_quartz("*/15 * * * *")
        '0 */15 * * * ?'

        >>> convert_cron_to_quartz("0 */6 * * *")
        '0 0 */6 * * ?'

        >>> convert_cron_to_quartz("0 0 * * *")
        '0 0 0 * * ?'

        >>> convert_cron_to_quartz("0 9 * * 1")
        '0 0 9 ? * 1'
    """
    parts = cron_expression.strip().split()

    if len(parts) != 5:
        # If already 6+ fields, assume it's Quartz format
        return cron_expression

    # Add seconds=0 at start
    minute, hour, day, month, dow = parts

    # In Quartz cron, you must use ? for either day-of-month OR day-of-week (not both can be *)
    # If day-of-week is *, use ? for day-of-week
    # If day-of-week is specified (not *), use ? for day-of-month
    if dow == '*':
        dow = '?'
    else:
        # day-of-week is specified, so day-of-month must be ?
        day = '?'

    quartz_cron = f"0 {minute} {hour} {day} {month} {dow}"
    return quartz_cron
