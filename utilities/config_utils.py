"""
Configuration validation and loading utilities.

Shared functions for processing and validating input configurations
across all connectors.
"""

import pandas as pd
from pathlib import Path


def process_input_config(
    df: pd.DataFrame,
    required_columns: list,
    default_values: dict = None,
    override_input_config: dict = None
) -> pd.DataFrame:
    """
    Validate and normalize input configuration DataFrame.

    This function ensures all required columns are present, adds optional columns
    with defaults if missing, and fills empty/NaN values appropriately.

    The input DataFrame can come from any source (CSV, Delta table, or code).
    The output DataFrame will have all required and optional columns with clean values
    (no NaN, empty strings replaced with defaults).

    Args:
        df (pd.DataFrame): Input DataFrame from any source (CSV, Delta, code)
        required_columns (list): List of required column names that must be present.
            Example:
            [
                'source_database', 'source_schema', 'source_table_name',
                'target_catalog', 'target_schema', 'target_table_name',
                'connection_name'
            ]
        default_values (dict, optional): Dictionary of optional columns with their default values.
            Missing columns will be added, NaN/empty values will be filled with defaults.
            If None, no default values will be applied.
            Example:
            {
                'schedule': '*/15 * * * *',
                'priority_flag': 0
            }
        override_input_config (dict, optional): Dictionary of column overrides.
            Values in these columns will be replaced with the override value for ALL rows.
            This is useful for forcing specific values across the entire configuration.
            Example:
            {
                'schedule': '*/30 * * * *',  # Override schedule for all rows
                'target_catalog': 'bronze'   # Force all to bronze catalog
            }

    Returns:
        pd.DataFrame: Normalized DataFrame with all required and optional columns,
                     NaN values filled, empty strings replaced with defaults,
                     and any overrides applied

    Raises:
        ValueError: If required columns are missing
        ValueError: If DataFrame is empty

    Example Usage:
        >>> required = [
        ...     'source_database', 'source_schema', 'source_table_name',
        ...     'target_catalog', 'target_schema', 'target_table_name',
        ...     'connection_name'
        ... ]
        >>> defaults = {
        ...     'schedule': '*/15 * * * *',
        ...     'priority_flag': 0
        ... }
        >>> override = {'schedule': '*/30 * * * *'}
        >>> normalized_df = process_input_config(df, required, defaults, override)
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Check if dataframe is empty
    if df.empty:
        raise ValueError("Input DataFrame is empty")

    # Validate required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}\n"
            f"Required columns: {', '.join(required_columns)}"
        )

    # Add optional columns if not present and handle NaN/empty values
    if default_values:
        for col_name, default_value in default_values.items():
            if col_name not in df.columns:
                print(f"Info: '{col_name}' column not found. Adding with default: {default_value}")
                df[col_name] = default_value
            else:
                # Fill NaN values with default (skip if default is None, as pandas doesn't support fillna(None))
                if default_value is not None:
                    df[col_name] = df[col_name].fillna(default_value)

                # Replace empty strings with default (for string columns)
                if isinstance(default_value, str):
                    mask = df[col_name].astype(str).str.strip() == ''
                    df.loc[mask, col_name] = default_value

    # Apply overrides if provided
    if override_input_config:
        for col_name, override_value in override_input_config.items():
            print(f"Info: Overriding '{col_name}' column with value: {override_value}")
            df[col_name] = override_value

    print(f"\n✓ Configuration validated: {len(df)} rows with all required and optional columns")

    return df


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
