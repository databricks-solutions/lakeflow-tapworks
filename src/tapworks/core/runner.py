"""
Shared runner for unified pipeline generation.

This module provides the core execution logic that can be called
from both CLI and notebook entry points.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Union

from .registry import get_connector, resolve_connector_name
from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def run_pipeline_generation(
    connector_name: str,
    input_source: Union[str, pd.DataFrame],
    output_dir: str,
    targets: Dict[str, Dict],
    default_values: Optional[Dict] = None,
    override_config: Optional[Dict] = None,
    max_tables_per_pipeline: int = 250,
    max_tables_per_gateway: int = 250,
    output_config: Optional[str] = None,
    spark_session=None,
) -> pd.DataFrame:
    """
    Run pipeline generation for any connector.

    This is the core function shared by CLI and notebook entry points.

    Args:
        connector_name: Name of the connector (e.g., 'salesforce', 'sql_server', 'postgresql')
        input_source: One of:
                     - Path to CSV file (str ending in .csv)
                     - Delta table name (str like 'catalog.schema.table')
                     - pandas DataFrame
        output_dir: Directory for generated DAB files
        targets: Dictionary of target environments with workspace configuration
        default_values: Optional default values for missing columns
        override_config: Optional overrides applied to all rows
        max_tables_per_pipeline: Maximum tables per pipeline (default: 250)
        max_tables_per_gateway: Maximum tables per gateway for database connectors (default: 250)
        output_config: Optional path to save processed configuration CSV
        spark_session: Optional Spark session for reading Delta tables

    Returns:
        Processed DataFrame with pipeline assignments

    Raises:
        ConfigurationError: If configuration is invalid
        ValueError: If connector is not found
        FileNotFoundError: If input CSV file doesn't exist

    Example:
        >>> from tapworks.core.runner import run_pipeline_generation
        >>> result = run_pipeline_generation(
        ...     connector_name='salesforce',
        ...     input_source='config.csv',
        ...     output_dir='./output',
        ...     targets={'dev': {'workspace_host': 'https://...'}},
        ... )
    """
    # Resolve connector name (handles aliases)
    canonical_name = resolve_connector_name(connector_name)
    logger.info(f"Using connector: {canonical_name}")

    # Get connector instance
    connector = get_connector(canonical_name)

    # Load input data
    df = _load_input(input_source, spark_session)
    logger.info(f"Loaded {len(df)} rows from input")

    # Validate targets
    if not targets:
        raise ConfigurationError("At least one target environment is required")

    # Build kwargs for connector
    kwargs = {
        'df': df,
        'output_dir': output_dir,
        'targets': targets,
        'max_tables_per_pipeline': max_tables_per_pipeline,
    }

    if default_values:
        kwargs['default_values'] = default_values

    if override_config:
        kwargs['override_input_config'] = override_config

    if output_config:
        kwargs['output_config'] = output_config

    # Add gateway limit for database connectors
    if hasattr(connector, 'run_complete_pipeline_generation'):
        # Check if connector accepts max_tables_per_gateway
        import inspect
        sig = inspect.signature(connector.run_complete_pipeline_generation)
        if 'max_tables_per_gateway' in sig.parameters:
            kwargs['max_tables_per_gateway'] = max_tables_per_gateway

    # Run pipeline generation
    result_df = connector.run_complete_pipeline_generation(**kwargs)

    logger.info(f"Generated pipelines for {len(result_df)} tables")
    logger.info(f"Output written to: {output_dir}")

    return result_df


def _load_input(
    input_source: Union[str, pd.DataFrame],
    spark_session=None,
) -> pd.DataFrame:
    """
    Load input data from various sources.

    Args:
        input_source: CSV path, Delta table name, or DataFrame
        spark_session: Spark session for Delta tables

    Returns:
        pandas DataFrame

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ConfigurationError: If Delta table specified without Spark session
    """
    # Already a DataFrame
    if isinstance(input_source, pd.DataFrame):
        return input_source.copy()

    # String input - could be CSV or Delta table
    if isinstance(input_source, str):
        # CSV file
        if input_source.endswith('.csv'):
            path = Path(input_source)
            if not path.exists():
                raise FileNotFoundError(f"Input CSV file not found: {input_source}")
            return pd.read_csv(input_source)

        # Delta table (format: catalog.schema.table or just table_name)
        if spark_session is None:
            # Try to get spark from globals (notebook environment)
            try:
                from pyspark.sql import SparkSession
                spark_session = SparkSession.getActiveSession()
            except ImportError:
                pass

        if spark_session is None:
            raise ConfigurationError(
                f"Cannot read Delta table '{input_source}' without a Spark session. "
                "Either pass spark_session parameter or run in a Databricks notebook."
            )

        logger.info(f"Reading Delta table: {input_source}")
        return spark_session.read.table(input_source).toPandas()

    raise ConfigurationError(
        f"Invalid input_source type: {type(input_source)}. "
        "Expected: CSV path (str), Delta table name (str), or pandas DataFrame"
    )


def get_connector_info(connector_name: str) -> dict:
    """
    Get information about a connector's configuration.

    Useful for understanding what columns are required/optional.

    Args:
        connector_name: Name or alias of the connector

    Returns:
        Dictionary with connector configuration details
    """
    from .registry import get_connector_info as _get_info
    return _get_info(connector_name)


def list_connectors() -> list:
    """
    List all available connectors.

    Returns:
        List of connector names
    """
    from .registry import list_connectors as _list
    return _list()
