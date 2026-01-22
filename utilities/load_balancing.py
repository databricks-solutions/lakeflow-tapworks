"""
Shared load balancing utilities for pipeline grouping across all connectors.

This module provides common functions for splitting pipeline groups based on capacity limits.
Used by Salesforce, SQL Server, Google Analytics, and other connectors.

Functions:
- split_groups_by_size: Low-level splitting function
- generate_saas_pipeline_config: High-level function for SaaS connectors (Salesforce, GA4, etc.)
- generate_database_pipeline_config: High-level function for database connectors (SQL Server, MySQL, etc.)
"""

import pandas as pd


def split_groups_by_size(
    df: pd.DataFrame,
    group_column: str,
    max_size: int,
    output_column: str,
    suffix: str = 'g'
) -> pd.DataFrame:
    """
    Split groups that exceed max_size into smaller chunks with sequential suffixes.

    This is the core load balancing function used across all connectors to split
    large pipeline groups into manageable chunks.

    Args:
        df (pd.DataFrame): Input DataFrame with groups to split
        group_column (str): Column containing group identifiers to split
        max_size (int): Maximum rows per group (e.g., 250 tables per pipeline)
        output_column (str): Column name for output group names
        suffix (str): Suffix pattern for split groups
            - 'g' → _g01, _g02, _g03 (for pipelines)
            - 'gw' → _gw01, _gw02, _gw03 (for gateways)

    Returns:
        pd.DataFrame: DataFrame with output_column populated

    Examples:
        >>> # Split SaaS connector pipelines (single-level)
        >>> df['base_group'] = df['prefix'] + '_' + df['priority']
        >>> df = split_groups_by_size(df, 'base_group', 250, 'pipeline_group', 'g')
        >>> # Result: marketing_01 (300 tables) → marketing_01_g01 (250), marketing_01_g02 (50)

        >>> # Split SQL Server gateways (two-level)
        >>> df['base_group'] = df['prefix'] + '_' + df['priority']
        >>> df = split_groups_by_size(df, 'base_group', 250, 'gateway', 'gw')
        >>> df = split_groups_by_size(df, 'gateway', 250, 'pipeline_group', 'g')
        >>> # Result: sales_01_gw01_g01, sales_01_gw01_g02, sales_01_gw02_g01

    Implementation Details:
        - Groups not exceeding max_size retain their original name
        - Groups exceeding max_size are split into chunks with sequential numbering
        - Suffix numbering is zero-padded to 2 digits (01, 02, ..., 99)
        - Original DataFrame is not modified (returns a copy)
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Initialize output column
    df[output_column] = ''

    # Process each unique group
    for group_name in df[group_column].unique():
        group_df = df[df[group_column] == group_name]

        if len(group_df) > max_size:
            # Split into chunks
            num_chunks = (len(group_df) - 1) // max_size + 1

            for i in range(num_chunks):
                start_idx = i * max_size
                end_idx = min((i + 1) * max_size, len(group_df))
                chunk_indices = group_df.iloc[start_idx:end_idx].index

                # Append suffix: _g01, _g02, etc. or _gw01, _gw02, etc.
                chunk_name = f"{group_name}_{suffix}{i+1:02d}"
                df.loc[chunk_indices, output_column] = chunk_name
        else:
            # No split needed - use group name as-is
            df.loc[group_df.index, output_column] = group_name

    return df


def generate_saas_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_pipeline: int = 250
) -> pd.DataFrame:
    """
    Generate pipeline configuration for SaaS connectors using single-level grouping.

    This is the standard implementation for SaaS connectors like Salesforce and Google Analytics
    that don't require gateway resources. Uses prefix + priority grouping with automatic splitting
    when groups exceed capacity.

    Args:
        df (pd.DataFrame): Input DataFrame with required columns:
            - prefix: Business unit or grouping identifier
            - priority: Priority level (will be padded to 2 digits)
            Plus connector-specific columns (source/target tables, catalogs, schemas, etc.)
        max_tables_per_pipeline (int): Maximum items per pipeline (default: 250)
            Groups exceeding this will be split into multiple pipelines (_g01, _g02, etc.)

    Returns:
        pd.DataFrame: Input DataFrame with added 'pipeline_group' column

    Example:
        >>> # Salesforce with 300 tables in one group
        >>> df = pd.DataFrame({
        ...     'prefix': ['sales'] * 300,
        ...     'priority': ['1'] * 300,
        ...     'source_table_name': [f'table_{i}' for i in range(300)],
        ...     ...
        ... })
        >>> result = generate_saas_pipeline_config(df, max_tables_per_pipeline=250)
        >>> result['pipeline_group'].unique()
        array(['sales_01_g01', 'sales_01_g02'])

    Used by:
        - salesforce/load_balancing/load_balancer.py
        - google_analytics/load_balancing/load_balancer.py
        - (future SaaS connectors)
    """
    # Make a copy to avoid modifying original
    df = df.copy()

    # Ensure consistent string formatting
    df['prefix'] = df['prefix'].astype(str)
    df['priority'] = df['priority'].astype(str).str.zfill(2)  # Pad to 2 digits (01, 02, etc.)

    # Generate base group from prefix + priority
    df['base_group'] = df['prefix'] + '_' + df['priority']

    # Split groups by capacity using shared function
    df = split_groups_by_size(
        df=df,
        group_column='base_group',
        max_size=max_tables_per_pipeline,
        output_column='pipeline_group',
        suffix='g'
    )

    # Drop temporary base_group column
    df = df.drop(columns=['base_group'])

    return df


def generate_database_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_gateway: int = 250,
    max_tables_per_pipeline: int = 250
) -> pd.DataFrame:
    """
    Generate pipeline configuration for database connectors using two-level grouping.

    This is the standard implementation for database connectors like SQL Server, MySQL, PostgreSQL
    that require gateway resources. Uses prefix + priority grouping with two-level splitting:
    1. Gateway level: Groups tables into gateways (max_tables_per_gateway)
    2. Pipeline level: Splits large gateways into multiple pipelines (max_tables_per_pipeline)

    Args:
        df (pd.DataFrame): Input DataFrame with required columns:
            - prefix: Business unit or grouping identifier
            - priority: Priority level (will be padded to 2 digits)
            Plus connector-specific columns (source/target tables, catalogs, schemas, gateway settings, etc.)
        max_tables_per_gateway (int): Maximum tables per gateway (default: 250)
            Groups exceeding this will be split into multiple gateways (_gw01, _gw02, etc.)
        max_tables_per_pipeline (int): Maximum tables per pipeline within gateway (default: 250)
            Gateways exceeding this will be split into multiple pipelines (_g01, _g02, etc.)

    Returns:
        pd.DataFrame: Input DataFrame with added columns:
            - gateway: Gateway identifier (prefix_priority or prefix_priority_gw01, _gw02, etc.)
            - pipeline_group: Pipeline group identifier

    Example:
        >>> # SQL Server with 600 tables in one group
        >>> df = pd.DataFrame({
        ...     'prefix': ['sales'] * 600,
        ...     'priority': ['1'] * 600,
        ...     'source_table_name': [f'table_{i}' for i in range(600)],
        ...     ...
        ... })
        >>> result = generate_database_pipeline_config(
        ...     df,
        ...     max_tables_per_gateway=250,
        ...     max_tables_per_pipeline=250
        ... )
        >>> result['gateway'].unique()
        array(['sales_01_gw01', 'sales_01_gw02', 'sales_01_gw03'])
        >>> result['pipeline_group'].unique()
        array(['sales_01_gw01', 'sales_01_gw02', 'sales_01_gw03'])

    Naming conventions:
        - Single gateway, single pipeline: prefix_priority (e.g., sales_01)
        - Multiple gateways, single pipeline each: prefix_priority_gw01, prefix_priority_gw02
        - Single gateway, multiple pipelines: prefix_priority_g01, prefix_priority_g02
        - Multiple gateways, multiple pipelines: prefix_priority_gw01_g01, prefix_priority_gw01_g02

    Used by:
        - sqlserver/load_balancing/load_balancer.py
        - (future database connectors: MySQL, PostgreSQL, Oracle, etc.)
    """
    # Make a copy to avoid modifying original
    df = df.copy()

    # Ensure consistent string formatting
    df['prefix'] = df['prefix'].astype(str)
    df['priority'] = df['priority'].astype(str).str.zfill(2)  # Pad to 2 digits (01, 02, etc.)

    # Generate base group from prefix + priority
    df['base_group'] = df['prefix'] + '_' + df['priority']

    # Step 1: Split by gateway capacity using shared function
    df = split_groups_by_size(
        df=df,
        group_column='base_group',
        max_size=max_tables_per_gateway,
        output_column='gateway',
        suffix='gw'
    )

    # Step 2: Split each gateway by pipeline capacity using shared function
    df = split_groups_by_size(
        df=df,
        group_column='gateway',
        max_size=max_tables_per_pipeline,
        output_column='pipeline_group',
        suffix='g'
    )

    # Drop temporary base_group column
    df = df.drop(columns=['base_group'])

    return df
