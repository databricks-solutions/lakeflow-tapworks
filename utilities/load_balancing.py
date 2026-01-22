"""
Shared load balancing utilities for pipeline grouping across all connectors.

This module provides common functions for splitting pipeline groups based on capacity limits.
Used by Salesforce, SQL Server, Google Analytics, and other connectors.
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
