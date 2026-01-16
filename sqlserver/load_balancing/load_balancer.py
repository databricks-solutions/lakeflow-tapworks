import pandas as pd
from pathlib import Path


def load_input_csv(
    input_csv: str
) -> pd.DataFrame:
    """
    Load and validate input CSV configuration file.

    Args:
        input_csv (str): Path to input CSV file

    Returns:
        pd.DataFrame: Loaded configuration dataframe

    Raises:
        FileNotFoundError: If input file does not exist
        ValueError: If CSV is empty or cannot be parsed
    """
    input_path = Path(input_csv)

    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_csv}\n\n"
            f"Please create an input CSV with the following columns:\n"
            f"  - source_database, source_schema, source_table_name\n"
            f"  - target_catalog, target_schema, target_table_name\n"
            f"  - priority_flag (optional)\n"
            f"  - connection_name (optional)\n"
            f"  - gateway_catalog, gateway_schema (optional)\n"
            f"  - gateway_worker_type, gateway_driver_type (optional)\n"
            f"  - schedule (optional)"
        )

    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        raise ValueError(f"Failed to parse CSV file: {e}")

    if df.empty:
        raise ValueError(f"Input CSV is empty: {input_csv}")

    print(f"✓ Loaded {len(df)} rows from {input_csv}")

    return df


def process_input_config(
    df: pd.DataFrame,
    required_columns: list,
    optional_columns: dict,
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
            Example for SQL Server:
            [
                'source_database', 'source_schema', 'source_table_name',
                'target_catalog', 'target_schema', 'target_table_name'
            ]
        optional_columns (dict): Dictionary of optional columns with their default values.
            Missing columns will be added, NaN/empty values will be filled with defaults.
            Example:
            {
                'priority_flag': 0,
                'connection_name': 'default_connection',
                'gateway_catalog': None,  # Will use target_catalog if None
                'gateway_schema': None,   # Will use target_schema if None
                'gateway_worker_type': None,
                'gateway_driver_type': None,
                'schedule': '*/15 * * * *'
            }
        override_input_config (dict, optional): Dictionary of column overrides.
            Values in these columns will be replaced with the override value for ALL rows.
            This is useful for forcing specific values across the entire configuration.
            Example:
            {
                'schedule': '*/30 * * * *',  # Override schedule for all rows
                'connection_name': 'prod_conn'
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
        ...     'target_catalog', 'target_schema', 'target_table_name'
        ... ]
        >>> optional = {
        ...     'priority_flag': 0,
        ...     'connection_name': 'my_connection',
        ...     'schedule': '*/15 * * * *'
        ... }
        >>> normalized_df = process_input_config(df, required, optional)
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
    for col_name, default_value in optional_columns.items():
        if col_name not in df.columns:
            print(f"Info: '{col_name}' column not found. Adding with default: {default_value}")
            df[col_name] = default_value
        else:
            # Fill NaN values with default
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


def generate_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_group: int = 250
):
    """
    Generate pipeline configuration from a list of source tables.

    This function expects a clean DataFrame (output from process_input_config).

    This script groups tables into pipeline_groups with the following logic:
    - Each source_database gets its own set of pipelines (gateways)
    - Tables are grouped into pipeline_groups with a maximum of max_tables_per_group per group
    - Priority tables (priority_flag=1) are placed in separate pipeline_groups from non-priority tables

    Args:
        df (pd.DataFrame): Clean input dataframe (from process_input_config) with columns:
            - source_database, source_schema, source_table_name
            - target_catalog, target_schema, target_table_name
            - priority_flag: 1 for priority tables, 0 for normal tables
            - connection_name, schedule (already validated)
            - gateway_catalog, gateway_schema (already validated)
            - gateway_worker_type, gateway_driver_type (already validated)
        max_tables_per_group (int): Maximum tables per pipeline group (default: 250)

    Returns:
        pd.DataFrame: The generated configuration dataframe with additional columns:
            - pipeline_group: Pipeline group identifier
            - gateway: Gateway identifier
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Initialize new columns
    df['pipeline_group'] = 0
    df['gateway'] = 0

    # Track global gateway and pipeline group counters
    global_gateway_id = 1
    global_pipeline_group_id = 1

    # Group by source_database to ensure each database gets its own pipeline/gateway
    for source_db, db_group in df.groupby('source_database'):
        print(f"\nProcessing database: {source_db} ({len(db_group)} tables)")

        # Assign gateway ID for this database
        gateway_id = global_gateway_id
        global_gateway_id += 1

        # Separate priority and non-priority tables
        priority_tables = db_group[db_group['priority_flag'] == 1]
        normal_tables = db_group[db_group['priority_flag'] == 0]

        # Process priority tables first
        if len(priority_tables) > 0:
            print(f"  Processing {len(priority_tables)} priority tables")
            num_priority_groups = (len(priority_tables) - 1) // max_tables_per_group + 1

            for i in range(num_priority_groups):
                start_idx = i * max_tables_per_group
                end_idx = min((i + 1) * max_tables_per_group, len(priority_tables))
                group_indices = priority_tables.iloc[start_idx:end_idx].index

                df.loc[group_indices, 'pipeline_group'] = global_pipeline_group_id
                df.loc[group_indices, 'gateway'] = gateway_id

                print(f"    Created priority pipeline_group {global_pipeline_group_id} with {len(group_indices)} tables")
                global_pipeline_group_id += 1

        # Process normal tables
        if len(normal_tables) > 0:
            print(f"  Processing {len(normal_tables)} normal tables")
            num_normal_groups = (len(normal_tables) - 1) // max_tables_per_group + 1

            for i in range(num_normal_groups):
                start_idx = i * max_tables_per_group
                end_idx = min((i + 1) * max_tables_per_group, len(normal_tables))
                group_indices = normal_tables.iloc[start_idx:end_idx].index

                df.loc[group_indices, 'pipeline_group'] = global_pipeline_group_id
                df.loc[group_indices, 'gateway'] = gateway_id

                print(f"    Created pipeline_group {global_pipeline_group_id} with {len(group_indices)} tables")
                global_pipeline_group_id += 1

    # Reorder columns to match expected output format
    output_columns = ['source_database', 'source_schema', 'source_table_name',
                     'target_catalog', 'target_schema', 'target_table_name',
                     'gateway_catalog', 'gateway_schema',
                     'gateway_worker_type', 'gateway_driver_type',
                     'pipeline_group', 'gateway', 'connection_name', 'schedule']
    df_output = df[output_columns]

    # Print summary statistics
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total tables processed: {len(df_output)}")
    print(f"Total databases: {df_output['source_database'].nunique()}")
    print(f"Total gateways: {df_output['gateway'].nunique()}")
    print(f"Total pipeline groups: {df_output['pipeline_group'].nunique()}")
    print("\nBreakdown by database:")
    for db in df_output['source_database'].unique():
        db_data = df_output[df_output['source_database'] == db]
        print(f"  {db}:")
        print(f"    - Tables: {len(db_data)}")
        print(f"    - Gateway: {db_data['gateway'].iloc[0]}")
        print(f"    - Pipeline groups: {db_data['pipeline_group'].nunique()}")
    print("="*60)

    return df_output


if __name__ == "__main__":
    # Example usage - modify these parameters as needed

    # Step 1: Load input CSV
    input_df = load_input_csv('examples/example_config.csv')

    # Step 2: Define required and optional columns for SQL Server
    required_columns = [
        'source_database', 'source_schema', 'source_table_name',
        'target_catalog', 'target_schema', 'target_table_name'
    ]
    optional_columns = {
        'priority_flag': 0,
        'connection_name': 'conn_1',
        'gateway_catalog': None,  # Will be set to target_catalog if None
        'gateway_schema': None,   # Will be set to target_schema if None
        'gateway_worker_type': None,  # None for serverless
        'gateway_driver_type': None,  # None for serverless
        'schedule': '*/15 * * * *'
    }

    # Step 3: Normalize and validate configuration
    normalized_df = process_input_config(
        df=input_df,
        required_columns=required_columns,
        optional_columns=optional_columns
    )

    # Handle gateway_catalog and gateway_schema defaults (use target values if None)
    # Note: pandas None/NaN should be handled in process_input_config, but for gateway
    # columns we use target values as the actual default
    if 'gateway_catalog' in normalized_df.columns:
        mask = normalized_df['gateway_catalog'].isna()
        normalized_df.loc[mask, 'gateway_catalog'] = normalized_df.loc[mask, 'target_catalog']

    if 'gateway_schema' in normalized_df.columns:
        mask = normalized_df['gateway_schema'].isna()
        normalized_df.loc[mask, 'gateway_schema'] = normalized_df.loc[mask, 'target_schema']

    # Step 4: Generate pipeline configuration
    # Note: CSV can contain connection_name, gateway_catalog, gateway_schema,
    #       gateway_worker_type, gateway_driver_type per row
    output_df = generate_pipeline_config(
        df=normalized_df,
        max_tables_per_group=1000
    )

    # Write output to CSV
    output_csv_path = 'examples/output_config.csv'
    output_df.to_csv(output_csv_path, index=False)
    print(f"\nOutput written to: {output_csv_path}")
