import pandas as pd


def generate_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_group: int = 250,
    default_connection_name: str,
    default_schedule: str = "*/15 * * * *",
    default_gateway_worker_type: str,
    default_gateway_driver_type: str
):
    """
    Generate pipeline configuration from a list of source tables.

    This script groups tables into pipeline_groups with the following logic:
    - Each source_database gets its own set of pipelines (gateways)
    - Tables are grouped into pipeline_groups with a maximum of max_tables_per_group per group
    - Priority tables (priority_flag=1) are placed in separate pipeline_groups from non-priority tables

    Args:
        df (pd.DataFrame): Input dataframe with columns:
            - source_database: Source database name
            - source_schema: Source schema name
            - source_table_name: Source table name
            - target_catalog: Target Databricks catalog
            - target_schema: Target Databricks schema
            - target_table_name: Target table name
            - priority_flag: 1 for priority tables, 0 for normal tables (optional)
            - connection_name: Databricks connection name (optional, will use default if not present)
            - gateway_catalog: Catalog for gateway storage (optional, defaults to target_catalog)
            - gateway_schema: Schema for gateway storage (optional, defaults to target_schema)
            - gateway_worker_type: Worker node type (optional, defaults to None for serverless)
            - gateway_driver_type: Driver node type (optional, defaults to None for serverless)
        max_tables_per_group (int): Maximum tables per pipeline group (default: 1000)
        default_connection_name (str): Default connection name if not in CSV (default: "conn_1")
        default_schedule (str): Default cron schedule (default: "*/15 * * * *")
        default_gateway_worker_type (str): Default worker node type if not in CSV (default: None)
        default_gateway_driver_type (str): Default driver node type if not in CSV (default: None)

    Returns:
        pd.DataFrame: The generated configuration dataframe with additional columns:
            - gateway_catalog: Catalog for gateway storage
            - gateway_schema: Schema for gateway storage
            - gateway_worker_type: Worker node type for cluster
            - gateway_driver_type: Driver node type for cluster
            - pipeline_group: Pipeline group identifier
            - gateway: Gateway identifier
            - connection_name: Databricks connection name
            - schedule: Cron schedule expression
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Validate required columns
    required_columns = ['source_database', 'source_schema', 'source_table_name',
                       'target_catalog', 'target_schema', 'target_table_name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Add priority_flag column if not present
    if 'priority_flag' not in df.columns:
        print("Warning: 'priority_flag' column not found. Setting all tables to priority_flag=0")
        df['priority_flag'] = 0

    # Add connection_name column if not present
    if 'connection_name' not in df.columns:
        print(f"Warning: 'connection_name' column not found. Using default: {default_connection_name}")
        df['connection_name'] = default_connection_name

    # Add gateway_catalog column if not present (default to target_catalog)
    if 'gateway_catalog' not in df.columns:
        print("Warning: 'gateway_catalog' column not found. Using target_catalog as default")
        df['gateway_catalog'] = df['target_catalog']

    # Add gateway_schema column if not present (default to target_schema)
    if 'gateway_schema' not in df.columns:
        print("Warning: 'gateway_schema' column not found. Using target_schema as default")
        df['gateway_schema'] = df['target_schema']

    # Add gateway_worker_type column if not present
    if 'gateway_worker_type' not in df.columns:
        if default_gateway_worker_type:
            print(f"Warning: 'gateway_worker_type' column not found. Using default: {default_gateway_worker_type}")
        df['gateway_worker_type'] = default_gateway_worker_type

    # Add gateway_driver_type column if not present
    if 'gateway_driver_type' not in df.columns:
        if default_gateway_driver_type:
            print(f"Warning: 'gateway_driver_type' column not found. Using default: {default_gateway_driver_type}")
        df['gateway_driver_type'] = default_gateway_driver_type

    # Initialize new columns
    df['pipeline_group'] = 0
    df['gateway'] = 0
    df['schedule'] = default_schedule

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

    # Load input CSV into a dataframe
    input_df = pd.read_csv('examples/example_config.csv')

    # Generate pipeline configuration
    # Note: CSV can contain connection_name, gateway_catalog, gateway_schema,
    #       gateway_worker_type, gateway_driver_type per row
    output_df = generate_pipeline_config(
        df=input_df,
        max_tables_per_group=1000,
        default_connection_name='conn_1',
        default_schedule='*/15 * * * *',
        default_gateway_worker_type=None,      # None for serverless
        default_gateway_driver_type=None       # None for serverless
    )

    # Write output to CSV
    output_csv_path = 'examples/output_config.csv'
    output_df.to_csv(output_csv_path, index=False)
    print(f"\nOutput written to: {output_csv_path}")
