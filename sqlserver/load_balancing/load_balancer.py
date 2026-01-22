import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utilities import process_input_config


def generate_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_gateway: int = 250,
    max_tables_per_pipeline: int = 250
):
    """
    Generate pipeline configuration from a list of source tables.

    This function expects a clean DataFrame (output from process_input_config).

    This script groups tables into pipeline_groups with the following logic:
    - Each unique combination of (prefix, priority) becomes one base group
    - If group exceeds max_tables_per_gateway, split into multiple gateways
    - Within each gateway, if tables exceed max_tables_per_pipeline, split into multiple pipelines
    - Naming: prefix_priority_gw01_p01 (gateway 1, pipeline 1)

    Args:
        df (pd.DataFrame): Clean input dataframe (from process_input_config) with columns:
            - source_database, source_schema, source_table_name
            - target_catalog, target_schema, target_table_name
            - prefix, priority
            - connection_name, schedule (already validated)
            - gateway_catalog, gateway_schema (already validated)
            - gateway_worker_type, gateway_driver_type (already validated)
        max_tables_per_gateway (int): Maximum tables per gateway (default: 250)
        max_tables_per_pipeline (int): Maximum tables per pipeline within gateway (default: 250)

    Returns:
        pd.DataFrame: The generated configuration dataframe with additional columns:
            - pipeline_group: Pipeline group identifier
            - gateway: Gateway identifier (string format: prefix_priority_gw01)
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Initialize new columns
    df['pipeline_group'] = ''
    df['gateway'] = ''

    # Generate base group from prefix + priority
    df['base_group'] = df['prefix'].astype(str) + '_' + df['priority'].astype(str)

    print("\n" + "="*80)
    print("SQL SERVER PIPELINE CONFIGURATION")
    print("="*80)
    print(f"Max tables per gateway: {max_tables_per_gateway}")
    print(f"Max tables per pipeline: {max_tables_per_pipeline}")

    # Group by project_name first to ensure independent processing per project
    for project_name, project_group in df.groupby('project_name'):
        print(f"\n{'='*60}")
        print(f"Processing project: {project_name} ({len(project_group)} tables)")
        print(f"{'='*60}")

        # Group by base_group (prefix_priority) within this project
        for base_group, group_df in project_group.groupby('base_group'):
            print(f"\n  Group: {base_group} ({len(group_df)} tables)")

            # Step 1: Split into gateways if needed
            num_gateways = (len(group_df) - 1) // max_tables_per_gateway + 1

            for gw_idx in range(num_gateways):
                gw_start = gw_idx * max_tables_per_gateway
                gw_end = min((gw_idx + 1) * max_tables_per_gateway, len(group_df))
                gateway_tables = group_df.iloc[gw_start:gw_end]

                # Gateway naming
                if num_gateways > 1:
                    gateway_name = f"{base_group}_gw{gw_idx+1:02d}"
                else:
                    gateway_name = base_group

                print(f"    Gateway: {gateway_name} ({len(gateway_tables)} tables)")

                # Step 2: Split gateway into pipelines if needed
                num_pipelines = (len(gateway_tables) - 1) // max_tables_per_pipeline + 1

                for p_idx in range(num_pipelines):
                    p_start = p_idx * max_tables_per_pipeline
                    p_end = min((p_idx + 1) * max_tables_per_pipeline, len(gateway_tables))
                    pipeline_tables_indices = gateway_tables.iloc[p_start:p_end].index

                    # Pipeline naming
                    if num_pipelines > 1:
                        if num_gateways > 1:
                            pipeline_name = f"{base_group}_gw{gw_idx+1:02d}_g{p_idx+1:02d}"
                        else:
                            # Single gateway, multiple pipelines: use _g01, _g02 (consistent with SaaS)
                            pipeline_name = f"{base_group}_g{p_idx+1:02d}"
                    else:
                        pipeline_name = gateway_name

                    # Assign to dataframe
                    df.loc[pipeline_tables_indices, 'gateway'] = gateway_name
                    df.loc[pipeline_tables_indices, 'pipeline_group'] = pipeline_name

                    print(f"      Pipeline: {pipeline_name} ({len(pipeline_tables_indices)} tables)")

    # Drop temporary base_group column
    df = df.drop(columns=['base_group'])

    # Reorder columns to match expected output format
    output_columns = ['project_name', 'source_database', 'source_schema', 'source_table_name',
                     'target_catalog', 'target_schema', 'target_table_name',
                     'gateway_catalog', 'gateway_schema',
                     'gateway_worker_type', 'gateway_driver_type',
                     'prefix', 'priority', 'pipeline_group', 'gateway', 'connection_name', 'schedule']
    df_output = df[output_columns]

    # Print summary statistics
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total tables processed: {len(df_output)}")
    print(f"Total projects: {df_output['project_name'].nunique()}")

    print("\nBreakdown by project:")
    for project in df_output['project_name'].unique():
        project_data = df_output[df_output['project_name'] == project]
        print(f"\n  {project}:")
        print(f"    - Tables: {len(project_data)}")
        print(f"    - Prefix+Priority groups: {project_data['base_group'].nunique() if 'base_group' in project_data.columns else 'N/A'}")
        print(f"    - Gateways: {project_data['gateway'].nunique()}")
        print(f"    - Pipelines: {project_data['pipeline_group'].nunique()}")

        # Show group breakdown within project
        for prefix in project_data['prefix'].unique():
            for priority in project_data[project_data['prefix'] == prefix]['priority'].unique():
                group_data = project_data[(project_data['prefix'] == prefix) & (project_data['priority'] == priority)]
                print(f"      • {prefix}_{priority}: {len(group_data)} tables, {group_data['gateway'].nunique()} gateways, {group_data['pipeline_group'].nunique()} pipelines")
    print("="*80)

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
    default_values = {
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
        default_values=default_values
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
