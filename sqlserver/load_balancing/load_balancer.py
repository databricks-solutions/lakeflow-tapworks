import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utilities import process_input_config, generate_database_pipeline_config, load_input_csv


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
    # Use shared database pipeline configuration function
    return generate_database_pipeline_config(df, max_tables_per_gateway, max_tables_per_pipeline)


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
    max_tables_per_gateway = 250
    max_tables_per_pipeline = 250

    print("\n" + "="*80)
    print("SQL SERVER PIPELINE CONFIGURATION")
    print("="*80)
    print(f"Max tables per gateway: {max_tables_per_gateway}")
    print(f"Max tables per pipeline: {max_tables_per_pipeline}")

    output_df = generate_pipeline_config(
        df=normalized_df,
        max_tables_per_gateway=max_tables_per_gateway,
        max_tables_per_pipeline=max_tables_per_pipeline
    )

    # Print detailed breakdown
    for project_name, project_group in output_df.groupby('project_name'):
        print(f"\n{'='*60}")
        print(f"Processing project: {project_name} ({len(project_group)} tables)")
        print(f"{'='*60}")

        for prefix in sorted(project_group['prefix'].unique()):
            for priority in sorted(project_group[project_group['prefix'] == prefix]['priority'].unique()):
                group_df = project_group[(project_group['prefix'] == prefix) & (project_group['priority'] == priority)]
                base_group = f"{prefix}_{priority}"
                print(f"\n  Group: {base_group} ({len(group_df)} tables)")

                for gateway in sorted(group_df['gateway'].unique()):
                    gateway_tables = group_df[group_df['gateway'] == gateway]
                    print(f"    Gateway: {gateway} ({len(gateway_tables)} tables)")

                    for pipeline in sorted(gateway_tables['pipeline_group'].unique()):
                        pipeline_tables = gateway_tables[gateway_tables['pipeline_group'] == pipeline]
                        print(f"      Pipeline: {pipeline} ({len(pipeline_tables)} tables)")

    # Print summary statistics
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total tables processed: {len(output_df)}")
    print(f"Total projects: {output_df['project_name'].nunique()}")

    print("\nBreakdown by project:")
    for project in output_df['project_name'].unique():
        project_data = output_df[output_df['project_name'] == project]
        print(f"\n  {project}:")
        print(f"    - Tables: {len(project_data)}")
        print(f"    - Gateways: {project_data['gateway'].nunique()}")
        print(f"    - Pipelines: {project_data['pipeline_group'].nunique()}")

        # Show group breakdown within project
        for prefix in project_data['prefix'].unique():
            for priority in project_data[project_data['prefix'] == prefix]['priority'].unique():
                group_data = project_data[(project_data['prefix'] == prefix) & (project_data['priority'] == priority)]
                print(f"      • {prefix}_{priority}: {len(group_data)} tables, {group_data['gateway'].nunique()} gateways, {group_data['pipeline_group'].nunique()} pipelines")
    print("="*80)

    # Write output to CSV
    output_csv_path = 'examples/output_config.csv'
    output_df.to_csv(output_csv_path, index=False)
    print(f"\n✓ Output written to: {output_csv_path}")
