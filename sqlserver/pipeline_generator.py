"""
Unified pipeline generation script that combines load balancing and YAML generation.

This script demonstrates the complete two-part process:
1. Load balancing: Groups tables into pipeline configurations
2. YAML generation: Creates Databricks Asset Bundle YAML files
"""

import pandas as pd
import os
from pathlib import Path
import sys

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from utilities import process_input_config, load_input_csv

# Import from local modules
from load_balancing.load_balancer import generate_pipeline_config
from deployment.connector_settings_generator import generate_yaml_files


def run_complete_pipeline_generation(
    df: pd.DataFrame,
    project_name: str,
    output_dir: str,
    workspace_host: str,
    root_path: str,
    max_tables_per_group: int = 250,
    default_schedule: str = "*/15 * * * *",
    default_gateway_worker_type: str = None,
    default_gateway_driver_type: str = None
):
    """
    Complete pipeline generation process from source table list to YAML files.

    Args:
        df (pd.DataFrame): Input DataFrame with source table list (required)
            Must contain: source_database, source_schema, source_table_name,
                         target_catalog, target_schema, target_table_name,
                         connection_name (all required)
        project_name (str): Project name prefix for all resources
        output_dir (str): Output directory for DAB project
        workspace_host (str): Workspace host URL
        root_path (str): Root path for bundle deployment
        max_tables_per_group (int): Maximum tables per pipeline group (default: 250)
        default_schedule (str): Default cron schedule (default: "*/15 * * * *")
        default_gateway_worker_type (str): Default worker node type if not in CSV (default: None)
        default_gateway_driver_type (str): Default driver node type if not in CSV (default: None)

    Returns:
        pd.DataFrame: The pipeline configuration dataframe

    Note:
        - connection_name is a required column in the DataFrame. Each row must specify
          which SQL Server connection to use.
        - Gateway settings (gateway_catalog, gateway_schema, gateway_worker_type, gateway_driver_type)
          are optional and can vary per source_database group.
        - If gateway_catalog/gateway_schema are not provided, they default to target_catalog/target_schema.
    """
    print("="*80)
    print("STARTING COMPLETE PIPELINE GENERATION PROCESS")
    print("="*80)

    # Step 1: Normalize and validate configuration
    print(f"\n[Step 1/3] Normalizing configuration")
    print(f"  - Input rows: {len(df)}")
    print(f"  - Databases: {df['source_database'].nunique()}")
    print(f"  - Max tables per group: {max_tables_per_group}")
    print(f"  - Default schedule: {default_schedule}")

    # Define required and optional columns for SQL Server
    required_columns = [
        'source_database', 'source_schema', 'source_table_name',
        'target_catalog', 'target_schema', 'target_table_name',
        'connection_name'
    ]
    default_values = {
        'priority_flag': 0,
        'gateway_catalog': None,  # Will be set to target_catalog if None
        'gateway_schema': None,   # Will be set to target_schema if None
        'gateway_worker_type': default_gateway_worker_type,
        'gateway_driver_type': default_gateway_driver_type,
        'schedule': default_schedule
    }

    normalized_df = process_input_config(
        df=df,
        required_columns=required_columns,
        default_values=default_values
    )

    # Handle gateway_catalog and gateway_schema defaults (use target values if None)
    if 'gateway_catalog' in normalized_df.columns:
        mask = normalized_df['gateway_catalog'].isna()
        normalized_df.loc[mask, 'gateway_catalog'] = normalized_df.loc[mask, 'target_catalog']

    if 'gateway_schema' in normalized_df.columns:
        mask = normalized_df['gateway_schema'].isna()
        normalized_df.loc[mask, 'gateway_schema'] = normalized_df.loc[mask, 'target_schema']

    # Step 2: Generate pipeline configuration (load balancing)
    print(f"\n[Step 2/3] Generating pipeline configuration with load balancing")

    pipeline_config_df = generate_pipeline_config(
        df=normalized_df,
        max_tables_per_group=max_tables_per_group
    )

    # Step 3: Generate YAML files
    print(f"\n[Step 3/3] Generating Databricks Asset Bundle YAML files")
    print(f"  - Project name: {project_name}")
    print(f"  - Output directory: {output_dir}")
    print(f"  - Root path: {root_path}")

    generate_yaml_files(
        df=pipeline_config_df,
        project_name=project_name,
        output_dir=output_dir,
        workspace_host=workspace_host,
        root_path=root_path
    )

    print("\n" + "="*80)
    print("PIPELINE GENERATION COMPLETE!")
    print("="*80)
    print(f"\nNext steps:")
    print(f"  1. Review the generated DAB project:")
    print(f"     - {output_dir}/databricks.yml")
    print(f"     - {output_dir}/resources/gateways.yml")
    print(f"     - {output_dir}/resources/pipelines.yml")
    print(f"  2. Deploy using Databricks Asset Bundles:")
    print(f"     cd {output_dir}")
    print(f"     databricks bundle deploy -t <environment>")
    print("="*80)

    return pipeline_config_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate SQL Server ingestion pipelines with gateway configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with default example
  python pipeline_generator.py \\
    --input-csv load_balancing/examples/example_config.csv \\
    --project-name my_project \\
    --workspace-host https://my-workspace.cloud.databricks.com \\
    --root-path '/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}'

  # With custom node types
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name prod_ingestion \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --root-path '/Users/user/.bundle/${bundle.name}/${bundle.target}' \\
    --worker-type m5d.large \\
    --driver-type c5a.8xlarge

  # With custom output and table limits
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name my_project \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --root-path '/Users/user/.bundle/${bundle.name}/${bundle.target}' \\
    --output-dir custom_output \\
    --max-tables 500

Note:
  connection_name must be provided in the input CSV file for each row
        """
    )

    # Required arguments
    parser.add_argument(
        '--input-csv',
        type=str,
        required=True,
        help='Path to input CSV with source table list (required)'
    )
    parser.add_argument(
        '--project-name',
        type=str,
        required=True,
        help='Project name prefix for all resources (required)'
    )
    parser.add_argument(
        '--workspace-host',
        type=str,
        required=True,
        help='Workspace host URL (required, e.g., https://workspace.cloud.databricks.com)'
    )
    parser.add_argument(
        '--root-path',
        type=str,
        required=True,
        help='Root path for bundle deployment (required, e.g., /Users/user/.bundle/${bundle.name}/${bundle.target})'
    )

    # Optional arguments
    parser.add_argument(
        '--max-tables',
        type=int,
        default=250,
        help='Maximum tables per pipeline group (default: 250)'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        default='*/15 * * * *',
        help='Default cron schedule (default: */15 * * * *)'
    )
    parser.add_argument(
        '--worker-type',
        type=str,
        default=None,
        help='Default gateway worker node type if not in CSV (default: None for serverless)'
    )
    parser.add_argument(
        '--driver-type',
        type=str,
        default=None,
        help='Default gateway driver node type if not in CSV (default: None for serverless)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='dab_project',
        help='Output directory for DAB project (default: dab_project)'
    )

    args = parser.parse_args()

    # Load input CSV
    print(f"Loading input CSV: {args.input_csv}")
    input_df = load_input_csv(args.input_csv)

    # Run the complete pipeline generation
    result_df = run_complete_pipeline_generation(
        df=input_df,
        project_name=args.project_name,
        output_dir=args.output_dir,
        workspace_host=args.workspace_host,
        root_path=args.root_path,
        max_tables_per_group=args.max_tables,
        default_schedule=args.schedule,
        default_gateway_worker_type=args.worker_type,
        default_gateway_driver_type=args.driver_type
    )

    # Optional: Save the intermediate pipeline configuration for reference
    output_csv = os.path.join(args.output_dir, 'generated_config.csv')
    result_df.to_csv(output_csv, index=False)
    print(f"\n✓ Intermediate configuration saved to: {output_csv}")
