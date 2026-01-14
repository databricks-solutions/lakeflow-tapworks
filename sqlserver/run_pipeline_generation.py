"""
Unified pipeline generation script that combines load balancing and YAML generation.

This script demonstrates the complete two-part process:
1. Load balancing: Groups tables into pipeline configurations
2. YAML generation: Creates Databricks Asset Bundle YAML files
"""

import pandas as pd
import os

# Import from local modules
from load_balancing.generate_pipeline_config import generate_pipeline_config
from deployment.generate_dab_yaml import generate_yaml_files


def run_complete_pipeline_generation(
    input_csv: str,
    project_name: str,
    output_dir: str,
    workspace_host: str,
    root_path: str,
    default_connection_name: str = "conn_1",
    max_tables_per_group: int = 250,
    default_schedule: str = "*/15 * * * *",
    default_gateway_worker_type: str = None,
    default_gateway_driver_type: str = None
):
    """
    Complete pipeline generation process from source table list to YAML files.

    Args:
        input_csv (str): Path to input CSV with source table list
        project_name (str): Project name prefix for all resources
        output_dir (str): Output directory for DAB project
        workspace_host (str): Workspace host URL
        root_path (str): Root path for bundle deployment
        default_connection_name (str): Default connection name if not in CSV (default: "conn_1")
        max_tables_per_group (int): Maximum tables per pipeline group (default: 250)
        default_schedule (str): Default cron schedule (default: "*/15 * * * *")
        default_gateway_worker_type (str): Default worker node type if not in CSV (default: None)
        default_gateway_driver_type (str): Default driver node type if not in CSV (default: None)

    Returns:
        pd.DataFrame: The pipeline configuration dataframe

    Note:
        - All gateway settings are read from the input CSV:
          connection_name, gateway_catalog, gateway_schema, gateway_worker_type, gateway_driver_type
        - If not present in CSV, defaults are used
        - Settings can vary per source_database group
    """
    print("="*80)
    print("STARTING COMPLETE PIPELINE GENERATION PROCESS")
    print("="*80)

    # Step 1: Load input CSV
    print(f"\n[Step 1/3] Loading input CSV: {input_csv}")
    input_df = pd.read_csv(input_csv)
    print(f"  ✓ Loaded {len(input_df)} tables from {input_df['source_database'].nunique()} databases")

    # Step 2: Generate pipeline configuration (load balancing)
    print(f"\n[Step 2/3] Generating pipeline configuration with load balancing")
    print(f"  - Max tables per group: {max_tables_per_group}")
    print(f"  - Default connection name: {default_connection_name}")
    print(f"  - Default schedule: {default_schedule}")

    pipeline_config_df = generate_pipeline_config(
        df=input_df,
        default_connection_name=default_connection_name,
        default_gateway_worker_type=default_gateway_worker_type,
        default_gateway_driver_type=default_gateway_driver_type,
        max_tables_per_group=max_tables_per_group,
        default_schedule=default_schedule
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
  python run_pipeline_generation.py \\
    --input-csv load_balancing/examples/example_config.csv \\
    --project-name my_project \\
    --workspace-host https://my-workspace.cloud.databricks.com \\
    --root-path '/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}'

  # With custom connection and node types
  python run_pipeline_generation.py \\
    --input-csv my_config.csv \\
    --project-name prod_ingestion \\
    --connection my_sql_conn \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --root-path '/Users/user/.bundle/${bundle.name}/${bundle.target}' \\
    --worker-type m5d.large \\
    --driver-type c5a.8xlarge

  # With custom output and table limits
  python run_pipeline_generation.py \\
    --input-csv my_config.csv \\
    --project-name my_project \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --root-path '/Users/user/.bundle/${bundle.name}/${bundle.target}' \\
    --output-dir custom_output \\
    --max-tables 500
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
        '--connection',
        type=str,
        default='conn_1',
        help='Default connection name if not in CSV (default: conn_1)'
    )
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

    # Run the complete pipeline generation
    result_df = run_complete_pipeline_generation(
        input_csv=args.input_csv,
        project_name=args.project_name,
        output_dir=args.output_dir,
        workspace_host=args.workspace_host,
        root_path=args.root_path,
        default_connection_name=args.connection,
        max_tables_per_group=args.max_tables,
        default_schedule=args.schedule,
        default_gateway_worker_type=args.worker_type,
        default_gateway_driver_type=args.driver_type
    )

    # Optional: Save the intermediate pipeline configuration for reference
    output_csv = os.path.join(args.output_dir, 'generated_config.csv')
    result_df.to_csv(output_csv, index=False)
    print(f"\n✓ Intermediate configuration saved to: {output_csv}")
