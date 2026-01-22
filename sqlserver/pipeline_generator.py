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
    output_dir: str,
    targets: dict,
    max_tables_per_gateway: int = 250,
    max_tables_per_pipeline: int = 250,
    output_config: str = None,
    default_values: dict = None,
    override_input_config: dict = None
):
    """
    Complete pipeline generation process from source table list to YAML files.

    Pipeline grouping is based on prefix + priority combinations from the input DataFrame.
    Each unique (prefix, priority) pair becomes a base group, then:
    - Split into gateways if exceeds max_tables_per_gateway
    - Split into pipelines within each gateway if exceeds max_tables_per_pipeline

    Args:
        df (pd.DataFrame): Input DataFrame with source table list (required)
            Must contain: source_database, source_schema, source_table_name,
                         target_catalog, target_schema, target_table_name,
                         connection_name (all required)
            Optional: project_name, prefix, priority
                - project_name: can be set via default_values or override_input_config
                - prefix: defaults to project_name if missing/empty
                - priority: defaults to "01" if missing/empty
        output_dir (str): Output directory for DAB project(s)
        targets (dict): Target environments configuration dict (required)
            Format: {'env_name': {'workspace_host': '...', 'root_path': '...'}, ...}
            Supports any number of environments (dev, staging, qa, prod, etc.)
        max_tables_per_gateway (int): Maximum tables per gateway (default: 250)
        max_tables_per_pipeline (int): Maximum tables per pipeline within gateway (default: 250)
        output_config (str, optional): Output path for intermediate configuration CSV
        default_values (dict, optional): Column defaults (e.g., {'project_name': 'my_project'})
        override_input_config (dict, optional): Override specific columns for all rows

    Note:
        - Always creates separate DAB packages per unique project_name
        - Output structure: output/{project_name}/databricks.yml for each project
        - project_name must be provided via CSV, default_values, or override_input_config

    Returns:
        pd.DataFrame: The pipeline configuration dataframe

    Example Usage:
        >>> # Single environment with project_name via default_values
        >>> run_complete_pipeline_generation(
        ...     df=df,
        ...     output_dir='output',
        ...     targets={
        ...         'dev': {
        ...             'workspace_host': 'https://workspace.com',
        ...             'root_path': '/Users/user/.bundle/${bundle.name}/${bundle.target}'
        ...         }
        ...     },
        ...     default_values={'project_name': 'my_project'}
        ... )

        >>> # Multiple environments with project_name via override_input_config
        >>> run_complete_pipeline_generation(
        ...     df=df,
        ...     output_dir='output',
        ...     targets={
        ...         'dev': {
        ...             'workspace_host': 'https://dev.databricks.com',
        ...             'root_path': '/Users/dev/.bundle/${bundle.name}/${bundle.target}'
        ...         },
        ...         'prod': {
        ...             'workspace_host': 'https://prod.databricks.com',
        ...             'root_path': '/Workspace/prod/${bundle.name}'
        ...         }
        ...     },
        ...     override_input_config={'project_name': 'my_project'}
        ... )

    Note:
        - connection_name is a required column in the DataFrame
        - prefix and priority are optional (prefix defaults to project_name, priority defaults to "01")
        - Gateway settings (gateway_catalog, gateway_schema, gateway_worker_type, gateway_driver_type)
          are optional and can vary per row
        - If gateway_catalog/gateway_schema are not provided, they default to target_catalog/target_schema
        - Tables are grouped by prefix+priority, NOT by source_database
    """
    print("="*80)
    print("STARTING COMPLETE PIPELINE GENERATION PROCESS")
    print("="*80)

    # Step 1: Normalize and validate configuration
    print(f"\n[Step 1/3] Normalizing configuration")
    print(f"  - Input rows: {len(df)}")
    print(f"  - Max tables per gateway: {max_tables_per_gateway}")
    print(f"  - Max tables per pipeline: {max_tables_per_pipeline}")

    # Define required columns for SQL Server
    required_columns = [
        'source_database', 'source_schema', 'source_table_name',
        'target_catalog', 'target_schema', 'target_table_name',
        'connection_name'
    ]

    # Add default project_name if not provided
    built_in_defaults = {'project_name': 'sqlserver_ingestion'}
    final_defaults = {**built_in_defaults, **(default_values or {})}

    normalized_df = process_input_config(
        df=df,
        required_columns=required_columns,
        default_values=final_defaults,
        override_input_config=override_input_config
    )

    # Handle prefix and priority defaults
    # If prefix is missing or empty, use project_name
    if 'prefix' not in normalized_df.columns:
        normalized_df['prefix'] = normalized_df['project_name']
    else:
        mask = normalized_df['prefix'].isna() | (normalized_df['prefix'].astype(str).str.strip() == '')
        normalized_df.loc[mask, 'prefix'] = normalized_df.loc[mask, 'project_name']

    # If priority is missing or empty, use "01"
    if 'priority' not in normalized_df.columns:
        normalized_df['priority'] = '01'
    else:
        mask = normalized_df['priority'].isna() | (normalized_df['priority'].astype(str).str.strip() == '')
        normalized_df.loc[mask, 'priority'] = '01'

    # Handle gateway_catalog and gateway_schema defaults (use target values if None)
    if 'gateway_catalog' in normalized_df.columns:
        mask = normalized_df['gateway_catalog'].isna()
        normalized_df.loc[mask, 'gateway_catalog'] = normalized_df.loc[mask, 'target_catalog']

    if 'gateway_schema' in normalized_df.columns:
        mask = normalized_df['gateway_schema'].isna()
        normalized_df.loc[mask, 'gateway_schema'] = normalized_df.loc[mask, 'target_schema']

    # Step 2: Generate pipeline configuration (prefix + priority grouping with gateway/pipeline splitting)
    print(f"\n[Step 2/3] Generating pipeline configuration with prefix+priority grouping")

    pipeline_config_df = generate_pipeline_config(
        df=normalized_df,
        max_tables_per_gateway=max_tables_per_gateway,
        max_tables_per_pipeline=max_tables_per_pipeline
    )

    # Step 3: Generate YAML files
    print(f"\n[Step 3/3] Generating Databricks Asset Bundle YAML files")
    print(f"  - Output directory: {output_dir}")

    # Print target environment info
    print(f"  - Target environments: {', '.join(targets.keys())}")

    generate_yaml_files(
        df=pipeline_config_df,
        output_dir=output_dir,
        targets=targets
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
        '--max-tables-gateway',
        type=int,
        default=250,
        help='Maximum tables per gateway (default: 250)'
    )
    parser.add_argument(
        '--max-tables-pipeline',
        type=int,
        default=250,
        help='Maximum tables per pipeline within gateway (default: 250)'
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

    # Build targets dict from CLI arguments
    targets = {
        'dev': {
            'workspace_host': args.workspace_host,
            'root_path': args.root_path
        }
    }

    # Build default values dict
    default_values = {
        'project_name': args.project_name,
        'gateway_worker_type': args.worker_type,
        'gateway_driver_type': args.driver_type
    }

    # Run the complete pipeline generation
    result_df = run_complete_pipeline_generation(
        df=input_df,
        output_dir=args.output_dir,
        targets=targets,
        max_tables_per_gateway=args.max_tables_gateway,
        max_tables_per_pipeline=args.max_tables_pipeline,
        default_values=default_values
    )

    # Optional: Save the intermediate pipeline configuration for reference
    output_csv = os.path.join(args.output_dir, 'generated_config.csv')
    result_df.to_csv(output_csv, index=False)
    print(f"\n✓ Intermediate configuration saved to: {output_csv}")
