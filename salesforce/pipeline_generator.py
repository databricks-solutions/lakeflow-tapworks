"""
Unified pipeline generation script for Salesforce that combines configuration and YAML generation.

This script demonstrates the complete two-part process:
1. Pipeline configuration: Groups Salesforce objects by prefix + priority
2. YAML generation: Creates Databricks Asset Bundle YAML files

Note: Unlike SQL Server, Salesforce is a SaaS connector and does NOT require gateways.
"""

import pandas as pd
import os
import sys
from pathlib import Path

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
    targets: dict,
    output_config: str = None,
    default_values: dict = None,
    override_input_config: dict = None
):
    """
    Complete pipeline generation process from Salesforce objects to YAML files.

    Pipeline grouping is based on prefix + priority combinations from the input DataFrame.
    Each unique (prefix, priority) pair becomes a separate pipeline.

    Creates a complete DAB structure with:
    - databricks.yml (root configuration with variables)
    - resources/pipelines.yml (pipeline definitions)
    - resources/jobs.yml (job definitions)

    Args:
        df (pd.DataFrame): Input DataFrame with Salesforce objects (required)
            Must contain: source_database, source_schema, source_table_name,
                         target_catalog, target_schema, target_table_name,
                         prefix, priority, connection_name (all required)
            Optional: project_name (will use default if missing/empty)
        project_name (str): Default project name used when project_name column is missing or empty
        output_dir (str): Output directory for DAB project(s)
        targets (dict): Target environments configuration dict (required)
            Format: {'env_name': {'workspace_host': '...'}, ...}
            Supports any number of environments (dev, staging, qa, prod, etc.)
        output_config (str, optional): Output path for intermediate configuration CSV
        default_values (dict, optional): Column defaults to override built-in defaults
        override_input_config (dict, optional): Override specific columns for all rows

    Note:
        - Always creates separate DAB packages per unique project_name
        - Output structure: output/{project_name}/databricks.yml for each project

    Returns:
        pd.DataFrame: The pipeline configuration dataframe

    Example Usage:
        >>> # Single environment
        >>> run_complete_pipeline_generation(
        ...     df=df,
        ...     project_name='my_project',
        ...     output_dir='output',
        ...     targets={'dev': {'workspace_host': 'https://workspace.com'}}
        ... )

        >>> # Multiple environments
        >>> run_complete_pipeline_generation(
        ...     df=df,
        ...     project_name='my_project',
        ...     output_dir='output',
        ...     targets={
        ...         'dev': {'workspace_host': 'https://dev.databricks.com'},
        ...         'staging': {'workspace_host': 'https://staging.databricks.com'},
        ...         'prod': {'workspace_host': 'https://prod.databricks.com'}
        ...     }
        ... )

    Note:
        connection_name is a required column in the DataFrame. Each row must specify
        which Salesforce connection to use.
    """
    print("="*80)
    print("SALESFORCE PIPELINE GENERATION")
    print("="*80)

    # Step 1: Normalize and validate configuration
    print(f"\n[Step 1/3] Normalizing configuration")
    print(f"  - Input rows: {len(df)}")
    # print(f"  - Default schedule: {default_schedule}")

    # Define required columns for Salesforce
    required_columns = [
        'source_database', 'source_schema', 'source_table_name',
        'target_catalog', 'target_schema', 'target_table_name',
        'prefix', 'priority', 'connection_name'
    ]

    # Build default values - project_name is a default value
    built_in_defaults = {
        'project_name': project_name
    }

    if default_values:
        # User-provided defaults override built-in defaults
        final_defaults = {**built_in_defaults, **default_values}
    else:
        final_defaults = built_in_defaults

    normalized_df = process_input_config(
        df=df,
        required_columns=required_columns,
        default_values=final_defaults,
        override_input_config=override_input_config
    )

    # Step 2: Generate pipeline configuration (prefix + priority grouping)
    print(f"\n[Step 2/3] Generating pipeline configuration using prefix + priority")

    pipeline_config_df = generate_pipeline_config(df=normalized_df)

    print(f"\n  ✓ Created {pipeline_config_df['pipeline_group'].nunique()} pipelines")
    print(f"  ✓ Configured {len(pipeline_config_df)} Salesforce objects")

    # Save intermediate configuration
    if output_config is not None:
        os.makedirs(os.path.dirname(output_config), exist_ok=True)
        pipeline_config_df.to_csv(output_config, index=False)
        print(f"  ✓ Saved configuration to: {output_config}")

    # Step 3: Generate YAML files (databricks.yml + resources/sfdc_pipeline.yml)
    print(f"\n[Step 3/3] Generating Databricks Asset Bundle YAML files")
    print(f"  - Project name: {project_name}")
    print(f"  - Output directory: {output_dir}")
    print(f"  - Target environments: {', '.join(targets.keys())}")

    generate_yaml_files(
        df=pipeline_config_df,
        project_name=project_name,
        targets=targets,
        output_dir=output_dir
    )

    print("\n" + "="*80)
    print("SALESFORCE PIPELINE GENERATION COMPLETE!")
    print("="*80)

    return pipeline_config_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Salesforce ingestion pipelines using prefix + priority grouping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with default example
  python pipeline_generator.py --input-csv load_balancing/examples/example_config.csv

  # With custom schedule
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --schedule "*/30 * * * *"

  # With custom project name and workspace
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name my_sfdc_project \\
    --workspace-host https://my-workspace.cloud.databricks.com \\
    --output-dir dab_project

Note: connection_name is now a required column in the CSV file.
        """
    )
    parser.add_argument(
        '--input-csv',
        type=str,
        required=True,
        help='Path to input CSV with Salesforce objects (required)'
    )
    parser.add_argument(
        '--project-name',
        type=str,
        default='sfdc_ingestion',
        help='Project name for the bundle (default: sfdc_ingestion)'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        default='*/15 * * * *',
        help='Default cron schedule (default: */15 * * * *)'
    )
    parser.add_argument(
        '--workspace-host',
        type=str,
        default=None,
        help='Workspace host URL (optional, can be updated later in databricks.yml)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='dab_deployment',
        help='Output directory for DAB project (default: dab_deployment)'
    )
    parser.add_argument(
        '--output-config',
        type=str,
        default='dab_deployment/generated_config.csv',
        help='Output path for intermediate config CSV (default: dab_deployment/generated_config.csv)'
    )

    args = parser.parse_args()

    # Load input CSV
    print(f"Loading input CSV: {args.input_csv}")
    input_df = load_input_csv(args.input_csv)

    # Run the complete pipeline generation
    result_df = run_complete_pipeline_generation(
        df=input_df,
        project_name=args.project_name,
        # default_schedule=args.schedule,
        workspace_host=args.workspace_host,
        output_dir=args.output_dir,
        output_config=args.output_config
    )
