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

# Import from local modules
from load_balancing.load_balancer import load_input_csv, generate_pipeline_config
from deployment.connector_settings_generator import generate_yaml_files


def run_complete_pipeline_generation(
    input_csv: str,
    project_name: str = "sfdc_ingestion",
    default_connection_name: str = "sfdc_connection",
    default_schedule: str = "*/15 * * * *",
    workspace_host: str = None,
    output_dir: str = "dab_deployment",
    output_config: str = "dab_deployment/generated_config.csv"
):
    """
    Complete pipeline generation process from Salesforce objects to YAML files.

    Pipeline grouping is based on prefix + priority combinations from the input CSV.
    Each unique (prefix, priority) pair becomes a separate pipeline.

    Creates a complete DAB structure with:
    - databricks.yml (root configuration with variables)
    - resources/sfdc_pipeline.yml (pipeline and job definitions)

    Args:
        input_csv (str): Path to input CSV with Salesforce objects (required)
            Must contain: source_database, source_schema, source_table_name,
                         target_catalog, target_schema, target_table_name,
                         prefix, priority
        project_name (str): Project name for the bundle (default: "sfdc_ingestion")
        default_connection_name (str): Default Salesforce connection name (default: "sfdc_connection")
        default_schedule (str): Default cron schedule (default: "*/15 * * * *")
        workspace_host (str): Workspace host URL (optional, can be updated later in databricks.yml)
        output_dir (str): Output directory for DAB project (default: "dab_deployment")
        output_config (str): Output path for intermediate configuration CSV

    Returns:
        pd.DataFrame: The pipeline configuration dataframe
    """
    print("="*80)
    print("SALESFORCE PIPELINE GENERATION")
    print("="*80)

    # Step 1: Load input CSV
    print(f"\n[Step 1/3] Loading input CSV: {input_csv}")
    input_df = load_input_csv(input_csv)
    print(f"  ✓ Loaded {len(input_df)} Salesforce objects")

    # Step 2: Generate pipeline configuration (prefix + priority grouping)
    print(f"\n[Step 2/3] Generating pipeline configuration using prefix + priority")
    print(f"  - Default connection: {default_connection_name}")
    print(f"  - Default schedule: {default_schedule}")

    pipeline_config_df = generate_pipeline_config(
        df=input_df,
        default_connection_name=default_connection_name,
        default_schedule=default_schedule
    )

    print(f"\n  ✓ Created {pipeline_config_df['pipeline_group'].nunique()} pipelines")
    print(f"  ✓ Configured {len(pipeline_config_df)} Salesforce objects")

    # Save intermediate configuration
    os.makedirs(os.path.dirname(output_config), exist_ok=True)
    pipeline_config_df.to_csv(output_config, index=False)
    print(f"  ✓ Saved configuration to: {output_config}")

    # Step 3: Generate YAML files (databricks.yml + resources/sfdc_pipeline.yml)
    print(f"\n[Step 3/3] Generating Databricks Asset Bundle YAML files")
    print(f"  - Project name: {project_name}")
    print(f"  - Output directory: {output_dir}")
    if workspace_host:
        print(f"  - Workspace host: {workspace_host}")

    # Get connection name from first row (assumes all use same connection)
    connection_name = pipeline_config_df['connection_name'].iloc[0]

    generate_yaml_files(
        df=pipeline_config_df,
        connection_name=connection_name,
        project_name=project_name,
        workspace_host=workspace_host,
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

  # With custom connection and schedule
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --connection my_sfdc_conn \\
    --schedule "*/30 * * * *"

  # With custom project name and workspace
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name my_sfdc_project \\
    --workspace-host https://my-workspace.cloud.databricks.com \\
    --output-dir dab_project
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
        '--connection',
        type=str,
        default='sfdc_connection',
        help='Default Salesforce connection name (default: sfdc_connection)'
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

    # Run the complete pipeline generation
    result_df = run_complete_pipeline_generation(
        input_csv=args.input_csv,
        project_name=args.project_name,
        default_connection_name=args.connection,
        default_schedule=args.schedule,
        workspace_host=args.workspace_host,
        output_dir=args.output_dir,
        output_config=args.output_config
    )
