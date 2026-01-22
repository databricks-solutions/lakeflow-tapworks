#!/usr/bin/env python3
"""
Unified pipeline generation script for Google Analytics 4 that combines configuration and YAML generation.

This script demonstrates the complete two-part process:
1. Pipeline configuration: Groups GA4 properties by prefix + priority
2. YAML generation: Creates Databricks Asset Bundle YAML files

Note: Google Analytics 4 is a SaaS connector and does NOT require gateways.
"""

import sys
import pandas as pd
import os
from pathlib import Path

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
    max_tables_per_pipeline: int = 250,
    output_config: str = None,
    default_values: dict = None,
    override_input_config: dict = None
):
    """
    Complete pipeline generation process from GA4 property list to YAML files.

    Pipeline grouping is based on prefix + priority combinations from the input DataFrame.
    Each unique (prefix, priority) pair becomes a separate pipeline, with automatic splitting
    if the group exceeds max_tables_per_pipeline.

    Args:
        df (pd.DataFrame): Input DataFrame with GA4 properties (required)
            Must contain: source_catalog, source_schema, tables,
                         target_catalog, target_schema,
                         connection_name (all required)
            Optional: project_name, prefix, priority
                - project_name: can be set via default_values or override_input_config
                - prefix: defaults to project_name if missing/empty
                - priority: defaults to "01" if missing/empty
        output_dir (str): Output directory for DAB project(s)
        targets (dict): Target environments configuration dict (required)
            Format: {'env_name': {'workspace_host': '...'}, ...}
            Supports any number of environments (dev, staging, qa, prod, etc.)
        max_tables_per_pipeline (int): Maximum properties per pipeline (default: 250)
            Groups exceeding this will be split into multiple pipelines (e.g., _g01, _g02)
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
        ...     targets={'dev': {'workspace_host': 'https://workspace.com'}},
        ...     default_values={'project_name': 'my_project'}
        ... )

        >>> # Multiple environments with project_name via override_input_config
        >>> run_complete_pipeline_generation(
        ...     df=df,
        ...     output_dir='output',
        ...     targets={
        ...         'dev': {'workspace_host': 'https://dev.databricks.com'},
        ...         'staging': {'workspace_host': 'https://staging.databricks.com'},
        ...         'prod': {'workspace_host': 'https://prod.databricks.com'}
        ...     },
        ...     override_input_config={'project_name': 'my_project'}
        ... )

    Note:
        - connection_name is a required column in the DataFrame
        - Properties are grouped by prefix+priority combinations
        - Each unique (prefix, priority) pair creates a separate pipeline
    """
    print("="*80)
    print("STARTING COMPLETE GA4 PIPELINE GENERATION PROCESS")
    print("="*80)

    # Step 1: Normalize and validate configuration
    print(f"\n[Step 1/3] Normalizing configuration")
    print(f"  - Input rows: {len(df)}")
    # print(f"  - Default schedule: {default_schedule}")

    # Define required columns for Google Analytics
    required_columns = [
        'source_catalog', 'source_schema', 'tables',
        'target_catalog', 'target_schema',
        'connection_name'
    ]

    # Add default project_name if not provided
    built_in_defaults = {'project_name': 'ga4_ingestion'}
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

    # Step 2: Generate pipeline configuration (prefix+priority grouping)
    print(f"\n[Step 2/3] Generating pipeline configuration with prefix+priority grouping")

    pipeline_config_df = generate_pipeline_config(
        df=normalized_df,
        max_tables_per_pipeline=max_tables_per_pipeline
    )

    # Step 3: Generate YAML files
    print(f"\n[Step 3/3] Generating Databricks Asset Bundle YAML files")
    print(f"  - Output directory: {output_dir}")

    # Print target environment info
    print(f"  - Target environments: {', '.join(targets.keys())}")

    generate_yaml_files(
        df=pipeline_config_df,
        targets=targets,
        output_dir=output_dir
    )

    # Save intermediate config
    config_output_path = os.path.join(output_dir, 'generated_config.csv')
    pipeline_config_df.to_csv(config_output_path, index=False)
    print(f"\n✓ Intermediate configuration saved to: {config_output_path}")

    print("\n" + "="*80)
    print("GA4 PIPELINE GENERATION COMPLETE!")
    print("="*80)

    return pipeline_config_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Google Analytics 4 ingestion pipelines using prefix + priority grouping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with default example
  python pipeline_generator.py \\
    --input-csv examples/example_config.csv \\
    --project-name my_ga4_project \\
    --workspace-host https://my-workspace.cloud.databricks.com

  # With custom schedule and max tables per pipeline
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name ga4_ingestion \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --schedule "0 */12 * * *" \\
    --max-tables 300

  # With custom output directory
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name my_project \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --output-dir custom_output

Note: connection_name is required in the CSV file for each row.
        """
    )

    # Required arguments
    parser.add_argument(
        '--input-csv',
        type=str,
        required=True,
        help='Path to input CSV with GA4 properties (required)'
    )
    parser.add_argument(
        '--project-name',
        type=str,
        required=True,
        help='Project name for the bundle (required)'
    )
    parser.add_argument(
        '--workspace-host',
        type=str,
        required=True,
        help='Workspace host URL (required, e.g., https://workspace.cloud.databricks.com)'
    )

    # Optional arguments
    parser.add_argument(
        '--max-tables',
        type=int,
        default=250,
        help='Maximum properties per pipeline (default: 250)'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        default='0 */6 * * *',
        help='Default cron schedule (default: 0 */6 * * *)'
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
            'workspace_host': args.workspace_host
        }
    }

    # Build default values dict
    default_values = {
        'project_name': args.project_name
    }

    # Run the complete pipeline generation
    result_df = run_complete_pipeline_generation(
        df=input_df,
        output_dir=args.output_dir,
        targets=targets,
        max_tables_per_pipeline=args.max_tables,
        default_values=default_values
    )

    # Optional: Save the intermediate pipeline configuration for reference
    output_csv = os.path.join(args.output_dir, 'generated_config.csv')
    result_df.to_csv(output_csv, index=False)
    print(f"\n✓ Intermediate configuration saved to: {output_csv}")
