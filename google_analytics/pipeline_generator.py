#!/usr/bin/env python3
"""
Google Analytics 4 Pipeline Generation - Unified Runner

This script orchestrates the complete GA4 pipeline configuration and YAML generation process.

Two modes:
1. CSV Mode: Read properties from input CSV with prefix/priority
2. Auto-discover Mode: Query BigQuery and auto-balance (uses existing auto_balance_ga4.py)

Usage:
    # CSV mode with prefix+priority grouping
    python pipeline_generator.py --csv input.csv

    # Auto-discover mode with bin-packing
    python pipeline_generator.py --auto-discover --pipelines 3

    # Full pipeline with custom output
    python pipeline_generator.py --csv input.csv --output-yaml resources/ga4_pipeline.yml

Example:
    python pipeline_generator.py --csv load_balancing/examples/example_config.csv
"""

import argparse
import sys
import subprocess
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
    project_name: str,
    output_dir: str,
    targets: dict,
    output_config: str = None,
    default_values: dict = None,
    override_input_config: dict = None
):
    """
    Complete pipeline generation process from GA4 property list to YAML files.

    Args:
        df (pd.DataFrame): Input DataFrame with GA4 properties (required)
            Must contain: source_catalog, source_schema, tables,
                         target_catalog, target_schema,
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

    # Step 2: Generate pipeline configuration (prefix+priority grouping)
    print(f"\n[Step 2/3] Generating pipeline configuration with prefix+priority grouping")

    pipeline_config_df = generate_pipeline_config(
        df=normalized_df
    )

    # Step 3: Generate YAML files
    print(f"\n[Step 3/3] Generating Databricks Asset Bundle YAML files")
    print(f"  - Project name: {project_name}")
    print(f"  - Output directory: {output_dir}")

    # Print target environment info
    print(f"  - Target environments: {', '.join(targets.keys())}")

    generate_yaml_files(
        df=pipeline_config_df,
        project_name=project_name,
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


def run_csv_mode(
    input_csv: str,
    output_config: str = None,
    output_yaml: str = "deployment/resources/ga4_pipeline.yml",
    schedule: str = "0 */6 * * *"
):
    """
    Run pipeline generation from CSV with prefix+priority grouping.

    Args:
        input_csv: Input CSV with GA4 properties
        output_config: Output CSV with pipeline_group added
        output_yaml: Output YAML file path
        schedule: Default schedule for pipelines
    """
    print("="*80)
    print("GA4 PIPELINE GENERATION - CSV MODE")
    print("="*80)
    print()

    # Default output config path
    if not output_config:
        output_config = input_csv.replace('.csv', '_config.csv')

    print(f"Input CSV: {input_csv}")
    print(f"Output Config: {output_config}")
    print(f"Output YAML: {output_yaml}")
    print()

    # Step 1: Generate pipeline configuration with prefix+priority grouping
    print("Step 1: Generating pipeline configuration...")
    print("-"*80)
    cmd = [
        sys.executable,
        "load_balancing/generate_pipeline_config.py",
        input_csv,
        output_config,
        "--schedule", schedule
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("\n✗ Failed to generate pipeline configuration")
        sys.exit(1)

    # Step 2: Generate DAB YAML from configuration
    print("\nStep 2: Generating Databricks Asset Bundle YAML...")
    print("-"*80)
    cmd = [
        sys.executable,
        "deployment/generate_dab_yaml.py",
        output_config,
        "--output", output_yaml
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("\n✗ Failed to generate YAML")
        sys.exit(1)

    print("\n" + "="*80)
    print("✓ PIPELINE GENERATION COMPLETE")
    print("="*80)
    print()
    print("Generated files:")
    print(f"  1. Configuration: {output_config}")
    print(f"  2. YAML: {output_yaml}")
    print()
    print("Next steps:")
    print(f"  1. Review YAML: {output_yaml}")
    print(f"  2. Update GA4 connection name if needed")
    print(f"  3. Deploy: cd ga4 && databricks bundle deploy -t dev")
    print()


def run_auto_discover_mode(
    num_pipelines: int = 3,
    output_config: str = "dab/examples/auto_balanced_config.csv",
    output_yaml: str = "deployment/resources/ga4_pipeline.yml",
    schedule: str = "0 */6 * * *"
):
    """
    Run pipeline generation with auto-discovery and bin-packing.

    Uses the existing auto_balance_ga4.py script to query BigQuery
    and distribute properties across pipelines using FFD algorithm.

    Args:
        num_pipelines: Number of pipelines to create
        output_config: Output CSV path
        output_yaml: Output YAML file path
        schedule: Default schedule for pipelines
    """
    print("="*80)
    print("GA4 PIPELINE GENERATION - AUTO-DISCOVER MODE")
    print("="*80)
    print()

    print(f"Number of pipelines: {num_pipelines}")
    print(f"Output Config: {output_config}")
    print(f"Output YAML: {output_yaml}")
    print()

    # Step 1: Run auto-balance script
    print("Step 1: Auto-discovering properties and balancing...")
    print("-"*80)
    cmd = [
        sys.executable,
        "dab/auto_balance_ga4.py",
        "--pipelines", str(num_pipelines),
        "--output", output_config,
        "--schedule", schedule
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("\n✗ Failed to auto-balance properties")
        sys.exit(1)

    # Step 2: Generate DAB YAML from balanced configuration
    print("\nStep 2: Generating Databricks Asset Bundle YAML...")
    print("-"*80)
    cmd = [
        sys.executable,
        "deployment/generate_dab_yaml.py",
        output_config,
        "--output", output_yaml
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("\n✗ Failed to generate YAML")
        sys.exit(1)

    print("\n" + "="*80)
    print("✓ AUTO-DISCOVER PIPELINE GENERATION COMPLETE")
    print("="*80)
    print()
    print("Generated files:")
    print(f"  1. Balanced Configuration: {output_config}")
    print(f"  2. YAML: {output_yaml}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Unified GA4 pipeline generation runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # CSV mode with prefix+priority grouping
  python pipeline_generator.py --csv input.csv

  # Auto-discover mode with bin-packing
  python pipeline_generator.py --auto-discover --pipelines 3

  # Custom outputs
  python pipeline_generator.py --csv input.csv --output-yaml resources/custom.yml

Modes:
  1. CSV Mode (--csv): Read properties from CSV with prefix/priority columns
     - Groups by prefix+priority (e.g., business_unit1_01)
     - Each unique (prefix, priority) = one pipeline

  2. Auto-discover Mode (--auto-discover): Query BigQuery and auto-balance
     - Uses FFD bin-packing algorithm
     - Distributes properties across N pipelines by size
     - Pipeline groups are numeric (1, 2, 3...)
        """
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        '--csv',
        metavar='FILE',
        help='CSV mode: Input CSV with GA4 properties (prefix+priority)'
    )
    mode_group.add_argument(
        '--auto-discover',
        action='store_true',
        help='Auto-discover mode: Query BigQuery and auto-balance'
    )

    parser.add_argument(
        '--pipelines', '-p',
        type=int,
        default=3,
        help='Number of pipelines (auto-discover mode only, default: 3)'
    )
    parser.add_argument(
        '--output-config', '-c',
        help='Output configuration CSV path'
    )
    parser.add_argument(
        '--output-yaml', '-y',
        default='deployment/resources/ga4_pipeline.yml',
        help='Output YAML path (default: deployment/resources/ga4_pipeline.yml)'
    )
    parser.add_argument(
        '--schedule', '-s',
        default='0 */6 * * *',
        help='Default cron schedule (default: 0 */6 * * *)'
    )

    args = parser.parse_args()

    try:
        if args.csv:
            # CSV mode
            run_csv_mode(
                input_csv=args.csv,
                output_config=args.output_config,
                output_yaml=args.output_yaml,
                schedule=args.schedule
            )
        elif args.auto_discover:
            # Auto-discover mode
            run_auto_discover_mode(
                num_pipelines=args.pipelines,
                output_config=args.output_config or "dab/examples/auto_balanced_config.csv",
                output_yaml=args.output_yaml,
                schedule=args.schedule
            )

    except KeyboardInterrupt:
        print("\n\n✗ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
