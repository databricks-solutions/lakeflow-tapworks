#!/usr/bin/env python3
"""
Google Analytics 4 Pipeline Configuration Generator

Reads GA4 configuration CSV with prefix and priority columns and generates
pipeline groupings based on prefix+priority combination.

Similar to SFDC pattern: Each unique (prefix, priority) combination creates
a separate pipeline group named "{prefix}_{priority}".

Input CSV Format:
    source_catalog,source_schema,tables,target_catalog,target_schema,prefix,priority,schedule

Output:
    Adds 'pipeline_group' column with format: {prefix}_{priority}

Usage:
    python generate_pipeline_config.py input.csv output.csv
    python generate_pipeline_config.py --help
"""

import pandas as pd
import argparse
import sys
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utilities import process_input_config, load_input_csv, generate_saas_pipeline_config


def generate_pipeline_config(
    df: pd.DataFrame,
    max_tables_per_pipeline: int = 250
) -> pd.DataFrame:
    """
    Generate pipeline configuration with prefix+priority grouping.

    This function expects a clean DataFrame (output from process_input_config).

    Logic:
    - Each unique combination of (prefix, priority) becomes one base pipeline group
    - If a group has more properties than max_tables_per_pipeline, it's split into sub-groups
    - Pipeline name format: {prefix}_{priority} or {prefix}_{priority}_g{NN} if split
    - Example: business_unit1_01_g01, business_unit1_01_g02, etc.

    Args:
        df: Clean input DataFrame (from process_input_config) with columns:
            - source_catalog, source_schema, tables
            - target_catalog, target_schema
            - prefix, priority
            - schedule (already validated)
        max_tables_per_pipeline (int): Maximum properties per pipeline (default: 250)
            If a prefix+priority group exceeds this, it will be split into multiple pipelines

    Returns:
        DataFrame with pipeline_group column added
    """
    # Use shared SaaS pipeline configuration function
    return generate_saas_pipeline_config(df, max_tables_per_pipeline)


def write_output_csv(df: pd.DataFrame, output_file: str):
    """Write configuration with pipeline_group to CSV"""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_file, index=False)
    print(f"Configuration saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate GA4 pipeline configuration with prefix+priority grouping',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate pipeline config from input CSV
  python generate_pipeline_config.py input.csv output.csv

  # Use custom default schedule
  python generate_pipeline_config.py input.csv output.csv --schedule "0 */12 * * *"

Input CSV Format:
  source_catalog,source_schema,tables,target_catalog,target_schema,prefix,priority,schedule
  my-gcp-project,analytics_123456789,events,ga4_connector,google_analytics,business_unit1,01,0 */6 * * *
  my-gcp-project,analytics_987654321,events,ga4_connector,google_analytics,business_unit1,02,0 */6 * * *

Output:
  Adds 'pipeline_group' column: business_unit1_01, business_unit1_02, etc.
        """
    )

    parser.add_argument(
        'input_csv',
        help='Input CSV file with GA4 properties'
    )
    parser.add_argument(
        'output_csv',
        nargs='?',
        default=None,
        help='Output CSV file with pipeline_group (default: input_config.csv)'
    )
    parser.add_argument(
        '--schedule', '-s',
        default='0 */6 * * *',
        help='Default cron schedule for pipelines (default: 0 */6 * * *)'
    )

    args = parser.parse_args()

    # Default output to same as input if not specified
    output_file = args.output_csv or args.input_csv.replace('.csv', '_config.csv')

    try:
        print("=" * 70)
        print("GA4 Pipeline Configuration Generator")
        print("=" * 70)
        print()

        # Step 1: Load input CSV
        df = load_input_csv(args.input_csv)

        # Step 2: Define required and optional columns for Google Analytics
        required_columns = [
            'source_catalog', 'source_schema', 'tables',
            'target_catalog', 'target_schema',
            'prefix', 'priority'
        ]
        default_values = {
            'schedule': args.schedule
        }

        # Step 3: Normalize and validate configuration
        normalized_df = process_input_config(
            df=df,
            required_columns=required_columns,
            default_values=default_values
        )

        # Step 4: Generate configuration with pipeline groups
        max_tables_per_pipeline = 250

        df_with_groups = generate_pipeline_config(normalized_df, max_tables_per_pipeline=max_tables_per_pipeline)

        # Print summary
        print(f"\nPipeline Configuration Summary:")
        print(f"  Total properties: {len(df_with_groups)}")
        print(f"  Unique prefixes: {df_with_groups['prefix'].nunique()}")
        print(f"  Unique priorities: {df_with_groups['priority'].nunique()}")
        print(f"  Total pipeline groups: {df_with_groups['pipeline_group'].nunique()}")
        print(f"  Max properties per pipeline: {max_tables_per_pipeline}")
        print()

        # Show grouping details
        print("Pipeline Groups:")
        for group_name in sorted(df_with_groups['pipeline_group'].unique()):
            group_df = df_with_groups[df_with_groups['pipeline_group'] == group_name]
            split_indicator = " (split)" if "_g" in group_name else ""
            print(f"  {group_name}{split_indicator}:")
            print(f"    Properties: {len(group_df)}")
            print(f"    Schemas: {', '.join(group_df['source_schema'].unique())}")
        print()

        # Write output
        write_output_csv(df_with_groups, output_file)

        print()
        print("=" * 70)
        print("Configuration generation complete!")
        print("=" * 70)
        print()
        print("Next steps:")
        print(f"  1. Review: {output_file}")
        print(f"  2. Generate YAML: python deployment/generate_dab_yaml.py {output_file}")
        print()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
