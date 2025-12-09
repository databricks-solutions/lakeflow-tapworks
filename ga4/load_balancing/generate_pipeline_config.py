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


def generate_pipeline_config(
    df: pd.DataFrame,
    default_schedule: str = "0 */6 * * *"
) -> pd.DataFrame:
    """
    Generate pipeline configuration with prefix+priority grouping.

    Args:
        df: Input DataFrame with GA4 configuration
        default_schedule: Default cron schedule if not specified

    Returns:
        DataFrame with pipeline_group column added
    """
    # Validate required columns
    required_columns = [
        'source_catalog',
        'source_schema',
        'tables',
        'target_catalog',
        'target_schema',
        'prefix',
        'priority'
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Fill optional columns with defaults
    if 'schedule' not in df.columns:
        df['schedule'] = default_schedule
    else:
        df['schedule'] = df['schedule'].fillna(default_schedule)

    # Ensure prefix and priority are strings for consistent concatenation
    df['prefix'] = df['prefix'].astype(str)
    df['priority'] = df['priority'].astype(str).str.zfill(2)  # Pad to 2 digits

    # Generate pipeline_group: prefix_priority
    df['pipeline_group'] = df['prefix'] + '_' + df['priority']

    # Print summary
    print(f"Pipeline Configuration Summary:")
    print(f"  Total properties: {len(df)}")
    print(f"  Unique prefixes: {df['prefix'].nunique()}")
    print(f"  Unique priorities: {df['priority'].nunique()}")
    print(f"  Total pipeline groups: {df['pipeline_group'].nunique()}")
    print()

    # Show grouping details
    print("Pipeline Groups:")
    for group_name in sorted(df['pipeline_group'].unique()):
        group_df = df[df['pipeline_group'] == group_name]
        print(f"  {group_name}:")
        print(f"    Properties: {len(group_df)}")
        print(f"    Schemas: {', '.join(group_df['source_schema'].unique())}")
    print()

    return df


def read_input_csv(input_file: str) -> pd.DataFrame:
    """Read and validate input CSV"""
    if not Path(input_file).exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    df = pd.read_csv(input_file)

    if df.empty:
        raise ValueError(f"Input CSV is empty: {input_file}")

    print(f"Read {len(df)} rows from {input_file}")
    return df


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

        # Read input
        df = read_input_csv(args.input_csv)

        # Generate configuration
        df_with_groups = generate_pipeline_config(df, args.schedule)

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
