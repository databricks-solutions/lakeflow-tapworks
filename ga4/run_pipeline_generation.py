#!/usr/bin/env python3
"""
Google Analytics 4 Pipeline Generation - Unified Runner

This script orchestrates the complete GA4 pipeline configuration and YAML generation process.

Two modes:
1. CSV Mode: Read properties from input CSV with prefix/priority
2. Auto-discover Mode: Query BigQuery and auto-balance (uses existing auto_balance_ga4.py)

Usage:
    # CSV mode with prefix+priority grouping
    python run_pipeline_generation.py --csv input.csv

    # Auto-discover mode with bin-packing
    python run_pipeline_generation.py --auto-discover --pipelines 3

    # Full pipeline with custom output
    python run_pipeline_generation.py --csv input.csv --output-yaml resources/ga4_pipeline.yml

Example:
    python run_pipeline_generation.py --csv load_balancing/examples/example_config.csv
"""

import argparse
import sys
import subprocess
from pathlib import Path


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
  python run_pipeline_generation.py --csv input.csv

  # Auto-discover mode with bin-packing
  python run_pipeline_generation.py --auto-discover --pipelines 3

  # Custom outputs
  python run_pipeline_generation.py --csv input.csv --output-yaml resources/custom.yml

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
