"""
Workday Reports pipeline generation using OOP architecture.

This script demonstrates the use of the WorkdayReportsConnector class for
pipeline generation. It provides a simplified interface with all
connector-specific logic encapsulated in the connector class.

Usage:
    # Command line
    python pipeline_generator.py --input-csv input.csv --output-dir output --workspace-host https://...

    # Programmatic
    from workday_reports.connector import WorkdayReportsConnector
    from utilities import load_input_csv

    connector = WorkdayReportsConnector()
    df = load_input_csv('input.csv')
    result = connector.run_complete_pipeline_generation(
        df=df,
        output_dir='output',
        targets={'dev': {'workspace_host': '...'}},
        default_values={'project_name': 'my_project'}
    )
"""

import argparse
import logging
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utilities import load_input_csv
from connector import WorkdayReportsConnector


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate Workday Reports ingestion pipelines using OOP architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name my_workday_project \\
    --workspace-host https://my-workspace.cloud.databricks.com

  # With custom schedule and output directory
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name workday_reports_ingestion \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --schedule "0 */12 * * *" \\
    --output-dir dab_project

Note:
  - connection_name must be provided in the input CSV file for each row
  - primary_keys must be provided for each report (comma-separated)
  - The connector automatically handles prefix and priority defaults
  - Supports include_columns, exclude_columns, and scd_type configuration
  - Default SCD type is SCD_TYPE_1 (can be overridden to SCD_TYPE_2 per report)
        """,
    )

    # Required arguments
    parser.add_argument(
        "--input-csv",
        type=str,
        required=True,
        help="Path to input CSV with Workday reports (required)",
    )
    parser.add_argument(
        "--project-name",
        type=str,
        required=True,
        help="Project name for the bundle (required)",
    )
    parser.add_argument(
        "--workspace-host",
        type=str,
        required=True,
        help="Workspace host URL (required, e.g., https://workspace.cloud.databricks.com)",
    )

    # Optional arguments
    parser.add_argument(
        "--max-reports",
        type=int,
        default=250,
        help="Maximum reports per pipeline (default: 250)",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        default="0 */6 * * *",
        help="Default cron schedule (default: 0 */6 * * * - every 6 hours)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="dab_deployment",
        help="Output directory for DAB project (default: dab_deployment)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging output",
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s"
    )

    try:
        # Load input CSV
        print(f"Loading input CSV: {args.input_csv}")
        input_df = load_input_csv(args.input_csv)

        # Build targets dict from CLI arguments
        targets = {"dev": {"workspace_host": args.workspace_host}}

        # Build default values dict
        default_values = {"project_name": args.project_name, "schedule": args.schedule}

        # Create connector instance
        connector = WorkdayReportsConnector()

        # Run the complete pipeline generation using OOP approach
        result_df = connector.run_complete_pipeline_generation(
            df=input_df,
            output_dir=args.output_dir,
            targets=targets,
            default_values=default_values,
            max_tables_per_pipeline=args.max_reports,
        )

        # Print next steps
        print(f"\nNext steps:")
        print(f"  1. Review the generated DAB project:")
        print(f"     - {args.output_dir}/databricks.yml")
        print(f"     - {args.output_dir}/resources/pipelines.yml")
        print(f"  2. Deploy using Databricks Asset Bundles:")
        print(f"     cd {args.output_dir}")
        print(f"     databricks bundle deploy -t dev")
        print("=" * 80)

        # Save the intermediate pipeline configuration for reference
        output_csv = f"{args.output_dir}/generated_config.csv"
        result_df.to_csv(output_csv, index=False)
        print(f"\n✓ Intermediate configuration saved to: {output_csv}")

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
