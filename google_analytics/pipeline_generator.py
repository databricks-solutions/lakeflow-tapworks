"""
Google Analytics 4 pipeline generation using OOP architecture.

This script demonstrates the use of the GoogleAnalyticsConnector class for
pipeline generation. It provides a simplified interface with all
connector-specific logic encapsulated in the connector class.

Usage:
    # Command line
    python pipeline_generator.py --input-csv input.csv --output-dir output --workspace-host https://...

    # Programmatic
    from google_analytics.connector import GoogleAnalyticsConnector
    from utilities import load_input_csv

    connector = GoogleAnalyticsConnector()
    df = load_input_csv('input.csv')
    result = connector.run_complete_pipeline_generation(
        df=df,
        output_dir='output',
        targets={'dev': {'workspace_host': '...'}},
        default_values={'project_name': 'my_project'}
    )
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utilities import load_input_csv
from connector import GoogleAnalyticsConnector


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate Google Analytics 4 ingestion pipelines using OOP architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
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

Note:
  - connection_name must be provided in the input CSV file for each row
  - The connector automatically handles prefix and subgroup defaults
  - Properties are grouped by prefix+subgroup combinations
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

    try:
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
            'project_name': args.project_name,
            'schedule': args.schedule
        }

        # Create connector instance
        connector = GoogleAnalyticsConnector()

        # Run the complete pipeline generation using OOP approach
        result_df = connector.run_complete_pipeline_generation(
            df=input_df,
            output_dir=args.output_dir,
            targets=targets,
            default_values=default_values,
            max_tables_per_pipeline=args.max_tables
        )

        # Print next steps
        print(f"\nNext steps:")
        print(f"  1. Review the generated DAB project:")
        print(f"     - {args.output_dir}/databricks.yml")
        print(f"     - {args.output_dir}/resources/pipelines.yml")
        print(f"  2. Deploy using Databricks Asset Bundles:")
        print(f"     cd {args.output_dir}")
        print(f"     databricks bundle deploy -t dev")
        print("="*80)

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
