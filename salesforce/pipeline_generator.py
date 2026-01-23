"""
Salesforce pipeline generation using OOP architecture.

This script demonstrates the use of the SalesforceConnector class for
pipeline generation. It provides a simplified interface with all
connector-specific logic encapsulated in the connector class.

Usage:
    # Command line
    python pipeline_generator.py --input-csv input.csv --output-dir output --workspace-host https://...

    # Programmatic
    from salesforce.connector import SalesforceConnector
    from utilities import load_input_csv

    connector = SalesforceConnector()
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
from connector import SalesforceConnector


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate Salesforce ingestion pipelines using OOP architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name my_sfdc_project \\
    --workspace-host https://my-workspace.cloud.databricks.com

  # With custom schedule and output directory
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name sfdc_ingestion \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --schedule "*/30 * * * *" \\
    --output-dir dab_project

Note:
  - connection_name must be provided in the input CSV file for each row
  - The connector automatically handles prefix and subgroup defaults
  - Supports include_columns and exclude_columns for column filtering
        """
    )

    # Required arguments
    parser.add_argument(
        '--input-csv',
        type=str,
        required=True,
        help='Path to input CSV with Salesforce objects (required)'
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
        help='Maximum objects per pipeline (default: 250)'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        default='*/15 * * * *',
        help='Default cron schedule (default: */15 * * * *)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='dab_deployment',
        help='Output directory for DAB project (default: dab_deployment)'
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
        connector = SalesforceConnector()

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
