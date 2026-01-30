"""
PostgreSQL pipeline generation using OOP architecture.

This script demonstrates the use of the PostgreSQLConnector class for
pipeline generation. It provides a simplified interface with all
connector-specific logic encapsulated in the connector class.

Usage:
    # Command line
    python pipeline_generator.py --input-csv input.csv --output-dir output --workspace-host https://... --root-path /Users/.../.bundle/${bundle.name}/${bundle.target}

    # Programmatic
    from postgres.connector import PostgreSQLConnector
    from utilities import load_input_csv

    connector = PostgreSQLConnector()
    df = load_input_csv('input.csv')
    result = connector.run_complete_pipeline_generation(
        df=df,
        output_dir='output',
        targets={'dev': {'workspace_host': '...', 'root_path': '...'}},
        default_values={'project_name': 'my_project'}
    )
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utilities import load_input_csv
from connector import PostgreSQLConnector


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate PostgreSQL ingestion pipelines using OOP architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python pipeline_generator.py \\
    --input-csv postgres/examples/basic/pipeline_config.csv \\
    --project-name postgres_ingestion \\
    --workspace-host https://my-workspace.cloud.databricks.com \\
    --root-path '/Users/user@company.com/.bundle/${bundle.name}/${bundle.target}'

  # With custom node types and table limits
  python pipeline_generator.py \\
    --input-csv postgres/examples/basic/pipeline_config.csv \\
    --project-name postgres_ingestion \\
    --workspace-host https://workspace.cloud.databricks.com \\
    --root-path '/Users/user/.bundle/${bundle.name}/${bundle.target}' \\
    --worker-type m6d.xlarge \\
    --driver-type m6d.xlarge \\
    --max-tables-gateway 500 \\
    --max-tables-pipeline 250

Note:
  - connection_name must be provided in the input CSV file for each row
  - UC POSTGRESQL connections do not accept a 'database' option; set the database per-row via source_database
        """,
    )

    # Required arguments
    parser.add_argument(
        "--input-csv",
        type=str,
        required=True,
        help="Path to input CSV with source table list (required)",
    )
    parser.add_argument(
        "--project-name",
        type=str,
        required=True,
        help="Project name prefix for all resources (required)",
    )
    parser.add_argument(
        "--workspace-host",
        type=str,
        required=True,
        help="Workspace host URL (required, e.g., https://workspace.cloud.databricks.com)",
    )
    parser.add_argument(
        "--root-path",
        type=str,
        required=True,
        help="Root path for bundle deployment (required, e.g., /Users/user/.bundle/${bundle.name}/${bundle.target})",
    )

    # Optional arguments
    parser.add_argument(
        "--max-tables-gateway",
        type=int,
        default=250,
        help="Maximum tables per gateway (default: 250)",
    )
    parser.add_argument(
        "--max-tables-pipeline",
        type=int,
        default=250,
        help="Maximum tables per pipeline within gateway (default: 250)",
    )
    parser.add_argument(
        "--schedule",
        type=str,
        default="*/15 * * * *",
        help="Default cron schedule (default: */15 * * * *)",
    )
    parser.add_argument(
        "--worker-type",
        type=str,
        default=None,
        help="Default gateway worker node type if not in CSV (default: None)",
    )
    parser.add_argument(
        "--driver-type",
        type=str,
        default=None,
        help="Default gateway driver node type if not in CSV (default: None)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="dab_project",
        help="Output directory for DAB project (default: dab_project)",
    )

    args = parser.parse_args()

    try:
        # Load input CSV
        print(f"Loading input CSV: {args.input_csv}")
        input_df = load_input_csv(args.input_csv)

        # Build targets dict from CLI arguments
        targets = {
            "dev": {
                "workspace_host": args.workspace_host,
                "root_path": args.root_path,
            }
        }

        # Build default values dict
        default_values = {
            "project_name": args.project_name,
            "schedule": args.schedule,
            "gateway_worker_type": args.worker_type,
            "gateway_driver_type": args.driver_type,
        }

        # Create connector instance
        connector = PostgreSQLConnector()

        # Run the complete pipeline generation using OOP approach
        result_df = connector.run_complete_pipeline_generation(
            df=input_df,
            output_dir=args.output_dir,
            targets=targets,
            default_values=default_values,
            max_tables_per_gateway=args.max_tables_gateway,
            max_tables_per_pipeline=args.max_tables_pipeline,
        )

        # Print next steps
        print("\nNext steps:")
        print("  1. Review the generated DAB project:")
        print(f"     - {args.output_dir}/databricks.yml")
        print(f"     - {args.output_dir}/resources/gateways.yml")
        print(f"     - {args.output_dir}/resources/pipelines.yml")
        print(f"     - {args.output_dir}/resources/jobs.yml")
        print("  2. Deploy using Databricks Asset Bundles:")
        print(f"     cd {args.output_dir}")
        print("     databricks bundle deploy -t dev")
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

