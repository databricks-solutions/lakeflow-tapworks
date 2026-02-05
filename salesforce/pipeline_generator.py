"""
Salesforce pipeline generation using OOP architecture.

This script demonstrates the use of the SalesforceConnector class for
pipeline generation. It provides a simplified interface with all
connector-specific logic encapsulated in the connector class.

Usage:
    # Using config file
    python pipeline_generator.py --input-csv input.csv --config config.json

    # Using inline JSON
    python pipeline_generator.py --input-csv input.csv \
        --targets '{"dev": {"workspace_host": "https://..."}}' \
        --default-values '{"project_name": "my_project"}'

    # Legacy CLI (backward compatible)
    python pipeline_generator.py --input-csv input.csv --project-name my_project --workspace-host https://...

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
import json
import logging
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utilities import load_input_csv
from connector import SalesforceConnector


def load_config_file(config_path: str) -> dict:
    """Load configuration from JSON or YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path) as f:
        if path.suffix in ['.yaml', '.yml']:
            import yaml
            return yaml.safe_load(f)
        else:
            return json.load(f)


def parse_json_arg(value: str) -> dict:
    """Parse a JSON string argument."""
    if not value:
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"Invalid JSON: {e}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate Salesforce ingestion pipelines using OOP architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Configuration Methods (in order of precedence):
  1. Inline JSON arguments (--targets, --default-values, --override)
  2. Config file (--config)
  3. Legacy CLI arguments (--project-name, --workspace-host, etc.)

Examples:
  # Using config file
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --config config.json

  # Using inline JSON
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --targets '{"dev": {"workspace_host": "https://workspace.cloud.databricks.com"}}' \\
    --default-values '{"project_name": "sfdc_ingestion", "schedule": "*/30 * * * *"}'

  # Legacy CLI (backward compatible)
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --project-name my_sfdc_project \\
    --workspace-host https://my-workspace.cloud.databricks.com

  # Mixed: config file with inline override
  python pipeline_generator.py \\
    --input-csv my_config.csv \\
    --config config.json \\
    --override '{"schedule": null}'

Config file format (JSON or YAML):
  {
    "targets": {
      "dev": {"workspace_host": "https://..."},
      "prod": {"workspace_host": "https://..."}
    },
    "default_values": {"project_name": "...", "schedule": "..."},
    "override_input_config": {"schedule": null},
    "max_tables_per_pipeline": 250
  }
        """
    )

    # Required arguments
    parser.add_argument(
        '--input-csv',
        type=str,
        required=True,
        help='Path to input CSV with Salesforce objects (required)'
    )

    # Config file option
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to JSON or YAML config file'
    )

    # Inline JSON options
    parser.add_argument(
        '--targets',
        type=str,
        help='Targets configuration as JSON string'
    )
    parser.add_argument(
        '--default-values',
        type=str,
        help='Default values as JSON string'
    )
    parser.add_argument(
        '--override',
        type=str,
        help='Override input config as JSON string'
    )

    # Legacy CLI arguments (for backward compatibility)
    parser.add_argument(
        '--project-name',
        type=str,
        help='Project name for the bundle'
    )
    parser.add_argument(
        '--workspace-host',
        type=str,
        help='Workspace host URL (e.g., https://workspace.cloud.databricks.com)'
    )
    parser.add_argument(
        '--max-tables',
        type=int,
        default=250,
        help='Maximum objects per pipeline (default: 250)'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        help='Default cron schedule'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='dab_deployment',
        help='Output directory for DAB project (default: dab_deployment)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging output'
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )

    try:
        # Load input CSV
        print(f"Loading input CSV: {args.input_csv}")
        input_df = load_input_csv(args.input_csv)

        # Build configuration from multiple sources
        # Priority: inline JSON > config file > legacy CLI args

        # Start with legacy CLI args
        targets = {}
        default_values = {}
        override_input_config = {}
        max_tables_per_pipeline = args.max_tables

        if args.workspace_host:
            targets = {'dev': {'workspace_host': args.workspace_host}}
        if args.project_name:
            default_values['project_name'] = args.project_name
        if args.schedule:
            default_values['schedule'] = args.schedule

        # Overlay config file
        if args.config:
            print(f"Loading config file: {args.config}")
            config = load_config_file(args.config)
            if 'targets' in config:
                targets = config['targets']
            if 'default_values' in config:
                default_values = {**default_values, **config['default_values']}
            if 'override_input_config' in config:
                override_input_config = config['override_input_config']
            if 'max_tables_per_pipeline' in config:
                max_tables_per_pipeline = config['max_tables_per_pipeline']

        # Overlay inline JSON args (highest priority)
        if args.targets:
            targets = parse_json_arg(args.targets)
        if args.default_values:
            inline_defaults = parse_json_arg(args.default_values)
            default_values = {**default_values, **inline_defaults}
        if args.override:
            override_input_config = parse_json_arg(args.override)

        # Validate required configuration
        if not targets:
            parser.error("No targets configured. Use --targets, --config, or --workspace-host")

        # Create connector instance
        connector = SalesforceConnector()

        # Run the complete pipeline generation using OOP approach
        result_df = connector.run_complete_pipeline_generation(
            df=input_df,
            output_dir=args.output_dir,
            targets=targets,
            default_values=default_values if default_values else None,
            override_input_config=override_input_config if override_input_config else None,
            max_tables_per_pipeline=max_tables_per_pipeline
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
        print(f"\nIntermediate configuration saved to: {output_csv}")

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
