#!/usr/bin/env python3
"""
Unified CLI for Databricks Asset Bundle pipeline generation.

This is the single entry point for all connector types, replacing the
individual pipeline_generator.py scripts in each connector directory.

Usage:
    # List available connectors
    python cli.py --list

    # Show connector info
    python cli.py salesforce --info

    # Generate pipelines using config file
    python cli.py salesforce --input-config config.csv --output-dir output --settings config.json

    # Generate pipelines using inline JSON
    python cli.py sqlserver --input-config config.csv --output-dir output \
        --targets '{"dev": {"workspace_host": "https://..."}}' \
        --default-values '{"project_name": "my_project"}'

    # Using connector aliases
    python cli.py sf --input-config config.csv --output-dir output --settings config.json
    python cli.py pg --input-config config.csv --output-dir output --settings config.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from core.registry import list_connectors, list_aliases, get_connector_info, resolve_connector_name
from core.runner import run_pipeline_generation
from core import BaseConnector


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


def print_connector_list():
    """Print list of available connectors and aliases."""
    print("\nAvailable connectors:")
    print("-" * 40)
    for name in list_connectors():
        print(f"  {name}")

    print("\nAliases:")
    print("-" * 40)
    aliases = list_aliases()
    for alias, canonical in sorted(aliases.items()):
        print(f"  {alias:<12} -> {canonical}")

    print("\nUse 'python cli.py <connector> --info' for connector details.")


def print_connector_info(connector_name: str):
    """Print detailed information about a connector."""
    try:
        info = get_connector_info(connector_name)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\nConnector: {info['name']}")
    print("=" * 50)

    print(f"\nType: {info['type']}")
    print(f"Default project name: {info['default_project_name']}")

    print(f"\nRequired columns:")
    for col in info['required_columns']:
        print(f"  - {col}")

    print(f"\nDefault values:")
    for key, value in info['default_values'].items():
        print(f"  {key}: {value}")

    print(f"\nSupported SCD types:")
    for scd_type in info['supported_scd_types']:
        print(f"  - {scd_type}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate Databricks Asset Bundle pipelines for any connector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all connectors
  python cli.py --list

  # Show Salesforce connector info
  python cli.py salesforce --info

  # Generate pipelines using settings file
  python cli.py salesforce --input-config tables.csv --output-dir output --settings settings.json

  # Generate pipelines with inline JSON
  python cli.py sqlserver --input-config tables.csv --output-dir output \\
    --targets '{"dev": {"workspace_host": "https://..."}}' \\
    --default-values '{"project_name": "my_project"}'

  # Use connector aliases
  python cli.py sf --input-config tables.csv --output-dir output --settings settings.json
  python cli.py pg --input-config tables.csv --output-dir output --settings settings.json

Connector aliases:
  sf, salesforce     -> salesforce
  sql, mssql         -> sqlserver
  pg, postgresql     -> postgres
  ga, ga4            -> google_analytics
  snow               -> servicenow
  wd, workday        -> workday_reports

Settings file format (settings.json):
  {
    "targets": {
      "dev": {"workspace_host": "https://...", "root_path": "/Shared/..."},
      "prod": {"workspace_host": "https://...", "root_path": "/Shared/..."}
    },
    "default_values": {"project_name": "my_project", "schedule": "*/15 * * * *"},
    "max_tables_per_pipeline": 250
  }
        """
    )

    # Global options
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all available connectors and aliases'
    )

    # Connector name (positional, optional if --list is used)
    parser.add_argument(
        'connector',
        nargs='?',
        help='Connector name or alias (e.g., salesforce, sqlserver, sf, pg)'
    )

    # Connector info
    parser.add_argument(
        '--info', '-i',
        action='store_true',
        help='Show detailed information about the connector'
    )

    # Input/output
    parser.add_argument(
        '--input-config',
        type=str,
        help='Path to table mappings (CSV file or Delta table name)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='dab_deployment',
        help='Output directory for DAB project (default: dab_deployment)'
    )
    parser.add_argument(
        '--output-config',
        type=str,
        help='Save processed configuration to CSV file'
    )

    # Settings file
    parser.add_argument(
        '--settings', '-s',
        type=str,
        help='Path to settings file (JSON or YAML) with targets, defaults, overrides'
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
        help='Override input config as JSON string (applied to all rows)'
    )

    # Pipeline limits
    parser.add_argument(
        '--max-tables-per-pipeline',
        type=int,
        default=BaseConnector.DEFAULT_MAX_TABLES_PER_PIPELINE,
        help=f'Maximum tables per pipeline (default: {BaseConnector.DEFAULT_MAX_TABLES_PER_PIPELINE})'
    )
    parser.add_argument(
        '--max-tables-per-gateway',
        type=int,
        default=BaseConnector.DEFAULT_MAX_TABLES_PER_GATEWAY,
        help=f'Maximum tables per gateway for database connectors (default: {BaseConnector.DEFAULT_MAX_TABLES_PER_GATEWAY})'
    )

    # Logging
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging output'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress all output except errors'
    )

    args = parser.parse_args()

    # Configure logging
    if args.quiet:
        log_level = logging.ERROR
    elif args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    # Handle --list
    if args.list:
        print_connector_list()
        sys.exit(0)

    # Connector name required for other operations
    if not args.connector:
        parser.print_help()
        print("\nError: connector name is required", file=sys.stderr)
        print("Use --list to see available connectors", file=sys.stderr)
        sys.exit(1)

    # Handle --info
    if args.info:
        print_connector_info(args.connector)
        sys.exit(0)

    # Input is required for generation
    if not args.input_config:
        print(f"Error: --input-config is required for pipeline generation", file=sys.stderr)
        print(f"Use 'python cli.py {args.connector} --info' for connector details", file=sys.stderr)
        sys.exit(1)

    try:
        # Build configuration from multiple sources
        # Priority: inline JSON > settings file
        targets = {}
        default_values = {}
        override_config = {}

        # Load from settings file
        if args.settings:
            logger.info(f"Loading settings file: {args.settings}")
            settings = load_config_file(args.settings)
            targets = settings.get('targets', {})
            default_values = settings.get('default_values', {})
            override_config = settings.get('override_input_config', {})

            # Allow settings file to override max tables
            if 'max_tables_per_pipeline' in settings:
                args.max_tables_per_pipeline = settings['max_tables_per_pipeline']
            if 'max_tables_per_gateway' in settings:
                args.max_tables_per_gateway = settings['max_tables_per_gateway']

        # Overlay inline JSON (higher priority)
        if args.targets:
            targets = parse_json_arg(args.targets)
        if args.default_values:
            inline_defaults = parse_json_arg(args.default_values)
            default_values.update(inline_defaults)
        if args.override:
            override_config = parse_json_arg(args.override)

        # Validate targets
        if not targets:
            print("Error: No targets specified", file=sys.stderr)
            print("Use --settings or --targets to specify workspace targets", file=sys.stderr)
            sys.exit(1)

        # Resolve connector name
        canonical_name = resolve_connector_name(args.connector)
        logger.info(f"Using connector: {canonical_name}")
        logger.info(f"Input: {args.input_config}")
        logger.info(f"Output: {args.output_dir}")

        # Run pipeline generation
        result_df = run_pipeline_generation(
            connector_name=canonical_name,
            input_source=args.input_config,
            output_dir=args.output_dir,
            targets=targets,
            default_values=default_values if default_values else None,
            override_config=override_config if override_config else None,
            max_tables_per_pipeline=args.max_tables_per_pipeline,
            max_tables_per_gateway=args.max_tables_per_gateway,
            output_config=args.output_config,
        )

        # Summary
        print(f"\nPipeline generation complete!")
        print(f"  Connector: {canonical_name}")
        print(f"  Tables processed: {len(result_df)}")
        print(f"  Pipeline groups: {result_df['pipeline_group'].nunique()}")
        print(f"  Output directory: {args.output_dir}")

        if 'gateway' in result_df.columns:
            print(f"  Gateways: {result_df['gateway'].nunique()}")

        if args.output_config:
            print(f"  Config saved to: {args.output_config}")

    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Pipeline generation failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
