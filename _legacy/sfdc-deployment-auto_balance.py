#!/usr/bin/env python3
"""
Salesforce Auto-Balance Script

Automatically distributes Salesforce objects across pipeline groups using
First Fit Decreasing (FFD) bin-packing algorithm.

Since Salesforce doesn't expose table sizes via API, this script balances by
ROW COUNT, which is the most practical approach.

Usage:
    # Salesforce API mode (queries for row counts)
    python auto_balance.py --pipelines 3 --max-tables 30

    # CSV input mode (no Salesforce access needed)
    python auto_balance.py --input-csv tables.csv --pipelines 3 --max-tables 30

Requirements:
    pip install simple-salesforce python-dotenv

Environment Variables (for Salesforce API mode):
    SFDC_USERNAME=your-username@company.com
    SFDC_PASSWORD=your-password
    SFDC_SECURITY_TOKEN=your-security-token
    SFDC_CONNECTION_NAME=sfdc_connection
    TARGET_CATALOG=salesforce_catalog
    TARGET_SCHEMA=salesforce_schema
"""

import os
import sys
import csv
import argparse
from typing import List, Dict, Tuple
from collections import defaultdict

try:
    from simple_salesforce import Salesforce
    from dotenv import load_dotenv
    SALESFORCE_AVAILABLE = True
except ImportError:
    SALESFORCE_AVAILABLE = False

# Load environment variables
load_dotenv()

# Standard Salesforce objects to ingest (customize as needed)
DEFAULT_OBJECTS = [
    'Account', 'Contact', 'Opportunity', 'Lead', 'Case',
    'Task', 'Event', 'Campaign', 'CampaignMember',
    'OpportunityLineItem', 'Quote', 'Asset'
]


class TableInfo:
    """Container for table metadata"""
    def __init__(self, name: str, row_count: int):
        self.name = name
        self.row_count = row_count

    def __repr__(self):
        return f"{self.name}: {self.row_count:,} rows"


def read_tables_from_csv(csv_file: str) -> List[TableInfo]:
    """
    Read table metadata from CSV file.

    CSV format:
        object_name,row_count
        Account,125000
        Contact,450000
    """
    print(f"Reading table metadata from CSV: {csv_file}\n")

    tables = []

    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)

        if 'object_name' not in reader.fieldnames:
            raise ValueError(
                f"CSV must contain 'object_name' column\n"
                f"Found columns: {', '.join(reader.fieldnames)}\n"
                f"Required: object_name, row_count"
            )

        for row in reader:
            name = row['object_name']
            row_count = int(row.get('row_count', 0))

            table = TableInfo(name, row_count)
            tables.append(table)

            print(f"  {name:30} {row_count:>10,} rows")

    print(f"\n✓ Loaded {len(tables)} objects")
    print(f"  Total: {sum(t.row_count for t in tables):,} rows\n")

    return tables


def connect_salesforce() -> Salesforce:
    """Connect to Salesforce using environment variables"""
    if not SALESFORCE_AVAILABLE:
        raise ImportError(
            "Salesforce packages not installed. Install with:\n"
            "  pip install simple-salesforce python-dotenv"
        )

    username = os.getenv('SFDC_USERNAME')
    password = os.getenv('SFDC_PASSWORD')
    security_token = os.getenv('SFDC_SECURITY_TOKEN')

    if not all([username, password, security_token]):
        raise ValueError(
            "Missing Salesforce credentials. Set environment variables:\n"
            "  SFDC_USERNAME, SFDC_PASSWORD, SFDC_SECURITY_TOKEN"
        )

    print(f"Connecting to Salesforce as {username}...")
    try:
        sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token
        )
        print("✓ Connected successfully\n")
        return sf
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)


def query_table_sizes(sf: Salesforce, objects: List[str]) -> List[TableInfo]:
    """Query Salesforce for row counts of specified objects"""
    print(f"Querying {len(objects)} Salesforce objects for row counts...\n")

    tables = []
    for obj_name in objects:
        try:
            result = sf.query(f"SELECT COUNT() FROM {obj_name}")
            row_count = result['totalSize']

            table = TableInfo(obj_name, row_count)
            tables.append(table)

            print(f"  {obj_name:30} {row_count:>10,} rows")

        except Exception as e:
            print(f"  {obj_name:30} ✗ Error: {e}")

    print(f"\n✓ Successfully queried {len(tables)} objects")
    print(f"  Total: {sum(t.row_count for t in tables):,} rows\n")

    return tables


def bin_packing_with_constraints(
    tables: List[TableInfo],
    num_pipelines: int,
    max_tables_per_pipeline: int = None
) -> Tuple[Dict[int, List[TableInfo]], Dict[int, int]]:
    """
    Bin-packing using First Fit Decreasing with table count constraints.
    Balances by ROW COUNT (most practical for Salesforce).
    """
    # Auto-calculate max tables if not provided
    if max_tables_per_pipeline is None:
        ideal_tables = 25
        max_tables_per_pipeline = max(ideal_tables, int(len(tables) / num_pipelines * 1.3))
        max_tables_per_pipeline = min(max_tables_per_pipeline, 50)
        print(f"Auto-calculated max tables per pipeline: {max_tables_per_pipeline}\n")

    # Validate capacity
    total_capacity = num_pipelines * max_tables_per_pipeline
    if len(tables) > total_capacity:
        raise ValueError(
            f"Cannot fit {len(tables)} tables into {num_pipelines} pipelines "
            f"with max {max_tables_per_pipeline} tables/pipeline.\n"
            f"Capacity: {num_pipelines} × {max_tables_per_pipeline} = {total_capacity} tables\n"
            f"Required: {len(tables)} tables\n\n"
            f"Solutions:\n"
            f"  1. Increase pipelines: --pipelines {int(len(tables) / max_tables_per_pipeline) + 1}\n"
            f"  2. Increase max tables: --max-tables {int(len(tables) / num_pipelines) + 1}\n"
            f"  3. Reduce number of objects"
        )

    # Sort tables by row count (largest first)
    sorted_tables = sorted(tables, key=lambda t: t.row_count, reverse=True)

    # Initialize bins
    bins = defaultdict(list)
    bin_loads = defaultdict(int)
    bin_table_counts = defaultdict(int)

    # Assign each table using FFD with table count constraint
    for table in sorted_tables:
        # Find pipelines that have space for more tables
        available_bins = [
            b for b in range(1, num_pipelines + 1)
            if bin_table_counts[b] < max_tables_per_pipeline
        ]

        if not available_bins:
            raise ValueError(
                f"Cannot assign table '{table.name}' - all pipelines at max capacity."
            )

        # Among available bins, pick the one with minimum load
        min_bin = min(available_bins, key=lambda b: bin_loads[b])

        # Add table to bin
        bins[min_bin].append(table)
        bin_loads[min_bin] += table.row_count
        bin_table_counts[min_bin] += 1

    # Print distribution
    print(f"Load-balanced distribution across {num_pipelines} pipelines:\n")
    print(f"{'Pipeline':<12} {'Tables':<10} {'Rows':<15} {'Status'}")
    print("=" * 50)

    for pipeline_id in sorted(bins.keys()):
        tables_in_bin = bins[pipeline_id]
        table_count = bin_table_counts[pipeline_id]
        total_rows = bin_loads[pipeline_id]

        # Status indicator
        if table_count > 40:
            status = "⚠️  High"
        elif table_count > max_tables_per_pipeline * 0.9:
            status = "⚠️  Near limit"
        else:
            status = "✓"

        print(f"Pipeline {pipeline_id:<3} {table_count:<10} {total_rows:<15,} {status}")

    print()

    # Print detailed table list
    print("Detailed distribution:\n")
    for pipeline_id in sorted(bins.keys()):
        print(f"Pipeline {pipeline_id} ({bin_table_counts[pipeline_id]} tables):")
        for table in bins[pipeline_id]:
            print(f"  - {table.name:30} {table.row_count:>10,} rows")
        print()

    return bins, bin_table_counts


def generate_csv(
    distribution: Dict[int, List[TableInfo]],
    output_file: str,
    schedule: str = "*/15 * * * *"
):
    """Generate CSV configuration file from distribution"""
    connection_name = os.getenv('SFDC_CONNECTION_NAME', 'sfdc_connection')
    target_catalog = os.getenv('TARGET_CATALOG', 'salesforce_catalog')
    target_schema = os.getenv('TARGET_SCHEMA', 'salesforce_schema')

    print(f"Generating CSV configuration: {output_file}\n")

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)

        # Write header (matches your deployment CSV format)
        writer.writerow([
            'source_schema',
            'source_table_name',
            'target_catalog',
            'target_schema',
            'target_table_name',
            'pipeline_group',
            'connection_name',
            'schedule',
            'include_columns',
            'exclude_columns'
        ])

        # Write rows
        for pipeline_id in sorted(distribution.keys()):
            for table in distribution[pipeline_id]:
                writer.writerow([
                    'standard',
                    table.name,
                    target_catalog,
                    target_schema,
                    table.name,
                    pipeline_id,
                    connection_name,
                    schedule,
                    '',  # include_columns (empty = all columns)
                    ''   # exclude_columns
                ])

    print(f"✓ CSV generated: {output_file}")
    print(f"\nNext steps:")
    print(f"  1. Review the generated CSV: {output_file}")
    print(f"  2. Generate DAB YAML: python generate_dab_yaml.py {output_file}")
    print(f"  3. Deploy: cd .. && databricks bundle deploy -t dev")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Auto-balance Salesforce pipeline configuration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Salesforce API mode
  python auto_balance.py --pipelines 3 --max-tables 30

  # CSV input mode (no Salesforce access)
  python auto_balance.py --input-csv tables.csv --pipelines 3 --max-tables 30

  # Auto-calculate pipelines
  python auto_balance.py --input-csv tables.csv --max-tables 25

Note: Balances by ROW COUNT (Salesforce doesn't expose table sizes via API)
        """
    )

    # Input mode
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        '--input-csv',
        help='CSV file with table metadata (alternative to Salesforce API)'
    )
    input_group.add_argument(
        '--api',
        action='store_true',
        help='Use Salesforce API (default if --input-csv not provided)'
    )

    parser.add_argument(
        '--output', '-o',
        default='examples/auto_balanced_config.csv',
        help='Output CSV file path (default: examples/auto_balanced_config.csv)'
    )
    parser.add_argument(
        '--pipelines', '-p',
        type=int,
        help='Number of pipelines to create (required unless auto-calculated)'
    )
    parser.add_argument(
        '--max-tables',
        type=int,
        help='Maximum tables per pipeline (default: auto-calculated, recommended: 20-30)'
    )
    parser.add_argument(
        '--objects', '-t',
        nargs='+',
        default=DEFAULT_OBJECTS,
        help='Salesforce objects to include (default: standard objects, only for API mode)'
    )
    parser.add_argument(
        '--schedule', '-s',
        default='*/15 * * * *',
        help='Cron schedule for all pipelines (default: */15 * * * *)'
    )

    args = parser.parse_args()

    print("="*70)
    print("Salesforce Auto-Balance Script")
    print("Balancing by ROW COUNT (most practical for Salesforce)")
    print("="*70)
    print()

    # Determine mode
    if args.input_csv:
        print(f"Mode: CSV Input\n")
        tables = read_tables_from_csv(args.input_csv)
    else:
        print(f"Mode: Salesforce API\n")
        sf = connect_salesforce()
        tables = query_table_sizes(sf, args.objects)

    if not tables:
        print("✗ No tables found. Exiting.")
        sys.exit(1)

    # Auto-calculate pipelines if not provided
    if not args.pipelines:
        if args.max_tables:
            import math
            args.pipelines = math.ceil(len(tables) / args.max_tables)
            print(f"Auto-calculated pipelines: {args.pipelines}\n")
        else:
            import math
            args.pipelines = max(1, math.ceil(len(tables) / 25))
            print(f"Auto-calculated pipelines: {args.pipelines}\n")

    # Apply bin-packing algorithm
    try:
        distribution, table_counts = bin_packing_with_constraints(
            tables,
            args.pipelines,
            args.max_tables
        )
    except ValueError as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

    # Generate CSV
    generate_csv(distribution, args.output, args.schedule)

    print("="*70)
    print("✓ Auto-balance complete!")
    print("="*70)


if __name__ == '__main__':
    main()
