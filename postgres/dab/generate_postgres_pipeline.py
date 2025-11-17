#!/usr/bin/env python3
"""
PostgreSQL Lakeflow Connect Pipeline Generator

Generates Databricks Asset Bundle (DAB) YAML for PostgreSQL ingestion pipelines.
Reads table configuration from CSV and creates gateway + ingestion pipelines.

Usage:
    python generate_postgres_pipeline.py <csv_path> [options]

Example:
    python generate_postgres_pipeline.py example_config.csv \
        --gateway-catalog bronze \
        --gateway-schema metadata \
        --project-name my_postgres_proj
"""
import yaml
import pandas as pd
import argparse
import sys
from pathlib import Path
from databricks.sdk import WorkspaceClient


def get_connection_id(workspace_client, connection_name):
    """Lookup connection ID from connection name."""
    try:
        connection = workspace_client.connections.get(connection_name)
        return connection.connection_id
    except Exception as e:
        print(f"[WARN]  Warning: Could not retrieve connection ID for '{connection_name}': {e}")
        return "00000000-0000-0000-0000-000000000000"  # Placeholder


def auto_assign_gateways_and_pipelines(df, tables_per_gateway=50, tables_per_pipeline=20):
    """
    Automatically assign gateway and pipeline_group values based on table distribution.

    Rules:
    - Same connection_name can share gateways
    - Max tables_per_gateway tables per gateway
    - Max tables_per_pipeline tables per pipeline group
    - Round-robin distribution within connection groups

    Args:
        df (pd.DataFrame): Input dataframe
        tables_per_gateway (int): Maximum tables per gateway (default: 50)
        tables_per_pipeline (int): Maximum tables per pipeline group (default: 20)

    Returns:
        pd.DataFrame: Dataframe with gateway and pipeline_group columns populated
    """
    df = df.copy()

    # Initialize columns if they don't exist
    if 'gateway' not in df.columns:
        df['gateway'] = None
    if 'pipeline_group' not in df.columns:
        df['pipeline_group'] = None

    gateway_counter = 1
    pipeline_counter = 1

    # Group by connection_name (same connection can share gateways)
    for connection_name, conn_group in df.groupby('connection_name'):
        tables_in_connection = len(conn_group)

        # Calculate number of gateways needed for this connection
        num_gateways_needed = max(1, (tables_in_connection + tables_per_gateway - 1) // tables_per_gateway)

        print(f"   Connection '{connection_name}': {tables_in_connection} tables -> {num_gateways_needed} gateway(s)")

        # Distribute tables across gateways for this connection
        for gateway_offset in range(num_gateways_needed):
            current_gateway_id = gateway_counter + gateway_offset

            # Get tables for this gateway (round-robin)
            start_idx = gateway_offset * tables_per_gateway
            end_idx = min((gateway_offset + 1) * tables_per_gateway, tables_in_connection)

            gateway_tables = conn_group.iloc[start_idx:end_idx]
            tables_in_gateway = len(gateway_tables)

            # Calculate pipelines needed for this gateway
            num_pipelines_needed = max(1, (tables_in_gateway + tables_per_pipeline - 1) // tables_per_pipeline)

            print(f"      Gateway {current_gateway_id}: {tables_in_gateway} tables -> {num_pipelines_needed} pipeline(s)")

            # Distribute tables across pipeline groups
            for pipeline_offset in range(num_pipelines_needed):
                current_pipeline_id = pipeline_counter + pipeline_offset

                # Get tables for this pipeline (round-robin)
                pipeline_start = pipeline_offset * tables_per_pipeline
                pipeline_end = min((pipeline_offset + 1) * tables_per_pipeline, tables_in_gateway)

                pipeline_tables = gateway_tables.iloc[pipeline_start:pipeline_end]

                # Assign gateway and pipeline_group
                df.loc[pipeline_tables.index, 'gateway'] = current_gateway_id
                df.loc[pipeline_tables.index, 'pipeline_group'] = current_pipeline_id

            pipeline_counter += num_pipelines_needed

        gateway_counter += num_gateways_needed

    # Convert to int type
    df['gateway'] = df['gateway'].astype(int)
    df['pipeline_group'] = df['pipeline_group'].astype(int)

    return df


def create_gateways(df, catalog, schema, workspace_client, project_name,
                   node_type_id='m5d.large', driver_node_type_id='c5a.8xlarge'):
    """Create gateway YAML configuration from dataframe."""
    gateways = {}
    unique_gateways = df.groupby('gateway').first()

    for gateway_id, row in unique_gateways.iterrows():
        connection_id = get_connection_id(workspace_client, row['connection_name'])
        gateway_name = f"{project_name}_gateway_{gateway_id}"
        pipeline_name = f"{project_name}_pipeline_{gateway_name}"

        # Get source_type from CSV or default to POSTGRESQL
        source_type = row.get('source_type', 'POSTGRESQL').upper()

        gateways[pipeline_name] = {
            'name': gateway_name,
            'clusters': [{
                'node_type_id': node_type_id,
                'driver_node_type_id': driver_node_type_id,
                'num_workers': 1
            }],
            'gateway_definition': {
                'connection_name': row['connection_name'],
                'connection_id': connection_id,
                'gateway_storage_catalog': catalog,
                'gateway_storage_schema': schema,
                'gateway_storage_name': gateway_name,
                'source_type': source_type
            },
            'target': schema,
            'continuous': True,
            'catalog': catalog
        }

    return {'resources': {'pipelines': gateways}}


def create_pipelines(df, project_name):
    """Create pipeline YAML configuration from dataframe."""
    pipelines = {}

    for pipeline_group, group_df in df.groupby('pipeline_group'):
        gateway_id = group_df.iloc[0]['gateway']
        pipeline_name = f"{project_name}_pipeline_ingestion_{pipeline_group}"

        tables = [{
            'table': {
                'source_catalog': row['source_database'],
                'source_schema': row['source_schema'],
                'source_table': row['source_table_name'],
                'destination_catalog': row['target_catalog'],
                'destination_schema': row['target_schema']
            }
        } for _, row in group_df.iterrows()]

        pipelines[pipeline_name] = {
            'name': f"{project_name}_ingestion_{pipeline_group}",
            'configuration': {
                'pipelines.***REMOVED***': '600'
            },
            'ingestion_definition': {
                'ingestion_gateway_id': f"${{resources.pipelines.{project_name}_pipeline_{project_name}_gateway_{gateway_id}.id}}",
                'objects': tables
            }
        }

    return {'resources': {'pipelines': pipelines}}


def generate_yaml_files(df, gateway_catalog, gateway_schema, workspace_client, project_name,
                       node_type_id='m5d.large', driver_node_type_id='c5a.8xlarge',
                       output_gateway='gateways.yml', output_pipeline='pipelines.yml'):
    """Generate gateway and pipeline YAML files from dataframe.

    Args:
        df (pd.DataFrame): Input dataframe with required columns:
            - source_database: Source database name (e.g., 'mydb')
            - source_schema: Source schema name (e.g., 'public')
            - source_table_name: Source table name (e.g., 'users')
            - target_catalog: Databricks catalog for destination (e.g., 'bronze')
            - target_schema: Databricks schema for destination (e.g., 'sales')
            - target_table_name: Destination table name (optional)
            - pipeline_group: Groups tables into pipelines (e.g., 1, 2, 3)
            - gateway: Gateway identifier (e.g., 1, 2, 3)
            - connection_name: Databricks connection name (e.g., 'postgres_prod')
            - source_type: Database type (optional, defaults to 'POSTGRESQL')
            - schedule: Cron schedule (optional)
        gateway_catalog (str): Catalog for gateway storage metadata
        gateway_schema (str): Schema for gateway storage metadata
        workspace_client (WorkspaceClient): Databricks workspace client
        project_name (str): Project name prefix for all resources
        node_type_id (str): Worker node type (default: 'm5d.large')
        driver_node_type_id (str): Driver node type (default: 'c5a.8xlarge')
        output_gateway (str): Output path for gateway YAML
        output_pipeline (str): Output path for pipeline YAML
    """
    gateways_yaml = create_gateways(df, gateway_catalog, gateway_schema, workspace_client,
                                    project_name, node_type_id, driver_node_type_id)
    pipelines_yaml = create_pipelines(df, project_name)

    with open(output_gateway, 'w') as f:
        yaml.dump(gateways_yaml, f, default_flow_style=False, sort_keys=False)

    with open(output_pipeline, 'w') as f:
        yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)

    print(f"[OK] Generated {output_gateway}")
    print(f"[OK] Generated {output_pipeline}")


def main():
    """Main execution function with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description='Generate Databricks Asset Bundle YAML for PostgreSQL ingestion',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Basic usage (requires gateway and pipeline_group in CSV)
  python generate_postgres_pipeline.py example_config.csv

  # Auto-balance tables across gateways and pipelines
  python generate_postgres_pipeline.py example_config.csv \\
      --auto-balance \\
      --tables-per-gateway 50 \\
      --tables-per-pipeline 20

  # Auto-balance with custom gateway/pipeline distribution
  python generate_postgres_pipeline.py example_config.csv \\
      --auto-balance \\
      --tables-per-gateway 100 \\
      --tables-per-pipeline 30

  # With custom options
  python generate_postgres_pipeline.py example_config.csv \\
      --gateway-catalog bronze \\
      --gateway-schema metadata \\
      --project-name my_postgres_proj \\
      --node-type m5d.xlarge

  # Specify output paths
  python generate_postgres_pipeline.py example_config.csv \\
      --output-gateway resources/gateways.yml \\
      --output-pipeline resources/pipelines.yml
        '''
    )

    parser.add_argument('csv_path', help='Path to CSV configuration file')
    parser.add_argument('--gateway-catalog', default='bronze',
                       help='Catalog for gateway storage metadata (default: bronze)')
    parser.add_argument('--gateway-schema', default='metadata',
                       help='Schema for gateway storage metadata (default: metadata)')
    parser.add_argument('--project-name', default='postgres_ingestion',
                       help='Project name prefix for all resources (default: postgres_ingestion)')
    parser.add_argument('--node-type', default='m5d.large',
                       help='Worker node type (default: m5d.large)')
    parser.add_argument('--driver-node-type', default='c5a.8xlarge',
                       help='Driver node type (default: c5a.8xlarge)')
    parser.add_argument('--output-gateway', default='resources/gateways.yml',
                       help='Output path for gateway YAML (default: resources/gateways.yml)')
    parser.add_argument('--output-pipeline', default='resources/pipelines.yml',
                       help='Output path for pipeline YAML (default: resources/pipelines.yml)')
    parser.add_argument('--auto-balance', action='store_true',
                       help='Automatically assign gateway and pipeline_group values')
    parser.add_argument('--tables-per-gateway', type=int, default=50,
                       help='Max tables per gateway when auto-balancing (default: 50)')
    parser.add_argument('--tables-per-pipeline', type=int, default=20,
                       help='Max tables per pipeline when auto-balancing (default: 20)')

    args = parser.parse_args()

    print("=" * 80)
    print("POSTGRESQL PIPELINE YAML GENERATOR")
    print("=" * 80)

    # Load CSV configuration
    print(f"\n1. Reading configuration from {args.csv_path}...")
    try:
        df = pd.read_csv(args.csv_path)
        print(f"   [OK] Loaded {len(df)} table(s)")
    except FileNotFoundError:
        print(f"   [FAIL] File not found: {args.csv_path}")
        sys.exit(1)
    except Exception as e:
        print(f"   [FAIL] Error reading CSV: {e}")
        sys.exit(1)

    # Validate required columns
    base_required_cols = ['source_database', 'source_schema', 'source_table_name',
                         'target_catalog', 'target_schema', 'connection_name']

    if args.auto_balance:
        # When auto-balancing, gateway and pipeline_group are optional
        required_cols = base_required_cols
    else:
        # When not auto-balancing, gateway and pipeline_group are required
        required_cols = base_required_cols + ['pipeline_group', 'gateway']

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"   [FAIL] Missing required columns: {', '.join(missing_cols)}")
        sys.exit(1)

    # Apply auto-balancing if requested
    if args.auto_balance:
        print(f"\n2. Auto-balancing table distribution...")
        print(f"   Tables per gateway: {args.tables_per_gateway}")
        print(f"   Tables per pipeline: {args.tables_per_pipeline}")
        df = auto_assign_gateways_and_pipelines(
            df,
            tables_per_gateway=args.tables_per_gateway,
            tables_per_pipeline=args.tables_per_pipeline
        )
        print(f"   [OK] Auto-balance complete")

    # Create output directories
    Path(args.output_gateway).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output_pipeline).parent.mkdir(parents=True, exist_ok=True)

    # Initialize Databricks client
    step = 3 if args.auto_balance else 2
    print(f"\n{step}. Connecting to Databricks workspace...")
    try:
        workspace_client = WorkspaceClient()
        print("   [OK] Connected successfully")
    except Exception as e:
        print(f"   [FAIL] Failed to connect: {e}")
        sys.exit(1)

    # Generate YAML files
    step += 1
    print(f"\n{step}. Generating YAML files...")
    generate_yaml_files(
        df=df,
        gateway_catalog=args.gateway_catalog,
        gateway_schema=args.gateway_schema,
        workspace_client=workspace_client,
        project_name=args.project_name,
        node_type_id=args.node_type,
        driver_node_type_id=args.driver_node_type,
        output_gateway=args.output_gateway,
        output_pipeline=args.output_pipeline
    )

    # Summary
    print("\n" + "=" * 80)
    print("[OK] GENERATION COMPLETE")
    print("=" * 80)
    print(f"\nGateway catalog: {args.gateway_catalog}")
    print(f"Gateway schema: {args.gateway_schema}")
    print(f"Project name: {args.project_name}")
    print(f"\nGenerated files:")
    print(f"  - {args.output_gateway}")
    print(f"  - {args.output_pipeline}")
    print("\nNext steps:")
    print("  1. Review generated YAML files")
    print("  2. Validate: databricks bundle validate -t dev")
    print("  3. Deploy: databricks bundle deploy -t dev")


if __name__ == "__main__":
    main()
