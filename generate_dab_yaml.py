import yaml
import pandas as pd
import os
from databricks.sdk import WorkspaceClient


def get_connection_id(workspace_client, connection_name):
    """Lookup connection ID from connection name."""
    connection = workspace_client.connections.get(connection_name)
    return connection.connection_id


def create_gateways(df, catalog, schema, workspace_client, project_name, node_type_id='m5d.large', driver_node_type_id='c5a.8xlarge'):
    """Create gateway YAML configuration from dataframe."""
    gateways = {}
    unique_gateways = df.groupby('gateway').first()

    for gateway_id, row in unique_gateways.iterrows():
        connection_id = get_connection_id(workspace_client, row['connection_name'])
        gateway_name = f"{project_name}_gateway_{gateway_id}"
        pipeline_name = f"{project_name}_pipeline_{gateway_name}"

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
                'source_type': 'SQLSERVER'
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


def generate_yaml_files(df, catalog, schema, workspace_client, project_name, node_type_id='m5d.large', driver_node_type_id='c5a.8xlarge', output_gateway='gateways.yml', output_pipeline='pipelines.yml'):
    """Generate gateway and pipeline YAML files from dataframe.

    Args:
        df (pd.DataFrame): Input dataframe with the following required columns:
            - source_database: Source database name (e.g., 'sales_db')
            - source_schema: Source schema name (e.g., 'dbo')
            - source_table_name: Source table name (e.g., 'customers')
            - target_catalog: Databricks catalog for destination tables (e.g., 'bronze')
            - target_schema: Databricks schema for destination tables (e.g., 'sales')
            - target_table_name: Destination table name (e.g., 'customers')
            - pipeline_group: Groups tables into pipelines (e.g., 'sales_ingestion')
            - gateway: Gateway identifier (e.g., 'sqlserver_gateway_1')
            - connection_name: Databricks connection name (e.g., 'sql_server_prod')
            - schedule: Cron schedule (optional, e.g., '0 0 * * *')
        catalog (str): Catalog for gateway storage metadata (not table destinations)
        schema (str): Schema for gateway storage metadata (not table destinations)
        workspace_client (WorkspaceClient): Databricks workspace client instance
        project_name (str): Project name prefix for all resources to avoid naming clashes
        node_type_id (str): Worker node type for cluster (default: 'm5d.large')
        driver_node_type_id (str): Driver node type for cluster (default: 'c5a.8xlarge')
        output_gateway (str): Output path for gateway YAML file
        output_pipeline (str): Output path for pipeline YAML file

    Note:
        - Pipelines use target_catalog and target_schema from the dataframe for each table
        - The catalog and schema parameters are only for gateway storage location
    """
    gateways_yaml = create_gateways(df, catalog, schema, workspace_client, project_name, node_type_id, driver_node_type_id)
    pipelines_yaml = create_pipelines(df, project_name)

    with open(output_gateway, 'w') as f:
        yaml.dump(gateways_yaml, f, default_flow_style=False, sort_keys=False)

    with open(output_pipeline, 'w') as f:
        yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)

    print(f"Generated {output_gateway} and {output_pipeline}")


if __name__ == "__main__":
    # Load config from CSV
    df = pd.read_csv('examples/example_config.csv')

    # Create resources directory if it doesn't exist
    os.makedirs('resources', exist_ok=True)

    # Initialize Databricks workspace client
    workspace_client = WorkspaceClient()

    # Generate YAML files
    generate_yaml_files(
        df=df,
        catalog='jack_demos',
        schema='ingestion_schema',
        workspace_client=workspace_client,
        project_name='my_project',
        output_gateway='resources/gateways.yml',
        output_pipeline='resources/pipelines.yml'
    )
