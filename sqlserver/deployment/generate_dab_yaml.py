import yaml
import pandas as pd
import os
from databricks.sdk import WorkspaceClient


def get_connection_id(workspace_client, connection_name):
    """Lookup connection ID from connection name."""
    connection = workspace_client.connections.get(connection_name)
    return connection.connection_id


def create_gateways(df, catalog, schema, project_name, node_type, driver_node_type):
    # def create_gateways(df, catalog, schema, workspace_client, project_name, node_type_id='m5d.large', driver_node_type_id='c5a.8xlarge'):
    """Create gateway YAML configuration from dataframe."""
    gateways = {}
    unique_gateways = df.groupby('gateway').first()

    for gateway_id, row in unique_gateways.iterrows():
        # connection_id = get_connection_id(workspace_client, row['connection_name'])
        gateway_name = f"{project_name}_gateway_{gateway_id}"
        pipeline_name = f"{project_name}_pipeline_{gateway_name}"

        if node_type and driver_node_type:       
            gateways[pipeline_name] = {
                'name': gateway_name,
                'clusters': [{
                    'node_type_id': node_type_id,
                    'driver_node_type_id': driver_node_type_id,
                    'num_workers': 1
                }],
                'gateway_definition': {
                    'connection_name': row['connection_name'],
                    # 'connection_id': connection_id,
                    'gateway_storage_catalog': catalog,
                    'gateway_storage_schema': schema,
                    'gateway_storage_name': gateway_name,
                },
                'target': schema,
                'continuous': True,
                'catalog': catalog
            }
        else:
            gateways[pipeline_name] = {
                'name': gateway_name,
                'gateway_definition': {
                    'connection_name': row['connection_name'],
                    # 'connection_id': connection_id,
                    'gateway_storage_catalog': catalog,
                    'gateway_storage_schema': schema,
                    'gateway_storage_name': gateway_name,
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
                'pipelines.cdcApplierFetchMetadataTimeoutSeconds': '600'
            },
            'ingestion_definition': {
                'ingestion_gateway_id': f"${{resources.pipelines.{project_name}_pipeline_{project_name}_gateway_{gateway_id}.id}}",
                'objects': tables
            }
        }

    return {'resources': {'pipelines': pipelines}}


def create_databricks_yml(project_name, workspace_host, root_path):
    """Create the main databricks.yml file for the DAB project.

    Args:
        project_name (str): Project name for the bundle
        workspace_host (str): Workspace host URL
        root_path (str): Root path for bundle deployment

    Returns:
        dict: The databricks.yml structure
    """

    return {
        'bundle': {
            'name': project_name
        },
        'include': [
            'resources/*.yml'
        ],
        'targets': {
            'prod': {
                'mode': 'production',
                'workspace': {
                    'host': f'{workspace_host}',
                    'root_path': f'{root_path}'
                }
            }
        }
    }


def generate_yaml_files(df, gateway_catalog, gateway_schema, project_name, node_type_id, driver_node_type_id, output_dir, workspace_host, root_path):

# def generate_yaml_files(df, gateway_catalog, gateway_schema, workspace_client, project_name, node_type_id='m5d.large', driver_node_type_id='c5a.8xlarge', output_dir='dab_project'):
    """Generate gateway and pipeline YAML files from dataframe in a proper DAB structure.

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
        gateway_catalog (str): Catalog for gateway storage metadata (not table destinations)
        gateway_schema (str): Schema for gateway storage metadata (not table destinations)
        # workspace_client (WorkspaceClient): Databricks workspace client instance
        project_name (str): Project name prefix for all resources to avoid naming clashes
        node_type_id (str): Worker node type for cluster (default: 'm5d.large')
        driver_node_type_id (str): Driver node type for cluster (default: 'c5a.8xlarge')
        output_dir (str): Output directory for the DAB project (default: 'dab_project')
        workspace_host (str): Workspace host URL
        root_path (str): Root path for bundle deployment

    Note:
        - Creates a proper DAB structure with root databricks.yml and resources subdirectory
        - Pipelines use target_catalog and target_schema from the dataframe for each table
        - The gateway_catalog and gateway_schema parameters are only for gateway storage location
    """
    # Create directory structure
    resources_dir = os.path.join(output_dir, 'resources')
    os.makedirs(resources_dir, exist_ok=True)

    # Generate YAML content
    # gateways_yaml = create_gateways(df, gateway_catalog, gateway_schema, workspace_client, project_name, node_type_id, driver_node_type_id)
    gateways_yaml = create_gateways(df, gateway_catalog, gateway_schema, project_name, node_type_id, driver_node_type_id)
    pipelines_yaml = create_pipelines(df, project_name)
    databricks_yaml = create_databricks_yml(project_name, workspace_host, root_path)

    # Define output paths
    databricks_yml_path = os.path.join(output_dir, 'databricks.yml')
    gateway_yml_path = os.path.join(resources_dir, 'gateways.yml')
    pipeline_yml_path = os.path.join(resources_dir, 'pipelines.yml')

    # Write databricks.yml
    with open(databricks_yml_path, 'w') as f:
        yaml.dump(databricks_yaml, f, default_flow_style=False, sort_keys=False)

    # Write gateway resources
    with open(gateway_yml_path, 'w') as f:
        yaml.dump(gateways_yaml, f, default_flow_style=False, sort_keys=False)

    # Write pipeline resources
    with open(pipeline_yml_path, 'w') as f:
        yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)

    print(f"Generated DAB project structure in: {output_dir}")
    print(f"  - {databricks_yml_path}")
    print(f"  - {gateway_yml_path}")
    print(f"  - {pipeline_yml_path}")


if __name__ == "__main__":
    # Load config from CSV
    df = pd.read_csv('examples/example_config.csv')

    # Initialize Databricks workspace client
    # workspace_client = WorkspaceClient()

    # Generate YAML files with proper DAB structure
    generate_yaml_files(
        df=df,
        gateway_catalog='jack_demos',
        gateway_schema='ingestion_schema',
        # workspace_client=workspace_client,
        project_name='my_project',
        output_dir='dab_project'
    )
