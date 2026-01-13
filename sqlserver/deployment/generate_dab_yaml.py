import yaml
import pandas as pd
import os


def create_gateways(df, project_name, worker_type, driver_type):
    """Create gateway YAML configuration from dataframe.

    Note: gateway_catalog and gateway_schema are read from the dataframe columns.
    """
    gateways = {}
    unique_gateways = df.groupby('gateway').first()

    for gateway_id, row in unique_gateways.iterrows():
        gateway_name = f"{project_name}_gateway_{gateway_id}"
        pipeline_name = f"{project_name}_pipeline_{gateway_name}"

        # Read gateway catalog and schema from the dataframe
        gateway_catalog = row['gateway_catalog']
        gateway_schema = row['gateway_schema']

        gateway_config = {
            'name': gateway_name,
            'gateway_definition': {
                'connection_name': row['connection_name'],
                'gateway_storage_catalog': gateway_catalog,
                'gateway_storage_schema': gateway_schema,
                'gateway_storage_name': gateway_name,
            },
            'target': gateway_schema,
            'continuous': True,
            'catalog': gateway_catalog
        }

        # Add cluster configuration if node types are provided
        if worker_type or driver_type:
            cluster_config = {'num_workers': 1}
            if worker_type:
                cluster_config['node_type_id'] = worker_type
            if driver_type:
                cluster_config['driver_node_type_id'] = driver_type
            gateway_config['clusters'] = [cluster_config]

        gateways[pipeline_name] = gateway_config


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


def generate_yaml_files(df, project_name, worker_type, driver_type, output_dir, workspace_host, root_path):
    """Generate gateway and pipeline YAML files from dataframe in a proper DAB structure.

    Args:
        df (pd.DataFrame): Input dataframe with the following required columns:
            - source_database: Source database name (e.g., 'sales_db')
            - source_schema: Source schema name (e.g., 'dbo')
            - source_table_name: Source table name (e.g., 'customers')
            - target_catalog: Databricks catalog for destination tables (e.g., 'bronze')
            - target_schema: Databricks schema for destination tables (e.g., 'sales')
            - target_table_name: Destination table name (e.g., 'customers')
            - gateway_catalog: Catalog for gateway storage metadata (e.g., 'bronze')
            - gateway_schema: Schema for gateway storage metadata (e.g., 'ingestion')
            - pipeline_group: Groups tables into pipelines (e.g., 'sales_ingestion')
            - gateway: Gateway identifier (e.g., 'sqlserver_gateway_1')
            - connection_name: Databricks connection name (e.g., 'sql_server_prod')
            - schedule: Cron schedule (optional, e.g., '0 0 * * *')
        project_name (str): Project name prefix for all resources to avoid naming clashes
        worker_type (str): Worker node type for cluster (optional, e.g., 'm5d.large')
        driver_type (str): Driver node type for cluster (optional, e.g., 'c5a.8xlarge')
        output_dir (str): Output directory for the DAB project (default: 'dab_project')
        workspace_host (str): Workspace host URL
        root_path (str): Root path for bundle deployment

    Note:
        - Creates a proper DAB structure with root databricks.yml and resources subdirectory
        - Pipelines use target_catalog and target_schema from the dataframe for each table
        - Gateway catalog and schema are read from the dataframe and can vary per gateway
    """
    # Create directory structure
    resources_dir = os.path.join(output_dir, 'resources')
    os.makedirs(resources_dir, exist_ok=True)

    # Generate YAML content
    gateways_yaml = create_gateways(df, project_name, worker_type, driver_type)
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

    # Generate YAML files with proper DAB structure
    # Note: This example is incomplete - workspace_host and root_path are required
    # Also note: gateway_catalog and gateway_schema should be in the dataframe
    # generate_yaml_files(
    #     df=df,
    #     project_name='my_project',
    #     worker_type=None,
    #     driver_type=None,
    #     output_dir='dab_project',
    #     workspace_host='https://your-workspace.cloud.databricks.com',
    #     root_path='/Users/your-email/.bundle/${bundle.name}/${bundle.target}'
    # )
