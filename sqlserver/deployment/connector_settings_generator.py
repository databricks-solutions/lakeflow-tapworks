import yaml
import pandas as pd
import os
import sys
from pathlib import Path

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from utilities import convert_cron_to_quartz, create_jobs, create_databricks_yml, generate_resource_names


def create_gateways(df, project_name):
    """Create gateway YAML configuration from dataframe.

    Note: All gateway configuration is read from the dataframe columns:
    - gateway_catalog, gateway_schema, connection_name
    - gateway_worker_type, gateway_driver_type (optional, for non-serverless)
    """
    gateways = {}
    unique_gateways = df.groupby('gateway').first()

    for gateway_id, row in unique_gateways.iterrows():
        gateway_name = f"{project_name}_gateway_{gateway_id}"
        pipeline_name = f"{project_name}_pipeline_{gateway_name}"

        # Read gateway configuration from the dataframe
        gateway_catalog = row['gateway_catalog']
        gateway_schema = row['gateway_schema']
        worker_type = row.get('gateway_worker_type')
        driver_type = row.get('gateway_driver_type')

        gateway_config = {
            'name': gateway_name,
            'gateway_definition': {
                'connection_name': row['connection_name'],
                'gateway_storage_catalog': gateway_catalog,
                'gateway_storage_schema': gateway_schema,
                'gateway_storage_name': gateway_name,
            },
            'schema': gateway_schema,
            'continuous': True,
            'catalog': gateway_catalog
        }

        # Add cluster configuration if node types are provided
        # If both are None/empty/NaN, use serverless (no cluster config)
        # Check for valid non-NaN values using pd.notna()
        has_worker_type = pd.notna(worker_type) and worker_type != '' and worker_type is not None
        has_driver_type = pd.notna(driver_type) and driver_type != '' and driver_type is not None

        if has_worker_type or has_driver_type:
            cluster_config = {'num_workers': 1}
            if has_worker_type:
                cluster_config['node_type_id'] = worker_type
            if has_driver_type:
                cluster_config['driver_node_type_id'] = driver_type
            gateway_config['clusters'] = [cluster_config]

        gateways[pipeline_name] = gateway_config


    return {'resources': {'pipelines': gateways}}


def create_pipelines(df, project_name):
    """Create pipeline YAML configuration from dataframe."""
    pipelines = {}

    for pipeline_group, group_df in df.groupby('pipeline_group'):
        gateway_id = group_df.iloc[0]['gateway']

        # Generate resource names using unified naming function
        names = generate_resource_names(pipeline_group, 'sqlserver')

        # Get target catalog and schema from first row (all tables in group should have same target)
        target_catalog = group_df.iloc[0]['target_catalog']
        target_schema = group_df.iloc[0]['target_schema']

        tables = [{
            'table': {
                'source_catalog': row['source_database'],
                'source_schema': row['source_schema'],
                'source_table': row['source_table_name'],
                'destination_catalog': row['target_catalog'],
                'destination_schema': row['target_schema'],
                'destination_table': row['target_table_name']
            }
        } for _, row in group_df.iterrows()]

        pipelines[names['pipeline_resource_name']] = {
            'name': names['pipeline_name'],
            'configuration': {
                'pipelines.***REMOVED***': '600'
            },
            'ingestion_definition': {
                'ingestion_gateway_id': f"${{resources.pipelines.{project_name}_pipeline_{project_name}_gateway_{gateway_id}.id}}",
                'objects': tables
            },
            'schema': target_schema,
            'catalog': target_catalog
        }

    return {'resources': {'pipelines': pipelines}}


def generate_yaml_files(df, project_name, output_dir, targets):
    """Generate gateway and pipeline YAML files from dataframe in a proper DAB structure.

    Creates separate DAB packages for each unique project_name in the dataframe.

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
            - gateway_worker_type: Worker node type (optional, e.g., 'm5d.large')
            - gateway_driver_type: Driver node type (optional, e.g., 'c5a.8xlarge')
            - pipeline_group: Groups tables into pipelines (e.g., 'sales_ingestion')
            - gateway: Gateway identifier (e.g., 'sqlserver_gateway_1')
            - connection_name: Databricks connection name (e.g., 'sql_server_prod')
            - schedule: Cron schedule (optional, e.g., '0 0 * * *')
            - project_name: Project name (required)
        project_name (str): Not used (kept for backward compatibility)
        output_dir (str): Output directory for the DAB project(s)
        targets (dict): Target environments configuration (required)
            Format: {'env_name': {'workspace_host': '...', 'root_path': '...'}, ...}

    Note:
        - Always creates separate DAB package for each unique project_name
        - Output structure: output/{project_name}/databricks.yml
        - Creates a proper DAB structure with root databricks.yml and resources subdirectory
        - Pipelines use target_catalog and target_schema from the dataframe for each table
        - Gateway catalog, schema, and node types are read from the dataframe and can vary per gateway
    """
    # Group by project_name and create separate DAB packages
    for project, project_df in df.groupby('project_name'):
        project_output_dir = os.path.join(output_dir, str(project))
        print(f"\nCreating DAB for project: {project}")
        print(f"  Tables: {len(project_df)}")
        print(f"  Pipelines: {project_df['pipeline_group'].nunique()}")
        print(f"  Output: {project_output_dir}")

        # Create directory structure
        resources_dir = os.path.join(project_output_dir, 'resources')
        os.makedirs(resources_dir, exist_ok=True)

        # Generate YAML content for this project
        gateways_yaml = create_gateways(project_df, str(project))
        pipelines_yaml = create_pipelines(project_df, str(project))
        jobs_yaml = create_jobs(project_df, str(project), connector_type='sqlserver')
        databricks_yaml = create_databricks_yml(
            project_name=str(project),
            targets=targets,
            default_target='dev'
        )

        # Define output paths
        databricks_yml_path = os.path.join(project_output_dir, 'databricks.yml')
        gateway_yml_path = os.path.join(resources_dir, 'gateways.yml')
        pipeline_yml_path = os.path.join(resources_dir, 'pipelines.yml')
        jobs_yml_path = os.path.join(resources_dir, 'jobs.yml')

        # Write YAML files
        with open(databricks_yml_path, 'w') as f:
            yaml.dump(databricks_yaml, f, default_flow_style=False, sort_keys=False)

        with open(gateway_yml_path, 'w') as f:
            yaml.dump(gateways_yaml, f, default_flow_style=False, sort_keys=False)

        with open(pipeline_yml_path, 'w') as f:
            yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)

        with open(jobs_yml_path, 'w') as f:
            yaml.dump(jobs_yaml, f, default_flow_style=False, sort_keys=False)

    print(f"Generated DAB project structure in: {output_dir}")
    print(f"  - {databricks_yml_path}")
    print(f"  - {gateway_yml_path}")
    print(f"  - {pipeline_yml_path}")
    print(f"  - {jobs_yml_path}")


if __name__ == "__main__":
    # Load config from CSV
    df = pd.read_csv('examples/example_config.csv')

    # Generate YAML files with proper DAB structure
    # Note: The dataframe should contain these columns:
    # - gateway_catalog, gateway_schema, connection_name
    # - gateway_worker_type, gateway_driver_type (optional for serverless)
    # generate_yaml_files(
    #     df=df,
    #     project_name='my_project',
    #     output_dir='dab_project',
    #     workspace_host='https://your-workspace.cloud.databricks.com',
    #     root_path='/Users/your-email/.bundle/${bundle.name}/${bundle.target}'
    # )
