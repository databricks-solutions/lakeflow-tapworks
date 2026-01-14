import yaml
import pandas as pd
import os


def convert_to_quartz_cron(cron_expression):
    """Convert standard 5-field cron to Quartz 6-field format.

    Standard cron format: minute hour day_of_month month day_of_week
    Quartz format: second minute hour day_of_month month day_of_week

    In Quartz, one of day_of_month or day_of_week must be '?' instead of '*'.
    This function replaces the last field (day_of_week) with '?' if it's '*'.

    Args:
        cron_expression (str): Standard cron expression (5 fields)
                               Example: "*/15 * * * *"

    Returns:
        str: Quartz cron expression (6 fields)
             Example: "0 */15 * * * ?"
    """
    fields = cron_expression.strip().split()
    if len(fields) == 5:
        # Replace day_of_week (last field) with '?' if it's '*'
        if fields[4] == '*':
            fields[4] = '?'
        # Prepend seconds field
        return f"0 {' '.join(fields)}"
    else:
        # If already in different format, just prepend 0
        return f"0 {cron_expression}"


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
        pipeline_name = f"{project_name}_pipeline_ingestion_{pipeline_group}"

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


def create_jobs(df, project_name):
    """Create job YAML configuration from dataframe.

    Creates a scheduled job for each pipeline that triggers the pipeline on a cron schedule.

    Args:
        df (pd.DataFrame): Input dataframe containing pipeline_group and schedule columns
        project_name (str): Project name prefix for all resources

    Returns:
        dict: Jobs YAML structure
    """
    jobs = {}

    for pipeline_group, group_df in df.groupby('pipeline_group'):
        schedule = group_df.iloc[0]['schedule']
        job_name = f"job_{project_name}_ingestion_{pipeline_group}"
        pipeline_resource_name = f"{project_name}_pipeline_ingestion_{pipeline_group}"

        # Convert standard cron to Quartz format
        quartz_cron = convert_to_quartz_cron(schedule)

        jobs[job_name] = {
            'name': f"{project_name} Pipeline Scheduler - {pipeline_group}",
            'schedule': {
                'quartz_cron_expression': quartz_cron,
                'timezone_id': 'UTC'
            },
            'tasks': [{
                'task_key': f'run_{project_name}_pipeline',
                'pipeline_task': {
                    'pipeline_id': f"${{resources.pipelines.{pipeline_resource_name}.id}}"
                }
            }]
        }

    return {'resources': {'jobs': jobs}}


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


def generate_yaml_files(df, project_name, output_dir, workspace_host, root_path):
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
            - gateway_worker_type: Worker node type (optional, e.g., 'm5d.large')
            - gateway_driver_type: Driver node type (optional, e.g., 'c5a.8xlarge')
            - pipeline_group: Groups tables into pipelines (e.g., 'sales_ingestion')
            - gateway: Gateway identifier (e.g., 'sqlserver_gateway_1')
            - connection_name: Databricks connection name (e.g., 'sql_server_prod')
            - schedule: Cron schedule (optional, e.g., '0 0 * * *')
        project_name (str): Project name prefix for all resources to avoid naming clashes
        output_dir (str): Output directory for the DAB project (default: 'dab_project')
        workspace_host (str): Workspace host URL
        root_path (str): Root path for bundle deployment

    Note:
        - Creates a proper DAB structure with root databricks.yml and resources subdirectory
        - Pipelines use target_catalog and target_schema from the dataframe for each table
        - Gateway catalog, schema, and node types are read from the dataframe and can vary per gateway
    """
    # Create directory structure
    resources_dir = os.path.join(output_dir, 'resources')
    os.makedirs(resources_dir, exist_ok=True)

    # Generate YAML content
    gateways_yaml = create_gateways(df, project_name)
    pipelines_yaml = create_pipelines(df, project_name)
    jobs_yaml = create_jobs(df, project_name)
    databricks_yaml = create_databricks_yml(project_name, workspace_host, root_path)

    # Define output paths
    databricks_yml_path = os.path.join(output_dir, 'databricks.yml')
    gateway_yml_path = os.path.join(resources_dir, 'gateways.yml')
    pipeline_yml_path = os.path.join(resources_dir, 'pipelines.yml')
    jobs_yml_path = os.path.join(resources_dir, 'jobs.yml')

    # Write databricks.yml
    with open(databricks_yml_path, 'w') as f:
        yaml.dump(databricks_yaml, f, default_flow_style=False, sort_keys=False)

    # Write gateway resources
    with open(gateway_yml_path, 'w') as f:
        yaml.dump(gateways_yaml, f, default_flow_style=False, sort_keys=False)

    # Write pipeline resources
    with open(pipeline_yml_path, 'w') as f:
        yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)

    # Write job resources
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
