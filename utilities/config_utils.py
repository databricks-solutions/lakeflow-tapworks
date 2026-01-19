"""
Configuration validation and loading utilities.

Shared functions for processing and validating input configurations
across all connectors.
"""

import pandas as pd
from pathlib import Path
from utilities.cron_utils import convert_cron_to_quartz


def process_input_config(
    df: pd.DataFrame,
    required_columns: list,
    default_values: dict = None,
    override_input_config: dict = None
) -> pd.DataFrame:
    """
    Validate and normalize input configuration DataFrame.

    This function ensures all required columns are present, adds optional columns
    with defaults if missing, and fills empty/NaN values appropriately.

    The input DataFrame can come from any source (CSV, Delta table, or code).
    The output DataFrame will have all required and optional columns with clean values
    (no NaN, empty strings replaced with defaults).

    Args:
        df (pd.DataFrame): Input DataFrame from any source (CSV, Delta, code)
        required_columns (list): List of required column names that must be present.
            Example:
            [
                'source_database', 'source_schema', 'source_table_name',
                'target_catalog', 'target_schema', 'target_table_name',
                'connection_name'
            ]
        default_values (dict, optional): Dictionary of optional columns with their default values.
            Missing columns will be added, NaN/empty values will be filled with defaults.
            If None, no default values will be applied.
            Example:
            {
                'schedule': '*/15 * * * *',
                'priority_flag': 0
            }
        override_input_config (dict, optional): Dictionary of column overrides.
            Values in these columns will be replaced with the override value for ALL rows.
            This is useful for forcing specific values across the entire configuration.
            Example:
            {
                'schedule': '*/30 * * * *',  # Override schedule for all rows
                'target_catalog': 'bronze'   # Force all to bronze catalog
            }

    Returns:
        pd.DataFrame: Normalized DataFrame with all required and optional columns,
                     NaN values filled, empty strings replaced with defaults,
                     and any overrides applied

    Raises:
        ValueError: If required columns are missing
        ValueError: If DataFrame is empty

    Example Usage:
        >>> required = [
        ...     'source_database', 'source_schema', 'source_table_name',
        ...     'target_catalog', 'target_schema', 'target_table_name',
        ...     'connection_name'
        ... ]
        >>> defaults = {
        ...     'schedule': '*/15 * * * *',
        ...     'priority_flag': 0
        ... }
        >>> override = {'schedule': '*/30 * * * *'}
        >>> normalized_df = process_input_config(df, required, defaults, override)
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # Check if dataframe is empty
    if df.empty:
        raise ValueError("Input DataFrame is empty")

    # Validate required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}\n"
            f"Required columns: {', '.join(required_columns)}"
        )

    # Add optional columns if not present and handle NaN/empty values
    if default_values:
        for col_name, default_value in default_values.items():
            if col_name not in df.columns:
                print(f"Info: '{col_name}' column not found. Adding with default: {default_value}")
                df[col_name] = default_value
            else:
                # Fill NaN values with default (skip if default is None, as pandas doesn't support fillna(None))
                if default_value is not None:
                    df[col_name] = df[col_name].fillna(default_value)

                # Replace empty strings with default (for string columns)
                if isinstance(default_value, str):
                    mask = df[col_name].astype(str).str.strip() == ''
                    df.loc[mask, col_name] = default_value

    # Apply overrides if provided
    if override_input_config:
        for col_name, override_value in override_input_config.items():
            print(f"Info: Overriding '{col_name}' column with value: {override_value}")
            df[col_name] = override_value

    print(f"\n✓ Configuration validated: {len(df)} rows with all required and optional columns")

    return df


def load_input_csv(
    input_csv: str,
    required_columns_hint: str = None
) -> pd.DataFrame:
    """
    Load and validate input CSV configuration file.

    Args:
        input_csv (str): Path to input CSV file
        required_columns_hint (str): Optional hint text for error message
            describing required columns for the specific connector

    Returns:
        pd.DataFrame: Loaded configuration dataframe

    Raises:
        FileNotFoundError: If input file does not exist
        ValueError: If CSV is empty or cannot be parsed

    Example Usage:
        >>> df = load_input_csv('config.csv')
        >>> # With custom hint:
        >>> df = load_input_csv(
        ...     'config.csv',
        ...     required_columns_hint="source_database, source_schema, source_table_name"
        ... )
    """
    input_path = Path(input_csv)

    if not input_path.exists():
        error_msg = f"Input file not found: {input_csv}\n\n"
        if required_columns_hint:
            error_msg += f"Please create an input CSV with required columns:\n{required_columns_hint}"
        else:
            error_msg += "Please create an input CSV with the required columns for your connector."
        raise FileNotFoundError(error_msg)

    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        raise ValueError(f"Failed to parse CSV file: {e}")

    if df.empty:
        raise ValueError(f"Input CSV is empty: {input_csv}")

    print(f"✓ Loaded {len(df)} rows from {input_csv}")

    return df


def create_jobs(
    df: pd.DataFrame,
    project_name: str,
    connector_type: str
) -> dict:
    """
    Create job YAML configuration from dataframe for any connector.

    Creates a scheduled job for each pipeline that triggers the pipeline on a cron schedule.
    This is a shared function used across all connectors (Salesforce, SQL Server, Google Analytics).

    Args:
        df (pd.DataFrame): Input dataframe containing pipeline_group and schedule columns
        project_name (str): Project name prefix for all resources
        connector_type (str): Connector type identifier (e.g., 'sfdc', 'ga4', 'sqlserver')
            Used for naming resources and tasks

    Returns:
        dict: Jobs YAML structure with format:
            {
                'resources': {
                    'jobs': {
                        'job_name': {
                            'name': 'Job Display Name',
                            'schedule': {...},
                            'tasks': [...]
                        }
                    }
                }
            }

    Example Usage:
        >>> # For Salesforce
        >>> jobs = create_jobs(df, 'my_project', 'sfdc')
        >>> # For Google Analytics
        >>> jobs = create_jobs(df, 'my_project', 'ga4')
        >>> # For SQL Server
        >>> jobs = create_jobs(df, 'my_project', 'sqlserver')
    """
    jobs = {}

    # Group by pipeline_group
    for pipeline_group, group_df in df.groupby('pipeline_group'):
        schedule = group_df.iloc[0]['schedule']

        # Only create job if schedule is defined
        if pd.notna(schedule) and schedule and str(schedule).strip():
            # Generate resource names based on connector type
            if connector_type == 'sqlserver':
                # SQL Server uses different naming pattern
                job_name = f"job_{project_name}_ingestion_{pipeline_group}"
                job_display = f"{project_name} Pipeline Scheduler - {pipeline_group}"
                pipeline_resource_name = f"{project_name}_pipeline_ingestion_{pipeline_group}"
                task_key = f'run_{project_name}_pipeline'
            else:
                # Salesforce and GA4 use simpler pattern
                job_name = f"job_{connector_type}_{pipeline_group}"
                job_display = f"{connector_type.upper()} Pipeline Scheduler - {pipeline_group}"
                pipeline_resource_name = f"pipeline_{connector_type}_{pipeline_group}"
                task_key = f'run_{connector_type}_pipeline'

            # Convert standard cron to Quartz format
            quartz_schedule = convert_cron_to_quartz(schedule)

            jobs[job_name] = {
                'name': job_display,
                'schedule': {
                    'quartz_cron_expression': quartz_schedule,
                    'timezone_id': 'UTC'
                },
                'tasks': [{
                    'task_key': task_key,
                    'pipeline_task': {
                        'pipeline_id': f"${{resources.pipelines.{pipeline_resource_name}.id}}"
                    }
                }]
            }

    return {'resources': {'jobs': jobs}}


def create_databricks_yml(
    project_name: str,
    targets: dict,
    default_target: str = 'dev'
) -> dict:
    """
    Create the main databricks.yml file for any connector with flexible target environments.

    Supports any number of environments (dev, staging, qa, prod, etc.).

    Args:
        project_name (str): Project name for the bundle
        targets (dict): Dictionary of target configurations where key is environment name
            and value is the configuration dict.

            Format:
            {
                'env_name': {
                    'workspace_host': 'https://workspace.com',  # Required
                    'root_path': '/path/to/bundle',  # Optional
                    'mode': 'development' | 'production'  # Optional, auto-determined if not provided
                },
                ...
            }

        default_target (str): Which target should be the default (default: 'dev')

    Returns:
        dict: The databricks.yml structure with all specified targets

    Example Usage:
        >>> # Simple: Just dev and prod with same workspace
        >>> yml = create_databricks_yml(
        ...     'my_project',
        ...     targets={
        ...         'dev': {'workspace_host': 'https://workspace.com'},
        ...         'prod': {'workspace_host': 'https://workspace.com'}
        ...     }
        ... )

        >>> # Advanced: Multiple environments with different configs
        >>> yml = create_databricks_yml(
        ...     'my_project',
        ...     targets={
        ...         'dev': {
        ...             'workspace_host': 'https://dev.databricks.com',
        ...             'root_path': '/Users/dev/.bundle/${bundle.name}/${bundle.target}'
        ...         },
        ...         'staging': {
        ...             'workspace_host': 'https://staging.databricks.com',
        ...             'root_path': '/Workspace/staging/${bundle.name}',
        ...             'mode': 'development'
        ...         },
        ...         'qa': {
        ...             'workspace_host': 'https://qa.databricks.com'
        ...         },
        ...         'prod': {
        ...             'workspace_host': 'https://prod.databricks.com',
        ...             'root_path': '/Workspace/prod/${bundle.name}',
        ...             'mode': 'production'
        ...         }
        ...     },
        ...     default_target='dev'
        ... )

        >>> # SQL Server with root_path
        >>> yml = create_databricks_yml(
        ...     'sql_project',
        ...     targets={
        ...         'dev': {
        ...             'workspace_host': 'https://workspace.com',
        ...             'root_path': '/Users/user/.bundle/${bundle.name}/${bundle.target}'
        ...         },
        ...         'prod': {
        ...             'workspace_host': 'https://workspace.com',
        ...             'root_path': '/Workspace/prod/.bundle/${bundle.name}/${bundle.target}'
        ...         }
        ...     }
        ... )

    Note:
        - Mode is auto-determined: 'production' for 'prod', 'development' for others
        - Users deploy to specific targets: databricks bundle deploy -t <target_name>
        - The default_target gets 'default: true' in the config
    """
    if not targets:
        raise ValueError("At least one target must be provided")

    if default_target not in targets:
        raise ValueError(f"default_target '{default_target}' must be one of the provided targets: {list(targets.keys())}")

    config = {
        'bundle': {'name': project_name},
        'include': ['resources/*.yml'],
        'targets': {}
    }

    # Build each target
    for target_name, target_config in targets.items():
        if 'workspace_host' not in target_config:
            raise ValueError(f"Target '{target_name}' must have 'workspace_host'")

        # Auto-determine mode if not explicitly provided
        mode = target_config.get('mode')
        if not mode:
            mode = 'production' if target_name == 'prod' else 'development'

        # Build target configuration
        target_def = {
            'mode': mode,
            'workspace': {
                'host': target_config['workspace_host']
            }
        }

        # Add root_path if provided
        if 'root_path' in target_config and target_config['root_path']:
            target_def['workspace']['root_path'] = target_config['root_path']

        # Mark as default if this is the default target
        if target_name == default_target:
            target_def['default'] = True

        config['targets'][target_name] = target_def

    return config
