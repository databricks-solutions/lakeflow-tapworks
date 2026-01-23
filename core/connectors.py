"""
Base connector classes for Databricks Lakeflow Connect pipeline generation.

This module provides abstract base classes for implementing connectors to various
data sources. Each connector handles CSV input processing, load balancing, and
YAML generation for Databricks Asset Bundle deployment.

Architecture:
    BaseConnector (ABC)
    ├── DatabaseConnector (ABC) - For database sources with gateways
    │   └── SQLServerConnector
    └── SaaSConnector (ABC) - For SaaS sources without gateways
        ├── SalesforceConnector
        └── GoogleAnalyticsConnector

Usage:
    # Instantiate a connector
    connector = SQLServerConnector()

    # Run complete pipeline generation
    result_df = connector.run_complete_pipeline_generation(
        df=input_df,
        output_dir='output',
        targets={'dev': {'workspace_host': '...', 'root_path': '...'}},
        default_values={'project_name': 'my_project'}
    )
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Optional
from pathlib import Path
import sys

# Import shared utilities
from utilities import process_input_config, load_input_csv, convert_cron_to_quartz


class BaseConnector(ABC):
    """
    Abstract base class for all connectors.

    Defines the common interface and workflow that all connectors must implement.
    Each connector provides connector-specific configuration (required columns,
    defaults, etc.) and inherits shared logic for CSV processing and pipeline generation.
    """

    def __init__(self):
        """Initialize the connector with its configuration."""
        self._validate_configuration()

    @property
    @abstractmethod
    def connector_type(self) -> str:
        """
        Return the connector type identifier (e.g., 'sqlserver', 'salesforce', 'ga4').

        Used for:
        - Naming conventions in generated resources
        - Default project naming
        - Logging and error messages
        """
        pass

    @property
    @abstractmethod
    def required_columns(self) -> list:
        """
        Return list of required columns for this connector's input CSV.

        These columns must be present and non-empty in the input data.
        The base class will validate their presence before processing.

        Example:
            ['source_database', 'source_schema', 'source_table_name',
             'target_catalog', 'target_schema', 'target_table_name',
             'connection_name']
        """
        pass

    @property
    @abstractmethod
    def default_values(self) -> Dict[str, any]:
        """
        Return default values for optional columns.

        These values are used when columns are missing or contain empty/NaN values.
        Should include defaults for all optional connector-specific columns.

        Example:
            {
                'schedule': '*/15 * * * *',
                'gateway_worker_type': None,
                'gateway_driver_type': None
            }
        """
        pass

    @property
    def default_project_name(self) -> str:
        """
        Return default project name if not provided.

        Uses connector_type as the default project name suffix.
        Override in subclass if different default is needed.
        """
        return f"{self.connector_type}_ingestion"

    def _validate_configuration(self):
        """
        Validate that the connector is properly configured.

        Ensures that all abstract properties have been implemented in the subclass.
        Raises TypeError if connector is not properly configured.
        """
        # This will fail if abstract properties aren't implemented
        try:
            _ = self.connector_type
            _ = self.required_columns
            _ = self.default_values
        except NotImplementedError:
            raise TypeError(
                f"{self.__class__.__name__} must implement all abstract properties"
            )

    def load_and_normalize_input(
        self,
        df: pd.DataFrame,
        default_values: Optional[Dict] = None,
        override_input_config: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Load and normalize input CSV data.

        Applies the configuration hierarchy:
        1. CSV values (base)
        2. Connector default_values (for missing/empty)
        3. User-provided default_values parameter (overrides connector defaults)
        4. override_input_config (overrides everything)

        Args:
            df: Input DataFrame to normalize
            default_values: User-provided defaults (overrides connector defaults)
            override_input_config: Values to force for all rows

        Returns:
            Normalized DataFrame with all required columns and defaults applied
        """
        # Merge connector defaults with user-provided defaults
        built_in_defaults = {'project_name': self.default_project_name}
        connector_defaults = {**built_in_defaults, **self.default_values}
        final_defaults = {**connector_defaults, **(default_values or {})}

        # Process input configuration using shared utility
        normalized_df = process_input_config(
            df=df,
            required_columns=self.required_columns,
            default_values=final_defaults,
            override_input_config=override_input_config
        )

        # Apply connector-specific normalization
        normalized_df = self._apply_connector_specific_normalization(normalized_df)

        return normalized_df

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply connector-specific normalization logic.

        Override in subclass to add custom normalization steps.
        Called after standard normalization is complete.

        Args:
            df: Normalized DataFrame

        Returns:
            DataFrame with connector-specific normalization applied
        """
        # Handle prefix and subgroup defaults (common to all connectors)
        if 'prefix' not in df.columns:
            df['prefix'] = df['project_name']
        else:
            mask = df['prefix'].isna() | (df['prefix'].astype(str).str.strip() == '')
            df.loc[mask, 'prefix'] = df.loc[mask, 'project_name']

        if 'subgroup' not in df.columns:
            df['subgroup'] = '01'
        else:
            mask = df['subgroup'].isna() | (df['subgroup'].astype(str).str.strip() == '')
            df.loc[mask, 'subgroup'] = '01'

        return df

    def _generate_resource_names(self, pipeline_group: str) -> Dict[str, str]:
        """
        Generate consistent resource names for pipelines and jobs.

        Args:
            pipeline_group: Pipeline group identifier (e.g., 'sales_01')

        Returns:
            Dictionary containing all resource names:
                - pipeline_name: Display name for pipeline
                - pipeline_resource_name: Resource identifier for pipeline
                - job_name: Resource identifier for job
                - job_display_name: Display name for job
                - task_key: Task key for pipeline task
        """
        # Normalize connector type for display
        connector_display = {
            'sfdc': 'SFDC',
            'salesforce': 'Salesforce',
            'sqlserver': 'SQL Server',
            'ga4': 'GA4'
        }.get(self.connector_type, self.connector_type.upper())

        return {
            'pipeline_name': f"{connector_display} Ingestion - {pipeline_group}",
            'pipeline_resource_name': f"pipeline_{self.connector_type}_{pipeline_group}",
            'job_name': f"job_{self.connector_type}_{pipeline_group}",
            'job_display_name': f"{connector_display} Pipeline Scheduler - {pipeline_group}",
            'task_key': "run_pipeline"
        }

    def _create_jobs(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create job YAML configuration from dataframe.

        Creates a scheduled job for each pipeline that triggers the pipeline on a cron schedule.

        Args:
            df: DataFrame containing pipeline_group and schedule columns
            project_name: Project name prefix for all resources

        Returns:
            Dictionary with jobs YAML structure
        """
        jobs = {}

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            schedule = group_df.iloc[0]['schedule']

            # Only create job if schedule is defined
            if pd.notna(schedule) and schedule and str(schedule).strip():
                names = self._generate_resource_names(pipeline_group)

                # Convert standard cron to Quartz format
                quartz_schedule = convert_cron_to_quartz(schedule)

                job_config = {
                    'name': names['job_display_name'],
                    'schedule': {
                        'quartz_cron_expression': quartz_schedule,
                        'timezone_id': 'UTC'
                    },
                    'tasks': [{
                        'task_key': names['task_key'],
                        'pipeline_task': {
                            'pipeline_id': f"${{resources.pipelines.{names['pipeline_resource_name']}.id}}"
                        }
                    }]
                }

                # Add pause_status if specified
                if 'pause_status' in group_df.columns:
                    pause_status = group_df.iloc[0]['pause_status']
                    if pd.notna(pause_status) and pause_status and str(pause_status).strip():
                        job_config['pause_status'] = str(pause_status).upper()

                jobs[names['job_name']] = job_config

        return {'resources': {'jobs': jobs}}

    def _create_databricks_yml(
        self,
        project_name: str,
        targets: Dict[str, Dict],
        default_target: str = 'dev'
    ) -> Dict:
        """
        Create the main databricks.yml file with flexible target environments.

        Args:
            project_name: Project name for the bundle
            targets: Dictionary of target configurations
                Format: {'env_name': {'workspace_host': '...', 'root_path': '...', 'mode': '...'}}
            default_target: Which target should be the default

        Returns:
            Dictionary with databricks.yml structure
        """
        if not targets:
            raise ValueError("At least one target must be provided")

        if default_target not in targets:
            raise ValueError(f"default_target '{default_target}' must be one of: {list(targets.keys())}")

        config = {
            'bundle': {'name': project_name},
            'include': ['resources/*.yml'],
            'targets': {}
        }

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

    @abstractmethod
    def generate_pipeline_config(
        self,
        df: pd.DataFrame,
        **kwargs
    ) -> pd.DataFrame:
        """
        Generate pipeline configuration with load balancing.

        Implements connector-specific load balancing logic.
        Must add 'pipeline_group' column to the DataFrame.
        Database connectors must also add 'gateway' column.

        Args:
            df: Normalized input DataFrame
            **kwargs: Connector-specific parameters (e.g., max_tables_per_pipeline)

        Returns:
            DataFrame with pipeline_group (and gateway for database connectors)
        """
        pass

    @abstractmethod
    def generate_yaml_files(
        self,
        df: pd.DataFrame,
        output_dir: str,
        targets: Dict[str, Dict]
    ):
        """
        Generate Databricks Asset Bundle YAML files.

        Creates the DAB project structure with all necessary YAML files.
        Implementation depends on whether connector is database (with gateways)
        or SaaS (without gateways).

        Args:
            df: DataFrame with pipeline configuration
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments
                Format: {'env_name': {'workspace_host': '...', 'root_path': '...'}}
        """
        pass

    def run_complete_pipeline_generation(
        self,
        df: pd.DataFrame,
        output_dir: str,
        targets: Dict[str, Dict],
        output_config: Optional[str] = None,
        default_values: Optional[Dict] = None,
        override_input_config: Optional[Dict] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Run the complete pipeline generation workflow.

        This is the main entry point for connector usage. Executes the full
        workflow from input CSV to generated DAB files:
        1. Normalize input data
        2. Generate pipeline configuration (load balancing)
        3. Generate YAML files

        Args:
            df: Input DataFrame with source table list
            output_dir: Output directory for DAB project(s)
            targets: Target environments configuration
                Format: {'env_name': {'workspace_host': '...', 'root_path': '...'}}
            output_config: Optional path to save intermediate CSV
            default_values: Default values for optional columns
            override_input_config: Values to force for all rows
            **kwargs: Connector-specific parameters passed to generate_pipeline_config

        Returns:
            DataFrame with complete pipeline configuration
        """
        print("="*80)
        print(f"STARTING {self.connector_type.upper()} PIPELINE GENERATION PROCESS")
        print("="*80)

        # Step 1: Normalize and validate configuration
        print(f"\n[Step 1/3] Normalizing configuration")
        print(f"  - Input rows: {len(df)}")

        normalized_df = self.load_and_normalize_input(
            df=df,
            default_values=default_values,
            override_input_config=override_input_config
        )

        # Step 2: Generate pipeline configuration
        print(f"\n[Step 2/3] Generating pipeline configuration")

        pipeline_config_df = self.generate_pipeline_config(
            df=normalized_df,
            **kwargs
        )

        print(f"  ✓ Created {pipeline_config_df['pipeline_group'].nunique()} pipelines")
        print(f"  ✓ Configured {len(pipeline_config_df)} tables/objects")

        # Save intermediate configuration if requested
        if output_config:
            pipeline_config_df.to_csv(output_config, index=False)
            print(f"  ✓ Saved configuration to: {output_config}")

        # Step 3: Generate YAML files
        print(f"\n[Step 3/3] Generating Databricks Asset Bundle YAML files")
        print(f"  - Output directory: {output_dir}")
        print(f"  - Target environments: {', '.join(targets.keys())}")

        self.generate_yaml_files(
            df=pipeline_config_df,
            output_dir=output_dir,
            targets=targets
        )

        print("\n" + "="*80)
        print(f"{self.connector_type.upper()} PIPELINE GENERATION COMPLETE!")
        print("="*80)

        return pipeline_config_df


class DatabaseConnector(BaseConnector):
    """
    Abstract base class for database connectors with gateway support.

    Database connectors use two-level load balancing:
    1. Split into gateways (max_tables_per_gateway)
    2. Split each gateway into pipelines (max_tables_per_pipeline)

    Examples: SQL Server, MySQL, PostgreSQL, Oracle
    """

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply database connector normalization including gateway defaults.

        Extends base normalization to handle gateway-specific columns.
        """
        # Call parent normalization first
        df = super()._apply_connector_specific_normalization(df)

        # Handle gateway_catalog and gateway_schema defaults
        # Use target values if gateway values are not provided
        if 'gateway_catalog' in df.columns:
            mask = df['gateway_catalog'].isna()
            df.loc[mask, 'gateway_catalog'] = df.loc[mask, 'target_catalog']

        if 'gateway_schema' in df.columns:
            mask = df['gateway_schema'].isna()
            df.loc[mask, 'gateway_schema'] = df.loc[mask, 'target_schema']

        return df

    def _split_groups_by_size(
        self,
        df: pd.DataFrame,
        group_column: str,
        max_size: int,
        output_column: str,
        suffix: str
    ) -> pd.DataFrame:
        """
        Split groups that exceed max_size into smaller chunks with sequential suffixes.

        Args:
            df: Input DataFrame with groups to split
            group_column: Column containing group identifiers to split
            max_size: Maximum rows per group
            output_column: Column name for output group names
            suffix: Suffix pattern for split groups ('g' for pipelines, 'gw' for gateways)

        Returns:
            DataFrame with output_column populated
        """
        df = df.copy()
        df[output_column] = ''

        for group_name in df[group_column].unique():
            group_df = df[df[group_column] == group_name]

            if len(group_df) > max_size:
                # Split into chunks
                num_chunks = (len(group_df) - 1) // max_size + 1

                for i in range(num_chunks):
                    start_idx = i * max_size
                    end_idx = min((i + 1) * max_size, len(group_df))
                    chunk_indices = group_df.iloc[start_idx:end_idx].index
                    chunk_name = f"{group_name}_{suffix}{i+1:02d}"
                    df.loc[chunk_indices, output_column] = chunk_name
            else:
                # No split needed
                df.loc[group_df.index, output_column] = group_name

        return df

    def generate_pipeline_config(
        self,
        df: pd.DataFrame,
        max_tables_per_gateway: int = 250,
        max_tables_per_pipeline: int = 250
    ) -> pd.DataFrame:
        """
        Generate database pipeline configuration with two-level load balancing.

        Uses prefix + subgroup grouping with two-level splitting:
        1. Gateway level: Split into gateways if exceeds max_tables_per_gateway
        2. Pipeline level: Split gateways into pipelines if exceeds max_tables_per_pipeline

        Args:
            df: Normalized input DataFrame
            max_tables_per_gateway: Maximum tables per gateway (default: 250)
            max_tables_per_pipeline: Maximum tables per pipeline (default: 250)

        Returns:
            DataFrame with 'gateway' and 'pipeline_group' columns added
        """
        df = df.copy()

        # Ensure consistent string formatting
        df['prefix'] = df['prefix'].astype(str)
        df['subgroup'] = df['subgroup'].astype(str).str.zfill(2)

        # Generate base group from prefix + subgroup
        df['base_group'] = df['prefix'] + '_' + df['subgroup']

        # Step 1: Split by gateway capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='base_group',
            max_size=max_tables_per_gateway,
            output_column='gateway',
            suffix='gw'
        )

        # Step 2: Split each gateway by pipeline capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='gateway',
            max_size=max_tables_per_pipeline,
            output_column='pipeline_group',
            suffix='g'
        )

        # Drop temporary base_group column
        df = df.drop(columns=['base_group'])

        return df


class SaaSConnector(BaseConnector):
    """
    Abstract base class for SaaS connectors without gateway support.

    SaaS connectors use single-level load balancing:
    - Split into pipelines when group exceeds max_tables_per_pipeline

    Examples: Salesforce, Google Analytics, ServiceNow, Workday
    """

    def _split_groups_by_size(
        self,
        df: pd.DataFrame,
        group_column: str,
        max_size: int,
        output_column: str,
        suffix: str
    ) -> pd.DataFrame:
        """
        Split groups that exceed max_size into smaller chunks with sequential suffixes.

        Args:
            df: Input DataFrame with groups to split
            group_column: Column containing group identifiers to split
            max_size: Maximum rows per group
            output_column: Column name for output group names
            suffix: Suffix pattern for split groups ('g' for pipelines)

        Returns:
            DataFrame with output_column populated
        """
        df = df.copy()
        df[output_column] = ''

        for group_name in df[group_column].unique():
            group_df = df[df[group_column] == group_name]

            if len(group_df) > max_size:
                # Split into chunks
                num_chunks = (len(group_df) - 1) // max_size + 1

                for i in range(num_chunks):
                    start_idx = i * max_size
                    end_idx = min((i + 1) * max_size, len(group_df))
                    chunk_indices = group_df.iloc[start_idx:end_idx].index
                    chunk_name = f"{group_name}_{suffix}{i+1:02d}"
                    df.loc[chunk_indices, output_column] = chunk_name
            else:
                # No split needed
                df.loc[group_df.index, output_column] = group_name

        return df

    def generate_pipeline_config(
        self,
        df: pd.DataFrame,
        max_tables_per_pipeline: int = 250
    ) -> pd.DataFrame:
        """
        Generate SaaS pipeline configuration with single-level load balancing.

        Uses prefix + subgroup grouping with single-level splitting:
        - Split into pipelines if exceeds max_tables_per_pipeline

        Args:
            df: Normalized input DataFrame
            max_tables_per_pipeline: Maximum items per pipeline (default: 250)

        Returns:
            DataFrame with 'pipeline_group' column added
        """
        df = df.copy()

        # Ensure consistent string formatting
        df['prefix'] = df['prefix'].astype(str)
        df['subgroup'] = df['subgroup'].astype(str).str.zfill(2)

        # Generate base group from prefix + subgroup
        df['base_group'] = df['prefix'] + '_' + df['subgroup']

        # Split groups by capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='base_group',
            max_size=max_tables_per_pipeline,
            output_column='pipeline_group',
            suffix='g'
        )

        # Drop temporary base_group column
        df = df.drop(columns=['base_group'])

        return df
