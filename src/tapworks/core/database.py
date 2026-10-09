"""
Database connector base classes for Databricks Lakeflow Connect pipeline generation.

Database connectors share the pipeline-building flow and table entries; each
ingestion mode subclasses DatabaseConnector and only defines how its pipelines
reach the source and which extra resources it needs.

Architecture:
    BaseConnector (ABC)
    └── DatabaseConnector (ABC) - Shared database logic
        ├── GatewayConnector (ABC) - CDC through an ingestion gateway
        │   ├── SQLServerConnector
        │   └── PostgreSQLConnector
        └── IntegratedCDCConnector (ABC) - CDC without a gateway
            └── OracleConnector
"""

import logging
from abc import abstractmethod
from pathlib import Path
from typing import Dict

import pandas as pd

from .connectors import BaseConnector
from .exceptions import YAMLGenerationError

# Configure module logger
logger = logging.getLogger(__name__)


class DatabaseConnector(BaseConnector):
    """
    Abstract base class for database connectors.

    Provides the shared pipeline-building flow and single-level load balancing
    (split into pipelines when a group exceeds max_tables_per_pipeline).
    Subclasses implement:
    - _ingestion_source(): how a pipeline reaches the source

    And may override:
    - generate_pipeline_config(): load balancing
    - _build_table_configuration(): per-table options
    - _build_pipeline(): pipeline-level additions
    - _create_extra_resource_files(): resource files beyond pipelines.yml and jobs.yml
    """

    # Validation configuration - fields that must be consistent within pipeline groups
    PIPELINE_CONSISTENCY_FIELDS = ['connection_name', 'pipeline_catalog', 'pipeline_schema', 'tags']

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply database connector normalization.

        Derives target_table_name from source_table_name if empty.
        """
        # Call parent normalization first
        df = super()._apply_connector_specific_normalization(df)

        # Derive target_table_name from source_table_name if empty
        if 'target_table_name' in df.columns and 'source_table_name' in df.columns:
            empty = df['target_table_name'].isna() | (df['target_table_name'].astype(str).str.strip() == '')
            df.loc[empty, 'target_table_name'] = df.loc[empty, 'source_table_name']

        return df

    def _validate_generated_names(self, df: pd.DataFrame) -> None:
        """
        Validate generated names for database connectors.

        Extends base validation with pipeline consistency checks.
        """
        # Base validation (job consistency + collision check)
        super()._validate_generated_names(df)

        # Pipeline consistency
        for pipeline_group, group_df in df.groupby('pipeline_group'):
            self._validate_group_consistency(
                group_df=group_df,
                group_name=pipeline_group,
                fields_to_validate=self.PIPELINE_CONSISTENCY_FIELDS,
                context='pipeline group'
            )

    def _add_base_group(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add 'base_group' column (prefix, or prefix_subgroup when subgroup is set).

        Load balancing splits each base group into gateways and/or pipelines.
        """
        df = df.copy()

        # Ensure consistent string formatting
        df['prefix'] = df['prefix'].astype(str)
        df['subgroup'] = df['subgroup'].astype(str).apply(lambda x: x.zfill(2) if x.strip() else x)

        # Generate base group from prefix + subgroup
        df['base_group'] = df.apply(
            lambda r: r['prefix'] + '_' + r['subgroup'] if r['subgroup'].strip() else r['prefix'],
            axis=1
        )

        return df

    def generate_pipeline_config(
        self,
        df: pd.DataFrame,
        max_tables_per_pipeline: int = None
    ) -> pd.DataFrame:
        """
        Generate database pipeline configuration with single-level load balancing.

        Uses prefix + subgroup grouping with single-level splitting:
        - Split into pipelines if exceeds max_tables_per_pipeline

        Args:
            df: Normalized input DataFrame
            max_tables_per_pipeline: Maximum tables per pipeline (default: DEFAULT_MAX_TABLES_PER_PIPELINE)

        Returns:
            DataFrame with 'pipeline_group' column added
        """
        # Apply default constant if not specified
        if max_tables_per_pipeline is None:
            max_tables_per_pipeline = self.DEFAULT_MAX_TABLES_PER_PIPELINE

        df = self._add_base_group(df)

        # Split groups by capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='base_group',
            max_size=max_tables_per_pipeline,
            output_column='pipeline_group',
            suffix='p'
        )

        # Drop temporary base_group column
        df = df.drop(columns=['base_group'])

        # Validate generated names and group consistency
        self._validate_generated_names(df)

        return df

    @abstractmethod
    def _ingestion_source(self, group_df: pd.DataFrame) -> Dict:
        """
        Return the ingestion_definition fields that tell a pipeline how to reach the source.

        Args:
            group_df: DataFrame for a single pipeline group

        Returns:
            Dictionary placed at the start of ingestion_definition
        """
        pass

    def _build_table_configuration(self, row: pd.Series) -> Dict:
        """
        Build the optional table_configuration for a single table.

        Args:
            row: Input row for the table

        Returns:
            Dictionary with table configuration (empty if nothing is set)
        """
        table_config = {}

        if 'include_columns' in row and pd.notna(row['include_columns']) and str(row['include_columns']).strip():
            table_config['include_columns'] = [c.strip() for c in str(row['include_columns']).split(',')]

        if 'exclude_columns' in row and pd.notna(row['exclude_columns']) and str(row['exclude_columns']).strip():
            table_config['exclude_columns'] = [c.strip() for c in str(row['exclude_columns']).split(',')]

        scd_type = self._validate_scd_type(row.get('scd_type'), row['source_table_name'])
        if scd_type:
            table_config['scd_type'] = scd_type

        return table_config

    def _build_table_entry(self, row: pd.Series) -> Dict:
        """
        Build the ingestion object for a single table.

        Args:
            row: Input row for the table

        Returns:
            Dictionary with the table object for ingestion_definition.objects
        """
        table_entry = {
            'table': {
                'source_catalog': row['source_database'],
                'source_schema': row['source_schema'],
                'source_table': row['source_table_name'],
                'destination_catalog': row['target_catalog'],
                'destination_schema': row['target_schema'],
                'destination_table': row['target_table_name']
            }
        }

        table_config = self._build_table_configuration(row)
        if table_config:
            table_entry['table']['table_configuration'] = table_config

        return table_entry

    def _build_pipeline(self, names: Dict[str, str], group_df: pd.DataFrame) -> Dict:
        """
        Build the pipeline definition for a single pipeline group.

        Args:
            names: Resource names from _generate_resource_names()
            group_df: DataFrame for a single pipeline group

        Returns:
            Dictionary with the pipeline definition
        """
        tables = [self._build_table_entry(row) for _, row in group_df.iterrows()]

        pipeline_def = {
            'name': names['pipeline_name'],
            'catalog': group_df.iloc[0]['pipeline_catalog'],
            'schema': group_df.iloc[0]['pipeline_schema'],
            'ingestion_definition': {
                **self._ingestion_source(group_df),
                'objects': tables
            },
        }

        # Optional: tags (applied to the ingestion pipeline)
        tags = self._parse_tags(group_df.iloc[0].get("tags"))
        if tags:
            pipeline_def["tags"] = tags

        return pipeline_def

    def _create_pipelines(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create pipeline YAML configuration from dataframe.

        Args:
            df: DataFrame with pipeline configuration
            project_name: Project name for resource naming

        Returns:
            Dictionary with pipeline YAML configuration
        """
        pipelines = {}

        for pipeline_group, group_df in df.groupby('pipeline_group'):
            names = self._generate_resource_names(pipeline_group)
            pipelines[names['pipeline_resource_name']] = self._build_pipeline(names, group_df)

        return {'resources': {'pipelines': pipelines}}

    def _create_extra_resource_files(self, df: pd.DataFrame, project_name: str) -> Dict[str, Dict]:
        """
        Return additional resource files to write, as {file_name: content}.

        Args:
            df: DataFrame with pipeline configuration for one project
            project_name: Project name for resource naming

        Returns:
            Dictionary of file names (under resources/) to YAML content
        """
        return {}

    def generate_yaml_files(
        self,
        df: pd.DataFrame,
        output_dir: str,
        targets: Dict[str, Dict]
    ):
        """
        Generate YAML files for database connectors.

        Creates a DAB structure for each project:
        - databricks.yml (root configuration)
        - resources/<extra files> (from _create_extra_resource_files, e.g. gateways.yml)
        - resources/pipelines.yml (pipeline definitions)
        - resources/jobs.yml (scheduled jobs)

        Args:
            df: DataFrame with pipeline configuration
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments

        Raises:
            YAMLGenerationError: If file writing fails
        """
        logger.info(f"Generating DAB YAML for {self.connector_type}")

        for project_name, project_df in df.groupby('project_name'):
            project_output_dir = Path(output_dir) / str(project_name)
            logger.info(f"Creating DAB for project: {project_name}")
            logger.debug(f"  Tables: {len(project_df)}, pipelines: {project_df['pipeline_group'].nunique()}")

            resources_dir = project_output_dir / 'resources'
            try:
                resources_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise YAMLGenerationError(f"Failed to create directory {resources_dir}: {e}")

            extra_files = self._create_extra_resource_files(project_df, str(project_name))
            pipelines_yaml = self._create_pipelines(project_df, str(project_name))
            jobs_yaml = self._create_jobs(project_df, str(project_name))
            databricks_yaml = self._create_databricks_yml(
                project_name=str(project_name),
                targets=targets,
                default_target='dev'
            )

            self._write_yaml_file(project_output_dir / 'databricks.yml', databricks_yaml)
            for file_name, content in extra_files.items():
                self._write_yaml_file(resources_dir / file_name, content)
            self._write_yaml_file(resources_dir / 'pipelines.yml', pipelines_yaml)
            self._write_yaml_file(resources_dir / 'jobs.yml', jobs_yaml)


class GatewayConnector(DatabaseConnector):
    """
    Abstract base class for database connectors that use an ingestion gateway.

    Gateway connectors use two-level load balancing:
    1. Split into gateways (max_tables_per_gateway)
    2. Split each gateway into pipelines (max_tables_per_pipeline)

    Examples: SQL Server, PostgreSQL
    """

    # Validation configuration - connection_name is checked per gateway, not per pipeline
    PIPELINE_CONSISTENCY_FIELDS = ['pipeline_catalog', 'pipeline_schema', 'tags']
    GATEWAY_CONSISTENCY_FIELDS = ['gateway_catalog', 'gateway_schema', 'connection_name', 'tags']

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply gateway connector normalization including gateway defaults.

        Extends database normalization to handle gateway-specific columns.
        """
        # Call parent normalization first
        df = super()._apply_connector_specific_normalization(df)

        # Handle gateway_catalog and gateway_schema defaults
        # Use target values if gateway values are not provided
        if 'gateway_catalog' in df.columns:
            df['gateway_catalog'] = df['gateway_catalog'].astype(object)
            mask = df['gateway_catalog'].isna()
            df.loc[mask, 'gateway_catalog'] = df.loc[mask, 'target_catalog']

        if 'gateway_schema' in df.columns:
            df['gateway_schema'] = df['gateway_schema'].astype(object)
            mask = df['gateway_schema'].isna()
            df.loc[mask, 'gateway_schema'] = df.loc[mask, 'target_schema']

        return df

    def _validate_generated_names(self, df: pd.DataFrame) -> None:
        """
        Validate generated names for gateway connectors.

        Extends database validation with gateway consistency checks.
        """
        # Base + pipeline consistency validation
        super()._validate_generated_names(df)

        # Gateway consistency
        for gateway_id, gateway_df in df.groupby('gateway'):
            self._validate_group_consistency(
                group_df=gateway_df,
                group_name=gateway_id,
                fields_to_validate=self.GATEWAY_CONSISTENCY_FIELDS,
                context='gateway'
            )

    def generate_pipeline_config(
        self,
        df: pd.DataFrame,
        max_tables_per_gateway: int = None,
        max_tables_per_pipeline: int = None
    ) -> pd.DataFrame:
        """
        Generate gateway connector configuration with two-level load balancing.

        Uses prefix + subgroup grouping with two-level splitting:
        1. Gateway level: Split into gateways if exceeds max_tables_per_gateway
        2. Pipeline level: Split gateways into pipelines if exceeds max_tables_per_pipeline

        Args:
            df: Normalized input DataFrame
            max_tables_per_gateway: Maximum tables per gateway (default: DEFAULT_MAX_TABLES_PER_GATEWAY)
            max_tables_per_pipeline: Maximum tables per pipeline (default: DEFAULT_MAX_TABLES_PER_PIPELINE)

        Returns:
            DataFrame with 'gateway' and 'pipeline_group' columns added
        """
        # Apply default constants if not specified
        if max_tables_per_gateway is None:
            max_tables_per_gateway = self.DEFAULT_MAX_TABLES_PER_GATEWAY
        if max_tables_per_pipeline is None:
            max_tables_per_pipeline = self.DEFAULT_MAX_TABLES_PER_PIPELINE

        df = self._add_base_group(df)

        # Step 1: Split by gateway capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='base_group',
            max_size=max_tables_per_gateway,
            output_column='gateway',
            suffix='g'
        )

        # Step 2: Split each gateway by pipeline capacity
        df = self._split_groups_by_size(
            df=df,
            group_column='gateway',
            max_size=max_tables_per_pipeline,
            output_column='pipeline_group',
            suffix='p',
            separator=''
        )

        # Drop temporary base_group column
        df = df.drop(columns=['base_group'])

        # Validate generated names and group consistency
        self._validate_generated_names(df)

        return df

    def _ingestion_source(self, group_df: pd.DataFrame) -> Dict:
        """Pipelines read from their gateway."""
        gateway_id = group_df.iloc[0]['gateway']
        return {'ingestion_gateway_id': f"${{resources.pipelines.gateway_{gateway_id}.id}}"}

    def _create_gateways(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create gateway YAML configuration from dataframe.

        Args:
            df: DataFrame with gateway configuration
            project_name: Project name for resource naming

        Returns:
            Dictionary with gateway YAML configuration
        """
        gateways = {}

        unique_gateways = df.groupby('gateway').first()

        for gateway_id, row in unique_gateways.iterrows():
            gateway_name = f"{gateway_id}"
            gateway_resource_name = f"gateway_{gateway_id}"

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

            # Optional: tags (applied to the gateway pipeline)
            tags = self._parse_tags(row.get("tags"))
            if tags:
                gateway_config["tags"] = tags

            # Add cluster configuration if node types are provided
            has_worker_type = self._is_value_set(worker_type)
            has_driver_type = self._is_value_set(driver_type)

            if has_worker_type or has_driver_type:
                cluster_config = {'num_workers': 1}
                if has_worker_type:
                    cluster_config['node_type_id'] = worker_type
                if has_driver_type:
                    cluster_config['driver_node_type_id'] = driver_type
                gateway_config['clusters'] = [cluster_config]

            gateways[gateway_resource_name] = gateway_config

        return {'resources': {'pipelines': gateways}}

    def _create_extra_resource_files(self, df: pd.DataFrame, project_name: str) -> Dict[str, Dict]:
        """Gateway connectors also write resources/gateways.yml."""
        return {'gateways.yml': self._create_gateways(df, project_name)}


class IntegratedCDCConnector(DatabaseConnector):
    """
    Abstract base class for integrated CDC database connectors.

    Each pipeline reads changes directly from the source through its Unity Catalog
    connection, without a separate gateway. Uses single-level load balancing.

    Examples: Oracle
    """

    def _ingestion_source(self, group_df: pd.DataFrame) -> Dict:
        """Pipelines connect to the source directly and use CDC ingestion."""
        return {
            'connection_name': group_df.iloc[0]['connection_name'],
            'connector_type': 'CDC',
        }

    def _build_pipeline(self, names: Dict[str, str], group_df: pd.DataFrame) -> Dict:
        """Integrated CDC pipelines must be created on the PREVIEW channel."""
        pipeline_def = super()._build_pipeline(names, group_df)
        pipeline_def['channel'] = 'PREVIEW'
        return pipeline_def
