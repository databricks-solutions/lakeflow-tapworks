"""
PostgreSQL connector implementation using OOP architecture.

This module provides the PostgreSQLConnector class which implements the
DatabaseConnector interface for PostgreSQL data sources.
"""

import sys
import os
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict

# Add parent directory to path to import core utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from core import DatabaseConnector


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL connector for Databricks Lakeflow Connect pipelines.

    Implements database connector pattern with:
    - Two-level load balancing (gateways + pipelines)
    - Gateway configuration (catalog, schema, worker/driver types)
    - Connection management per row

    Required CSV columns:
    - source_database: Source PostgreSQL database name
    - source_schema: Source schema name (usually 'public')
    - source_table_name: Table name to ingest
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for PostgreSQL

    Optional CSV columns:
    - project_name: Project identifier (default: 'postgres_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - subgroup: Subgroup identifier (default: '01')
    - gateway_catalog: Gateway storage catalog (default: target_catalog)
    - gateway_schema: Gateway storage schema (default: target_schema)
    - gateway_worker_type: Worker node type (default: None)
    - gateway_driver_type: Driver node type (default: None)
    - schedule: Cron schedule (default: */15 * * * *)
    """

    @property
    def connector_type(self) -> str:
        return "postgres"

    @property
    def required_columns(self) -> list:
        return [
            "source_database",
            "source_schema",
            "source_table_name",
            "target_catalog",
            "target_schema",
            "target_table_name",
            "connection_name",
        ]

    @property
    def default_values(self) -> dict:
        return {
            "schedule": "*/15 * * * *",
            "gateway_catalog": None,  # Will fall back to target_catalog
            "gateway_schema": None,  # Will fall back to target_schema
            "gateway_worker_type": None,
            "gateway_driver_type": None,
        }

    @property
    def default_project_name(self) -> str:
        return "postgres_ingestion"

    def _create_gateways(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create gateway YAML configuration from dataframe.

        Uses the same gateway resource model as other database connectors.
        """
        gateways = {}
        unique_gateways = df.groupby("gateway").first()

        for gateway_id, row in unique_gateways.iterrows():
            gateway_name = f"{project_name}_gateway_{gateway_id}"
            pipeline_name = f"{project_name}_pipeline_{gateway_name}"

            gateway_catalog = row["gateway_catalog"]
            gateway_schema = row["gateway_schema"]
            worker_type = row.get("gateway_worker_type")
            driver_type = row.get("gateway_driver_type")

            gateway_def = {
                "connection_name": row["connection_name"],
                "gateway_storage_catalog": gateway_catalog,
                "gateway_storage_schema": gateway_schema,
                "gateway_storage_name": gateway_name,
            }

            gateway_config = {
                "name": gateway_name,
                "gateway_definition": gateway_def,
                "schema": gateway_schema,
                "continuous": True,
                "catalog": gateway_catalog,
            }

            # Add cluster configuration if node types are provided
            has_worker_type = pd.notna(worker_type) and str(worker_type).strip()
            has_driver_type = pd.notna(driver_type) and str(driver_type).strip()
            if has_worker_type or has_driver_type:
                cluster_config = {"num_workers": 1}
                if has_worker_type:
                    cluster_config["node_type_id"] = str(worker_type)
                if has_driver_type:
                    cluster_config["driver_node_type_id"] = str(driver_type)
                gateway_config["clusters"] = [cluster_config]

            gateways[pipeline_name] = gateway_config

        return {"resources": {"pipelines": gateways}}

    def _create_pipelines(self, df: pd.DataFrame, project_name: str) -> Dict:
        """
        Create pipeline YAML configuration from dataframe.
        """
        pipelines = {}

        for pipeline_group, group_df in df.groupby("pipeline_group"):
            gateway_id = group_df.iloc[0]["gateway"]

            names = self._generate_resource_names(pipeline_group)

            target_catalog = group_df.iloc[0]["target_catalog"]
            target_schema = group_df.iloc[0]["target_schema"]

            tables = [
                {
                    "table": {
                        "source_catalog": row["source_database"],
                        "source_schema": row["source_schema"],
                        "source_table": row["source_table_name"],
                        "destination_catalog": row["target_catalog"],
                        "destination_schema": row["target_schema"],
                        "destination_table": row["target_table_name"],
                    }
                }
                for _, row in group_df.iterrows()
            ]

            pipelines[names["pipeline_resource_name"]] = {
                "name": names["pipeline_name"],
                "configuration": {
                    "pipelines.***REMOVED***": "600",
                },
                "ingestion_definition": {
                    "ingestion_gateway_id": f"${{resources.pipelines.{project_name}_pipeline_{project_name}_gateway_{gateway_id}.id}}",
                    "objects": tables,
                },
                "schema": target_schema,
                "catalog": target_catalog,
            }

        return {"resources": {"pipelines": pipelines}}

    def generate_yaml_files(self, df: pd.DataFrame, output_dir: str, targets: Dict[str, Dict]):
        """
        Generate YAML files for PostgreSQL connector with gateways.

        Creates a complete DAB structure per project_name:
        - databricks.yml (root configuration)
        - resources/gateways.yml (gateway definitions)
        - resources/pipelines.yml (pipeline definitions)
        - resources/jobs.yml (scheduled jobs)
        """
        for project, project_df in df.groupby("project_name"):
            project_output_dir = os.path.join(output_dir, str(project))
            resources_dir = os.path.join(project_output_dir, "resources")
            os.makedirs(resources_dir, exist_ok=True)

            gateways_yaml = self._create_gateways(project_df, str(project))
            pipelines_yaml = self._create_pipelines(project_df, str(project))
            jobs_yaml = self._create_jobs(project_df, str(project))
            databricks_yaml = self._create_databricks_yml(
                project_name=str(project),
                targets=targets,
                default_target="dev",
            )

            databricks_yml_path = os.path.join(project_output_dir, "databricks.yml")
            gateway_yml_path = os.path.join(resources_dir, "gateways.yml")
            pipeline_yml_path = os.path.join(resources_dir, "pipelines.yml")
            jobs_yml_path = os.path.join(resources_dir, "jobs.yml")

            with open(databricks_yml_path, "w") as f:
                yaml.dump(databricks_yaml, f, default_flow_style=False, sort_keys=False)
            with open(gateway_yml_path, "w") as f:
                yaml.dump(gateways_yaml, f, default_flow_style=False, sort_keys=False)
            with open(pipeline_yml_path, "w") as f:
                yaml.dump(pipelines_yaml, f, default_flow_style=False, sort_keys=False)
            with open(jobs_yml_path, "w") as f:
                yaml.dump(jobs_yaml, f, default_flow_style=False, sort_keys=False)

        print(f"Generated DAB project structure in: {output_dir}")

