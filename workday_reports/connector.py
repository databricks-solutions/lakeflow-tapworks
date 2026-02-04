"""
Workday Reports connector implementation using OOP architecture.

This module provides the WorkdayReportsConnector class which implements the
SaaSConnector interface for Workday Reports data sources.
"""

import sys
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict
from collections import defaultdict

# Add parent directory to path to import utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from core import SaaSConnector


class WorkdayReportsConnector(SaaSConnector):
    """
    Workday Reports connector for Databricks Lakeflow Connect pipelines.

    Implements SaaS connector pattern with:
    - Single-level load balancing (pipelines only, no gateways)
    - OAuth connection management
    - Column filtering support (include/exclude)
    - SCD type configuration (SCD_TYPE_1 or SCD_TYPE_2)
    - Primary key configuration (required for Workday reports)

    Required CSV columns:
    - source_url: Workday report URL (e.g., 'https://wd2-impl-services1.workday.com/ccx/service/...')
    - target_catalog: Target Databricks catalog
    - target_schema: Target Databricks schema
    - target_table_name: Destination table name
    - connection_name: Databricks connection name for Workday
    - primary_keys: Comma-separated list of primary key columns

    Optional CSV columns:
    - project_name: Project identifier (default: 'workday_reports_ingestion')
    - prefix: Grouping prefix (default: project_name)
    - priority: Priority/subgroup identifier (default: '01')
    - schedule: Cron schedule (e.g., '0 */6 * * *', default: '0 */6 * * *')
    - include_columns: Comma-separated list of columns to include (default: '')
    - exclude_columns: Comma-separated list of columns to exclude (default: '')
    - scd_type: SCD type (SCD_TYPE_1 or SCD_TYPE_2, default: 'SCD_TYPE_1')
    """

    @property
    def connector_type(self) -> str:
        """Return connector type identifier."""
        return "workday_reports"

    @property
    def required_columns(self) -> list:
        """
        Return required columns for Workday Reports input CSV.

        All these columns must be present and non-empty in the input.
        """
        return [
            "source_url",
            "target_catalog",
            "target_schema",
            "target_table_name",
            "connection_name",
            "primary_keys",
        ]

    @property
    def default_values(self) -> dict:
        """
        Return default values for optional Workday Reports columns.

        These values are used when columns are missing or empty.
        """
        return {
            "schedule": "0 */6 * * *",  # Every 6 hours by default
            "scd_type": "SCD_TYPE_1",
        }

    @property
    def default_project_name(self) -> str:
        """Return default project name for Workday Reports connector."""
        return "workday_reports_ingestion"

    def _apply_connector_specific_normalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply Workday Reports-specific normalization including priority mapping.

        Maps 'priority' column to 'subgroup' if present.
        """
        # Call parent normalization first
        df = super()._apply_connector_specific_normalization(df)

        # Map priority to subgroup if priority column exists
        if "priority" in df.columns:
            # Use priority as subgroup if subgroup wasn't explicitly set
            mask = (df["subgroup"] == "01") | df["subgroup"].isna()
            df.loc[mask, "subgroup"] = df.loc[mask, "priority"].astype(str).str.zfill(2)

        return df

    def _generate_resource_names(self, pipeline_group: str) -> Dict[str, str]:
        """
        Generate consistent resource names for Workday Reports pipelines and jobs.

        Overrides base class to add "Workday Reports" prefix to pipeline names.

        Args:
            pipeline_group: Pipeline group identifier (e.g., 'hr_01')

        Returns:
            Dictionary containing all resource names with Workday Reports branding
        """
        return {
            "pipeline_name": f"Workday Reports Ingestion - {pipeline_group}",
            "pipeline_resource_name": f"pipeline_{pipeline_group}",
            "job_name": f"job_{pipeline_group}",
            "job_display_name": f"Workday Reports Pipeline Scheduler - {pipeline_group}",
            "task_key": "run_pipeline",
        }

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

        # Group reports by pipeline_group
        groups = defaultdict(list)
        for idx, row in df.iterrows():
            groups[row["pipeline_group"]].append(row)

        print("\n" + "-" * 80)
        print("Pipeline Details:")
        print("-" * 80)

        for pipeline_group in sorted(groups.keys()):
            group_reports = groups[pipeline_group]

            # Generate resource names
            names = self._generate_resource_names(pipeline_group)

            # Get catalog, schema, and connection_name from first report in group
            target_catalog = group_reports[0]["target_catalog"]
            target_schema = group_reports[0]["target_schema"]
            connection_name = group_reports[0]["connection_name"]

            print(f"\nPipeline: {pipeline_group}")
            print(f"  Name: {names['pipeline_resource_name']}")
            print(f"  Target: {target_catalog}.{target_schema}")
            print(f"  Connection: {connection_name}")
            print(f"  Reports: {len(group_reports)}")

            # Create pipeline definition
            pipeline_def = {
                "name": names["pipeline_name"],
                "catalog": target_catalog,
                "schema": target_schema,
                "ingestion_definition": {
                    "connection_name": connection_name,
                    "objects": [],
                },
            }

            # Add reports to this pipeline
            for item in group_reports:
                report_entry = {
                    "report": {
                        "source_url": item["source_url"],
                        "destination_catalog": item["target_catalog"],
                        "destination_schema": item["target_schema"],
                        "destination_table": item["target_table_name"],
                    }
                }

                # Add table_configuration (primary_keys are required for Workday Reports)
                table_config = {}

                # Add primary keys (required for Workday Reports)
                if (
                    "primary_keys" in item
                    and pd.notna(item["primary_keys"])
                    and item["primary_keys"].strip()
                ):
                    primary_keys = [
                        key.strip() for key in str(item["primary_keys"]).split(",")
                    ]
                    table_config["primary_keys"] = primary_keys
                else:
                    raise ValueError(
                        f"Missing required primary_keys for report '{item['target_table_name']}'. "
                        f"Workday Reports require at least one primary key column. "
                        f"Please specify primary_keys in your CSV (e.g., 'Employee_ID' or 'Worker_ID,Effective_Date')"
                    )

                # Add include_columns if specified
                if (
                    "include_columns" in item
                    and pd.notna(item["include_columns"])
                    and item["include_columns"].strip()
                ):
                    include_cols = [
                        col.strip() for col in str(item["include_columns"]).split(",")
                    ]
                    table_config["include_columns"] = include_cols

                # Add exclude_columns if specified
                if (
                    "exclude_columns" in item
                    and pd.notna(item["exclude_columns"])
                    and item["exclude_columns"].strip()
                ):
                    exclude_cols = [
                        col.strip() for col in str(item["exclude_columns"]).split(",")
                    ]
                    table_config["exclude_columns"] = exclude_cols

                # Add SCD type if specified
                if (
                    "scd_type" in item
                    and pd.notna(item["scd_type"])
                    and item["scd_type"].strip()
                ):
                    scd_type = str(item["scd_type"]).strip().upper()
                    # Validate SCD type
                    if scd_type in ["SCD_TYPE_1", "SCD_TYPE_2"]:
                        table_config["scd_type"] = scd_type
                    else:
                        print(
                            f"  Warning: Invalid scd_type '{scd_type}' for report {item['target_table_name']}, skipping"
                        )

                if table_config:
                    report_entry["report"]["table_configuration"] = table_config

                pipeline_def["ingestion_definition"]["objects"].append(report_entry)

                # Show report in output
                report_name = item["target_table_name"]
                col_info = ""
                scd_info = ""
                pk_info = f" [PK: {', '.join(table_config.get('primary_keys', []))}]"

                if "include_columns" in table_config:
                    col_info = (
                        f" [includes: {len(table_config['include_columns'])} cols]"
                    )
                elif "exclude_columns" in table_config:
                    col_info = (
                        f" [excludes: {len(table_config['exclude_columns'])} cols]"
                    )

                if "scd_type" in table_config:
                    scd_info = f" [{table_config['scd_type']}]"

                print(f"    - {report_name}{pk_info}{col_info}{scd_info}")

            pipelines[names["pipeline_resource_name"]] = pipeline_def

        return {"resources": {"pipelines": pipelines}}

    def generate_yaml_files(
        self, df: pd.DataFrame, output_dir: str, targets: Dict[str, Dict]
    ):
        """
        Generate YAML files for Workday Reports connector without gateways.

        Creates a DAB structure:
        - databricks.yml (root configuration)
        - resources/pipelines.yml (pipeline definitions)
        - resources/jobs.yml (scheduled jobs)

        Args:
            df: DataFrame with pipeline configuration
            output_dir: Output directory for DAB files
            targets: Dictionary of target environments
        """
        print("\n" + "=" * 80)
        print("GENERATING DATABRICKS ASSET BUNDLE YAML FOR WORKDAY REPORTS")
        print("=" * 80)

        # Validate required columns
        required_columns = [
            "source_url",
            "target_catalog",
            "target_schema",
            "target_table_name",
            "pipeline_group",
            "connection_name",
            "schedule",
            "project_name",
            "primary_keys",
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        print(f"\nConfiguration:")
        print(f"  Total reports: {len(df)}")
        print(f"  Unique pipelines: {df['pipeline_group'].nunique()}")
        print(f"  Unique projects: {df['project_name'].nunique()}")

        # Group by project_name and create separate DAB packages
        for project, project_df in df.groupby("project_name"):
            project_output_dir = Path(output_dir) / str(project)
            print(f"\n  Creating DAB for project: {project}")
            print(f"    - Reports: {len(project_df)}")
            print(f"    - Pipelines: {project_df['pipeline_group'].nunique()}")
            print(f"    - Output: {project_output_dir}")

            # Generate YAML content for this project
            pipelines_yaml = self._create_pipelines(project_df, str(project))
            jobs_yaml = self._create_jobs(project_df, str(project))
            databricks_yaml = self._create_databricks_yml(
                project_name=str(project), targets=targets, default_target="dev"
            )

            # Create directory structure
            resources_dir = project_output_dir / "resources"
            resources_dir.mkdir(parents=True, exist_ok=True)

            # Define output paths
            databricks_yml_path = project_output_dir / "databricks.yml"
            pipelines_yml_path = resources_dir / "pipelines.yml"
            jobs_yml_path = resources_dir / "jobs.yml"

            # Write YAML files
            with open(databricks_yml_path, "w") as f:
                yaml.dump(
                    databricks_yaml,
                    f,
                    sort_keys=False,
                    default_flow_style=False,
                    indent=2,
                )

            with open(pipelines_yml_path, "w") as f:
                yaml.dump(
                    pipelines_yaml,
                    f,
                    sort_keys=False,
                    default_flow_style=False,
                    indent=2,
                )

            with open(jobs_yml_path, "w") as f:
                yaml.dump(
                    jobs_yaml, f, sort_keys=False, default_flow_style=False, indent=2
                )

        print("\n" + "=" * 80)
        print("YAML GENERATION COMPLETE")
        print("=" * 80)
        print(f"\nGenerated DAB project structure in: {output_dir}")
        print(f"  ✓ {databricks_yml_path}")
        print(f"  ✓ {pipelines_yml_path}")
        print(f"  ✓ {jobs_yml_path}")
