"""
Unified pipeline generation script that combines load balancing and YAML generation.

This script demonstrates the complete two-part process:
1. Load balancing: Groups tables into pipeline configurations
2. YAML generation: Creates Databricks Asset Bundle YAML files
"""

import pandas as pd
import os
from databricks.sdk import WorkspaceClient

# Import from local modules
from load_balancing.generate_pipeline_config import generate_pipeline_config
from deployment.generate_dab_yaml import generate_yaml_files


def run_complete_pipeline_generation(
    input_csv: str,
    gateway_catalog: str,
    gateway_schema: str,
    project_name: str,
    max_tables_per_group: int = 1000,
    default_connection_name: str = "conn_1",
    default_schedule: str = "*/15 * * * *",
    node_type_id: str = "m5d.large",
    driver_node_type_id: str = "c5a.8xlarge",
    output_gateway: str = "deployment/resources/gateways.yml",
    output_pipeline: str = "deployment/resources/pipelines.yml"
):
    """
    Complete pipeline generation process from source table list to YAML files.

    Args:
        input_csv (str): Path to input CSV with source table list
        gateway_catalog (str): Catalog for gateway storage metadata
        gateway_schema (str): Schema for gateway storage metadata
        project_name (str): Project name prefix for all resources
        max_tables_per_group (int): Maximum tables per pipeline group (default: 1000)
        default_connection_name (str): Default connection name (default: "conn_1")
        default_schedule (str): Default cron schedule (default: "*/15 * * * *")
        node_type_id (str): Worker node type (default: "m5d.large")
        driver_node_type_id (str): Driver node type (default: "c5a.8xlarge")
        output_gateway (str): Output path for gateway YAML
        output_pipeline (str): Output path for pipeline YAML

    Returns:
        pd.DataFrame: The pipeline configuration dataframe
    """
    print("="*80)
    print("STARTING COMPLETE PIPELINE GENERATION PROCESS")
    print("="*80)

    # Step 1: Load input CSV
    print(f"\n[Step 1/4] Loading input CSV: {input_csv}")
    input_df = pd.read_csv(input_csv)
    print(f"  ✓ Loaded {len(input_df)} tables from {input_df['source_database'].nunique()} databases")

    # Step 2: Generate pipeline configuration (load balancing)
    print(f"\n[Step 2/4] Generating pipeline configuration with load balancing")
    print(f"  - Max tables per group: {max_tables_per_group}")
    print(f"  - Connection name: {default_connection_name}")
    print(f"  - Schedule: {default_schedule}")

    pipeline_config_df = generate_pipeline_config(
        df=input_df,
        max_tables_per_group=max_tables_per_group,
        default_connection_name=default_connection_name,
        default_schedule=default_schedule
    )

    # Step 3: Initialize Databricks workspace client
    print(f"\n[Step 3/4] Initializing Databricks workspace client")
    workspace_client = WorkspaceClient()
    print("  ✓ Workspace client initialized")

    # Step 4: Generate YAML files
    print(f"\n[Step 4/4] Generating Databricks Asset Bundle YAML files")
    print(f"  - Gateway catalog: {gateway_catalog}")
    print(f"  - Gateway schema: {gateway_schema}")
    print(f"  - Project name: {project_name}")
    print(f"  - Output gateway: {output_gateway}")
    print(f"  - Output pipeline: {output_pipeline}")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_gateway), exist_ok=True)

    generate_yaml_files(
        df=pipeline_config_df,
        gateway_catalog=gateway_catalog,
        gateway_schema=gateway_schema,
        workspace_client=workspace_client,
        project_name=project_name,
        node_type_id=node_type_id,
        driver_node_type_id=driver_node_type_id,
        output_gateway=output_gateway,
        output_pipeline=output_pipeline
    )

    print("\n" + "="*80)
    print("PIPELINE GENERATION COMPLETE!")
    print("="*80)
    print(f"\nNext steps:")
    print(f"  1. Review the generated YAML files:")
    print(f"     - {output_gateway}")
    print(f"     - {output_pipeline}")
    print(f"  2. Deploy using Databricks Asset Bundles:")
    print(f"     databricks bundle deploy -t <environment>")
    print("="*80)

    return pipeline_config_df


if __name__ == "__main__":
    # Example usage - modify these parameters as needed

    result_df = run_complete_pipeline_generation(
        input_csv='load_balancing/examples/example_config.csv',
        gateway_catalog='jack_demos',
        gateway_schema='ingestion_schema',
        project_name='my_project',
        max_tables_per_group=1000,
        default_connection_name='conn_1',
        default_schedule='*/15 * * * *',
        node_type_id='m5d.large',
        driver_node_type_id='c5a.8xlarge',
        output_gateway='deployment/resources/gateways.yml',
        output_pipeline='deployment/resources/pipelines.yml'
    )

    # Optional: Save the intermediate pipeline configuration for reference
    result_df.to_csv('deployment/examples/generated_config.csv', index=False)
    print(f"\n✓ Intermediate configuration saved to: deployment/examples/generated_config.csv")
