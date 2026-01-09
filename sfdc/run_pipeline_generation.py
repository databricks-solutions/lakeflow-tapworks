"""
Unified pipeline generation script for Salesforce that combines configuration and YAML generation.

This script demonstrates the complete two-part process:
1. Pipeline configuration: Groups Salesforce objects by prefix + priority
2. YAML generation: Creates Databricks Asset Bundle YAML files

Note: Unlike SQL Server, Salesforce is a SaaS connector and does NOT require gateways.
"""

import pandas as pd
import os
import sys

# Import from local modules
from load_balancing.generate_pipeline_config import generate_pipeline_config
from deployment.generate_dab_yaml import generate_yaml_files


def run_complete_pipeline_generation(
    input_csv: str,
    default_connection_name: str = "sfdc_connection",
    default_schedule: str = "*/15 * * * *",
    output_yaml: str = "deployment/resources/sfdc_pipeline.yml",
    output_config: str = "deployment/examples/generated_config.csv"
):
    """
    Complete pipeline generation process from Salesforce objects to YAML files.

    Pipeline grouping is based on prefix + priority combinations from the input CSV.
    Each unique (prefix, priority) pair becomes a separate pipeline.

    Args:
        input_csv (str): Path to input CSV with Salesforce objects (required)
            Must contain: source_database, source_schema, source_table_name,
                         target_catalog, target_schema, target_table_name,
                         prefix, priority
        default_connection_name (str): Default Salesforce connection name (default: "sfdc_connection")
        default_schedule (str): Default cron schedule (default: "*/15 * * * *")
        output_yaml (str): Output path for pipeline YAML
        output_config (str): Output path for intermediate configuration CSV

    Returns:
        pd.DataFrame: The pipeline configuration dataframe
    """
    print("="*80)
    print("SALESFORCE PIPELINE GENERATION")
    print("="*80)

    # Step 1: Load input CSV
    print(f"\n[Step 1/3] Loading input CSV: {input_csv}")
    input_df = pd.read_csv(input_csv)
    print(f"  ✓ Loaded {len(input_df)} Salesforce objects")

    # Step 2: Generate pipeline configuration (prefix + priority grouping)
    print(f"\n[Step 2/3] Generating pipeline configuration using prefix + priority")
    print(f"  - Default connection: {default_connection_name}")
    print(f"  - Default schedule: {default_schedule}")

    pipeline_config_df = generate_pipeline_config(
        df=input_df,
        default_connection_name=default_connection_name,
        default_schedule=default_schedule
    )

    print(f"\n  ✓ Created {pipeline_config_df['pipeline_group'].nunique()} pipelines")
    print(f"  ✓ Configured {len(pipeline_config_df)} Salesforce objects")

    # Save intermediate configuration
    os.makedirs(os.path.dirname(output_config), exist_ok=True)
    pipeline_config_df.to_csv(output_config, index=False)
    print(f"  ✓ Saved configuration to: {output_config}")

    # Step 3: Generate YAML file
    print(f"\n[Step 3/3] Generating Databricks Asset Bundle YAML file")
    print(f"  - Output YAML: {output_yaml}")

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_yaml), exist_ok=True)

    # Get connection name from first row (assumes all use same connection)
    connection_name = pipeline_config_df['connection_name'].iloc[0]

    generate_yaml_files(
        df=pipeline_config_df,
        connection_name=connection_name,
        output_path=output_yaml
    )

    print("\n" + "="*80)
    print("SALESFORCE PIPELINE GENERATION COMPLETE!")
    print("="*80)
    print(f"\nGenerated files:")
    print(f"  ✓ {output_yaml}")
    print(f"\nNext steps:")
    print(f"  1. Review the generated YAML file")
    print(f"  2. Ensure Salesforce connection '{connection_name}' exists in Databricks")
    print(f"  3. Deploy using Databricks Asset Bundles:")
    print(f"     cd deployment")
    print(f"     databricks bundle validate -t dev")
    print(f"     databricks bundle deploy -t dev")
    print("="*80)

    return pipeline_config_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Salesforce ingestion pipelines using prefix + priority grouping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with default example
  python run_pipeline_generation.py --input-csv load_balancing/examples/example_config.csv

  # With custom connection and schedule
  python run_pipeline_generation.py \\
    --input-csv my_config.csv \\
    --connection my_sfdc_conn \\
    --schedule "*/30 * * * *"

  # With custom output paths
  python run_pipeline_generation.py \\
    --input-csv my_config.csv \\
    --output deployment/resources/prod_pipeline.yml
        """
    )
    parser.add_argument(
        '--input-csv',
        type=str,
        required=True,
        help='Path to input CSV with Salesforce objects (required)'
    )
    parser.add_argument(
        '--connection',
        type=str,
        default='sfdc_connection',
        help='Default Salesforce connection name (default: sfdc_connection)'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        default='*/15 * * * *',
        help='Default cron schedule (default: */15 * * * *)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='deployment/resources/sfdc_pipeline.yml',
        help='Output path for pipeline YAML (default: deployment/resources/sfdc_pipeline.yml)'
    )
    parser.add_argument(
        '--output-config',
        type=str,
        default='deployment/examples/generated_config.csv',
        help='Output path for intermediate config CSV (default: deployment/examples/generated_config.csv)'
    )

    args = parser.parse_args()

    # Run the complete pipeline generation
    result_df = run_complete_pipeline_generation(
        input_csv=args.input_csv,
        default_connection_name=args.connection,
        default_schedule=args.schedule,
        output_yaml=args.output,
        output_config=args.output_config
    )
