"""
DEPRECATED PIPELINE GENERATION MODES

These functions are kept for reference only and will not work.
They reference non-existent scripts from an older architecture.

Current implementation: Use run_complete_pipeline_generation() from pipeline_generator.py
"""

import sys


def run_csv_mode(
    input_csv: str,
    output_config: str = None,
    output_yaml: str = "deployment/resources/ga4_pipeline.yml",
    schedule: str = "0 */6 * * *"
):
    """
    DEPRECATED: Run pipeline generation from CSV with prefix+priority grouping.

    This function references non-existent scripts and will not work.
    Use run_complete_pipeline_generation() instead.

    Original logic:
    - Step 1: Call load_balancing/generate_pipeline_config.py to generate config
    - Step 2: Call deployment/generate_dab_yaml.py to generate YAML

    Args:
        input_csv: Input CSV with GA4 properties
        output_config: Output CSV with pipeline_group added
        output_yaml: Output YAML file path
        schedule: Default schedule for pipelines
    """
    import subprocess

    print("="*80)
    print("GA4 PIPELINE GENERATION - CSV MODE")
    print("="*80)
    print()

    # Default output config path
    if not output_config:
        output_config = input_csv.replace('.csv', '_config.csv')

    print(f"Input CSV: {input_csv}")
    print(f"Output Config: {output_config}")
    print(f"Output YAML: {output_yaml}")
    print()

    # Step 1: Generate pipeline configuration with prefix+priority grouping
    print("Step 1: Generating pipeline configuration...")
    print("-"*80)
    cmd = [
        sys.executable,
        "load_balancing/generate_pipeline_config.py",  # Does not exist
        input_csv,
        output_config,
        "--schedule", schedule
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("\n✗ Failed to generate pipeline configuration")
        sys.exit(1)

    # Step 2: Generate DAB YAML from configuration
    print("\nStep 2: Generating Databricks Asset Bundle YAML...")
    print("-"*80)
    cmd = [
        sys.executable,
        "deployment/generate_dab_yaml.py",  # Does not exist
        output_config,
        "--output", output_yaml
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("\n✗ Failed to generate YAML")
        sys.exit(1)

    print("\n" + "="*80)
    print("✓ PIPELINE GENERATION COMPLETE")
    print("="*80)
    print()
    print("Generated files:")
    print(f"  1. Configuration: {output_config}")
    print(f"  2. YAML: {output_yaml}")
    print()
    print("Next steps:")
    print(f"  1. Review YAML: {output_yaml}")
    print(f"  2. Update GA4 connection name if needed")
    print(f"  3. Deploy: cd ga4 && databricks bundle deploy -t dev")
    print()


def run_auto_discover_mode(
    num_pipelines: int = 3,
    output_config: str = "dab/examples/auto_balanced_config.csv",
    output_yaml: str = "deployment/resources/ga4_pipeline.yml",
    schedule: str = "0 */6 * * *"
):
    """
    DEPRECATED: Run pipeline generation with auto-discovery and bin-packing.

    This function references non-existent scripts and will not work.

    Original logic:
    - Step 1: Query BigQuery to discover all GA4 properties
    - Step 2: Use FFD (First Fit Decreasing) bin-packing algorithm to distribute properties
    - Step 3: Generate YAML from balanced configuration

    The bin-packing approach would query property sizes and optimally distribute them
    across N pipelines to balance load.

    Args:
        num_pipelines: Number of pipelines to create
        output_config: Output CSV path
        output_yaml: Output YAML file path
        schedule: Default schedule for pipelines
    """
    import subprocess

    print("="*80)
    print("GA4 PIPELINE GENERATION - AUTO-DISCOVER MODE")
    print("="*80)
    print()

    print(f"Number of pipelines: {num_pipelines}")
    print(f"Output Config: {output_config}")
    print(f"Output YAML: {output_yaml}")
    print()

    # Step 1: Run auto-balance script
    print("Step 1: Auto-discovering properties and balancing...")
    print("-"*80)
    cmd = [
        sys.executable,
        "dab/auto_balance_ga4.py",  # Does not exist
        "--pipelines", str(num_pipelines),
        "--output", output_config,
        "--schedule", schedule
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("\n✗ Failed to auto-balance properties")
        sys.exit(1)

    # Step 2: Generate DAB YAML from balanced configuration
    print("\nStep 2: Generating Databricks Asset Bundle YAML...")
    print("-"*80)
    cmd = [
        sys.executable,
        "deployment/generate_dab_yaml.py",  # Does not exist
        output_config,
        "--output", output_yaml
    ]

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print("\n✗ Failed to generate YAML")
        sys.exit(1)

    print("\n" + "="*80)
    print("✓ AUTO-DISCOVER PIPELINE GENERATION COMPLETE")
    print("="*80)
    print()
    print("Generated files:")
    print(f"  1. Balanced Configuration: {output_config}")
    print(f"  2. YAML: {output_yaml}")
    print()
