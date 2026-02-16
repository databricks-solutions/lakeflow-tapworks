"""
Example: Group-Based Configuration for Defaults and Overrides

This script demonstrates how to use group-based defaults and overrides
to apply different configuration values to different pipeline groups
from a single CSV file.

Matching precedence:
1. pipeline_group (prefix_subgroup) - most specific (e.g., 'sales_02')
2. prefix (e.g., 'sales')
3. project_name
4. '*' (global)
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.runner import run_pipeline_generation

# Configuration
INPUT_CSV = Path(__file__).parent / "input" / "salesforce_config.csv"
OUTPUT_DIR = Path(__file__).parent / "output"

TARGETS = {
    "dev": {
        "workspace_host": "https://dev-workspace.cloud.databricks.com",
        "root_path": "/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}",
    },
    "prod": {
        "workspace_host": "https://prod-workspace.cloud.databricks.com",
        "root_path": "/Shared/.bundle/${bundle.name}/${bundle.target}",
    },
}

# Group-based defaults
# - Global (*): default schedule for all groups
# - sales: different schedule for all sales_* pipeline groups
# - sales_2: even more specific schedule for sales_2 only (note: CSV subgroup '02' becomes '2')
# - hr: different schedule for hr pipeline groups
DEFAULT_VALUES = {
    "*": {
        "schedule": "0 */6 * * *",  # Every 6 hours (global default)
    },
    "sales": {
        "schedule": "*/15 * * * *",  # Every 15 minutes for sales
    },
    "sales_2": {
        "schedule": "*/30 * * * *",  # Every 30 minutes for sales_2 specifically
    },
    "hr": {
        "schedule": "0 0 * * *",  # Daily at midnight for HR
    },
    # finance will use global default (every 6 hours)
}

# Group-based overrides
# - Global (*): all pipelines unpaused by default
# - finance: finance pipelines are paused (e.g., for compliance review)
OVERRIDE_CONFIG = {
    "*": {
        "pause_status": "UNPAUSED",
    },
    "finance": {
        "pause_status": "PAUSED",
    },
}


def main():
    print("=" * 60)
    print("Group-Based Configuration Example")
    print("=" * 60)
    print()
    print("Input CSV:", INPUT_CSV)
    print("Output Dir:", OUTPUT_DIR)
    print()
    print("Default Values (per group):")
    for group, values in DEFAULT_VALUES.items():
        print(f"  {group}: {values}")
    print()
    print("Override Config (per group):")
    for group, values in OVERRIDE_CONFIG.items():
        print(f"  {group}: {values}")
    print()
    print("-" * 60)

    # Run pipeline generation
    result_df = run_pipeline_generation(
        connector_name="salesforce",
        input_source=str(INPUT_CSV),
        output_dir=str(OUTPUT_DIR),
        targets=TARGETS,
        default_values=DEFAULT_VALUES,
        override_config=OVERRIDE_CONFIG,
    )

    print()
    print("Results:")
    print("-" * 60)

    # Show what was applied to each pipeline group
    for _, row in result_df.iterrows():
        print(f"  {row['prefix']}_{row['subgroup']}: "
              f"schedule={row['schedule']}, "
              f"pause_status={row['pause_status']}")

    print()
    print(f"Generated DAB templates in: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
