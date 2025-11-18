#!/usr/bin/env python3
"""
Create Unity Catalog catalog and schema for Salesforce ingestion.

This script creates catalog and schema for Salesforce data.

Usage:
    python create_salesforce_catalog.py [catalog_name] [schema_name]

Examples:
    python create_salesforce_catalog.py
    python create_salesforce_catalog.py salesforce_connector salesforce_dev
    python create_salesforce_catalog.py salesforce_connector salesforce_prod
"""

import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogType


def create_salesforce_catalog(catalog_name: str = "salesforce_connector",
                              schema_name: str = "salesforce"):
    """Create catalog and schema for Salesforce data."""

    print("=" * 80)
    print("SALESFORCE CATALOG SETUP")
    print("=" * 80)

    # Initialize Databricks client
    print("\n1. Initializing Databricks client...")
    w = WorkspaceClient()
    print(f"   [OK] Connected to: {w.config.host}")
    print(f"   [OK] User: {w.current_user.me().user_name}")

    # Create catalog
    print(f"\n2. Creating catalog '{catalog_name}'...")
    try:
        catalog = w.catalogs.create(
            name=catalog_name,
            comment="Catalog for Salesforce ingestion data via Lakeflow Connect"
        )
        print(f"   [OK] Catalog created successfully!")
        print(f"      Name: {catalog.name}")
        print(f"      Full Name: {catalog.full_name}")
        print(f"      Owner: {catalog.owner}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   [INFO]  Catalog '{catalog_name}' already exists")
            catalog = w.catalogs.get(catalog_name)
        else:
            print(f"   [ERROR] Error creating catalog: {e}")
            return False

    # Create schema
    print(f"\n3. Creating schema '{catalog_name}.{schema_name}'...")
    try:
        schema = w.schemas.create(
            name=schema_name,
            catalog_name=catalog_name,
            comment="Schema for Salesforce standard and custom objects"
        )
        print(f"   [OK] Schema created successfully!")
        print(f"      Name: {schema.name}")
        print(f"      Full Name: {schema.full_name}")
        print(f"      Catalog: {schema.catalog_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   [INFO]  Schema '{catalog_name}.{schema_name}' already exists")
        else:
            print(f"   [ERROR] Error creating schema: {e}")
            return False

    print("\n" + "=" * 80)
    print("[OK] SUCCESS!")
    print("=" * 80)
    print(f"\nCreated Unity Catalog resources:")
    print(f"  Catalog: {catalog_name}")
    print(f"  Schema: {catalog_name}.{schema_name}")
    print(f"\nData from Salesforce will be stored in:")
    print(f"  {catalog_name}.{schema_name}.<table_name>")
    print(f"\nExample tables that will be created:")
    print(f"  - {catalog_name}.{schema_name}.Account")
    print(f"  - {catalog_name}.{schema_name}.Contact")
    print(f"  - {catalog_name}.{schema_name}.Opportunity")
    print(f"  - ... (and 7 more from your config)")

    return True

def main():
        # Parse command-line arguments
    catalog_name = sys.argv[1] if len(sys.argv) > 0 else "salesforce_catalog"
    schema_name = sys.argv[2] if len(sys.argv) > 1 else "salesforce"

    print(f"\nCreating catalog: {catalog_name}")
    print(f"Creating schema: {catalog_name}.{schema_name}")
    print()

    success = create_salesforce_catalog(catalog_name, schema_name)

    if success:
        print("\n" + "=" * 80)
        print("NEXT STEPS:")
        print("=" * 80)
        print("\n1. UPDATE DATABRICKS.YML:")
        print("   Update salesforce_dab/databricks.yml variables:")
        print(f"     dest_catalog: {catalog_name}")
        print(f"     dest_schema: {schema_name}")
        print("\n2. UPDATE CSV CONFIGURATION:")
        print("   Update salesforce_config.csv to use:")
        print(f"     target_catalog: {catalog_name}")
        print(f"     target_schema: {schema_name}")
        print("\n3. REGENERATE YAML:")
        print("   python generate_dab_yaml.py salesforce_config.csv")
        print("\n4. DEPLOY PIPELINE:")
        print("   cd salesforce_dab")
        print("   databricks bundle deploy -t dev")
        sys.exit(0)
    else:
        print("\n[ERROR] Catalog setup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
