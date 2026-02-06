#!/usr/bin/env python3
"""
Create Unity Catalog catalog and schema for GA4 ingestion.

This script creates:
1. Catalog: ga4_connector
2. Schema: ga4_connector.ga4

Usage:
    python create_ga4_catalog.py
"""

from databricks.sdk import WorkspaceClient


def create_ga4_catalog():
    """Create catalog and schema for GA4 data."""

    print("=" * 80)
    print("GA4 CATALOG SETUP")
    print("=" * 80)

    # Initialize Databricks client
    print("\n1. Initializing Databricks client...")
    w = WorkspaceClient()
    print(f"   ✅ Connected to: {w.config.host}")
    print(f"   ✅ User: {w.current_user.me().user_name}")

    catalog_name = "ga4_connector"
    schema_name = "ga4"

    # Create catalog
    print(f"\n2. Creating catalog '{catalog_name}'...")
    try:
        catalog = w.catalogs.create(
            name=catalog_name,
            comment="Catalog for Google Analytics 4 ingestion data via Lakeflow Connect"
        )
        print(f"   ✅ Catalog created successfully!")
        print(f"      Name: {catalog.name}")
        print(f"      Full Name: {catalog.full_name}")
        print(f"      Owner: {catalog.owner}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Catalog '{catalog_name}' already exists")
            catalog = w.catalogs.get(catalog_name)
        else:
            print(f"   ❌ Error creating catalog: {e}")
            return False

    # Create schema
    print(f"\n3. Creating schema '{catalog_name}.{schema_name}'...")
    try:
        schema = w.schemas.create(
            name=schema_name,
            catalog_name=catalog_name,
            comment="Schema for Google Analytics 4 events, users, and related tables"
        )
        print(f"   ✅ Schema created successfully!")
        print(f"      Name: {schema.name}")
        print(f"      Full Name: {schema.full_name}")
        print(f"      Catalog: {schema.catalog_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Schema '{catalog_name}.{schema_name}' already exists")
        else:
            print(f"   ❌ Error creating schema: {e}")
            return False

    print("\n" + "=" * 80)
    print("✅ SUCCESS!")
    print("=" * 80)
    print(f"\nCreated Unity Catalog resources:")
    print(f"  Catalog: {catalog_name}")
    print(f"  Schema: {catalog_name}.{schema_name}")
    print(f"\nData from GA4 will be stored in:")
    print(f"  {catalog_name}.{schema_name}.<table_name>")
    print(f"\nExample tables that will be created:")
    print(f"  - {catalog_name}.{schema_name}.events")
    print(f"  - {catalog_name}.{schema_name}.events_intraday")
    print(f"  - {catalog_name}.{schema_name}.users")

    return True


if __name__ == "__main__":
    success = create_ga4_catalog()

    if success:
        print("\n" + "=" * 80)
        print("NEXT STEPS:")
        print("=" * 80)
        print("\n1. UPDATE CSV CONFIGURATION:")
        print("   Update ga4_config.csv to use:")
        print("     target_catalog: ga4_connector")
        print("     target_schema: ga4")
        print("\n2. REGENERATE YAML:")
        print("   python generate_ga4_pipeline.py ga4_config.csv")
        print("\n3. DEPLOY PIPELINE:")
        print("   cd ga4_dab")
        print("   databricks bundle deploy -t dev --auto-approve")
        exit(0)
    else:
        print("\n❌ Catalog setup failed!")
        exit(1)
