#!/usr/bin/env python3
"""
Create Databricks GA4 connection using service account credentials from secrets.

This script:
1. Reads Google Cloud service account JSON from Databricks secrets
2. Creates Unity Catalog connection for GA4 data ingestion
3. All credentials stored in secrets - nothing hardcoded!

Usage:
    python create_ga4_connection.py <connection_name> <secret_scope> [--force]

Example:
    python create_ga4_connection.py my_ga4_connection ga4_secrets

Required Secrets:
    - service_account_json: Complete Google service account JSON

Setup:
    1. Create secret scope:
       databricks secrets create-scope ga4_secrets

    2. Store service account JSON:
       databricks secrets put-secret ga4_secrets service_account_json \
         --string-value '{"type":"service_account",...}'

    3. Run this script to create connection
"""

import sys
import json
import subprocess
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType


def get_secret_value(scope, key):
    """Get secret value using Databricks CLI."""
    import base64
    try:
        result = subprocess.run(
            ["databricks", "secrets", "get-secret", scope, key, "--output", "json"],
            capture_output=True,
            text=True,
            check=True
        )
        output = json.loads(result.stdout)
        value = output.get("value", "")
        # Databricks returns base64-encoded values, decode them
        try:
            decoded = base64.b64decode(value).decode('utf-8')
            return decoded
        except:
            # If decoding fails, return as-is
            return value
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Error reading secret {scope}/{key}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"   ❌ Invalid JSON response from secrets API")
        return None


def create_ga4_connection(connection_name, secret_scope, force=False):
    """
    Create GA4 connection using service account from secrets.
    """
    print("=" * 80)
    print("GA4 CONNECTION CREATOR")
    print("=" * 80)

    # Initialize Databricks client
    print("\n1. Initializing Databricks client...")
    w = WorkspaceClient()
    print(f"   ✅ Connected to: {w.config.host}")
    print(f"   ✅ User: {w.current_user.me().user_name}")

    # Check if connection exists
    print(f"\n2. Checking if connection '{connection_name}' exists...")
    try:
        existing = w.connections.get(connection_name)
        print(f"   ⚠️  Connection already exists: {existing.connection_type}")

        if not force:
            print("\n   ❌ Use --force to recreate")
            return False

        print(f"   🗑️  Deleting existing connection...")
        w.connections.delete(connection_name)
        print(f"   ✅ Deleted")
    except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e) or "does not exist" in str(e):
            print(f"   ✅ Connection does not exist (will create new)")
        else:
            raise

    # Read service account JSON from secrets
    print(f"\n3. Reading service account from secrets (scope: {secret_scope})...")
    service_account_json = get_secret_value(secret_scope, "service_account_json")

    if not service_account_json:
        print("   ❌ Missing service_account_json secret")
        print("\n   Store it with:")
        print(f"      databricks secrets put-secret {secret_scope} service_account_json \\")
        print(f"        --string-value '{{\"type\":\"service_account\",...}}'")
        return False

    # Validate it's valid JSON
    try:
        sa_data = json.loads(service_account_json)
        project_id = sa_data.get("project_id", "unknown")
        client_email = sa_data.get("client_email", "unknown")
        print(f"   ✅ Service Account JSON loaded")
        print(f"      Project ID: {project_id}")
        print(f"      Client Email: {client_email}")
    except json.JSONDecodeError:
        print("   ❌ service_account_json is not valid JSON")
        return False

    # Create connection
    print(f"\n4. Creating GA4 connection '{connection_name}'...")
    try:
        connection = w.connections.create(
            name=connection_name,
            connection_type=ConnectionType.GA4_RAW_DATA,
            options={
                "service_account_json": service_account_json
            },
            comment=f"GA4 connection created via SDK using {secret_scope} secrets"
        )

        print(f"   ✅ Connection created successfully!")
        print(f"      Name: {connection.name}")
        print(f"      Type: {connection.connection_type}")
        print(f"      ID: {connection.connection_id}")
        print(f"\n   ℹ️  Connection uses:")
        print(f"      - service_account_json: {{{{secrets/{secret_scope}/service_account_json}}}}")

        return True

    except Exception as e:
        print(f"   ❌ Error creating connection: {e}")
        print("\n   Common issues:")
        print("   1. Invalid service account JSON")
        print("   2. Insufficient permissions in GCP project")
        print("   3. GA4 BigQuery export not enabled")
        print("   4. Databricks workspace not authorized for GA4")
        import traceback
        traceback.print_exc()
        return False


def main():
    if len(sys.argv) < 3:
        print("\n❌ Error: Missing required arguments")
        print("\nUsage:")
        print(f"  {sys.argv[0]} <connection_name> <secret_scope> [--force]")
        print("\nExample:")
        print(f"  {sys.argv[0]} my_ga4_connection ga4_secrets")
        print("\nRequired Secrets:")
        print("  - service_account_json (Google Cloud service account JSON)")
        print("\nSetup:")
        print("  See README.md for detailed instructions")
        sys.exit(1)

    connection_name = sys.argv[1]
    secret_scope = sys.argv[2]
    force = "--force" in sys.argv

    success = create_ga4_connection(connection_name, secret_scope, force)

    if success:
        print("\n" + "=" * 80)
        print("✅ SUCCESS!")
        print("=" * 80)
        print(f"\nConnection '{connection_name}' is ready to use in:")
        print(f"  - Lakeflow Connect ingestion pipelines")
        print(f"  - Databricks Asset Bundles (DABs)")
        print(f"  - SQL queries")
        print(f"\nYour YAML files reference this connection by NAME:")
        print(f"  connection_name: {connection_name}")
        print(f"\nNo credentials are hardcoded anywhere!")
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("❌ FAILED!")
        print("=" * 80)
        print("\nConnection creation failed. Review errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
