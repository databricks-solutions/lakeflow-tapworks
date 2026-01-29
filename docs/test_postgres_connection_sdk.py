#!/usr/bin/env python3
"""
Test PostgreSQL connection creation via Databricks SDK.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

def create_postgres_connection():
    w = WorkspaceClient()

    connection_name = "postgres_sdk_test_connection"

    print(f"Testing PostgreSQL connection creation via SDK...")
    print(f"Connection name: {connection_name}")

    # Check if connection already exists
    try:
        existing = w.connections.get(connection_name)
        print(f"✓ Connection '{connection_name}' already exists")
        print(f"  Owner: {existing.owner}")
        print(f"  Type: {existing.connection_type}")
        return existing
    except Exception:
        print(f"Connection '{connection_name}' does not exist, creating...")

    try:
        # Create PostgreSQL connection with static credentials from secrets
        connection = w.connections.create(
            name=connection_name,
            connection_type=ConnectionType.POSTGRESQL,
            options={
                "host": "localhost/postgres",
                "port": "5432",
                "user": "{{secrets/postgres_secrets/postgres_username}}",
                "password": "{{secrets/postgres_secrets/postgres_password}}"
            }
        )

        print(f"✓ Connection created successfully!")
        print(f"  Name: {connection.name}")
        print(f"  Type: {connection.connection_type}")
        print(f"  Owner: {connection.owner}")
        return connection

    except Exception as e:
        print(f"✗ Failed to create connection: {e}")
        return None

if __name__ == "__main__":
    create_postgres_connection()
