#!/usr/bin/env python3
"""
Create PostgreSQL Unity Catalog Connection

Creates a Databricks Unity Catalog connection for PostgreSQL using credentials
stored in Databricks secrets.

Usage:
    python create_postgres_connection.py <connection_name> <secret_scope> \\
        --host <hostname> --port <port>

Examples:
    # Basic usage (prompts for host if not provided)
    python create_postgres_connection.py postgres_prod_conn jdbc_secrets

    # With all options
    python create_postgres_connection.py postgres_prod_conn jdbc_secrets \\
        --host postgres.example.com \\
        --port 5432 \\
        --username-key postgres_username \\
        --password-key postgres_password
"""
import sys
import argparse
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType


def create_postgres_connection(connection_name, secret_scope, host, port,
                               username_key='postgres_username',
                               password_key='postgres_password'):
    """
    Create a Unity Catalog connection for PostgreSQL.

    Args:
        connection_name: Name for the connection
        secret_scope: Databricks secret scope containing credentials
        host: PostgreSQL hostname
        port: PostgreSQL port
        username_key: Secret key for username
        password_key: Secret key for password

    Returns:
        Created connection object
    """
    print("=" * 80)
    print("POSTGRESQL CONNECTION CREATOR")
    print("=" * 80)

    w = WorkspaceClient()

    print(f"\nConnection Details:")
    print(f"  Name: {connection_name}")
    print(f"  Type: POSTGRESQL")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Secret Scope: {secret_scope}")
    print(f"  Username Key: {username_key}")
    print(f"  Password Key: {password_key}")
    print("\nNote:")
    print("  - Unity Catalog POSTGRESQL connections do not accept a 'database' option.")
    print("  - Choose the database per-table in your pipeline config (source_database).")

    print(f"\nCreating connection '{connection_name}'...")

    try:
        connection = w.connections.create(
            name=connection_name,
            connection_type=ConnectionType.POSTGRESQL,
            options={
                "host": str(host),
                "port": str(port),
                "user": f"{{{{secrets/{secret_scope}/{username_key}}}}}",
                "password": f"{{{{secrets/{secret_scope}/{password_key}}}}}"
            }
        )

        print("\n" + "=" * 80)
        print("[OK] CONNECTION CREATED SUCCESSFULLY")
        print("=" * 80)
        print(f"\nConnection Name: {connection.name}")
        print(f"Connection ID: {connection.connection_id}")
        print(f"Connection Type: {connection.connection_type}")

        print("\nNext Steps:")
        print("  1. Test the connection in Databricks UI")
        print("  2. Grant permissions if needed:")
        print(f"     databricks connections grant '{connection_name}' USE_CONNECTION <principal>")
        print("  3. Use this connection in your pipeline configuration CSV")

        return connection

    except Exception as e:
        print("\n" + "=" * 80)
        print("[FAIL] CONNECTION CREATION FAILED")
        print("=" * 80)
        print(f"\nError: {e}")
        print("\nTroubleshooting:")
        print("  1. Verify secret scope exists:")
        print(f"     databricks secrets list-scopes | grep {secret_scope}")
        print("  2. Verify secrets exist:")
        print(f"     databricks secrets list --scope {secret_scope}")
        print("  3. Check PostgreSQL connectivity from Databricks")
        print("  4. Verify you have CREATE CONNECTION permissions")
        sys.exit(1)


def main():
    """Main execution with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description='Create PostgreSQL Unity Catalog connection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Prompt for host/port/database
  python create_postgres_connection.py postgres_prod_conn jdbc_secrets

  # Specify all options
  python create_postgres_connection.py postgres_prod_conn jdbc_secrets \\
      --host postgres.example.com \\
      --port 5432

  # Custom secret keys
  python create_postgres_connection.py postgres_prod_conn jdbc_secrets \\
      --host postgres.example.com \\
      --port 5432 \\
      --username-key pg_user \\
      --password-key pg_pass

Prerequisites:
  1. Create secret scope:
     databricks secrets create-scope jdbc_secrets

  2. Store PostgreSQL credentials:
     databricks secrets put-secret jdbc_secrets postgres_username --string-value "myuser"
     databricks secrets put-secret jdbc_secrets postgres_password --string-value "mypassword"

  3. Enable logical replication on PostgreSQL (for CDC):
     In postgresql.conf:
       wal_level = logical
       max_replication_slots = 10
       max_wal_senders = 10

  4. Grant permissions to replication user:
     CREATE USER databricks_repl WITH REPLICATION LOGIN PASSWORD 'secure_password';
     GRANT SELECT ON ALL TABLES IN SCHEMA public TO databricks_repl;
        '''
    )

    parser.add_argument('connection_name', help='Name for the Databricks connection')
    parser.add_argument('secret_scope', help='Databricks secret scope containing credentials')
    parser.add_argument('--host', help='PostgreSQL hostname (e.g., postgres.example.com)')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('--username-key', default='postgres_username',
                       help='Secret key for username (default: postgres_username)')
    parser.add_argument('--password-key', default='postgres_password',
                       help='Secret key for password (default: postgres_password)')

    args = parser.parse_args()

    # Prompt for required fields if not provided
    host = args.host
    if not host:
        host = input("PostgreSQL Hostname: ").strip()
        if not host:
            print("[FAIL] Error: Hostname is required")
            sys.exit(1)

    create_postgres_connection(
        connection_name=args.connection_name,
        secret_scope=args.secret_scope,
        host=host,
        port=args.port,
        username_key=args.username_key,
        password_key=args.password_key
    )


if __name__ == "__main__":
    main()
