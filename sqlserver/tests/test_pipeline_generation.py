"""
Unit tests for SQL Server pipeline generation.

Tests cover three main scenarios:
1. Simple config: CSV without gateway config columns (uses all defaults)
2. Mixed config: CSV with columns but some rows have empty values (uses defaults for empty)
3. Full config: CSV with all values specified (some databases use defaults for node types)
"""

import unittest
import pandas as pd
import tempfile
import os
import shutil
import yaml
from pathlib import Path

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from load_balancing.load_balancer import generate_pipeline_config
from deployment.connector_settings_generator import generate_yaml_files, create_gateways
from pipeline_generator import run_complete_pipeline_generation


class TestSimpleConfig(unittest.TestCase):
    """Test scenario 1: CSV without gateway config columns."""

    def setUp(self):
        """Create test data matching simple_tapworks_config.csv."""
        self.test_data = pd.DataFrame({
            'source_database': ['db1', 'db1', 'db1', 'db2', 'db2', 'db2', 'db3', 'db3', 'db3'],
            'source_schema': ['dbo'] * 9,
            'source_table_name': ['products', 'customers', 'orders', 'sales', 'transactions', 'payments', 'logs', 'events', 'audit'],
            'target_catalog': ['tapworks_catalog'] * 3 + ['bronze_catalog'] * 3 + ['silver_catalog'] * 3,
            'target_schema': ['tapworks'] * 3 + ['sales'] * 3 + ['logs'] * 3,
            'target_table_name': ['products', 'customers', 'orders', 'sales', 'transactions', 'payments', 'logs', 'events', 'audit']
        })

        self.default_connection_name = 'my_sqlserver_connection'
        self.default_gateway_worker_type = 'm6d.xlarge'
        self.default_gateway_driver_type = 'm6d.xlarge'

    def test_generate_pipeline_config_uses_defaults(self):
        """Test that all gateway configs use default values when columns are missing."""
        result_df = generate_pipeline_config(
            df=self.test_data,
            default_connection_name=self.default_connection_name,
            default_gateway_worker_type=self.default_gateway_worker_type,
            default_gateway_driver_type=self.default_gateway_driver_type,
            max_tables_per_group=1000,
            default_schedule='*/15 * * * *'
        )

        # Check that all rows have the default connection name
        self.assertEqual(result_df['connection_name'].nunique(), 1)
        self.assertEqual(result_df['connection_name'].iloc[0], self.default_connection_name)

        # Check that all rows have default node types
        self.assertEqual(result_df['gateway_worker_type'].nunique(), 1)
        self.assertEqual(result_df['gateway_worker_type'].iloc[0], self.default_gateway_worker_type)
        self.assertEqual(result_df['gateway_driver_type'].iloc[0], self.default_gateway_driver_type)

        # Check that gateway_catalog and gateway_schema fall back to target values
        for idx, row in result_df.iterrows():
            self.assertEqual(row['gateway_catalog'], row['target_catalog'])
            self.assertEqual(row['gateway_schema'], row['target_schema'])

        # Check that we have 3 databases = 3 gateways
        self.assertEqual(result_df['gateway'].nunique(), 3)

        # Check that we have 3 gateways (one per database)
        # Note: Actual pipeline group count depends on subgroup values in test data

    def test_gateway_yaml_has_cluster_config(self):
        """Test that generated YAML includes cluster configuration when node types are provided."""
        result_df = generate_pipeline_config(
            df=self.test_data,
            default_connection_name=self.default_connection_name,
            default_gateway_worker_type=self.default_gateway_worker_type,
            default_gateway_driver_type=self.default_gateway_driver_type,
            max_tables_per_group=1000
        )

        gateway_yaml = create_gateways(result_df, 'test_project')

        # Check that all gateways have cluster configuration
        for pipeline_name, config in gateway_yaml['resources']['pipelines'].items():
            self.assertIn('clusters', config)
            self.assertEqual(len(config['clusters']), 1)
            self.assertEqual(config['clusters'][0]['node_type_id'], self.default_gateway_worker_type)
            self.assertEqual(config['clusters'][0]['driver_node_type_id'], self.default_gateway_driver_type)


class TestMixedConfig(unittest.TestCase):
    """Test scenario 2: CSV with columns but some rows have empty values."""

    def setUp(self):
        """Create test data matching mixed_tapworks_config.csv."""
        self.test_data = pd.DataFrame({
            'source_database': ['db1', 'db1', 'db1', 'db2', 'db2', 'db2', 'db3', 'db3', 'db3'],
            'source_schema': ['dbo'] * 9,
            'source_table_name': ['products', 'customers', 'orders', 'sales', 'transactions', 'payments', 'logs', 'events', 'audit'],
            'target_catalog': ['tapworks_catalog'] * 3 + ['bronze_catalog'] * 3 + ['silver_catalog'] * 3,
            'target_schema': ['tapworks'] * 3 + ['sales'] * 3 + ['logs'] * 3,
            'target_table_name': ['products', 'customers', 'orders', 'sales', 'transactions', 'payments', 'logs', 'events', 'audit'],
            'gateway_catalog': ['tapworks_catalog_gw'] * 3 + [None] * 3 + ['silver_catalog_gw'] * 3,
            'gateway_schema': ['tapworks_gw'] * 3 + [None] * 3 + ['logs_gw'] * 3,
            'connection_name': ['sqlserver_conn_1'] * 3 + [None] * 3 + ['sqlserver_conn_3'] * 3,
            'gateway_worker_type': ['m5d.large'] * 3 + [None] * 6,
            'gateway_driver_type': ['m5d.large'] * 3 + [None] * 6
        })

        self.default_connection_name = 'my_sqlserver_connection_2'
        self.default_gateway_worker_type = 'm7d.xlarge'
        self.default_gateway_driver_type = 'm7d.xlarge'

    def test_empty_values_use_defaults(self):
        """Test that empty/null values in CSV are replaced with defaults."""
        result_df = generate_pipeline_config(
            df=self.test_data,
            default_connection_name=self.default_connection_name,
            default_gateway_worker_type=self.default_gateway_worker_type,
            default_gateway_driver_type=self.default_gateway_driver_type,
            max_tables_per_group=1000
        )

        # db1 (gateway 1): Should use values from CSV
        db1_rows = result_df[result_df['source_database'] == 'db1']
        self.assertEqual(db1_rows['connection_name'].iloc[0], 'sqlserver_conn_1')
        self.assertEqual(db1_rows['gateway_catalog'].iloc[0], 'tapworks_catalog_gw')
        self.assertEqual(db1_rows['gateway_schema'].iloc[0], 'tapworks_gw')
        self.assertEqual(db1_rows['gateway_worker_type'].iloc[0], 'm5d.large')
        self.assertEqual(db1_rows['gateway_driver_type'].iloc[0], 'm5d.large')

        # db2 (gateway 2): Should use default values since CSV has None/empty
        db2_rows = result_df[result_df['source_database'] == 'db2']
        self.assertEqual(db2_rows['connection_name'].iloc[0], self.default_connection_name)
        self.assertEqual(db2_rows['gateway_catalog'].iloc[0], 'bronze_catalog')  # Falls back to target_catalog
        self.assertEqual(db2_rows['gateway_schema'].iloc[0], 'sales')  # Falls back to target_schema
        self.assertEqual(db2_rows['gateway_worker_type'].iloc[0], self.default_gateway_worker_type)
        self.assertEqual(db2_rows['gateway_driver_type'].iloc[0], self.default_gateway_driver_type)

        # db3 (gateway 3): Should use CSV values for connection/catalog/schema, defaults for node types
        db3_rows = result_df[result_df['source_database'] == 'db3']
        self.assertEqual(db3_rows['connection_name'].iloc[0], 'sqlserver_conn_3')
        self.assertEqual(db3_rows['gateway_catalog'].iloc[0], 'silver_catalog_gw')
        self.assertEqual(db3_rows['gateway_schema'].iloc[0], 'logs_gw')
        self.assertEqual(db3_rows['gateway_worker_type'].iloc[0], self.default_gateway_worker_type)
        self.assertEqual(db3_rows['gateway_driver_type'].iloc[0], self.default_gateway_driver_type)

    def test_yaml_no_null_values(self):
        """Test that YAML output has no null values for gateway configs."""
        result_df = generate_pipeline_config(
            df=self.test_data,
            default_connection_name=self.default_connection_name,
            default_gateway_worker_type=self.default_gateway_worker_type,
            default_gateway_driver_type=self.default_gateway_driver_type,
            max_tables_per_group=1000
        )

        gateway_yaml = create_gateways(result_df, 'test_project')

        # Check that no gateway has null values
        for pipeline_name, config in gateway_yaml['resources']['pipelines'].items():
            self.assertIsNotNone(config['gateway_definition']['connection_name'])
            self.assertIsNotNone(config['gateway_definition']['gateway_storage_catalog'])
            self.assertIsNotNone(config['gateway_definition']['gateway_storage_schema'])
            self.assertIsNotNone(config['catalog'])
            self.assertIsNotNone(config['schema'])


class TestFullConfig(unittest.TestCase):
    """Test scenario 3: Fully specified CSV with different configs per database."""

    def setUp(self):
        """Create test data matching tapworks_config.csv."""
        self.test_data = pd.DataFrame({
            'source_database': ['db1', 'db1', 'db1', 'db2', 'db2', 'db2', 'db3', 'db3', 'db3'],
            'source_schema': ['dbo'] * 9,
            'source_table_name': ['products', 'customers', 'orders', 'sales', 'transactions', 'payments', 'logs', 'events', 'audit'],
            'target_catalog': ['tapworks_catalog'] * 3 + ['bronze_catalog'] * 3 + ['silver_catalog'] * 3,
            'target_schema': ['tapworks'] * 3 + ['sales'] * 3 + ['logs'] * 3,
            'target_table_name': ['products', 'customers', 'orders', 'sales', 'transactions', 'payments', 'logs', 'events', 'audit'],
            'gateway_catalog': ['tapworks_catalog_gw'] * 3 + ['bronze_catalog_gw'] * 3 + ['silver_catalog_gw'] * 3,
            'gateway_schema': ['tapworks_gw'] * 3 + ['sales_gw'] * 3 + ['logs_gw'] * 3,
            'connection_name': ['sqlserver_conn_1'] * 3 + ['sqlserver_conn_2'] * 3 + ['sqlserver_conn_3'] * 3,
            'gateway_worker_type': ['m5d.large'] * 3 + ['c5a.xlarge'] * 3 + [None] * 3,
            'gateway_driver_type': ['c5a.8xlarge'] * 3 + ['c5a.2xlarge'] * 3 + [None] * 3
        })

        # These defaults should only be used for db3 which has None for node types
        self.default_connection_name = 'fallback_connection'
        self.default_gateway_worker_type = None  # Serverless
        self.default_gateway_driver_type = None  # Serverless

    def test_different_configs_per_database(self):
        """Test that each database can have different gateway configurations."""
        result_df = generate_pipeline_config(
            df=self.test_data,
            default_connection_name=self.default_connection_name,
            default_gateway_worker_type=self.default_gateway_worker_type,
            default_gateway_driver_type=self.default_gateway_driver_type,
            max_tables_per_group=1000
        )

        # Check db1 configuration
        db1_rows = result_df[result_df['source_database'] == 'db1']
        self.assertEqual(db1_rows['connection_name'].iloc[0], 'sqlserver_conn_1')
        self.assertEqual(db1_rows['gateway_worker_type'].iloc[0], 'm5d.large')
        self.assertEqual(db1_rows['gateway_driver_type'].iloc[0], 'c5a.8xlarge')

        # Check db2 configuration (different from db1)
        db2_rows = result_df[result_df['source_database'] == 'db2']
        self.assertEqual(db2_rows['connection_name'].iloc[0], 'sqlserver_conn_2')
        self.assertEqual(db2_rows['gateway_worker_type'].iloc[0], 'c5a.xlarge')
        self.assertEqual(db2_rows['gateway_driver_type'].iloc[0], 'c5a.2xlarge')

        # Check db3 configuration (None for node types)
        db3_rows = result_df[result_df['source_database'] == 'db3']
        self.assertEqual(db3_rows['connection_name'].iloc[0], 'sqlserver_conn_3')
        self.assertIsNone(db3_rows['gateway_worker_type'].iloc[0])
        self.assertIsNone(db3_rows['gateway_driver_type'].iloc[0])

    def test_gateway_without_cluster_config(self):
        """Test that gateways with None node types don't have cluster configuration."""
        result_df = generate_pipeline_config(
            df=self.test_data,
            default_connection_name=self.default_connection_name,
            default_gateway_worker_type=self.default_gateway_worker_type,
            default_gateway_driver_type=self.default_gateway_driver_type,
            max_tables_per_group=1000
        )

        gateway_yaml = create_gateways(result_df, 'test_project')

        # Gateway 1 (db1) and Gateway 2 (db2) should have cluster config
        gateway_1 = gateway_yaml['resources']['pipelines']['test_project_pipeline_test_project_gateway_1']
        gateway_2 = gateway_yaml['resources']['pipelines']['test_project_pipeline_test_project_gateway_2']
        self.assertIn('clusters', gateway_1)
        self.assertIn('clusters', gateway_2)

        # Gateway 3 (db3) should NOT have cluster config
        gateway_3 = gateway_yaml['resources']['pipelines']['test_project_pipeline_test_project_gateway_3']
        self.assertNotIn('clusters', gateway_3)


class TestCompleteIntegration(unittest.TestCase):
    """Integration tests for the complete pipeline generation process."""

    def setUp(self):
        """Create temporary directory for test outputs."""
        self.test_dir = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.test_dir, 'test_config.csv')

    def tearDown(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.test_dir)

    def test_complete_pipeline_with_simple_config(self):
        """Test complete pipeline generation with simple config."""
        # Create simple config CSV
        test_data = pd.DataFrame({
            'source_database': ['db1', 'db1', 'db2'],
            'source_schema': ['dbo'] * 3,
            'source_table_name': ['table1', 'table2', 'table3'],
            'target_catalog': ['catalog1'] * 2 + ['catalog2'],
            'target_schema': ['schema1'] * 2 + ['schema2'],
            'target_table_name': ['table1', 'table2', 'table3']
        })
        test_data.to_csv(self.csv_path, index=False)

        output_dir = os.path.join(self.test_dir, 'output')

        # Run complete pipeline generation
        result_df = run_complete_pipeline_generation(
            input_csv=self.csv_path,
            project_name='test_project',
            output_dir=output_dir,
            workspace_host='https://test.cloud.databricks.com',
            root_path='/Users/test/.bundle/${bundle.name}/${bundle.target}',
            default_connection_name='test_connection',
            default_gateway_worker_type='m5d.large',
            default_gateway_driver_type='m5d.large',
            max_tables_per_group=1000
        )

        # Verify output files were created
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'databricks.yml')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'resources', 'gateways.yml')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'resources', 'pipelines.yml')))

        # Verify dataframe structure
        self.assertEqual(len(result_df), 3)
        self.assertEqual(result_df['gateway'].nunique(), 2)  # 2 databases = 2 gateways

        # Verify YAML content
        with open(os.path.join(output_dir, 'resources', 'gateways.yml'), 'r') as f:
            gateways = yaml.safe_load(f)
            # Check that both gateways have valid connection names
            for pipeline_name, config in gateways['resources']['pipelines'].items():
                self.assertEqual(config['gateway_definition']['connection_name'], 'test_connection')


    def test_max_tables_per_group_limit(self):
        """Test that tables are split into multiple groups when exceeding max_tables_per_group."""
        # Create config with 5 tables and max_tables_per_group=2
        test_data = pd.DataFrame({
            'source_database': ['db1'] * 5,
            'source_schema': ['dbo'] * 5,
            'source_table_name': [f'table{i}' for i in range(1, 6)],
            'target_catalog': ['catalog1'] * 5,
            'target_schema': ['schema1'] * 5,
            'target_table_name': [f'table{i}' for i in range(1, 6)]
        })

        result_df = generate_pipeline_config(
            df=test_data,
            default_connection_name='test_connection',
            default_gateway_worker_type=None,
            default_gateway_driver_type=None,
            max_tables_per_group=2  # Should create 3 groups (2+2+1)
        )

        # Should have 3 pipeline groups
        self.assertEqual(result_df['pipeline_group'].nunique(), 3)

        # Check group sizes
        group_sizes = result_df.groupby('pipeline_group').size()
        self.assertEqual(group_sizes.iloc[0], 2)
        self.assertEqual(group_sizes.iloc[1], 2)
        self.assertEqual(group_sizes.iloc[2], 1)


if __name__ == '__main__':
    unittest.main()
