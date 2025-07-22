"""
Test script for the Snowflake to PostgreSQL Migration Tool
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import sys
import os

# Add the current directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import (
    sanitize_column_name,
    convert_snowflake_type_to_postgres,
    prepare_dataframe_for_postgres,
    validate_migration_config,
    estimate_migration_time,
    format_duration,
    format_number,
    create_postgres_create_table_sql,
)
from config import Config


class TestUtils(unittest.TestCase):
    """Test utility functions"""

    def test_sanitize_column_name(self):
        """Test column name sanitization"""
        test_cases = [
            ("Column Name", "column_name"),
            ("Column-Name", "column_name"),
            ("Column.Name", "column_name"),
            ("123Column", "col_123column"),
            ("Valid_Column", "valid_column"),
            ("A" * 70, "a" * 60 + "_tr"),  # Test truncation
        ]

        for input_name, expected in test_cases:
            with self.subTest(input_name=input_name):
                result = sanitize_column_name(input_name)
                self.assertEqual(result, expected)

    def test_convert_snowflake_type_to_postgres(self):
        """Test Snowflake to PostgreSQL type conversion"""
        test_cases = [
            ("VARCHAR(255)", "VARCHAR(255)"),
            ("NUMBER(10,2)", "NUMERIC(10,2)"),
            ("TIMESTAMP_NTZ", "TIMESTAMP"),
            ("VARIANT", "JSONB"),
            ("BOOLEAN", "BOOLEAN"),
            ("FLOAT", "DOUBLE PRECISION"),
        ]

        for sf_type, expected_pg_type in test_cases:
            with self.subTest(sf_type=sf_type):
                result = convert_snowflake_type_to_postgres(sf_type)
                self.assertEqual(result, expected_pg_type)

    def test_validate_migration_config(self):
        """Test migration configuration validation"""
        valid_sf_config = {
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "table": "TEST_TABLE",
            "query_type": "Entire Table",
        }

        valid_pg_config = {
            "host": "localhost",
            "port": 5432,
            "username": "user",
            "password": "pass",
            "database": "testdb",
            "schema": "public",
            "table": "test_table",
        }

        # Test valid configuration
        errors = validate_migration_config(valid_sf_config, valid_pg_config)
        self.assertEqual(len(errors), 0)

        # Test invalid port
        invalid_pg_config = valid_pg_config.copy()
        invalid_pg_config["port"] = "invalid"
        errors = validate_migration_config(valid_sf_config, invalid_pg_config)
        self.assertTrue(any("port must be a valid number" in error for error in errors))

    def test_estimate_migration_time(self):
        """Test migration time estimation"""
        result = estimate_migration_time(100000, 10000, 1000)

        self.assertEqual(result["total_batches"], 10)
        self.assertEqual(result["estimated_seconds"], 100.0)
        self.assertEqual(result["records_per_batch"], 10000)

    def test_format_duration(self):
        """Test duration formatting"""
        test_cases = [(30, "30.0 seconds"), (90, "1.5 minutes"), (3661, "1h 1m")]

        for seconds, expected in test_cases:
            with self.subTest(seconds=seconds):
                result = format_duration(seconds)
                self.assertEqual(result, expected)

    def test_format_number(self):
        """Test number formatting"""
        self.assertEqual(format_number(1000), "1,000")
        self.assertEqual(format_number(1000000), "1,000,000")


class TestConfig(unittest.TestCase):
    """Test configuration management"""

    def test_get_optimal_batch_size(self):
        """Test optimal batch size calculation"""
        test_cases = [
            (50000, Config.BATCH_SIZES["small"]),
            (500000, Config.BATCH_SIZES["medium"]),
            (5000000, Config.BATCH_SIZES["large"]),
            (50000000, Config.BATCH_SIZES["xlarge"]),
        ]

        for total_records, expected_batch_size in test_cases:
            with self.subTest(total_records=total_records):
                result = Config.get_optimal_batch_size(total_records)
                self.assertEqual(result, expected_batch_size)


class TestDataframePreparation(unittest.TestCase):
    """Test DataFrame preparation for PostgreSQL"""

    def test_prepare_dataframe_for_postgres(self):
        """Test DataFrame preparation"""
        import numpy as np

        # Create test DataFrame
        df = pd.DataFrame(
            {
                "id": [1, 2, np.nan],
                "name": ["Alice", "Bob", None],
                "amount": [100.5, np.nan, 200.0],
                "is_active": [True, False, None],
            }
        )

        column_types = {
            "id": "INTEGER",
            "name": "TEXT",
            "amount": "NUMERIC",
            "is_active": "BOOLEAN",
        }

        result_df = prepare_dataframe_for_postgres(df, column_types)

        # Check that NaN values are properly handled
        self.assertIsNone(result_df.loc[2, "id"])
        self.assertIsNone(result_df.loc[1, "amount"])


if __name__ == "__main__":
    # Create a test suite
    test_suite = unittest.TestSuite()

    # Add test classes
    test_classes = [TestUtils, TestConfig, TestDataframePreparation]

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")

    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")

    if result.wasSuccessful():
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
