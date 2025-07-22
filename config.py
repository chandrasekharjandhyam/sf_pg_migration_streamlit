"""
Configuration utilities for the Snowflake to PostgreSQL migration tool.
"""

import os
from typing import Dict, Any


class Config:
    """Configuration management for the migration tool"""

    # Default batch sizes for different scenarios
    BATCH_SIZES = {
        "small": 1000,  # < 100K records
        "medium": 10000,  # 100K - 1M records
        "large": 50000,  # 1M - 10M records
        "xlarge": 100000,  # > 10M records
    }

    # Default connection timeouts
    TIMEOUTS = {
        "snowflake_connect": 30,
        "postgres_connect": 15,
        "query_timeout": 300,
        "bulk_insert_timeout": 600,
    }

    # Performance tuning parameters
    PERFORMANCE = {
        "max_workers": 4,
        "checkpoint_interval": 1000,  # Save checkpoint every N batches
        "memory_limit_mb": 1024,  # Memory limit per batch
        "retry_attempts": 3,
        "retry_delay": 5,  # seconds
    }

    @staticmethod
    def get_optimal_batch_size(total_records: int) -> int:
        """Get optimal batch size based on total records"""
        if total_records < 100000:
            return Config.BATCH_SIZES["small"]
        elif total_records < 1000000:
            return Config.BATCH_SIZES["medium"]
        elif total_records < 10000000:
            return Config.BATCH_SIZES["large"]
        else:
            return Config.BATCH_SIZES["xlarge"]

    @staticmethod
    def get_snowflake_config() -> Dict[str, Any]:
        """Get Snowflake configuration from environment variables"""
        return {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "timeout": Config.TIMEOUTS["snowflake_connect"],
        }

    @staticmethod
    def is_snowflake_deployment() -> bool:
        """Check if running in Snowflake environment"""
        try:
            from snowflake.snowpark.context import get_active_session

            get_active_session()
            return True
        except:
            return False


# Data type mappings
SNOWFLAKE_TO_POSTGRES_TYPES = {
    "VARCHAR": "TEXT",
    "CHAR": "CHAR",
    "STRING": "TEXT",
    "TEXT": "TEXT",
    "NUMBER": "NUMERIC",
    "DECIMAL": "DECIMAL",
    "NUMERIC": "NUMERIC",
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "SMALLINT",
    "BYTEINT": "SMALLINT",
    "FLOAT": "DOUBLE PRECISION",
    "FLOAT4": "REAL",
    "FLOAT8": "DOUBLE PRECISION",
    "DOUBLE": "DOUBLE PRECISION",
    "DOUBLE PRECISION": "DOUBLE PRECISION",
    "REAL": "REAL",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP_LTZ": "TIMESTAMPTZ",
    "TIMESTAMP_NTZ": "TIMESTAMP",
    "TIMESTAMP_TZ": "TIMESTAMPTZ",
    "VARIANT": "JSONB",
    "OBJECT": "JSONB",
    "ARRAY": "JSONB",
    "GEOGRAPHY": "TEXT",
    "GEOMETRY": "TEXT",
    "BINARY": "BYTEA",
    "VARBINARY": "BYTEA",
}

# SQL templates for common operations
SQL_TEMPLATES = {
    "get_primary_keys": """
        SELECT column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_schema = '{schema}'
        AND tc.table_name = '{table}'
        AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
    """,
    "get_table_columns": """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """,
    "create_index": """
        CREATE INDEX IF NOT EXISTS idx_{table}_{columns}
        ON {schema}.{table} ({columns})
    """,
    "analyze_table": """
        ANALYZE {schema}.{table}
    """,
}
