"""
Utility functions for the Snowflake to PostgreSQL migration tool.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional
import re
from datetime import datetime

logger = logging.getLogger(__name__)


def sanitize_column_name(column_name: str) -> str:
    """Sanitize column names for PostgreSQL compatibility"""
    # Convert to lowercase
    sanitized = column_name.lower()

    # Replace spaces and special characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized)

    # Ensure it starts with a letter or underscore
    if not re.match(r"^[a-zA-Z_]", sanitized):
        sanitized = f"col_{sanitized}"

    # Truncate if too long (PostgreSQL limit is 63 characters)
    if len(sanitized) > 63:
        sanitized = sanitized[:60] + "_tr"

    return sanitized


def convert_snowflake_type_to_postgres(sf_type: str) -> str:
    """Convert Snowflake data type to PostgreSQL equivalent"""
    from config import SNOWFLAKE_TO_POSTGRES_TYPES

    # Extract base type (remove precision/scale if present)
    base_type = sf_type.split("(")[0].upper()

    # Handle special cases with precision/scale
    if base_type in ["NUMBER", "DECIMAL", "NUMERIC"]:
        # Preserve precision and scale for numeric types
        if "(" in sf_type:
            return f"NUMERIC{sf_type[sf_type.find('('):]}"
        else:
            return "NUMERIC"

    elif base_type in ["VARCHAR", "CHAR"]:
        # Handle string types with length
        if "(" in sf_type:
            return f"VARCHAR{sf_type[sf_type.find('('):]}"
        else:
            return "TEXT"

    # Use mapping for other types
    return SNOWFLAKE_TO_POSTGRES_TYPES.get(base_type, "TEXT")


def prepare_dataframe_for_postgres(df: pd.DataFrame, column_types: Dict[str, str]) -> pd.DataFrame:
    """Prepare DataFrame for PostgreSQL insertion"""
    df_copy = df.copy()

    for column, pg_type in column_types.items():
        if column not in df_copy.columns:
            continue

        # Handle different PostgreSQL types
        if pg_type.startswith("NUMERIC") or pg_type in ["INTEGER", "BIGINT", "SMALLINT"]:
            # Handle numeric types - replace NaN with None
            df_copy[column] = df_copy[column].replace({np.nan: None, np.inf: None, -np.inf: None})

        elif pg_type in ["TIMESTAMP", "TIMESTAMPTZ", "DATE"]:
            # Handle datetime types
            if df_copy[column].dtype == "object":
                try:
                    df_copy[column] = pd.to_datetime(df_copy[column], errors="coerce")
                except:
                    pass
            df_copy[column] = df_copy[column].replace({pd.NaT: None})

        elif pg_type == "BOOLEAN":
            # Handle boolean types
            df_copy[column] = df_copy[column].astype("boolean").replace({pd.NA: None})

        elif pg_type in ["TEXT", "VARCHAR"]:
            # Handle string types - convert None to empty string if needed
            df_copy[column] = df_copy[column].astype(str).replace({"nan": None, "None": None})

        elif pg_type == "JSONB":
            # Handle JSON types - ensure proper JSON formatting
            def format_json(val):
                if pd.isna(val) or val is None:
                    return None
                if isinstance(val, str):
                    try:
                        import json

                        json.loads(val)  # Validate JSON
                        return val
                    except:
                        return json.dumps(val)  # Convert to JSON string
                else:
                    import json

                    return json.dumps(val)

            df_copy[column] = df_copy[column].apply(format_json)

    return df_copy


def validate_migration_config(sf_config: Dict, pg_config: Dict) -> List[str]:
    """Validate migration configuration and return list of errors"""
    errors = []

    # Validate Snowflake config
    required_sf_fields = ["database", "schema", "table"]
    for field in required_sf_fields:
        if not sf_config.get(field):
            errors.append(f"Snowflake {field} is required")

    if sf_config.get("query_type") == "Custom Query" and not sf_config.get("custom_query"):
        errors.append("Custom query is required when query type is 'Custom Query'")

    # Validate PostgreSQL config
    required_pg_fields = ["host", "port", "username", "password", "database", "schema", "table"]
    for field in required_pg_fields:
        if not pg_config.get(field):
            errors.append(f"PostgreSQL {field} is required")

    # Validate port number
    try:
        port = int(pg_config.get("port", 0))
        if port <= 0 or port > 65535:
            errors.append("PostgreSQL port must be between 1 and 65535")
    except (ValueError, TypeError):
        errors.append("PostgreSQL port must be a valid number")

    return errors


def estimate_migration_time(
    total_records: int, batch_size: int, avg_speed: float = 1000
) -> Dict[str, Any]:
    """Estimate migration time based on records and performance metrics"""
    if avg_speed <= 0:
        avg_speed = 1000  # Default assumption: 1000 records/second

    total_time_seconds = total_records / avg_speed
    total_batches = (total_records + batch_size - 1) // batch_size  # Ceiling division

    return {
        "total_batches": total_batches,
        "estimated_seconds": total_time_seconds,
        "estimated_minutes": total_time_seconds / 60,
        "estimated_hours": total_time_seconds / 3600,
        "records_per_batch": batch_size,
        "assumed_speed": avg_speed,
    }


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        return f"{seconds/60:.1f} minutes"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_number(number: int) -> str:
    """Format large numbers with thousand separators"""
    return f"{number:,}"


def calculate_progress_stats(current: int, total: int, start_time: datetime) -> Dict[str, Any]:
    """Calculate progress statistics"""
    elapsed_time = (datetime.now() - start_time).total_seconds()
    progress_percent = (current / total * 100) if total > 0 else 0

    if elapsed_time > 0 and current > 0:
        speed = current / elapsed_time
        remaining = total - current
        eta_seconds = remaining / speed if speed > 0 else 0
    else:
        speed = 0
        eta_seconds = 0

    return {
        "progress_percent": progress_percent,
        "elapsed_seconds": elapsed_time,
        "speed_records_per_second": speed,
        "eta_seconds": eta_seconds,
        "eta_formatted": format_duration(eta_seconds),
        "elapsed_formatted": format_duration(elapsed_time),
    }


def create_postgres_create_table_sql(table_name: str, schema: str, columns: List[Dict]) -> str:
    """Generate CREATE TABLE SQL for PostgreSQL"""
    column_definitions = []

    for col in columns:
        col_name = sanitize_column_name(col["name"])
        pg_type = convert_snowflake_type_to_postgres(col["type"])
        nullable = "" if col.get("nullable", True) else "NOT NULL"

        column_definitions.append(f'"{col_name}" {pg_type} {nullable}'.strip())

    sql = f"""
CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    {',\n    '.join(column_definitions)}
);"""

    return sql


def log_migration_event(event_type: str, details: Dict[str, Any]):
    """Log migration events for monitoring and debugging"""
    timestamp = datetime.now().isoformat()
    logger.info(f"Migration Event: {event_type} at {timestamp}", extra={"details": details})


class PerformanceMonitor:
    """Monitor and track performance metrics during migration"""

    def __init__(self):
        self.start_time = datetime.now()
        self.batch_times = []
        self.error_count = 0
        self.total_records = 0
        self.checkpoint_times = []

    def record_batch(self, batch_size: int, batch_time: float):
        """Record batch processing metrics"""
        self.batch_times.append(batch_time)
        self.total_records += batch_size

    def record_error(self):
        """Record an error occurrence"""
        self.error_count += 1

    def record_checkpoint(self, checkpoint_time: float):
        """Record checkpoint save time"""
        self.checkpoint_times.append(checkpoint_time)

    def get_statistics(self) -> Dict[str, Any]:
        """Get performance statistics"""
        total_time = (datetime.now() - self.start_time).total_seconds()
        avg_batch_time = sum(self.batch_times) / len(self.batch_times) if self.batch_times else 0
        avg_speed = self.total_records / total_time if total_time > 0 else 0

        return {
            "total_runtime_seconds": total_time,
            "total_records_processed": self.total_records,
            "average_batch_time": avg_batch_time,
            "average_speed_records_per_second": avg_speed,
            "total_batches": len(self.batch_times),
            "error_count": self.error_count,
            "average_checkpoint_time": (
                sum(self.checkpoint_times) / len(self.checkpoint_times)
                if self.checkpoint_times
                else 0
            ),
        }
