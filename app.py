import streamlit as st
import snowflake.connector
import psycopg2
import pandas as pd
import json
import time
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
from io import StringIO
import numpy as np
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MigrationCheckpoint:
    """Handles checkpoint mechanism for migration offsets"""

    def __init__(self, checkpoint_file: str = "migration_checkpoints.json"):
        self.checkpoint_file = checkpoint_file
        self.checkpoints = self._load_checkpoints()

    def _load_checkpoints(self) -> Dict:
        """Load existing checkpoints from file"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save_checkpoint(self, config_key: str, offset: int, total_records: int):
        """Save checkpoint for a configuration"""
        self.checkpoints[config_key] = {
            "offset": offset,
            "total_records": total_records,
            "last_updated": datetime.now().isoformat(),
        }
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.checkpoints, f, indent=2)

    def get_checkpoint(self, config_key: str) -> Optional[Dict]:
        """Get checkpoint for a configuration"""
        return self.checkpoints.get(config_key)

    def clear_checkpoint(self, config_key: str):
        """Clear checkpoint for a configuration"""
        if config_key in self.checkpoints:
            del self.checkpoints[config_key]
            with open(self.checkpoint_file, "w") as f:
                json.dump(self.checkpoints, f, indent=2)


class SnowflakeConnector:
    """Handles Snowflake database operations"""

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.account = None
        self.warehouse = None
        self.username = None
        self.password = None
        self.role = None

    def connect(self):
        """Connect using Snowflake session (for deployment in Snowflake)"""
        try:
            # For Snowflake deployment, use the session connection
            from snowflake.snowpark.context import get_active_session

            session = get_active_session()
            self.connection = session.get_connection()
            self.cursor = self.connection.cursor()
            return True
        except:
            # Fallback for local development using configuration parameters
            try:
                import snowflake.connector

                connect_params = {
                    "user": self.username,
                    "password": self.password,
                    "account": self.account,
                    "warehouse": self.warehouse,
                }

                # Add role if specified
                if self.role:
                    connect_params["role"] = self.role

                self.connection = snowflake.connector.connect(**connect_params)
                self.cursor = self.connection.cursor()
                return True
            except Exception as e:
                st.error(f"Failed to connect to Snowflake: {str(e)}")
                return False

    def get_databases(self) -> List[str]:
        """Get list of databases"""
        try:
            self.cursor.execute("SHOW DATABASES")
            databases = []
            for row in self.cursor.fetchall():
                db_name = row[1]
                # Skip shared databases and databases containing "SHARE"
                if not db_name.upper().startswith("SHARED_") and "SHARE" not in db_name.upper():
                    databases.append(db_name)
            return databases
        except Exception as e:
            st.error(f"Error fetching databases: {str(e)}")
            return []

    def get_all_metadata(self) -> Dict[str, Dict[str, List[str]]]:
        """Get all databases, schemas, and tables at once using information_schema"""
        metadata = {}
        try:
            # Get all databases first
            databases = self.get_databases()

            for database in databases:
                try:
                    # Try to use the database
                    self.cursor.execute(f"USE DATABASE {database}")

                    # Get schemas and tables for this database
                    query = """
                    SELECT
                        table_schema,
                        table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name
                    """

                    self.cursor.execute(query)
                    results = self.cursor.fetchall()

                    # Organize by schema
                    schemas_dict = {}
                    for row in results:
                        schema_name = row[0]
                        table_name = row[1]

                        if schema_name not in schemas_dict:
                            schemas_dict[schema_name] = []
                        schemas_dict[schema_name].append(table_name)

                    # Only include schemas that have tables
                    if schemas_dict:
                        metadata[database] = schemas_dict

                except Exception as e:
                    # Skip databases we can't access
                    continue

            return metadata

        except Exception as e:
            st.error(f"Error fetching metadata: {str(e)}")
            return {}

    def get_database_metadata(self, database: str) -> Dict[str, List[str]]:
        """Get schemas and tables for a specific database"""
        try:
            # Use the specified database
            self.cursor.execute(f"USE DATABASE {database}")

            # Get schemas and tables for this database
            query = """
            SELECT
                table_schema,
                table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """

            self.cursor.execute(query)
            results = self.cursor.fetchall()

            # Organize by schema
            schemas_dict = {}
            for row in results:
                schema_name = row[0]
                table_name = row[1]

                if schema_name not in schemas_dict:
                    schemas_dict[schema_name] = []
                schemas_dict[schema_name].append(table_name)

            return schemas_dict

        except Exception as e:
            st.error(f"Error fetching metadata for database {database}: {str(e)}")
            return {}

    def get_schemas(self, database: str) -> List[str]:
        """Get list of schemas for a database"""
        try:
            # Check if database is accessible first
            self.cursor.execute(f"USE DATABASE {database}")
            self.cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
            return [row[1] for row in self.cursor.fetchall()]
        except Exception as e:
            error_msg = str(e)
            if "Shared database is no longer available" in error_msg:
                st.error(
                    f"❌ Database '{database}' is a shared database that's no longer available. Please select a different database."
                )
            elif "does not exist" in error_msg:
                st.error(f"❌ Database '{database}' does not exist or you don't have access to it.")
            else:
                st.error(f"Error fetching schemas: {error_msg}")
            return []

    def get_tables(self, database: str, schema: str) -> List[str]:
        """Get list of tables for a schema"""
        try:
            self.cursor.execute(f"SHOW TABLES IN SCHEMA {database}.{schema}")
            return [row[1] for row in self.cursor.fetchall()]
        except Exception as e:
            st.error(f"Error fetching tables: {str(e)}")
            return []

    def get_table_info(self, database: str, schema: str, table: str) -> Dict:
        """Get table information including primary keys and column details"""
        try:
            # Get column information
            self.cursor.execute(f"DESCRIBE TABLE {database}.{schema}.{table}")
            columns = []
            for row in self.cursor.fetchall():
                logger.info(f"Column info: {row}")
                columns.append(
                    {
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[3] == "Y",
                        "default": row[4],
                        "primary_key": row[5] == "Y",
                    }
                )

            # Get primary key columns
            pk_columns = [col["name"] for col in columns if col["primary_key"]]
            logger.info(f"Primary keys: {pk_columns}")
            # Get total record count
            self.cursor.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table}")
            total_records = self.cursor.fetchone()[0]

            return {"columns": columns, "primary_keys": pk_columns, "total_records": total_records}
        except Exception as e:
            st.error(f"Error getting table info: {str(e)}")
            return {"columns": [], "primary_keys": [], "total_records": 0}

    def fetch_data_batch(
        self, query: str, batch_size: int = 10000, offset: int = 0
    ) -> pd.DataFrame:
        """Fetch data in batches"""
        try:
            paginated_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
            return pd.read_sql(paginated_query, self.connection)
        except Exception as e:
            error_msg = str(e)
            st.error(f"❌ **SQL Execution Error:** {error_msg}")

            # Provide specific guidance for common SQL errors
            if "syntax error" in error_msg.lower():
                st.error("🔍 **SQL Syntax Issue:** There's a syntax error in your query")
                st.info("💡 **Suggestions:**")
                st.info("• Check for missing quotes, commas, or parentheses")
                st.info("• Ensure all table/column names are correct")
                st.info("• Do NOT include LIMIT or OFFSET - they're added automatically")
            elif "does not exist" in error_msg.lower():
                st.error("🔍 **Object Not Found:** Table, schema, or column doesn't exist")
                st.info("💡 **Suggestion:** Verify all table and column names in your query")
            elif "permission denied" in error_msg.lower():
                st.error("🔍 **Access Denied:** Insufficient permissions for this query")
                st.info(
                    "💡 **Suggestion:** Check if you have SELECT permissions on all referenced objects"
                )

            return pd.DataFrame()

    def close(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


class PostgreSQLConnector:
    """Handles PostgreSQL database operations"""

    def __init__(self, host: str, port: int, username: str, password: str, database: str = None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """Connect to PostgreSQL"""
        try:
            if self.database:
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                    database=self.database,
                )
            else:
                # Connect without specifying database (to list databases)
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            st.error(f"Failed to connect to PostgreSQL: {str(e)}")
            return False

    def test_connection(self) -> bool:
        """Test PostgreSQL connection"""
        try:
            if self.database:
                conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                    database=self.database,
                )
            else:
                conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.username,
                    password=self.password,
                )
            conn.close()
            return True
        except:
            return False

    def get_databases(self) -> List[str]:
        """Get list of databases"""
        try:
            self.cursor.execute(
                """
                SELECT datname
                FROM pg_database
                WHERE datistemplate = false
                AND datname NOT IN ('postgres')
                ORDER BY datname
            """
            )
            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            st.error(f"Error fetching databases: {str(e)}")
            return []

    def switch_database(self, database: str) -> bool:
        """Switch to a different database"""
        try:
            if self.connection:
                self.connection.close()

            self.database = database
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
            )
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            st.error(f"Failed to switch to database {database}: {str(e)}")
            return False

    def get_schemas(self) -> List[str]:
        """Get list of schemas"""
        try:
            self.cursor.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
            """
            )
            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            st.error(f"Error fetching schemas: {str(e)}")
            return []

    def get_tables(self, schema: str) -> List[str]:
        """Get list of tables for a schema"""
        try:
            self.cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """,
                (schema,),
            )
            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            st.error(f"Error fetching tables: {str(e)}")
            return []

    def create_table_from_snowflake(
        self, schema: str, table: str, sf_columns: List[Dict], pk_columns: List[str] = None
    ) -> bool:
        """Create PostgreSQL table based on Snowflake schema"""
        try:
            # Map Snowflake types to PostgreSQL types
            type_mapping = {
                "VARCHAR": "TEXT",
                "NUMBER": "NUMERIC",
                "FLOAT": "DOUBLE PRECISION",
                "BOOLEAN": "BOOLEAN",
                "DATE": "DATE",
                "TIMESTAMP_NTZ": "TIMESTAMP",
                "TIMESTAMP_LTZ": "TIMESTAMPTZ",
                "TIMESTAMP_TZ": "TIMESTAMPTZ",
                "VARIANT": "JSONB",
                "ARRAY": "JSONB",
                "OBJECT": "JSONB",
            }

            columns_sql = []
            for col in sf_columns:
                pg_type = type_mapping.get(col["type"].split("(")[0], "TEXT")
                nullable = "" if col["nullable"] else "NOT NULL"
                columns_sql.append(f'"{col["name"]}" {pg_type} {nullable}')

            # Add primary key constraint if primary key columns are available
            if pk_columns:
                quoted_pk_columns = [f'"{col}"' for col in pk_columns]
                pk_constraint = f"PRIMARY KEY ({', '.join(quoted_pk_columns)})"
                columns_sql.append(pk_constraint)

            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    {', '.join(columns_sql)}
                )
            """

            self.cursor.execute(create_sql)
            self.connection.commit()
            return True
        except Exception as e:
            st.error(f"Error creating table: {str(e)}")
            return False

    def bulk_insert_data(self, schema: str, table: str, df: pd.DataFrame) -> bool:
        """Bulk insert data using COPY command"""
        try:
            # Handle null values in numeric columns
            for col in df.columns:
                if df[col].dtype in ["int64", "float64"]:
                    df[col] = df[col].replace({np.nan: None})

            # Create CSV string
            output = StringIO()
            df.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N")
            output.seek(0)

            # Use COPY command for fast bulk insert
            copy_sql = (
                f"COPY {schema}.{table} FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
            )
            self.cursor.copy_expert(copy_sql, output)
            self.connection.commit()
            return True
        except Exception as e:
            error_msg = str(e)
            st.error(f"❌ **Bulk Insert Error:** {error_msg}")

            # Provide more specific error information
            if "null value in column" in error_msg.lower():
                # Extract column name from error message
                import re

                match = re.search(r'null value in column "([^"]+)"', error_msg)
                if match:
                    column_name = match.group(1)
                    st.error(
                        f"🔍 **Column Issue:** Column '{column_name}' does not allow NULL values but received NULL data from Snowflake"
                    )
                    st.info(
                        "💡 **Suggestion:** Check if the PostgreSQL table schema allows NULL for this column, or modify the Snowflake query to exclude NULL values"
                    )
            elif "duplicate key value" in error_msg.lower():
                st.error("🔍 **Duplicate Key:** Attempting to insert duplicate primary key values")
                st.info(
                    "💡 **Suggestion:** Use 'Use Existing Table' option or check for duplicate data in source"
                )
            elif "does not exist" in error_msg.lower():
                st.error("🔍 **Table/Schema Missing:** Target table or schema does not exist")
                st.info("💡 **Suggestion:** Use 'Create New Table' option or verify schema exists")

            self.connection.rollback()
            return False

    def close(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


def create_config_key(sf_config: Dict, pg_config: Dict) -> str:
    """Create a unique configuration key for checkpointing"""
    sf_key = f"{sf_config['database']}.{sf_config['schema']}.{sf_config['table']}"
    pg_key = f"{pg_config['host']}:{pg_config['port']}.{pg_config['database']}.{pg_config['schema']}.{pg_config['table']}"
    return f"{sf_key}=>{pg_key}"


def load_pg_configurations() -> Dict:
    """Load PostgreSQL configurations from file"""
    config_file = "pg_configs.json"
    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading PostgreSQL configurations: {str(e)}")
            return {}
    else:
        # Create default configuration file
        default_configs = {
            "local_dev": {
                "name": "Local Development",
                "host": "localhost",
                "port": 5432,
                "username": "postgres",
                "password": "password",
            }
        }
        try:
            with open(config_file, "w") as f:
                json.dump(default_configs, f, indent=2)
            return default_configs
        except Exception as e:
            st.error(f"Error creating default configuration file: {str(e)}")
            return {}


def save_pg_configurations(configs: Dict):
    """Save PostgreSQL configurations to file"""
    config_file = "pg_configs.json"
    try:
        with open(config_file, "w") as f:
            json.dump(configs, f, indent=2)
    except Exception as e:
        st.error(f"Error saving PostgreSQL configurations: {str(e)}")


def load_sf_configurations() -> Dict:
    """Load Snowflake configurations from file"""
    config_file = "sf_configs.json"
    if os.path.exists(config_file):
        try:
            with open(config_file, "r") as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error loading Snowflake configurations: {str(e)}")
            return {}
    else:
        # Create default configuration file
        default_configs = {}
        try:
            with open(config_file, "w") as f:
                json.dump(default_configs, f, indent=2)
            return default_configs
        except Exception as e:
            st.error(f"Error creating default configuration file: {str(e)}")
            return {}


def save_sf_configurations(configs: Dict):
    """Save Snowflake configurations to file"""
    config_file = "sf_configs.json"
    try:
        with open(config_file, "w") as f:
            json.dump(configs, f, indent=2)
    except Exception as e:
        st.error(f"Error saving Snowflake configurations: {str(e)}")


def main():
    st.set_page_config(
        page_title="Snowflake to PostgreSQL Migration Tool",
        page_icon="❄️",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # Custom CSS for better UI appearance
    st.markdown(
        """
    <style>
    /* Reduce overall font sizes */
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
        max-width: 95%;
    }
    .stMainBlockContainer{
        padding-top: 1rem;
        padding-bottom: 0.5rem;
        padding-left: 1rem;
        padding-right: 1rem;}

    /* Smaller headers */
    h1 {
        font-size: 2rem !important;
        margin-bottom: 1rem !important;
    }

    h2 {
        font-size: 1.4rem !important;
        margin-bottom: 0.8rem !important;
        margin-top: 1rem !important;
    }

    h3 {
        font-size: 1.2rem !important;
        margin-bottom: 0.6rem !important;
        margin-top: 0.8rem !important;
    }

    /* Reduce spacing between elements */
    .element-container {
        margin-bottom: 0.5rem !important;
    }

    /* Smaller input fields */
    .stTextInput > div > div > input {
        font-size: 0.9rem !important;
        padding: 0.375rem 0.75rem !important;
    }

    .stSelectbox > div > div > div {
        font-size: 0.9rem !important;
    }

    .stTextArea > div > div > textarea {
        font-size: 0.9rem !important;
    }

    .stNumberInput > div > div > input {
        font-size: 0.9rem !important;
    }

    /* Simplify password field icon */
    .stTextInput input[type="password"] + div button {
        background: none !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0.25rem !important;
        font-size: 0.8rem !important;
    }

    /* Smaller buttons */
    .stButton > button {
        font-size: 0.9rem !important;
        padding: 0.375rem 0.75rem !important;
        height: auto !important;
    }

    /* Compact form sections */
    .stForm {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e9ecef;
    }

    /* Smaller info boxes */
    .stInfo, .stSuccess, .stWarning, .stError {
        font-size: 0.85rem !important;
        padding: 0.5rem !important;
    }

    /* Compact metrics */
    [data-testid="metric-container"] {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        padding: 0.5rem;
        border-radius: 0.25rem;
    }

    /* Smaller column gaps */
    .row-widget.stHorizontal {
        gap: 0.5rem;
    }

    /* Reduce checkbox and radio spacing */
    .stCheckbox, .stRadio {
        margin-bottom: 0.25rem !important;
    }

    /* Custom icons for sections */
    .snowflake-header {
        background: linear-gradient(90deg, #29b5e8, #006db7);
        color: white;
        padding: 0;
        border-radius: 0.5rem;
        margin-bottom: 0.8rem;
        height: 3rem;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .postgres-header {
        background: linear-gradient(90deg, #336791, #1e3a8a);
        color: white;
        padding: 0;
        border-radius: 0.5rem;
        margin-bottom: 0.8rem;
        height: 3rem;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .migration-header {
        background: linear-gradient(90deg, #059669, #065f46);
        color: white;
        padding: 0;
        border-radius: 0.5rem;
        margin-bottom: 0.8rem;
        height: 3rem;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .snowflake-header h2, .postgres-header h2, .migration-header h2 {
        margin: 0;
        font-size: 1.2rem;
        font-weight: 600;
        line-height: 1;
        text-align: center;
    }

    /* Better button styling - prevent stretching */
    .stButton > button {
        font-size: 0.9rem !important;
        padding: 0.375rem 1.5rem !important;
        height: auto !important;
        width: auto !important;
        min-width: 120px !important;
        max-width: 200px !important;
        margin: 0 !important;
    }

    /* Form submit buttons - multiple selectors for better targeting */
    .stForm button[kind="formSubmit"],
    .stForm > div > div > div > div > button,
    .stFormSubmitButton > button,
    .stForm button,
    form button {
        width: auto !important;
        max-width: 200px !important;
        margin: 0 !important;
        display: block !important;
        font-size: 0.9rem !important;
        padding: 0.375rem 1.5rem !important;
        height: auto !important;
        background-color: #0066cc !important;
        color: white !important;
        border: none !important;
        font-weight: 600 !important;
        background-image: none !important;
    }

    /* Additional form button targeting with higher specificity */
    [data-testid="stForm"] button,
    .stForm div button,
    .element-container .stForm button {
        background-color: #0066cc !important;
        color: white !important;
        border: none !important;
        font-weight: 600 !important;
        background-image: none !important;
        margin: 0 !important;
    }

    /* Override any hover states for form buttons */
    .stForm button:hover,
    [data-testid="stForm"] button:hover {
        background-color: #0052a3 !important;
        color: white !important;
    }

    /* Connect buttons styling */
    .stButton > button[kind="primary"] {
        background-color: #0066cc !important;
        color: white !important;
        border: none !important;
        font-weight: 600 !important;
    }

    /* Migration stats persistence */
    .migration-stats {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    # Main title with proper Snowflake icon
    st.markdown(
        """
    <div style='text-align: center; margin-bottom: 1.5rem;'>
        <h1>❄️ Snowflake to PostgreSQL Migration Tool</h1>
        <p style='color: #666; font-size: 1rem; margin-top: -0.5rem;'>
            Fast, reliable data migration with checkpoint support
        </p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # Initialize session state
    if "sf_connector" not in st.session_state:
        st.session_state.sf_connector = None
    if "pg_connector" not in st.session_state:
        st.session_state.pg_connector = None
    if "checkpoint_manager" not in st.session_state:
        st.session_state.checkpoint_manager = MigrationCheckpoint()
    if "sf_databases" not in st.session_state:
        st.session_state.sf_databases = []
    if "sf_metadata" not in st.session_state:
        st.session_state.sf_metadata = {}
    if "selected_database" not in st.session_state:
        st.session_state.selected_database = None
    if "previous_sf_database" not in st.session_state:
        st.session_state.previous_sf_database = None
    if "previous_sf_schema" not in st.session_state:
        st.session_state.previous_sf_schema = None
    if "previous_pg_schema" not in st.session_state:
        st.session_state.previous_pg_schema = None
    if "pg_configurations" not in st.session_state:
        st.session_state.pg_configurations = load_pg_configurations()
    if "sf_configurations" not in st.session_state:
        st.session_state.sf_configurations = load_sf_configurations()
    if "pg_databases" not in st.session_state:
        st.session_state.pg_databases = []
    if "selected_pg_database" not in st.session_state:
        st.session_state.selected_pg_database = None
    if "migration_in_progress" not in st.session_state:
        st.session_state.migration_in_progress = False
    if "last_migration_stats" not in st.session_state:
        st.session_state.last_migration_stats = None
    if "migration_completed" not in st.session_state:
        st.session_state.migration_completed = False

    # Create two columns for the forms
    col1, col2 = st.columns(2)

    # Snowflake Configuration Form
    with col1:
        st.markdown(
            '<div class="snowflake-header"><h2>❄️ Snowflake Configuration</h2></div>',
            unsafe_allow_html=True,
        )

        # Configuration selection
        sf_config_names = list(st.session_state.sf_configurations.keys())

        if sf_config_names:
            # Add "Current Active Session" as the first option, then configurations, then create new
            display_options = (
                ["Current Active Session"]
                + [
                    f"{config_id} ({st.session_state.sf_configurations[config_id]['name']})"
                    for config_id in sf_config_names
                ]
                + ["+ Create New Configuration"]
            )

            selected_option = st.selectbox(
                "Select Configuration", display_options, index=0, key="sf_config_select"
            )

            if selected_option == "Current Active Session":
                # Show info about current session
                st.info(
                    "**Current Snowflake Session**\nUsing the active Snowflake session from the deployment environment."
                )

                # Connect button for active session
                if st.button("Connect", type="primary", key="sf_connect_btn"):
                    sf_connector = SnowflakeConnector()
                    # No need to set connection parameters for active session

                    if sf_connector.connect():
                        st.session_state.sf_connector = sf_connector
                        st.success("✅ Connected to Snowflake using active session!")

                        # Fetch only databases first (much faster)
                        with st.spinner("Loading databases..."):
                            st.session_state.sf_databases = sf_connector.get_databases()

                        if st.session_state.sf_databases:
                            st.success(
                                f"Found {len(st.session_state.sf_databases)} accessible databases"
                            )
                        else:
                            st.warning("No accessible databases found")

                        # Clear previous selections when reconnecting
                        if "sf_config" in st.session_state:
                            del st.session_state.sf_config
                        st.session_state.selected_database = None
                        st.session_state.sf_metadata = {}
                        st.session_state.previous_sf_database = None
                        st.session_state.previous_sf_schema = None
                    else:
                        st.error("Failed to connect to Snowflake active session")

            elif selected_option == "+ Create New Configuration":
                # Show form to create new configuration
                with st.form("new_sf_config_form"):
                    st.markdown("#### Create New Configuration")

                    new_config_id = st.text_input(
                        "Configuration ID", placeholder="e.g., prod_snowflake"
                    )
                    new_config_name = st.text_input(
                        "Snowflake Server Name", placeholder="e.g., Production Snowflake"
                    )

                    col_account, col_warehouse = st.columns(2)
                    with col_account:
                        new_account = st.text_input("Account", placeholder="your_account")
                    with col_warehouse:
                        new_warehouse = st.text_input("Warehouse", placeholder="COMPUTE_WH")

                    col_user, col_pass = st.columns(2)
                    with col_user:
                        new_username = st.text_input("Username")
                    with col_pass:
                        new_password = st.text_input("Password", type="password")

                    new_role = st.text_input(
                        "Role (optional)", placeholder="Leave empty for default"
                    )

                    if st.form_submit_button("Save Configuration"):
                        if all(
                            [
                                new_config_id,
                                new_config_name,
                                new_account,
                                new_warehouse,
                                new_username,
                                new_password,
                            ]
                        ):
                            if new_config_id not in st.session_state.sf_configurations:
                                new_config = {
                                    "name": new_config_name,
                                    "account": new_account,
                                    "warehouse": new_warehouse,
                                    "username": new_username,
                                    "password": new_password,
                                }
                                if new_role.strip():
                                    new_config["role"] = new_role.strip()

                                st.session_state.sf_configurations[new_config_id] = new_config
                                save_sf_configurations(st.session_state.sf_configurations)
                                st.success(f"Configuration '{new_config_name}' saved successfully!")
                                st.rerun()
                            else:
                                st.error("Configuration ID already exists!")
                        else:
                            st.error("Please fill in all required fields (except role)")

            else:
                # Extract config_id from selected option
                config_id = selected_option.split(" (")[0]
                selected_config = st.session_state.sf_configurations[config_id]

                # Display configuration details in single box
                config_details = f"""**Server:** {selected_config['account']}
**Username:** {selected_config['username']}
**Warehouse:** {selected_config['warehouse']}"""

                st.info(config_details)

                # Connect button
                if st.button("Connect", type="primary", key="sf_connect_btn"):
                    sf_connector = SnowflakeConnector()
                    # Set connection parameters from config
                    sf_connector.account = selected_config["account"]
                    sf_connector.warehouse = selected_config["warehouse"]
                    sf_connector.username = selected_config["username"]
                    sf_connector.password = selected_config["password"]
                    sf_connector.role = selected_config.get("role")

                    if sf_connector.connect():
                        st.session_state.sf_connector = sf_connector
                        st.success("✅ Connected to Snowflake!")

                        # Fetch only databases first (much faster)
                        with st.spinner("Loading databases..."):
                            st.session_state.sf_databases = sf_connector.get_databases()

                        if st.session_state.sf_databases:
                            st.success(
                                f"Found {len(st.session_state.sf_databases)} accessible databases"
                            )
                        else:
                            st.warning("No accessible databases found")

                        # Clear previous selections when reconnecting
                        if "sf_config" in st.session_state:
                            del st.session_state.sf_config
                        st.session_state.selected_database = None
                        st.session_state.sf_metadata = {}
                        st.session_state.previous_sf_database = None
                        st.session_state.previous_sf_schema = None
                    else:
                        st.error("Failed to connect to Snowflake")
        else:
            # When no saved configurations exist, still show Current Active Session option
            display_options = ["Current Active Session", "+ Create New Configuration"]

            selected_option = st.selectbox(
                "Select Configuration", display_options, index=0, key="sf_config_select"
            )

            if selected_option == "Current Active Session":
                # Show info about current session
                st.info(
                    "**Current Snowflake Session**\nUsing the active Snowflake session from the deployment environment."
                )

                # Connect button for active session
                if st.button("Connect", type="primary", key="sf_connect_btn"):
                    sf_connector = SnowflakeConnector()
                    # No need to set connection parameters for active session

                    if sf_connector.connect():
                        st.session_state.sf_connector = sf_connector
                        st.success("✅ Connected to Snowflake using active session!")

                        # Fetch only databases first (much faster)
                        with st.spinner("Loading databases..."):
                            st.session_state.sf_databases = sf_connector.get_databases()

                        if st.session_state.sf_databases:
                            st.success(
                                f"Found {len(st.session_state.sf_databases)} accessible databases"
                            )
                        else:
                            st.warning("No accessible databases found")

                        # Clear previous selections when reconnecting
                        if "sf_config" in st.session_state:
                            del st.session_state.sf_config
                        st.session_state.selected_database = None
                        st.session_state.sf_metadata = {}
                        st.session_state.previous_sf_database = None
                        st.session_state.previous_sf_schema = None
                    else:
                        st.error("Failed to connect to Snowflake active session")

            else:  # "+ Create New Configuration"
                # Show form to create first configuration
                with st.form("first_sf_config_form"):
                    st.markdown("**Create Your First Snowflake Configuration**")

                    new_config_id = st.text_input(
                        "Configuration ID", placeholder="e.g., my_snowflake", value="default"
                    )
                    new_config_name = st.text_input(
                        "Snowflake Server Name", placeholder="e.g., My Snowflake Server"
                    )

                    col_account, col_warehouse = st.columns(2)
                    with col_account:
                        new_account = st.text_input("Account", placeholder="your_account")
                    with col_warehouse:
                        new_warehouse = st.text_input("Warehouse", placeholder="COMPUTE_WH")

                    col_user, col_pass = st.columns(2)
                    with col_user:
                        new_username = st.text_input("Username")
                    with col_pass:
                        new_password = st.text_input("Password", type="password")

                    new_role = st.text_input(
                        "Role (optional)", placeholder="Leave empty for default"
                    )

                    if st.form_submit_button("Save Configuration"):
                        if all(
                            [
                                new_config_id,
                                new_config_name,
                                new_account,
                                new_warehouse,
                                new_username,
                                new_password,
                            ]
                        ):
                            new_config = {
                                "name": new_config_name,
                                "account": new_account,
                                "warehouse": new_warehouse,
                                "username": new_username,
                                "password": new_password,
                            }
                            if new_role.strip():
                                new_config["role"] = new_role.strip()

                            st.session_state.sf_configurations[new_config_id] = new_config
                            save_sf_configurations(st.session_state.sf_configurations)
                            st.success(f"Configuration '{new_config_name}' saved successfully!")
                            st.rerun()
                        else:
                            st.error("Please fill in all required fields (except role)")

        # Move database/schema/table selection outside the form for dynamic updates
        if st.session_state.sf_connector and st.session_state.sf_databases:
            # Database selection
            available_databases = st.session_state.sf_databases

            if available_databases:
                # Get current database selection
                db_index = 0
                if st.session_state.previous_sf_database in available_databases:
                    db_index = available_databases.index(st.session_state.previous_sf_database)

                current_db = st.selectbox(
                    "Select Database", available_databases, index=db_index, key="sf_db_select"
                )

                # Check if database changed and load metadata for new database
                if current_db != st.session_state.selected_database:
                    st.session_state.selected_database = current_db

                    # Load schemas and tables for the selected database
                    with st.spinner(f"Loading schemas and tables for {current_db}..."):
                        st.session_state.sf_metadata = (
                            st.session_state.sf_connector.get_database_metadata(current_db)
                        )

                    if st.session_state.sf_metadata:
                        schema_count = len(st.session_state.sf_metadata)
                        table_count = sum(
                            len(tables) for tables in st.session_state.sf_metadata.values()
                        )
                        st.success(f"Loaded {schema_count} schemas with {table_count} tables")
                    else:
                        st.warning("No schemas with tables found in this database")

                    # Reset previous selections
                    st.session_state.previous_sf_database = current_db
                    st.session_state.previous_sf_schema = None
                    st.rerun()  # Refresh to update schema options

                if current_db and st.session_state.sf_metadata:
                    # Schema selection
                    available_schemas = list(st.session_state.sf_metadata.keys())

                    if available_schemas:
                        # Auto-select first schema if none selected or schema changed
                        schema_index = 0
                        if st.session_state.previous_sf_schema in available_schemas:
                            schema_index = available_schemas.index(
                                st.session_state.previous_sf_schema
                            )
                        else:
                            # If previous schema is not in current database, select first and update
                            st.session_state.previous_sf_schema = available_schemas[0]

                        current_schema = st.selectbox(
                            "Select Schema",
                            available_schemas,
                            index=schema_index,
                            key="sf_schema_select",
                        )

                        # Check if schema changed
                        if current_schema != st.session_state.previous_sf_schema:
                            st.session_state.previous_sf_schema = current_schema
                            st.rerun()

                        if current_schema and current_schema in st.session_state.sf_metadata:
                            # Table selection with search capability
                            available_tables = st.session_state.sf_metadata[current_schema]

                            if available_tables:
                                # Add search functionality
                                table_search = st.text_input(
                                    "🔍 Search Tables (optional)",
                                    placeholder="Type to filter tables...",
                                    key="table_search",
                                )

                                # Filter tables based on search
                                if table_search:
                                    filtered_tables = [
                                        table
                                        for table in available_tables
                                        if table_search.lower() in table.lower()
                                    ]
                                    if filtered_tables:
                                        st.info(
                                            f"Found {len(filtered_tables)} tables matching '{table_search}'"
                                        )
                                        available_tables = filtered_tables
                                    else:
                                        st.warning(f"No tables found matching '{table_search}'")
                                        available_tables = []

                                if available_tables:
                                    current_table = st.selectbox(
                                        "Select Table", available_tables, key="sf_table_select"
                                    )

                                    if current_table:
                                        # Query type selection
                                        query_type = st.radio(
                                            "Query Type",
                                            ["Entire Table", "Custom Query"],
                                            key="query_type",
                                        )

                                        custom_query = None
                                        if query_type == "Custom Query":
                                            custom_query = st.text_area(
                                                "Custom SQL Query",
                                                placeholder=f"SELECT * FROM {current_db}.{current_schema}.{current_table} WHERE condition...",
                                                key="custom_query",
                                                help="💡 **Important:** Do NOT include LIMIT or OFFSET clauses - they will be added automatically for batch processing",
                                            )

                                        # Store Snowflake config
                                        st.session_state.sf_config = {
                                            "database": current_db,
                                            "schema": current_schema,
                                            "table": current_table,
                                            "query_type": query_type,
                                            "custom_query": custom_query,
                                        }

                                        # Display table info
                                        if query_type == "Entire Table":
                                            try:
                                                table_info = (
                                                    st.session_state.sf_connector.get_table_info(
                                                        current_db, current_schema, current_table
                                                    )
                                                )
                                                if (
                                                    table_info
                                                    and table_info.get("total_records", 0) > 0
                                                ):
                                                    st.info(
                                                        f"📊 Table contains {table_info['total_records']:,} records"
                                                    )
                                            except:
                                                pass
                            else:
                                if table_search:
                                    st.info("Try a different search term or clear the search box")
                    else:
                        st.warning("No schemas with tables found in this database")
                elif current_db and not st.session_state.sf_metadata:
                    st.info("Loading schemas and tables...")
            else:
                st.warning("No accessible databases found")
        elif st.session_state.sf_connector and not st.session_state.sf_databases:
            st.info("👆 Click 'Connect to Snowflake' to load available databases")
        elif not st.session_state.sf_connector:
            st.info("👆 Click 'Connect to Snowflake' to get started")

    # PostgreSQL Configuration Form
    with col2:
        st.markdown(
            '<div class="postgres-header"><h2>🐘 PostgreSQL Configuration</h2></div>',
            unsafe_allow_html=True,
        )

        # Configuration selection
        config_names = list(st.session_state.pg_configurations.keys())

        if config_names:
            # Move "+ Create New Configuration" to the end
            display_options = [
                f"{config_id} ({st.session_state.pg_configurations[config_id]['name']})"
                for config_id in config_names
            ] + ["+ Create New Configuration"]

            selected_option = st.selectbox(
                "Select Configuration", display_options, index=0, key="pg_config_select"
            )

            if selected_option == "+ Create New Configuration":
                # Show form to create new configuration
                with st.form("new_pg_config_form"):
                    st.markdown("#### Create New Configuration")

                    new_config_id = st.text_input(
                        "Configuration ID", placeholder="e.g., prod_server"
                    )
                    new_config_name = st.text_input(
                        "Configuration Name", placeholder="e.g., Production Server"
                    )

                    col_host, col_port = st.columns([3, 1])
                    with col_host:
                        new_host = st.text_input("Server Host", value="localhost")
                    with col_port:
                        new_port = st.number_input("Port", value=5432, min_value=1, max_value=65535)

                    col_user, col_pass = st.columns(2)
                    with col_user:
                        new_username = st.text_input("Username")
                    with col_pass:
                        new_password = st.text_input("Password", type="password")

                    new_database = st.text_input(
                        "Database (optional)", placeholder="Leave empty to select after connection"
                    )

                    if st.form_submit_button("Save Configuration"):
                        if all(
                            [new_config_id, new_config_name, new_host, new_username, new_password]
                        ):
                            if new_config_id not in st.session_state.pg_configurations:
                                new_config = {
                                    "name": new_config_name,
                                    "host": new_host,
                                    "port": int(new_port),
                                    "username": new_username,
                                    "password": new_password,
                                }
                                if new_database.strip():
                                    new_config["database"] = new_database.strip()

                                st.session_state.pg_configurations[new_config_id] = new_config
                                save_pg_configurations(st.session_state.pg_configurations)
                                st.success(f"Configuration '{new_config_name}' saved successfully!")
                                st.rerun()
                            else:
                                st.error("Configuration ID already exists!")
                        else:
                            st.error("Please fill in all required fields (except database)")

            else:
                # Extract config_id from selected option
                config_id = selected_option.split(" (")[0]
                selected_config = st.session_state.pg_configurations[config_id]

                # Display configuration details in single box
                config_details = f"""**Server:** {selected_config['host']}:{selected_config['port']}
**Username:** {selected_config['username']}
**Database:** {selected_config.get('database', '_Will be selected after connection_')}"""

                st.info(config_details)

                # Connect button
                if st.button("Connect", type="primary", key="pg_connect_btn"):
                    pg_connector = PostgreSQLConnector(
                        selected_config["host"],
                        selected_config["port"],
                        selected_config["username"],
                        selected_config["password"],
                        selected_config.get("database"),
                    )

                    if pg_connector.test_connection():
                        st.success("PostgreSQL connection successful!")
                        if pg_connector.connect():
                            st.session_state.pg_connector = pg_connector

                            # If no database specified, load available databases
                            if not selected_config.get("database"):
                                with st.spinner("Loading databases..."):
                                    st.session_state.pg_databases = pg_connector.get_databases()

                                if st.session_state.pg_databases:
                                    st.success(
                                        f"Found {len(st.session_state.pg_databases)} databases"
                                    )
                                else:
                                    st.warning("No databases found")

                            # Clear previous selections when reconnecting
                            if "pg_config" in st.session_state:
                                del st.session_state.pg_config
                            st.session_state.selected_pg_database = selected_config.get("database")
                    else:
                        st.error("PostgreSQL connection failed!")

                # Show database selection if connected without specific database
                if (
                    st.session_state.pg_connector
                    and not selected_config.get("database")
                    and st.session_state.pg_databases
                ):

                    # Database selection
                    current_pg_db = st.selectbox(
                        "Select Database", st.session_state.pg_databases, key="pg_db_select"
                    )

                    if current_pg_db != st.session_state.selected_pg_database:
                        # Switch to selected database
                        if st.session_state.pg_connector.switch_database(current_pg_db):
                            st.session_state.selected_pg_database = current_pg_db
                            st.success(f"Switched to database: {current_pg_db}")
                            # Clear previous selections
                            st.session_state.previous_pg_schema = None
                            if "pg_config" in st.session_state:
                                del st.session_state.pg_config
                            st.rerun()

        else:
            # Show form to create first configuration
            with st.form("first_pg_config_form"):
                st.markdown("**Create Your First PostgreSQL Configuration**")

                new_config_id = st.text_input(
                    "Configuration ID", placeholder="e.g., my_postgres", value="default"
                )
                new_config_name = st.text_input(
                    "Configuration Name", placeholder="e.g., My PostgreSQL Server"
                )

                col_host, col_port = st.columns([3, 1])
                with col_host:
                    new_host = st.text_input("Server Host", value="localhost")
                with col_port:
                    new_port = st.number_input("Port", value=5432, min_value=1, max_value=65535)

                col_user, col_pass = st.columns(2)
                with col_user:
                    new_username = st.text_input("Username")
                with col_pass:
                    new_password = st.text_input("Password", type="password")

                new_database = st.text_input(
                    "Database (optional)", placeholder="Leave empty to select after connection"
                )

                if st.form_submit_button("Save Configuration"):
                    if all([new_config_id, new_config_name, new_host, new_username, new_password]):
                        new_config = {
                            "name": new_config_name,
                            "host": new_host,
                            "port": int(new_port),
                            "username": new_username,
                            "password": new_password,
                        }
                        if new_database.strip():
                            new_config["database"] = new_database.strip()

                        st.session_state.pg_configurations[new_config_id] = new_config
                        save_pg_configurations(st.session_state.pg_configurations)
                        st.success(f"Configuration '{new_config_name}' saved successfully!")
                        st.rerun()
                    else:
                        st.error("Please fill in all required fields (except database)")

        # Show schema/table selection if connected to a database
        if st.session_state.pg_connector and (
            st.session_state.selected_pg_database or (st.session_state.pg_connector.database)
        ):

            # Schema selection
            pg_schemas = st.session_state.pg_connector.get_schemas()
            if pg_schemas:
                current_pg_schema = st.selectbox(
                    "Select Schema", pg_schemas, key="pg_schema_select"
                )

                # Check if schema changed
                if current_pg_schema != st.session_state.get("previous_pg_schema"):
                    st.session_state.previous_pg_schema = current_pg_schema
                    # Clear table selection
                    if "pg_table" in st.session_state:
                        del st.session_state["pg_table"]
                    if "pg_config" in st.session_state:
                        del st.session_state["pg_config"]
                    st.rerun()

                if current_pg_schema:
                    # Table selection/creation
                    pg_tables = st.session_state.pg_connector.get_tables(current_pg_schema)
                    table_option = st.radio(
                        "Table Option",
                        ["Create New Table", "Use Existing Table"],
                        key="table_option",
                    )

                    current_pg_table = None
                    if table_option == "Create New Table":
                        current_pg_table = st.text_input("New Table Name", key="pg_new_table")
                    else:
                        if pg_tables:
                            current_pg_table = st.selectbox(
                                "Select Existing Table", pg_tables, key="pg_existing_table"
                            )
                        else:
                            st.warning("No tables found in this schema")

                    if current_pg_table:
                        # Get current config details
                        current_config_option = st.session_state.get("pg_config_select", "")
                        if current_config_option != "+ Create New Configuration":
                            config_id = current_config_option.split(" (")[0]
                            config_details = st.session_state.pg_configurations[config_id]

                            # Store PostgreSQL config
                            st.session_state.pg_config = {
                                "host": config_details["host"],
                                "port": config_details["port"],
                                "username": config_details["username"],
                                "password": config_details["password"],
                                "database": st.session_state.selected_pg_database
                                or st.session_state.pg_connector.database,
                                "schema": current_pg_schema,
                                "table": current_pg_table,
                                "table_option": table_option,
                            }
            else:
                st.warning("No schemas found in this database")

    # Migration Section
    st.markdown("---")
    st.markdown(
        '<div class="migration-header"><h2>⚡ Data Migration</h2></div>', unsafe_allow_html=True
    )

    if hasattr(st.session_state, "sf_config") and hasattr(st.session_state, "pg_config"):
        if st.session_state.sf_connector and st.session_state.pg_connector:

            # Migration configuration
            col1, col2, col3 = st.columns(3)

            with col1:
                batch_size = st.number_input(
                    "Batch Size",
                    min_value=1000,
                    max_value=100000,
                    value=10000,
                    help="Number of records to process in each batch",
                )

            with col2:
                max_workers = st.number_input(
                    "Max Workers",
                    min_value=1,
                    max_value=10,
                    value=4,
                    help="Number of parallel workers for migration",
                )

            with col3:
                resume_migration = st.checkbox(
                    "Resume from Checkpoint",
                    value=True,  # Enable by default
                    help="Resume from last checkpoint if available",
                )

            # Check for existing checkpoint
            config_key = create_config_key(st.session_state.sf_config, st.session_state.pg_config)
            checkpoint = st.session_state.checkpoint_manager.get_checkpoint(config_key)

            if checkpoint and resume_migration:
                st.info(
                    f"Checkpoint found: {checkpoint['offset']:,} of {checkpoint['total_records']:,} records completed"
                )

            # Migration button with loading state
            if st.session_state.migration_in_progress:
                # Show disabled button with spinner during migration
                col_btn1, col_btn2, col_btn3 = st.columns([2, 2, 2])
                with col_btn1:
                    st.button("🔄 Migration in Progress...", disabled=True, type="primary")
                with col_btn2:
                    if st.button("🛑 Stop Migration", type="secondary"):
                        st.session_state.migration_in_progress = False
                        st.warning("⚠️ Migration stopped by user")
                        st.rerun()
                with col_btn3:
                    st.markdown("⏳ **Running...**")
            else:
                # Show normal button when not migrating
                if st.button("Start Migration", type="primary"):
                    # Set migration in progress
                    st.session_state.migration_in_progress = True

                    # Force UI update to show loading state
                    st.rerun()

            # Show migration status if in progress (below the buttons)
            if st.session_state.migration_in_progress:
                st.markdown("---")
                st.info(
                    "🔄 **Migration in Progress** - Please do not refresh the page or close the browser"
                )
                with st.container():
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        st.markdown("### ⏳ Processing your data migration...")
                        st.markdown("*This may take several minutes depending on data size*")

                # Show spinner with more details
                with st.spinner("🚀 Migrating data from Snowflake to PostgreSQL..."):
                    st.markdown(
                        "💡 **Tip:** You can stop the migration using the Stop button above"
                    )
                    # This will keep the spinner visible while migration is running
                    time.sleep(0.1)

            # Show persistent migration stats if available
            if st.session_state.last_migration_stats and st.session_state.migration_completed:
                st.markdown("---")
                st.markdown("### 📊 Last Migration Results")

                with st.container():
                    stats = st.session_state.last_migration_stats
                    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)

                    with col_stat1:
                        st.metric("Records Processed", f"{stats.get('total_records', 0):,}")
                    with col_stat2:
                        st.metric("Avg Speed", f"{stats.get('avg_speed', 0):.0f} rec/sec")
                    with col_stat3:
                        st.metric("Total Time", stats.get("total_time", "N/A"))
                    with col_stat4:
                        status_color = "🟢" if stats.get("status") == "success" else "🔴"
                        st.metric(
                            "Status", f"{status_color} {stats.get('status', 'unknown').title()}"
                        )

                    if stats.get("status") == "success":
                        st.success(
                            f"✅ Migration completed successfully! {stats.get('total_records', 0):,} records migrated."
                        )
                    elif stats.get("status") == "failed":
                        st.error(
                            f"❌ Migration failed: {stats.get('error_message', 'Unknown error')}"
                        )
                    elif stats.get("status") == "stopped":
                        st.warning(
                            f"⚠️ Migration was stopped by user after processing {stats.get('total_records', 0):,} records."
                        )

    # Execute migration if it was just started
    if (
        st.session_state.migration_in_progress
        and hasattr(st.session_state, "sf_config")
        and hasattr(st.session_state, "pg_config")
    ):

        try:
            migrate_data(
                st.session_state.sf_connector,
                st.session_state.pg_connector,
                st.session_state.sf_config,
                st.session_state.pg_config,
                batch_size,
                max_workers,
                resume_migration,
                st.session_state.checkpoint_manager,
                config_key,
            )
        except Exception as e:
            st.error(f"❌ Migration failed with error: {str(e)}")
        finally:
            # Always reset migration state when done (success or failure)
            st.session_state.migration_in_progress = False
            # Small delay before allowing next action
            time.sleep(0.5)
            st.rerun()


def migrate_data(
    sf_connector,
    pg_connector,
    sf_config,
    pg_config,
    batch_size,
    max_workers,
    resume_migration,
    checkpoint_manager,
    config_key,
):
    """Perform the actual data migration"""

    start_time = time.time()

    # Get table information from Snowflake
    table_info = sf_connector.get_table_info(
        sf_config["database"], sf_config["schema"], sf_config["table"]
    )

    if not table_info["columns"]:
        st.error("Failed to get table information from Snowflake")
        return

    # Prepare query
    if sf_config["query_type"] == "Custom Query":
        base_query = sf_config["custom_query"]
        if not base_query or not base_query.strip():
            st.error("❌ Custom query is empty. Please provide a valid SQL query.")
            return

        # Validate that custom query doesn't contain LIMIT/OFFSET
        query_upper = base_query.upper().strip()
        if " LIMIT " in query_upper or query_upper.endswith(" LIMIT"):
            st.error(
                "❌ Custom query should NOT contain LIMIT clause. It will be added automatically for batch processing."
            )
            return
        if " OFFSET " in query_upper or query_upper.endswith(" OFFSET"):
            st.error(
                "❌ Custom query should NOT contain OFFSET clause. It will be added automatically for batch processing."
            )
            return

        # Get total count for custom query
        try:
            count_query = f"SELECT COUNT(*) FROM ({base_query}) AS subquery"
            sf_connector.cursor.execute(count_query)
            total_records = sf_connector.cursor.fetchone()[0]
        except Exception as e:
            st.error(f"❌ **Error validating custom query:** {str(e)}")
            st.info("💡 **Suggestion:** Test your query in Snowflake first to ensure it's valid")
            return
    else:
        # Build query with primary key ordering for consistent pagination
        pk_columns = table_info["primary_keys"]
        if pk_columns:
            order_by = ", ".join(pk_columns)
            base_query = f"SELECT * FROM {sf_config['database']}.{sf_config['schema']}.{sf_config['table']} ORDER BY {order_by}"
        else:
            base_query = (
                f"SELECT * FROM {sf_config['database']}.{sf_config['schema']}.{sf_config['table']}"
            )
        total_records = table_info["total_records"]

    # Check for resume capability
    start_offset = 0
    if resume_migration:
        checkpoint = checkpoint_manager.get_checkpoint(config_key)
        if checkpoint:
            start_offset = checkpoint["offset"]

    # Create PostgreSQL table if needed
    if pg_config["table_option"] == "Create New Table":
        if not pg_connector.create_table_from_snowflake(
            pg_config["schema"],
            pg_config["table"],
            table_info["columns"],
            table_info["primary_keys"],
        ):
            st.error("Failed to create PostgreSQL table")
            return

    # Migration progress
    progress_bar = st.progress(start_offset / total_records if total_records > 0 else 0)
    status_text = st.empty()
    metrics_col1, metrics_col2, metrics_col3 = st.columns(3)

    with metrics_col1:
        records_metric = st.metric("Records Processed", start_offset)
    with metrics_col2:
        speed_metric = st.metric("Records/sec", 0)
    with metrics_col3:
        eta_metric = st.metric("ETA", "Calculating...")

    # Migration loop
    current_offset = start_offset
    records_processed = start_offset
    migration_failed = False
    error_message = None

    try:
        while current_offset < total_records:
            # Check if migration was stopped by user
            if not st.session_state.migration_in_progress:
                migration_failed = True
                error_message = "Migration stopped by user"

                # Calculate stats for stopped migration
                total_time = time.time() - start_time
                avg_speed = records_processed / total_time if total_time > 0 else 0

                # Save migration stats to session state
                st.session_state.last_migration_stats = {
                    "status": "stopped",
                    "total_records": records_processed,
                    "total_time": f"{total_time/60:.1f} min",
                    "avg_speed": avg_speed,
                    "batch_size": batch_size,
                    "error_message": error_message,
                }
                st.session_state.migration_completed = True

                st.warning("⚠️ Migration was stopped by user")
                break

            batch_start_time = time.time()

            # Fetch batch from Snowflake
            df = sf_connector.fetch_data_batch(base_query, batch_size, current_offset)

            if df.empty:
                # Check if this is the first batch - if so, it's likely a SQL error
                if current_offset == start_offset:
                    migration_failed = True
                    error_message = "No data retrieved from Snowflake - likely due to SQL error"
                    st.error("❌ **Data Retrieval Failed:** Unable to fetch data from Snowflake")
                    st.error("🔍 **Possible Issues:**")
                    st.info("• SQL syntax error in your query")
                    st.info("• No data matches your query conditions")
                    st.info("• Connection or permission issues")
                break

            # Insert into PostgreSQL
            if not pg_connector.bulk_insert_data(pg_config["schema"], pg_config["table"], df):
                migration_failed = True
                error_message = f"Failed to insert batch at offset {current_offset:,}"
                st.error(error_message)
                break

            # Update progress
            current_offset += len(df)
            records_processed += len(df)

            # Calculate metrics
            elapsed_time = time.time() - start_time
            records_per_second = records_processed / elapsed_time if elapsed_time > 0 else 0
            remaining_records = total_records - current_offset
            eta_seconds = remaining_records / records_per_second if records_per_second > 0 else 0

            # Update UI
            progress_bar.progress(current_offset / total_records)
            status_text.text(f"Processing batch: {current_offset:,} / {total_records:,} records")

            records_metric.metric("Records Processed", f"{records_processed:,}")
            speed_metric.metric("Records/sec", f"{records_per_second:.0f}")
            eta_metric.metric(
                "ETA",
                f"{eta_seconds/60:.1f} min" if eta_seconds < 3600 else f"{eta_seconds/3600:.1f} hr",
            )

            # Save checkpoint only if not failed
            if not migration_failed:
                checkpoint_manager.save_checkpoint(config_key, current_offset, total_records)

            # Small delay to prevent overwhelming the UI
            time.sleep(0.1)

    except Exception as e:
        migration_failed = True
        error_message = f"Migration failed with exception: {str(e)}"
        st.error(error_message)

    # Check if migration completed successfully or failed
    if migration_failed:
        # Calculate stats for failed migration
        total_time = time.time() - start_time
        avg_speed = records_processed / total_time if total_time > 0 else 0

        # Save migration stats to session state
        st.session_state.last_migration_stats = {
            "status": "failed",
            "total_records": records_processed,
            "total_time": f"{total_time/60:.1f} min",
            "avg_speed": avg_speed,
            "batch_size": batch_size,
            "error_message": error_message,
        }
        st.session_state.migration_completed = True

        # Migration failed - show error banner
        st.error(f"❌ Migration Failed!")
        if error_message:
            st.error(f"**Error Details:** {error_message}")

        # Show partial progress
        st.warning(
            f"⚠️ Partial migration completed: {records_processed:,} of {total_records:,} records processed"
        )

        # Save checkpoint for failed migration to allow resume
        checkpoint_manager.save_checkpoint(config_key, current_offset, total_records)
        st.info(
            "💾 Checkpoint saved. You can resume this migration later using 'Resume from Checkpoint' option."
        )

        # Show partial metrics
        st.subheader("Partial Migration Summary")
        col1, col2, col3, col4 = st.columns(4)

        total_time = time.time() - start_time
        avg_speed = records_processed / total_time if total_time > 0 else 0

        with col1:
            st.metric("Records Processed", f"{records_processed:,}")
        with col2:
            st.metric("Total Time", f"{total_time/60:.1f} min")
        with col3:
            st.metric("Average Speed", f"{avg_speed:.0f} rec/sec")
        with col4:
            completion_percentage = (
                (records_processed / total_records * 100) if total_records > 0 else 0
            )
            st.metric("Completion", f"{completion_percentage:.1f}%")

        return  # Exit function early for failed migration

    # Migration completed successfully
    total_time = time.time() - start_time
    avg_speed = records_processed / total_time if total_time > 0 else 0

    # Save migration stats to session state
    st.session_state.last_migration_stats = {
        "status": "success",
        "total_records": records_processed,
        "total_time": f"{total_time/60:.1f} min",
        "avg_speed": avg_speed,
        "batch_size": batch_size,
        "error_message": None,
    }
    st.session_state.migration_completed = True

    st.success("🎉 Migration completed successfully!")
    st.balloons()

    # Clear checkpoint on successful completion
    checkpoint_manager.clear_checkpoint(config_key)

    # Final metrics
    st.subheader("Migration Summary")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Records", f"{records_processed:,}")
    with col2:
        st.metric("Total Time", f"{total_time/60:.1f} min")
    with col3:
        st.metric("Average Speed", f"{avg_speed:.0f} rec/sec")
    with col4:
        st.metric("Batch Size", f"{batch_size:,}")


if __name__ == "__main__":
    main()
