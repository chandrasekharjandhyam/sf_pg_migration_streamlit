#!/usr/bin/env python3
"""
Deployment script for Snowflake to PostgreSQL Migration Tool
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def check_requirements():
    """Check if required packages are installed"""
    required_packages = [
        "streamlit",
        "snowflake-connector-python",
        "psycopg2-binary",
        "pandas",
        "sqlalchemy",
        "numpy",
    ]

    missing_packages = []

    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print(f"Missing required packages: {', '.join(missing_packages)}")
        print("Run: pip install -r requirements.txt")
        return False

    return True


def create_snowflake_deployment_files():
    """Create files needed for Snowflake deployment"""

    # Create Snowflake deployment manifest
    manifest_content = """
{
  "manifest_version": "1.0",
  "version": {
    "name": "1.0.0",
    "label": "Snowflake to PostgreSQL Migration Tool v1.0.0",
    "comment": "Data migration tool with checkpointing and performance monitoring"
  },
  "artifacts": {
    "setup_script": "setup.sql",
    "default_streamlit": "app.py",
    "extension_code": true
  },
  "configuration": {
    "log_level": "INFO",
    "query_warehouse": "COMPUTE_WH"
  }
}
"""

    with open("manifest.yml", "w") as f:
        f.write(manifest_content.strip())

    # Create Snowflake setup SQL
    setup_sql = """
-- Setup script for Snowflake deployment
-- This script will be run when the Streamlit app is deployed

-- Create necessary objects if they don't exist
CREATE SCHEMA IF NOT EXISTS MIGRATION_TOOLS;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA MIGRATION_TOOLS TO ROLE SYSADMIN;
GRANT CREATE TABLE ON SCHEMA MIGRATION_TOOLS TO ROLE SYSADMIN;

-- Set context
USE SCHEMA MIGRATION_TOOLS;

-- Create a table to store migration logs (optional)
CREATE TABLE IF NOT EXISTS MIGRATION_LOGS (
    log_id STRING,
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_config VARIANT,
    target_config VARIANT,
    status STRING,
    records_processed NUMBER,
    duration_seconds NUMBER,
    error_message STRING
);
"""

    with open("setup.sql", "w") as f:
        f.write(setup_sql.strip())

    print("✅ Snowflake deployment files created:")
    print("   - manifest.yml")
    print("   - setup.sql")


def validate_local_deployment():
    """Validate local deployment"""
    print("🔍 Validating local deployment...")

    # Check if all required files exist
    required_files = [
        "app.py",
        "requirements.txt",
        "config.py",
        "utils.py",
        ".streamlit/config.toml",
    ]

    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)

    if missing_files:
        print(f"❌ Missing required files: {', '.join(missing_files)}")
        return False

    # Check Python syntax
    try:
        import py_compile

        py_compile.compile("app.py", doraise=True)
        py_compile.compile("config.py", doraise=True)
        py_compile.compile("utils.py", doraise=True)
        print("✅ Python syntax validation passed")
    except py_compile.PyCompileError as e:
        print(f"❌ Python syntax error: {e}")
        return False

    return True


def run_local():
    """Run the application locally"""
    if not check_requirements():
        sys.exit(1)

    if not validate_local_deployment():
        sys.exit(1)

    print("🚀 Starting Streamlit application...")
    print("📱 Application will be available at: http://localhost:8501")
    print("🛑 Press Ctrl+C to stop the application")

    try:
        subprocess.run(["streamlit", "run", "app.py"], check=True)
    except KeyboardInterrupt:
        print("\n👋 Application stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running Streamlit: {e}")
        sys.exit(1)


def deploy_snowflake():
    """Prepare files for Snowflake deployment"""
    print("📦 Preparing Snowflake deployment...")

    if not validate_local_deployment():
        sys.exit(1)

    create_snowflake_deployment_files()

    print("\n📋 Snowflake Deployment Instructions:")
    print("1. Upload all Python files to a Snowflake stage:")
    print("   PUT file://app.py @your_stage;")
    print("   PUT file://config.py @your_stage;")
    print("   PUT file://utils.py @your_stage;")
    print("   PUT file://requirements.txt @your_stage;")
    print("   PUT file://manifest.yml @your_stage;")
    print("   PUT file://setup.sql @your_stage;")
    print()
    print("2. Create Streamlit app in Snowflake:")
    print("   CREATE STREAMLIT your_app_name")
    print("   ROOT_LOCATION = '@your_stage'")
    print("   MAIN_FILE = 'app.py'")
    print("   QUERY_WAREHOUSE = 'your_warehouse';")
    print()
    print("3. Grant necessary permissions:")
    print("   GRANT USAGE ON STREAMLIT your_app_name TO ROLE your_role;")


def main():
    parser = argparse.ArgumentParser(
        description="Deployment script for Snowflake to PostgreSQL Migration Tool"
    )
    parser.add_argument(
        "action", choices=["local", "snowflake", "validate"], help="Deployment action to perform"
    )

    args = parser.parse_args()

    if args.action == "local":
        run_local()
    elif args.action == "snowflake":
        deploy_snowflake()
    elif args.action == "validate":
        if validate_local_deployment() and check_requirements():
            print("✅ All validations passed!")
        else:
            print("❌ Validation failed!")
            sys.exit(1)


if __name__ == "__main__":
    main()
