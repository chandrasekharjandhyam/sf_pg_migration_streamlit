# Quick Start Guide

## Snowflake to PostgreSQL Migration Tool

### Prerequisites
- Python 3.8 or higher
- Access to Snowflake account
- PostgreSQL database for target

### Installation

1. **Clone and setup the project:**
```powershell
git clone <repository-url>
cd sf_pg_migration_streamlit
pip install -r requirements.txt
```

2. **Run the application:**
```powershell
streamlit run app.py
```

Or use the deployment script:
```powershell
python deploy.py local
```

### Using the Application

#### Step 1: Snowflake Configuration
1. Click "Connect to Snowflake" (automatically uses session when deployed in Snowflake)
2. Select Database, Schema, and Table
3. Choose "Entire Table" or "Custom Query"

#### Step 2: PostgreSQL Configuration
1. Enter connection details (host, port, username, password, database)
2. Click "Test PostgreSQL Connection"
3. Select target schema and table (create new or use existing)

#### Step 3: Migration Settings
1. Set batch size (default: 10,000 for optimal performance)
2. Configure max workers (default: 4)
3. Enable "Resume from Checkpoint" if continuing a previous migration

#### Step 4: Start Migration
1. Click "Start Migration"
2. Monitor real-time progress with metrics:
   - Records processed
   - Processing speed
   - Estimated time remaining

### Performance Tips

#### For Large Datasets (10M+ records):
- Use batch sizes between 10,000-50,000
- Ensure stable network connection
- Run during off-peak hours
- Monitor PostgreSQL performance

#### Optimal Settings by Data Size:
- **< 100K records**: Batch size 1,000
- **100K - 1M records**: Batch size 10,000
- **1M - 10M records**: Batch size 50,000
- **> 10M records**: Batch size 100,000

### Checkpoint System

The application automatically saves progress every batch:
- Resumes from last successful batch on interruption
- Stores offsets in `migration_checkpoints.json`
- Maintains data consistency with primary key ordering

### Troubleshooting

#### Connection Issues:
1. Verify network connectivity
2. Check firewall settings
3. Validate credentials
4. Ensure proper permissions

#### Performance Issues:
1. Reduce batch size if memory errors occur
2. Increase batch size if transfer is slow
3. Check network latency
4. Monitor database resource usage

#### Data Issues:
1. Verify column compatibility
2. Check for unsupported data types
3. Handle special characters in data
4. Review null value handling

### Deployment in Snowflake

For production deployment:
```powershell
python deploy.py snowflake
```

Follow the generated instructions to deploy in your Snowflake account.

### Testing

Run the test suite:
```powershell
python test_migration_tool.py
```

### Support

- Check logs for detailed error information
- Review the README.md for comprehensive documentation
- Monitor real-time metrics during migration
- Use checkpoint system for large migrations
