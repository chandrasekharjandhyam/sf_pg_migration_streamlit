# Snowflake to PostgreSQL Migration Tool

A comprehensive Streamlit application for migrating data from Snowflake to PostgreSQL with advanced features like checkpointing, bulk transfers, and progress tracking.

## Features

- 🏂 **Snowflake Integration**: Direct deployment in Snowflake account
- 🐘 **PostgreSQL Support**: Connect to any PostgreSQL database
- 📊 **Flexible Data Selection**: Choose entire tables or custom queries
- 🚀 **High Performance**: Optimized for large datasets (10M+ records)
- 📍 **Checkpoint System**: Resume migrations from interruption points
- 🔄 **Batch Processing**: Configurable batch sizes for optimal performance
- 📈 **Real-time Progress**: Live metrics and ETA calculations
- 🛡️ **Data Integrity**: Maintains column order and handles null values
- 🔧 **Easy Configuration**: Intuitive forms for both source and target

## Architecture

### Key Components

1. **SnowflakeConnector**: Handles Snowflake database operations
2. **PostgreSQLConnector**: Manages PostgreSQL connections and operations
3. **MigrationCheckpoint**: Implements checkpoint mechanism for resumable migrations
4. **Main Application**: Streamlit UI with dual-form configuration

### Migration Process

1. **Source Configuration**: Select Snowflake database, schema, and table
2. **Target Configuration**: Configure PostgreSQL connection and destination
3. **Migration Execution**: Batch processing with real-time monitoring
4. **Checkpoint Management**: Automatic saving of progress for resumability

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd sf_pg_migration_streamlit
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
streamlit run app.py
```

## Deployment in Snowflake

For deployment within Snowflake account:

1. Upload the application files to a Snowflake stage
2. Create a Streamlit app using the uploaded files
3. The application will automatically use Snowflake session for authentication

## Configuration

### Snowflake Configuration
- Database, Schema, and Table selection
- Option for entire table or custom SQL queries
- Automatic primary key detection for consistent ordering

### PostgreSQL Configuration
- Connection details (host, port, username, password, database)
- Schema and table selection
- Option to create new tables or use existing ones

### Migration Settings
- **Batch Size**: Number of records per batch (default: 10,000)
- **Max Workers**: Parallel processing capability (default: 4)
- **Resume Option**: Continue from last checkpoint

## Performance Optimizations

### For Large Datasets (10M+ records):

1. **Optimal Batch Size**: 10,000-50,000 records per batch
2. **Primary Key Ordering**: Ensures consistent pagination
3. **COPY Command**: Uses PostgreSQL's fastest bulk insert method
4. **Binary Mode**: Efficient data transfer format
5. **Null Handling**: Proper handling of null values in numeric columns

### Best Practices:

- Use appropriate batch sizes based on available memory
- Monitor network latency between Snowflake and PostgreSQL
- Consider using connection pooling for high-volume migrations
- Schedule migrations during off-peak hours

## Checkpoint System

The application maintains migration state in `migration_checkpoints.json`:

```json
{
  "config_key": {
    "offset": 50000,
    "total_records": 1000000,
    "last_updated": "2025-01-19T10:30:00"
  }
}
```

## Error Handling

- Connection validation for both Snowflake and PostgreSQL
- Batch-level error recovery
- Automatic rollback on insertion failures
- Detailed error logging and user feedback

## Monitoring and Metrics

Real-time tracking includes:
- Records processed count
- Processing speed (records/second)
- Estimated time to completion (ETA)
- Progress percentage
- Average migration speed

## Data Type Mapping

| Snowflake Type | PostgreSQL Type |
|----------------|----------------|
| VARCHAR        | TEXT           |
| NUMBER         | NUMERIC        |
| FLOAT          | DOUBLE PRECISION |
| BOOLEAN        | BOOLEAN        |
| DATE           | DATE           |
| TIMESTAMP_NTZ  | TIMESTAMP      |
| TIMESTAMP_LTZ  | TIMESTAMPTZ    |
| TIMESTAMP_TZ   | TIMESTAMPTZ    |
| VARIANT        | JSONB          |
| ARRAY          | JSONB          |
| OBJECT         | JSONB          |

## Security Considerations

- Passwords are handled securely using Streamlit's password input
- No credentials are stored in checkpoint files
- Connection strings are not logged
- Use environment variables for Snowflake authentication in production

## Troubleshooting

### Common Issues:

1. **Connection Timeouts**: Increase batch size or reduce parallel workers
2. **Memory Issues**: Decrease batch size
3. **Permission Errors**: Verify database permissions for both source and target
4. **Data Type Errors**: Check column compatibility between Snowflake and PostgreSQL

### Performance Tuning:

- Monitor system resources during migration
- Adjust batch size based on network latency
- Use appropriate PostgreSQL configuration for bulk inserts
- Consider temporarily disabling indexes during large migrations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Check the troubleshooting section
- Review application logs
- Submit issues with detailed error messages and configuration details