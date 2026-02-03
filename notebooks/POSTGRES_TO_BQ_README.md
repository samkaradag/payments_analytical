# PostgreSQL to BigQuery Ingestion Pipeline

## Overview

This notebook provides a **production-ready template** for ingesting data from PostgreSQL to BigQuery with support for:

✅ **Incremental Loading** - Only sync new/changed records  
✅ **Data Validation** - Quality checks and null detection  
✅ **Change Tracking** - Hash-based duplicate detection  
✅ **Metadata Tracking** - Source and ingestion timestamps  
✅ **Error Handling** - Comprehensive logging and error reporting  
✅ **Flexible Configuration** - Widget-based parameter passing  
✅ **Parallel Processing** - Configurable partition count  

---

## Quick Start

### 1. **Basic Configuration (Widget Parameters)**

When running the notebook, provide these parameters:

```
postgres_host = "your-postgres-host"
postgres_port = "5432"
postgres_database = "your_database"
postgres_user = "your_user"
postgres_password = "your_password"
postgres_ssl_mode = "require"

source_schema = "public"
source_table = "transactions"

bq_project_id = "your-gcp-project"
bq_dataset = "analytical_data"
bq_table = "transactions"

bq_write_mode = "append"  # or "overwrite"
incremental_load = "true"
incremental_column = "updated_at"
```

### 2. **Using Databricks Secrets**

For security, store credentials in Databricks Secret Scope:

```python
postgres_password = dbutils.secrets.get(scope="postgres", key="password")
bq_credentials = dbutils.secrets.get(scope="bigquery", key="service_account")
```

### 3. **Run the Notebook**

```bash
# Full load
%run "./postgres_to_bigquery_ingestion" $postgres_host="localhost" $source_table="transactions"

# Incremental load
%run "./postgres_to_bigquery_ingestion" $incremental_load="true" $incremental_column="updated_at"
```

---

## Configuration Options

### PostgreSQL Connection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `postgres_host` | localhost | PostgreSQL server hostname |
| `postgres_port` | 5432 | PostgreSQL port |
| `postgres_database` | analytics_db | Database name |
| `postgres_user` | - | Database username |
| `postgres_password` | - | Database password |
| `postgres_ssl_mode` | require | SSL mode (require/disable/allow) |

### Source Table

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_schema` | public | Schema name |
| `source_table` | transactions | Table name |
| `incremental_load` | false | Enable incremental loading |
| `incremental_column` | updated_at | Column for incremental tracking |
| `batch_size` | 10000 | JDBC batch size |
| `parallel_partitions` | 4 | Parallel read partitions |

### BigQuery Target

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bq_project_id` | - | GCP project ID |
| `bq_dataset` | analytical_data | Dataset name |
| `bq_table` | transactions | Table name |
| `bq_write_mode` | append | Write mode (append/overwrite/upsert) |
| `bq_partition_field` | ingestion_date | Partition field |

### Processing Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `add_metadata` | true | Add source/ingestion metadata |
| `add_hash_column` | true | Add record hash for change detection |
| `validate_data` | true | Run data quality validation |
| `log_level` | INFO | Logging level (DEBUG/INFO/WARNING/ERROR) |

---

## Pipeline Steps

### Step 1: Read from PostgreSQL
- Connects to PostgreSQL using JDBC driver
- Uses parallel partitions for faster reads
- Applies batch size for memory efficiency

### Step 2: Apply Incremental Filter (Optional)
- Queries BigQuery for max timestamp
- Filters source data to only new/updated records
- Falls back to full load if incremental data unavailable

### Step 3: Add Metadata Columns
- `source_table` - Source table name
- `source_schema` - Source schema name
- `ingestion_timestamp` - Current timestamp
- `ingestion_date` - Current date (for partitioning)

### Step 4: Add Hash Column
- Creates MD5 hash of record content
- Useful for duplicate detection
- Can be used for change data capture (CDC)

### Step 5: Data Validation
- Checks for null values in all columns
- Reports null percentages
- Logs warnings for data quality issues

### Step 6: Write to BigQuery
- Writes data using BigQuery Spark connector
- Supports append, overwrite, and upsert modes
- Partitions data by specified field

### Step 7: Summary Report
- Logs total rows read and written
- Confirms successful completion
- Provides execution timestamps

---

## Advanced Usage

### Incremental Load with Change Detection

```python
# First run: Full load
incremental_load = "false"

# Subsequent runs: Only new/changed records
incremental_load = "true"
incremental_column = "updated_at"
```

### Upsert Mode (Update Existing Records)

```python
bq_write_mode = "upsert"
# Requires primary key definition in BigQuery table
```

### Custom Data Validation

Modify the `validate_data()` function to add custom rules:

```python
def validate_data(df, config):
    # Check for negative amounts
    negative_amounts = df.filter(col("amount") < 0).count()
    if negative_amounts > 0:
        logger.warning(f"Found {negative_amounts} negative amounts")
    
    # Check date ranges
    future_dates = df.filter(col("created_at") > current_timestamp()).count()
    if future_dates > 0:
        logger.warning(f"Found {future_dates} future dates")
```

### Parallel Processing Tuning

```python
# For large tables, increase partitions
parallel_partitions = "16"  # More parallelism

# For small tables, reduce partitions
parallel_partitions = "2"   # Less overhead
```

---

## Troubleshooting

### Error: "Cannot connect to PostgreSQL"
- Verify hostname, port, and database name
- Check firewall rules allow Databricks cluster access
- Test credentials in psql: `psql -h host -U user -d database`

### Error: "Table not found in BigQuery"
- Verify project ID and dataset exist
- Check table name spelling
- Ensure service account has write permissions

### Slow Performance
- Increase `parallel_partitions` to 8-16
- Reduce `batch_size` for memory-constrained clusters
- Check PostgreSQL indexes on `incremental_column`
- Monitor Spark UI for shuffle operations

### Data Quality Issues
- Review null counts in validation step
- Check for data type mismatches
- Validate source data in PostgreSQL
- Add custom validation rules as needed

---

## Monitoring & Logging

### Log Levels

```
DEBUG   - Detailed execution information
INFO    - General progress updates (recommended)
WARNING - Data quality issues, missing data
ERROR   - Pipeline failures
```

### Key Metrics to Monitor

- **Rows Read**: Total records from PostgreSQL
- **Rows Written**: Records successfully written to BigQuery
- **Null Counts**: Data quality indicator
- **Execution Time**: Performance tracking
- **Hash Distribution**: Change detection effectiveness

### Example Log Output

```
================================================================================
Starting PostgreSQL to BigQuery Ingestion Pipeline
Timestamp: 2024-01-15T10:30:45.123456
================================================================================

[STEP 1] Reading from PostgreSQL...
✓ Ingested 150,000 rows from public.transactions
  Columns: id, amount, created_at, updated_at, ...
  
[STEP 2] Applying incremental filter...
✓ Last incremental load: 2024-01-15 08:00:00
✓ Incremental load: 5,432 new/updated rows

[STEP 3] Adding metadata columns...
✓ Metadata columns added

[STEP 4] Adding hash column...
✓ Hash column added for change tracking

[STEP 5] Validating data...
  Column amount: 0 nulls (0.00%)
✓ Data validation complete

[STEP 6] Writing to BigQuery...
✓ Successfully wrote data to my-project.analytical_data.transactions

[STEP 7] Ingestion Summary
================================================================================
Source: public.transactions
Target: my-project.analytical_data.transactions
Initial rows read: 150,000
Rows written: 5,432
Write mode: append
Completion time: 2024-01-15T10:35:12.654321
================================================================================
✓ Pipeline completed successfully!
```

---

## Best Practices

### 1. **Use Incremental Loads**
- Reduces data transfer and processing time
- Minimizes BigQuery costs
- Enables real-time data freshness

### 2. **Enable Data Validation**
- Catches data quality issues early
- Prevents bad data in BigQuery
- Provides audit trail

### 3. **Add Metadata Columns**
- Enables data lineage tracking
- Supports data governance
- Helps with troubleshooting

### 4. **Use Appropriate Write Modes**
- `append`: Fast, for new data only
- `overwrite`: Use sparingly, full table replacement
- `upsert`: For dimension tables with updates

### 5. **Monitor Execution**
- Check logs for warnings
- Monitor null counts
- Track execution time trends

### 6. **Secure Credentials**
- Never hardcode passwords
- Use Databricks Secret Scopes
- Rotate credentials regularly

### 7. **Optimize for Your Data**
- Tune `parallel_partitions` based on table size
- Adjust `batch_size` for memory availability
- Index `incremental_column` in PostgreSQL

---

## Example Configurations

### Real-time Transactions Table

```
source_table = "transactions"
incremental_load = "true"
incremental_column = "updated_at"
parallel_partitions = "8"
bq_write_mode = "append"
```

### Customer Dimension Table

```
source_table = "customers"
incremental_load = "true"
incremental_column = "modified_date"
parallel_partitions = "4"
bq_write_mode = "upsert"
add_hash_column = "true"
```

### Large Historical Load

```
source_table = "events"
incremental_load = "false"
parallel_partitions = "16"
batch_size = "50000"
bq_write_mode = "overwrite"
```

---

## Support & Maintenance

- **Issues**: Check Spark logs in Databricks UI
- **Performance**: Profile with Spark UI
- **Updates**: Keep PostgreSQL JDBC driver current
- **Testing**: Run on dev/staging first

---

**Last Updated**: 2024-01-15  
**Version**: 1.0  
**Compatibility**: Databricks Runtime 10.4+, Spark 3.2+