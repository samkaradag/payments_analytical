# Databricks notebook source
# PostgreSQL to BigQuery Ingestion Pipeline
# This notebook provides a configurable template for ingesting data from PostgreSQL to BigQuery
# with support for incremental loads, data validation, and error handling.

# COMMAND ----------

# MAGIC %md
# MAGIC # PostgreSQL to BigQuery Ingestion Pipeline
# MAGIC 
# MAGIC This notebook provides a production-ready template for:
# MAGIC - Reading data from PostgreSQL tables
# MAGIC - Applying transformations and validations
# MAGIC - Writing data to BigQuery with incremental load support
# MAGIC - Comprehensive logging and error handling

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, md5, concat_ws, 
    when, coalesce, to_date, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import logging
from datetime import datetime, timedelta
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Section
# MAGIC 
# MAGIC Define all parameters here for easy customization

# COMMAND ----------

# Configuration Dictionary
CONFIG = {
    # PostgreSQL Connection Details
    "postgres": {
        "host": dbutils.widgets.get("postgres_host", "localhost"),
        "port": int(dbutils.widgets.get("postgres_port", "5432")),
        "database": dbutils.widgets.get("postgres_database", "analytics_db"),
        "user": dbutils.widgets.get("postgres_user", ""),
        "password": dbutils.widgets.get("postgres_password", ""),
        "ssl_mode": dbutils.widgets.get("postgres_ssl_mode", "require"),
    },
    
    # Source Table Configuration
    "source": {
        "schema": dbutils.widgets.get("source_schema", "public"),
        "table": dbutils.widgets.get("source_table", "transactions"),
        "incremental": dbutils.widgets.get("incremental_load", "false").lower() == "true",
        "incremental_column": dbutils.widgets.get("incremental_column", "updated_at"),
        "batch_size": int(dbutils.widgets.get("batch_size", "10000")),
    },
    
    # BigQuery Configuration
    "bigquery": {
        "project_id": dbutils.widgets.get("bq_project_id", ""),
        "dataset": dbutils.widgets.get("bq_dataset", "analytical_data"),
        "table": dbutils.widgets.get("bq_table", "transactions"),
        "write_mode": dbutils.widgets.get("bq_write_mode", "append"),  # append, overwrite, upsert
        "partition_field": dbutils.widgets.get("bq_partition_field", "ingestion_date"),
    },
    
    # Ingestion Configuration
    "ingestion": {
        "add_metadata": True,
        "add_hash_column": True,
        "validate_data": True,
        "parallel_partitions": int(dbutils.widgets.get("parallel_partitions", "4")),
    },
    
    # Logging
    "logging": {
        "level": dbutils.widgets.get("log_level", "INFO"),
    }
}

# COMMAND ----------

# Setup logging
logging.basicConfig(level=getattr(logging, CONFIG["logging"]["level"]))
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def create_postgres_connection_string(config):
    """Create PostgreSQL JDBC connection string"""
    pg_config = config["postgres"]
    return (
        f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/"
        f"{pg_config['database']}?sslmode={pg_config['ssl_mode']}"
    )

def log_ingestion_stats(df, table_name):
    """Log ingestion statistics"""
    row_count = df.count()
    logger.info(f"✓ Ingested {row_count:,} rows from {table_name}")
    logger.info(f"  Columns: {', '.join(df.columns)}")
    logger.info(f"  Schema: {df.schema}")
    return row_count

def add_metadata_columns(df, source_table):
    """Add metadata columns for tracking"""
    return df.select(
        "*",
        lit(source_table).alias("source_table"),
        lit(CONFIG["source"]["schema"]).alias("source_schema"),
        current_timestamp().alias("ingestion_timestamp"),
        lit(datetime.now().date()).alias("ingestion_date")
    )

def add_hash_column(df, hash_columns=None):
    """Add hash column for duplicate detection and change tracking"""
    if hash_columns is None:
        hash_columns = [col for col in df.columns if col not in 
                       ["ingestion_timestamp", "ingestion_date", "source_table", "source_schema"]]
    
    return df.withColumn(
        "record_hash",
        md5(concat_ws("||", *[col(c).cast("string") for c in hash_columns]))
    )

def validate_data(df, config):
    """Validate data quality"""
    logger.info("Starting data validation...")
    
    # Check for null values in key columns
    null_counts = df.select([
        (col(c).isNull().cast("int")).alias(f"{c}_nulls") 
        for c in df.columns
    ]).collect()[0].asDict()
    
    total_rows = df.count()
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            pct = (null_count / total_rows) * 100
            logger.warning(f"  Column {col_name}: {null_count} nulls ({pct:.2f}%)")
    
    logger.info("✓ Data validation complete")
    return True

def get_incremental_filter(spark, config):
    """Get filter condition for incremental load"""
    if not config["source"]["incremental"]:
        return None
    
    bq_table = f"{config['bigquery']['project_id']}.{config['bigquery']['dataset']}.{config['bigquery']['table']}"
    incremental_col = config["source"]["incremental_column"]
    
    try:
        # Get max timestamp from BigQuery
        max_ts_df = spark.read.format("bigquery").option("table", bq_table).load()
        max_ts = max_ts_df.agg({incremental_col: "max"}).collect()[0][0]
        logger.info(f"✓ Last incremental load: {max_ts}")
        return max_ts
    except Exception as e:
        logger.warning(f"Could not retrieve last incremental timestamp: {e}")
        logger.warning("Performing full load instead")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Ingestion Pipeline

# COMMAND ----------

def main():
    """Main ingestion pipeline"""
    try:
        logger.info("=" * 80)
        logger.info("Starting PostgreSQL to BigQuery Ingestion Pipeline")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 80)
        
        # Initialize Spark Session
        spark = SparkSession.builder.appName("postgres-to-bigquery-ingestion").getOrCreate()
        logger.info("✓ Spark session created")
        
        # STEP 1: Read from PostgreSQL
        logger.info("\n[STEP 1] Reading from PostgreSQL...")
        pg_url = create_postgres_connection_string(CONFIG)
        source_table = f"{CONFIG['source']['schema']}.{CONFIG['source']['table']}"
        
        read_options = {
            "url": pg_url,
            "dbtable": source_table,
            "user": CONFIG["postgres"]["user"],
            "password": CONFIG["postgres"]["password"],
            "numPartitions": CONFIG["ingestion"]["parallel_partitions"],
            "fetchsize": CONFIG["source"]["batch_size"],
        }
        
        df = spark.read.format("jdbc").options(**read_options).load()
        initial_count = log_ingestion_stats(df, source_table)
        
        # STEP 2: Apply Incremental Filter (if enabled)
        if CONFIG["source"]["incremental"]:
            logger.info("\n[STEP 2] Applying incremental filter...")
            max_ts = get_incremental_filter(spark, CONFIG)
            if max_ts:
                df = df.filter(col(CONFIG["source"]["incremental_column"]) > max_ts)
                incremental_count = df.count()
                logger.info(f"✓ Incremental load: {incremental_count:,} new/updated rows")
            else:
                logger.info("✓ Performing full load")
        else:
            logger.info("\n[STEP 2] Skipping incremental filter (full load mode)")
        
        # STEP 3: Add Metadata Columns
        logger.info("\n[STEP 3] Adding metadata columns...")
        df = add_metadata_columns(df, source_table)
        logger.info("✓ Metadata columns added")
        
        # STEP 4: Add Hash Column for Change Detection
        if CONFIG["ingestion"]["add_hash_column"]:
            logger.info("\n[STEP 4] Adding hash column...")
            df = add_hash_column(df)
            logger.info("✓ Hash column added for change tracking")
        else:
            logger.info("\n[STEP 4] Skipping hash column")
        
        # STEP 5: Data Validation
        if CONFIG["ingestion"]["validate_data"]:
            logger.info("\n[STEP 5] Validating data...")
            validate_data(df, CONFIG)
        else:
            logger.info("\n[STEP 5] Skipping data validation")
        
        # STEP 6: Write to BigQuery
        logger.info("\n[STEP 6] Writing to BigQuery...")
        bq_table = f"{CONFIG['bigquery']['project_id']}.{CONFIG['bigquery']['dataset']}.{CONFIG['bigquery']['table']}"
        
        write_options = {
            "table": bq_table,
            "writeMethod": "direct",
            "partitionField": CONFIG["bigquery"]["partition_field"],
        }
        
        df.write.format("bigquery").mode(CONFIG["bigquery"]["write_mode"]).options(**write_options).save()
        logger.info(f"✓ Successfully wrote data to {bq_table}")
        
        # STEP 7: Summary
        logger.info("\n[STEP 7] Ingestion Summary")
        logger.info("=" * 80)
        logger.info(f"Source: {source_table}")
        logger.info(f"Target: {bq_table}")
        logger.info(f"Initial rows read: {initial_count:,}")
        logger.info(f"Rows written: {df.count():,}")
        logger.info(f"Write mode: {CONFIG['bigquery']['write_mode']}")
        logger.info(f"Completion time: {datetime.now().isoformat()}")
        logger.info("=" * 80)
        logger.info("✓ Pipeline completed successfully!")
        
        return True
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"✗ Pipeline failed with error: {str(e)}")
        logger.error("=" * 80)
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline

# COMMAND ----------

if __name__ == "__main__":
    success = main()
    dbutils.notebook.exit(json.dumps({"status": "success" if success else "failed"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Reference
# MAGIC 
# MAGIC ### PostgreSQL Parameters
# MAGIC - `postgres_host`: PostgreSQL server hostname
# MAGIC - `postgres_port`: PostgreSQL port (default: 5432)
# MAGIC - `postgres_database`: Database name
# MAGIC - `postgres_user`: Database user
# MAGIC - `postgres_password`: Database password
# MAGIC - `postgres_ssl_mode`: SSL mode (require/disable/allow)
# MAGIC 
# MAGIC ### Source Table Parameters
# MAGIC - `source_schema`: Schema name (default: public)
# MAGIC - `source_table`: Table name
# MAGIC - `incremental_load`: Enable incremental loading (true/false)
# MAGIC - `incremental_column`: Column to track incremental changes
# MAGIC - `batch_size`: JDBC batch size (default: 10000)
# MAGIC - `parallel_partitions`: Number of parallel partitions (default: 4)
# MAGIC 
# MAGIC ### BigQuery Parameters
# MAGIC - `bq_project_id`: Google Cloud project ID
# MAGIC - `bq_dataset`: BigQuery dataset name
# MAGIC - `bq_table`: BigQuery table name
# MAGIC - `bq_write_mode`: Write mode (append/overwrite/upsert)
# MAGIC - `bq_partition_field`: Partition field name
# MAGIC 
# MAGIC ### Logging
# MAGIC - `log_level`: Logging level (DEBUG/INFO/WARNING/ERROR)