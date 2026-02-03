# Databricks notebook source
# PostgreSQL to BigQuery Ingestion - Usage Examples
# This notebook demonstrates various ways to run the ingestion pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC # PostgreSQL to BigQuery Ingestion - Usage Examples
# MAGIC 
# MAGIC This notebook shows practical examples of running the ingestion pipeline with different configurations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Full Load (Initial Load)
# MAGIC 
# MAGIC Load all data from PostgreSQL table to BigQuery. Use this for the first run or when you need to reload all data.

# COMMAND ----------

# EXAMPLE 1: Full Load
%run "../notebooks/postgres_to_bigquery_ingestion" $\
  postgres_host="postgres.example.com" $\
  postgres_port="5432" $\
  postgres_database="analytics_db" $\
  postgres_user="postgres_user" $\
  postgres_password="secure_password" $\
  source_schema="public" $\
  source_table="transactions" $\
  bq_project_id="my-gcp-project" $\
  bq_dataset="analytical_data" $\
  bq_table="transactions" $\
  incremental_load="false" $\
  bq_write_mode="overwrite"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Incremental Load (Daily Sync)
# MAGIC 
# MAGIC Only load new and updated records based on a timestamp column. Ideal for daily scheduled jobs.

# COMMAND ----------

# EXAMPLE 2: Incremental Load
%run "../notebooks/postgres_to_bigquery_ingestion" $\
  postgres_host="postgres.example.com" $\
  postgres_port="5432" $\
  postgres_database="analytics_db" $\
  postgres_user="postgres_user" $\
  postgres_password="secure_password" $\
  source_schema="public" $\
  source_table="transactions" $\
  incremental_load="true" $\
  incremental_column="updated_at" $\
  bq_project_id="my-gcp-project" $\
  bq_dataset="analytical_data" $\
  bq_table="transactions" $\
  bq_write_mode="append" $\
  parallel_partitions="8"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Customer Dimension Table (Upsert)
# MAGIC 
# MAGIC Load customer data with upsert mode to handle updates to existing records.

# COMMAND ----------

# EXAMPLE 3: Customer Dimension (Upsert)
%run "../notebooks/postgres_to_bigquery_ingestion" $\
  postgres_host="postgres.example.com" $\
  postgres_database="analytics_db" $\
  postgres_user="postgres_user" $\
  postgres_password="secure_password" $\
  source_schema="public" $\
  source_table="customers" $\
  incremental_load="true" $\
  incremental_column="modified_date" $\
  bq_project_id="my-gcp-project" $\
  bq_dataset="analytical_data" $\
  bq_table="dim_customers" $\
  bq_write_mode="upsert" $\
  parallel_partitions="4"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Large Table with Optimized Settings
# MAGIC 
# MAGIC Load a large table with increased parallelism and batch size for better performance.

# COMMAND ----------

# EXAMPLE 4: Large Table Optimization
%run "../notebooks/postgres_to_bigquery_ingestion" $\
  postgres_host="postgres.example.com" $\
  postgres_database="analytics_db" $\
  postgres_user="postgres_user" $\
  postgres_password="secure_password" $\
  source_schema="public" $\
  source_table="events" $\
  incremental_load="false" $\
  batch_size="50000" $\
  parallel_partitions="16" $\
  bq_project_id="my-gcp-project" $\
  bq_dataset="analytical_data" $\
  bq_table="fact_events" $\
  bq_write_mode="overwrite"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: Using Databricks Secrets
# MAGIC 
# MAGIC Secure approach using Databricks Secret Scope for credentials (recommended for production).

# COMMAND ----------

# EXAMPLE 5: Using Secrets (Production-Ready)
postgres_password = dbutils.secrets.get(scope="postgres", key="password")
postgres_user = dbutils.secrets.get(scope="postgres", key="username")
bq_credentials = dbutils.secrets.get(scope="bigquery", key="project_id")

%run "../notebooks/postgres_to_bigquery_ingestion" $\
  postgres_host="postgres.example.com" $\
  postgres_database="analytics_db" $\
  postgres_user=postgres_user $\
  postgres_password=postgres_password $\
  source_schema="public" $\
  source_table="transactions" $\
  incremental_load="true" $\
  incremental_column="updated_at" $\
  bq_project_id=bq_credentials $\
  bq_dataset="analytical_data" $\
  bq_table="transactions" $\
  bq_write_mode="append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Multiple Tables in Sequence
# MAGIC 
# MAGIC Load multiple tables in a single job by calling the notebook multiple times.

# COMMAND ----------

# EXAMPLE 6: Multiple Tables
tables = [
    {"source": "transactions", "target": "fact_transactions"},
    {"source": "customers", "target": "dim_customers"},
    {"source": "products", "target": "dim_products"},
    {"source": "orders", "target": "fact_orders"},
]

for table_config in tables:
    print(f"Loading {table_config['source']} -> {table_config['target']}")
    
    %run "../notebooks/postgres_to_bigquery_ingestion" $\
      postgres_host="postgres.example.com" $\
      postgres_database="analytics_db" $\
      postgres_user="postgres_user" $\
      postgres_password="secure_password" $\
      source_schema="public" $\
      source_table=table_config["source"] $\
      incremental_load="true" $\
      incremental_column="updated_at" $\
      bq_project_id="my-gcp-project" $\
      bq_dataset="analytical_data" $\
      bq_table=table_config["target"] $\
      bq_write_mode="append"
    
    print(f"✓ Completed {table_config['source']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 7: Environment-Specific Configuration
# MAGIC 
# MAGIC Use different settings for dev, staging, and production environments.

# COMMAND ----------

# EXAMPLE 7: Environment-Specific Config
import os

# Determine environment
environment = dbutils.widgets.get("environment", "dev")

# Environment-specific settings
env_config = {
    "dev": {
        "postgres_host": "postgres-dev.example.com",
        "bq_dataset": "analytical_data_dev",
        "parallel_partitions": "4",
    },
    "staging": {
        "postgres_host": "postgres-staging.example.com",
        "bq_dataset": "analytical_data_staging",
        "parallel_partitions": "8",
    },
    "prod": {
        "postgres_host": "postgres-prod.example.com",
        "bq_dataset": "analytical_data_prod",
        "parallel_partitions": "16",
    }
}

config = env_config[environment]

%run "../notebooks/postgres_to_bigquery_ingestion" $\
  postgres_host=config["postgres_host"] $\
  postgres_database="analytics_db" $\
  postgres_user="postgres_user" $\
  postgres_password="secure_password" $\
  source_schema="public" $\
  source_table="transactions" $\
  incremental_load="true" $\
  incremental_column="updated_at" $\
  bq_project_id="my-gcp-project" $\
  bq_dataset=config["bq_dataset"] $\
  bq_table="transactions" $\
  bq_write_mode="append" $\
  parallel_partitions=config["parallel_partitions"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 8: Error Handling and Retry Logic
# MAGIC 
# MAGIC Implement retry logic for failed ingestions.

# COMMAND ----------

# EXAMPLE 8: Error Handling with Retry
import time

def run_ingestion_with_retry(max_retries=3, backoff_seconds=60):
    """Run ingestion with retry logic"""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempt {attempt}/{max_retries}")
            
            %run "../notebooks/postgres_to_bigquery_ingestion" $\
              postgres_host="postgres.example.com" $\
              postgres_database="analytics_db" $\
              postgres_user="postgres_user" $\
              postgres_password="secure_password" $\
              source_schema="public" $\
              source_table="transactions" $\
              incremental_load="true" $\
              incremental_column="updated_at" $\
              bq_project_id="my-gcp-project" $\
              bq_dataset="analytical_data" $\
              bq_table="transactions" $\
              bq_write_mode="append"
            
            print("✓ Ingestion successful!")
            return True
            
        except Exception as e:
            print(f"✗ Attempt {attempt} failed: {str(e)}")
            
            if attempt < max_retries:
                print(f"Retrying in {backoff_seconds} seconds...")
                time.sleep(backoff_seconds)
                backoff_seconds *= 2  # Exponential backoff
            else:
                print("All retries exhausted. Failing.")
                raise

# Run with retry logic
run_ingestion_with_retry(max_retries=3, backoff_seconds=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 9: Conditional Load Based on Data Changes
# MAGIC 
# MAGIC Only run ingestion if there are actually changes in the source system.

# COMMAND ----------

# EXAMPLE 9: Conditional Load
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-changes").getOrCreate()

# Check if there are any new records
jdbc_url = "jdbc:postgresql://postgres.example.com:5432/analytics_db"
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "public.transactions") \
    .option("user", "postgres_user") \
    .option("password", "secure_password") \
    .load()

new_records = df.filter(col("updated_at") > (current_timestamp() - expr("interval 1 hour"))).count()

print(f"Found {new_records} records updated in the last hour")

if new_records > 0:
    print("Running ingestion...")
    %run "../notebooks/postgres_to_bigquery_ingestion" $\
      postgres_host="postgres.example.com" $\
      postgres_database="analytics_db" $\
      postgres_user="postgres_user" $\
      postgres_password="secure_password" $\
      source_schema="public" $\
      source_table="transactions" $\
      incremental_load="true" $\
      incremental_column="updated_at" $\
      bq_project_id="my-gcp-project" $\
      bq_dataset="analytical_data" $\
      bq_table="transactions" $\
      bq_write_mode="append"
else:
    print("No changes detected. Skipping ingestion.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC 
# MAGIC 1. **Always use incremental loads** for daily/hourly jobs to save time and cost
# MAGIC 2. **Store credentials in Databricks Secrets** never hardcode passwords
# MAGIC 3. **Use retry logic** for production jobs to handle transient failures
# MAGIC 4. **Monitor ingestion logs** for data quality issues
# MAGIC 5. **Test in dev environment** before running in production
# MAGIC 6. **Schedule jobs** using Databricks Jobs API for automation
# MAGIC 7. **Set up alerts** for failed ingestions
# MAGIC 8. **Validate data** after ingestion to catch issues early