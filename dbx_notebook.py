# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Aggregation POC - Databricks Notebook

# COMMAND ----------

import sys
import os
import time

# Add the src directory to Python path
sys.path.append("/Workspace/Users/eran@seemplicity.io/spark-aggregation-poc/src")

# COMMAND ----------

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.main import run_aggregation_from_dbx

# COMMAND ----------

# Monitor cluster configuration
print("=== Cluster Configuration ===")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
print(f"Application ID: {spark.sparkContext.applicationId}")
print(f"Spark UI URL: {spark.sparkContext.uiWebUrl}")

# COMMAND ----------

config = Config(
    postgres_url="jdbc:postgresql://vpce-0bcb337939c5a06a6-o7h3s15r.vpce-svc-0418e833a2533b2a4.eu-central-1.vpce.amazonaws.com:54320/postgres?currentSchema=seemplicitydemo",
    postgres_properties={
        "user": "postgres",
        "password": "xjmYaNSrQK6hqHQD",
        "driver": "org.postgresql.Driver"
    },
    is_databricks=True
)

# COMMAND ----------

# Enable monitoring
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

print("=== Starting Execution ===")
start_time = time.time()

run_aggregation_from_dbx(spark, config=config)

end_time = time.time()
print(f"Total execution time: {end_time - start_time:.2f} seconds")

# COMMAND ----------