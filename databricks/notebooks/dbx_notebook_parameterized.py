import sys
import os
import json


# Databricks notebook source
# MAGIC %md
# MAGIC # Parameterized Customer Aggregation
# MAGIC
# MAGIC This notebook processes aggregation for a single customer specified by parameters.

# COMMAND ----------

# Get the JSON input from for_each_task
dbutils.widgets.text("input", "")
input_json = dbutils.widgets.get("input")

print(f"Raw input: {input_json}")

# Parse the JSON to extract customer_schema
try:
    input_data = json.loads(input_json)
    customer_schema = input_data["customer_schema"]
    postgres_port = int(input_data["postgres_port"])
    postgres_db = input_data["postgres_db"]
    postgres_host = dbutils.secrets.get("my-scope", f"{customer_schema}-postgres-host")
    postgres_user = dbutils.secrets.get("my-scope", f"{customer_schema}-postgres-user")
    postgres_password = dbutils.secrets.get("my-scope", f"{customer_schema}-postgres-password")
except Exception as e:
    print(f"Error parsing JSON: {e}")
    customer_schema = "unknown"

# COMMAND ----------

# Import your code
import sys
sys.path.append("/Workspace/Users/eran@seemplicity.io/spark-aggregation-poc/src")

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.run_aggregation import run_aggregation

# COMMAND ----------

# Create customer-specific configuration
config = Config(
    postgres_url=f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}?currentSchema={customer_schema}",
    postgres_properties={
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver"
    },
    is_databricks=True,
    catalog_table_prefix="general_data.default.",
    customer=customer_schema

)

# COMMAND ----------

# Run aggregation for this customer
try:
    run_aggregation(spark, config)
    print(f"✅ Successfully completed aggregation for {customer_schema}")
except Exception as e:
    print(f"❌ Failed aggregation for {customer_schema}: {e}")
    raise e