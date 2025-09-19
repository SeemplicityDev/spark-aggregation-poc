import sys
import os


# Databricks notebook source
# MAGIC %md
# MAGIC # Parameterized Customer Aggregation
# MAGIC
# MAGIC This notebook processes aggregation for a single customer specified by parameters.

# COMMAND ----------

# Get parameters
dbutils.widgets.text("customer_schema", "carlsberg", "Customer Schema")
dbutils.widgets.text("postgres_host", "vpce-0ab78c002b187f211-1l5gyxsr.vpce-svc-0ec8249541aa206d0.eu-central-1.vpce.amazonaws.com", "PostgreSQL Host")
dbutils.widgets.text("postgres_port", "54323", "PostgreSQL Port")
dbutils.widgets.text("postgres_db", "postgres", "PostgreSQL Database")
dbutils.widgets.text("db_schema", "carlsberg", "Database customer schema")
dbutils.widgets.text("postgres_user", "postgres", "PostgreSQL User")
dbutils.widgets.text("postgres_password", "vCo8trJkeg57Vp53", "PostgreSQL Password")

customer_schema = dbutils.widgets.get("customer_schema")
postgres_host = dbutils.widgets.get("postgres_host")
postgres_port = dbutils.widgets.get("postgres_port")
postgres_db = dbutils.widgets.get("postgres_db")
postgres_user = dbutils.widgets.get("postgres_user")
postgres_password = dbutils.widgets.get("postgres_password")

print(f"Processing customer: {customer_schema}")

# COMMAND ----------

# Import your code
import sys
sys.path.append("/Workspace/Users/eran@seemplicity.io/spark-aggregation-poc/src")

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.main import run_aggregation

# COMMAND ----------

# Create customer-specific configuration
config = Config(
    postgres_url=f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}?currentSchema={customer_schema}",
    postgres_properties={
        "user": {postgres_user},
        "password": {postgres_password},
        "driver": "org.postgresql.Driver"
    },
    customer=customer_schema,
    is_databricks=True
)

# COMMAND ----------

# Run aggregation for this customer
try:
    run_aggregation(spark, config)
    print(f"✅ Successfully completed aggregation for {customer_schema}")
except Exception as e:
    print(f"❌ Failed aggregation for {customer_schema}: {e}")
    raise e