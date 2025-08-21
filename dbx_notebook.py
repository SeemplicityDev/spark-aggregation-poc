import sys
import os

# Add the src directory to Python path
sys.path.append("/Workspace/Users/eran@seemplicity.io/spark-aggregation-poc/src")

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.main import run_aggregation_from_dbx

config = Config(
    postgres_url="jdbc:postgresql://vpce-0bcb337939c5a06a6-o7h3s15r.vpce-svc-0418e833a2533b2a4.eu-central-1.vpce.amazonaws.com:54320/postgres?currentSchema=seemplicitydemo",
    postgres_properties={
        "user": "postgres",
        "password": "xjmYaNSrQK6hqHQD",
        "driver": "org.postgresql.Driver"
    },
    is_databricks=True
)
# Cell 1: Monitor cluster configuration
print("=== Cluster Configuration ===")
print(f"Executor memory: {spark.conf.get('spark.executor.memory')}")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

run_aggregation_from_dbx(spark, config=config)