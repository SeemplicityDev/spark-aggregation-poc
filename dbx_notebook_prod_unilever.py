import sys
import os

# Add the src directory to Python path
sys.path.append("/Workspace/Users/eran@seemplicity.io/spark-aggregation-poc/src")

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.main import run_aggregation_from_dbx


config: Config = Config(postgres_url="jdbc:postgresql://vpce-0ab78c002b187f211-1l5gyxsr.vpce-svc-0ec8249541aa206d0.eu-central-1.vpce.amazonaws.com:54322/postgres?currentSchema=unilever",
                        postgres_properties={
                            "user": "postgres",
                            "password": "T66AWCNJGW!!jqML",
                            "driver": "org.postgresql.Driver"
                        },
                        is_databricks = True
)
# Cell 1: Monitor cluster configuration
print("=== Cluster Configuration ===")
print(f"Executor memory: {spark.conf.get('spark.executor.memory')}")

run_aggregation_from_dbx(spark, config=config)