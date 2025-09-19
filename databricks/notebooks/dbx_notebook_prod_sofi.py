import sys
import os

from spark_aggregation_poc.run_aggregation import run_aggregation

# Add the src directory to Python path
sys.path.append("/Workspace/Users/eran@seemplicity.io/spark-aggregation-poc/src")

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.run_aggregation import run_aggregation


config: Config = Config(postgres_url="jdbc:postgresql://vpce-0ab78c002b187f211-1l5gyxsr.vpce-svc-0ec8249541aa206d0.eu-central-1.vpce.amazonaws.com:54320/postgres?currentSchema=sofi",
                        postgres_properties={
                            "user": "postgres",
                            "password": "99PKFxw!AoZDsfe6",
                            "driver": "org.postgresql.Driver"
                        },
                        is_databricks = True
)
# Cell 1: Monitor cluster configuration
print("=== Cluster Configuration ===")
print(f"Executor memory: {spark.conf.get('spark.executor.memory')}")

run_aggregation(spark, config=config)