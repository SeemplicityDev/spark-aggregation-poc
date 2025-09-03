import sys
import os

# Add the src directory to Python path
sys.path.append("/Workspace/Users/eran@seemplicity.io/spark-aggregation-poc/src")

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.main import run_aggregation_from_dbx


config: Config = Config(postgres_url="jdbc:postgresql://<host>>:<port>/postgres?currentSchema=unilever",
                        postgres_properties={
                            "user": "postgres",
                            "password": "4HSve3Xr9V0qyWM4",
                            "driver": "org.postgresql.Driver"
                        },
                        is_databricks = True
)
# Cell 1: Monitor cluster configuration
print("=== Cluster Configuration ===")
print(f"Executor memory: {spark.conf.get('spark.executor.memory')}")

run_aggregation_from_dbx(spark, config=config)