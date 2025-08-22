import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import spark_partition_id, count, min as spark_min, max as spark_max

from spark_aggregation_poc.config.config import Config


class ReadServicePrePartition:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession) -> DataFrame:
        """Use partitioned_findings instead of direct PostgreSQL query"""

        print("=== Pre-partition base tables approach ===")

        # Step 1: Create partitioned_findings view
        self.create_partitioned_findings_view(spark)

        # Step 2: Load other tables as temp views
        self.load_supporting_tables(spark)

        # Step 3: Run your modified aggregation query
        result_df = self.run_aggregation_query(spark)

        return result_df

    def create_partitioned_findings_view(self, spark: SparkSession):
        """Create the partitioned_findings temporary view"""
        findings_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings",
            properties=self.postgres_properties,
            column="id",
            lowerBound=1,
            upperBound=1000000,
            numPartitions=4
        )

        findings_df.createOrReplaceTempView("partitioned_findings")


    def load_supporting_tables(self, spark: SparkSession):
        """Load all other tables referenced in your query"""
        tables = [
            "finding_sla_rule_connections",
            "plain_resources",
            "findings_scores",
            "user_status",
            "findings_additional_data",
            "statuses",
            "aggregation_groups",
            "findings_info",
            "aggregation_rules_findings_excluder"
        ]

        for table in tables:
            try:
                df = spark.read.jdbc(
                    url=self.postgres_url,
                    table=table,
                    properties=self.postgres_properties
                )
                df.createOrReplaceTempView(table)
                print(f"✓ Loaded {table}")
            except Exception as e:
                print(f"⚠ Could not load {table}: {e}")

    def run_aggregation_query(self, spark: SparkSession) -> DataFrame:
        """Run your aggregation query with minimal changes"""

        # Your existing query with just the table name changed
        agg_query: str = self.get_agg_query()

        return spark.sql(agg_query)



    def get_agg_query(self) -> str:
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to spark_aggregation_poc, then into data directory
        sql_file_path = os.path.join(current_dir, "..", "data", "aggregation_query1_pre_partition.sql")
        sql_file_path = os.path.normpath(sql_file_path)  # Clean up the path

        with open(sql_file_path, "r") as f:
            content = f.read()

        return content