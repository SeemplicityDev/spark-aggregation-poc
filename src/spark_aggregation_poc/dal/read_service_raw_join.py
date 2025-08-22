import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import spark_partition_id, count, min as spark_min, max as spark_max

from spark_aggregation_poc.config.config import Config


class ReadServiceRawJoin:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession) -> DataFrame:
        """Read raw joined data without GROUP BY for Spark-side aggregation"""

        print("=== Reading raw joined data from PostgreSQL ===")

        # Optimized JDBC properties for large raw dataset
        optimized_properties = self.postgres_properties.copy()
        optimized_properties.update({
            "fetchsize": "20000",  # Larger fetch for raw data
            "queryTimeout": "0",  # No query timeout
            "loginTimeout": "120",  # Longer login timeout
            "tcpKeepAlive": "true",  # Keep connections alive
            "socketTimeout": "0",  # No socket timeout
            "batchsize": "20000"  # Larger batch size
        })

        raw_query = self.get_join_query()

        # For large datasets, use partitioning on findings.id
        df = spark.read.jdbc(
            url=self.postgres_url,
            table=raw_query,
            properties=optimized_properties,
            column="finding_id",  # Partition on findings.id
            lowerBound=1,
            upperBound=10000000,  # Adjust based on your data range
            numPartitions=16  # More partitions for raw data
        )

        # Immediate optimizations for large raw dataset
        df_optimized = df.repartition(16, "package_name").cache()  # Partition by group key

        print("=== Raw data loaded, verifying... ===")
        row_count = df_optimized.count()
        print(f"Raw data rows: {row_count}")

        # Show sample
        df_optimized.show(5)

        return df_optimized



    def get_join_query(self):
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to spark_aggregation_poc, then into data directory
        sql_file_path = os.path.join(current_dir, "..", "data", "raw_join_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)  # Clean up the path

        with open(sql_file_path, "r") as f:
            content = f.read()

        return content