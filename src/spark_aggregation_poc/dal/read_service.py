import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import spark_partition_id, count, min as spark_min, max as spark_max

from spark_aggregation_poc.config.config import Config


class ReadService:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession) -> DataFrame:
        # Read from PostgreSQL people table
        print("=== Reading from PostgreSQL 'findings, etc.' tables ===")
        join_query: str = self.get_join_query()
        # print(f"join_query: {join_query}")
        df: DataFrame = spark.read.jdbc(
            url=self.postgres_url,
            table=join_query,
            properties=self.postgres_properties
        )

        # Log partition information using DataFrame operations
        # self.log_partition_info_dataframe(df)

        # Show DataFrame statistics
        row_count = df.count()
        print(f"Number of rows from DB: {row_count}")
        print("=== Current DataFrame ===")
        df.show(10)

        return df

    def log_partition_info_dataframe(self, df: DataFrame):
        """Log partition information using DataFrame operations"""
        print("=== Partition Distribution Analysis ===")

        # Add partition ID to DataFrame
        df_with_partition = df.withColumn("partition_id", spark_partition_id())

        # Analyze partition distribution
        partition_stats = df_with_partition.groupBy("partition_id").agg(
            count("*").alias("row_count"),
            spark_min("min_finding_id").alias("min_finding_id"),
            spark_max("max_finding_id").alias("max_finding_id")
        ).orderBy("partition_id")

        print("Partition distribution:")
        partition_stats.show()



    def get_join_query(self):
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to spark_aggregation_poc, then into data directory
        sql_file_path = os.path.join(current_dir, "..", "data", "aggregation_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)  # Clean up the path

        with open(sql_file_path, "r") as f:
            content = f.read()

        return content