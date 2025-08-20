import os

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.models.finding_data import FindingData
from spark_aggregation_poc.utils.parse_utils import row_to_finding_data


class ReadService:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession) -> tuple[DataFrame, list[FindingData]]:
        # Read from PostgreSQL people table
        print("=== Reading from PostgreSQL 'findings, etc.' tables ===")
        join_query: str = self.get_join_query()
        print(f"join_query: {join_query}")
        df: DataFrame = spark.read.jdbc(
            url=self.postgres_url,
            table=join_query,
            properties=self.postgres_properties,
            # # Add these for parallel reading:
            column="finding_id",  # Use a numeric column for partitioning
            lowerBound=1,
            upperBound=1000000,
            numPartitions=4  # Number of parallel reads
        )
        # Show DataFrame
        print(f"Number of rows from DB:", df.count())
        print("=== Current DataFrame ===")
        df.show(10)
        # Direct deserialization: DB → Person objects (using RDD map)
        print("=== Converting directly to FindingsData objects ===")
        findings_data_rdd: RDD[FindingData] = df.rdd.map(row_to_finding_data)
        findings_data: list[FindingData] = findings_data_rdd.collect()  # Only collect once, after transformation
        return df, findings_data

    def get_join_query(self):
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to spark_aggregation_poc, then into data directory
        sql_file_path = os.path.join(current_dir, "..", "data", "aggregation_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)  # Clean up the path

        with open(sql_file_path, "r") as f:
            content = f.read()

        return content