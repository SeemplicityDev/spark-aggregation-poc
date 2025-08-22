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
        """Create the partitioned_findings temporary view with optimizations"""

        print("=== Creating partitioned_findings view with optimizations ===")

        # HARDCODED TEST: Optimized JDBC properties for large table read
        optimized_properties = self.postgres_properties.copy()
        optimized_properties.update({
            "fetchsize": "10000",  # Larger fetch for base table
            "queryTimeout": "0",  # No query timeout
            "loginTimeout": "60",  # 60 second login timeout
            "tcpKeepAlive": "true",  # Keep connections alive
            "socketTimeout": "0"  # No socket timeout
        })

        # Adjust partitioning based on your cluster (rd-fleet.xlarge: 4 cores)
        # More partitions for large base table
        num_partitions = 8  # 2x cores for I/O heavy base table read

        findings_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings",
            properties=optimized_properties,  # Use optimized properties
            column="id",
            lowerBound=1,
            upperBound=1000000,  # Adjust based on your actual data range
            numPartitions=num_partitions
        )

        # Cache immediately since this will be used for joins
        findings_df.cache()
        findings_df.createOrReplaceTempView("partitioned_findings")

        # Verify the partitioning worked and get row count
        import time
        start_time = time.time()
        row_count = findings_df.count()
        load_time = time.time() - start_time

        print(f"✓ Created partitioned_findings: {row_count} rows in {load_time:.2f}s with {num_partitions} partitions")

        # Log partition distribution using DataFrame operations (no RDD)
        self.log_findings_partition_distribution(findings_df)

    def log_findings_partition_distribution(self, df: DataFrame):
        """Log how the findings data is distributed across partitions"""
        from pyspark.sql.functions import spark_partition_id, count, min as spark_min, max as spark_max

        print("=== Findings Partition Distribution ===")

        partition_stats = df.withColumn("partition_id", spark_partition_id()) \
            .groupBy("partition_id") \
            .agg(
            count("*").alias("row_count"),
            spark_min("id").alias("min_id"),
            spark_max("id").alias("max_id")
        ).orderBy("partition_id")

        print("Base findings partition distribution:")
        partition_stats.show()


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