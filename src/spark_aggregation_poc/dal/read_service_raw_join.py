import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import spark_partition_id, count, min as spark_min, max as spark_max, coalesce, collect_list, \
    when, col, array

from spark_aggregation_poc.config.config import Config


class ReadServiceRawJoin:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession) -> DataFrame:
        """Use hash-based partitioning for even distribution"""

        print("=== Reading with hash-based partitioning ===")

        raw_query = self.get_join_query()
        num_partitions = 32

        partition_dfs = []

        for i in range(num_partitions):
            print(f"Reading hash partition {i + 1}/{num_partitions}")

            # Use modulo hash for partitioning
            hash_partition_query = f"""
            SELECT * FROM ({raw_query}) as base_query 
            WHERE ABS(HASHTEXT(finding_id::text)) % {num_partitions} = {i}
            """

            partition_df = spark.read.jdbc(
                url=self.postgres_url,
                table=f"({hash_partition_query}) as hash_partition_{i}",
                properties=self.postgres_properties
            )

            partition_dfs.append(partition_df)

        # Union all partitions
        final_df = partition_dfs[0]
        for df in partition_dfs[1:]:
            final_df = final_df.union(df)

        return final_df

    def read_findings_data_bak(self, spark: SparkSession) -> DataFrame:
        """Read raw joined data without GROUP BY for Spark-side aggregation"""

        print("=== Reading raw joined data from PostgreSQL ===")

        optimized_properties = self.appl_optimized_config(spark)

        raw_query = self.get_join_query()

        # Get actual data range first
        bounds_query = "(SELECT MIN(findings.id) as min_id, MAX(findings.id) as max_id FROM findings WHERE findings.package_name IS NOT NULL) as bounds"

        bounds_df = spark.read.jdbc(
            url=self.postgres_url,
            table=bounds_query,
            properties=optimized_properties
        )

        bounds_row = bounds_df.collect()[0]
        actual_min = bounds_row['min_id'] or 1
        actual_max = bounds_row['max_id'] or 1000000

        print(f"Actual finding_id range: {actual_min} to {actual_max}")

        # Use actual bounds with more partitions
        df = spark.read.jdbc(
            url=self.postgres_url,
            table=raw_query,
            properties=optimized_properties,
            column="finding_id",
            lowerBound=actual_min,  # Use actual minimum
            upperBound=actual_max,  # Use actual maximum
            numPartitions=32  # Double the partitions
        )

        # Immediate optimizations for large raw dataset
        # df_optimized = df.repartition(16, "package_name").cache()  # Partition by group key

        print("=== Raw data loaded, verifying... ===")
        row_count = df.count()
        print(f"Raw data rows: {row_count}")

        # Show sample
        df.show(5)

        # self.test_group_by(df_optimized)

        return df

    def test_group_by(self, df_optimized):
        # test group by
        result_df = df_optimized.groupBy("package_name").agg(
            coalesce(
                collect_list(
                    when(col("aggregation_group_id").isNull(), col("finding_id"))
                ),
                array().cast("array<int>")
            ).alias("finding_ids_without_group")
        )
        print("Group by package_name")
        result_df.show()
        result_df = df_optimized.groupBy("root_cloud_account").agg(
            coalesce(
                collect_list(
                    when(col("aggregation_group_id").isNull(), col("finding_id"))
                ),
                array().cast("array<int>")
            ).alias("finding_ids_without_group")
        )
        print("Group by cloud_account")
        result_df.show()

    def appl_optimized_config(self, spark):
        # Cell 1: Anti-skew optimizations
        print("=== Adding anti-skew optimizations ===")
        # Your existing optimizations PLUS anti-skew settings
        # spark.conf.set("spark.executor.heartbeatInterval", "120s")
        # spark.conf.set("spark.network.timeout", "1200s")
        # spark.conf.set("spark.sql.broadcastTimeout", "7200")
        # Anti-skew configurations
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")  # Lower threshold
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")  # Smaller partitions
        # More aggressive partitioning
        spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "32MB")  # Smaller min size
        print("✓ Applied anti-skew optimizations")
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
        return optimized_properties

    def get_join_query(self):
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to spark_aggregation_poc, then into data directory
        sql_file_path = os.path.join(current_dir, "..", "data", "raw_join_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)  # Clean up the path

        with open(sql_file_path, "r") as f:
            content = f.read()

        return content

