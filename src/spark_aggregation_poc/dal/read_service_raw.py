import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, collect_list, \
    when, col, array, broadcast

from spark_aggregation_poc.config.config import Config


class ReadServiceRaw:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession) -> DataFrame:
        """Read tables separately using hash-based partitioning for large tables"""

        print("=== Reading tables separately with hash-based partitioning for large tables ===")

        aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df, plain_resources_df, statuses_df = self.read_small_medium_tables(
            spark)

        return aggregation_groups_df

        # STEP 3: Read LARGE tables with hash-based partitioning
        # print("Reading large tables with hash-based partitioning...")
        #
        # findings_additional_data_df, findings_df, findings_info_df, findings_scores_df, user_status_df = self.read_large_tables(
        #     spark)
        #
        # # STEP 4: Perform joins in Spark following raw_join_query1.sql order
        # print("Performing joins in Spark following raw_join_query1.sql structure...")
        #
        # result_df = self.join_tables(aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df,
        #                              findings_additional_data_df, findings_df, findings_info_df, findings_scores_df,
        #                              plain_resources_df, statuses_df, user_status_df)
        #
        # # STEP 5: Select columns exactly as in raw_join_query1.sql
        # final_df = result_df.select(
        #     findings_df.id.alias("finding_id"),
        #     findings_df.package_name,
        #     findings_df.main_resource_id,
        #     findings_df.aggregation_group_id,
        #     finding_sla_connections_df.finding_id.alias("sla_connection_id"),
        #     plain_resources_df.id.alias("resource_id"),
        #     plain_resources_df.cloud_account.alias("root_cloud_account"),
        #     plain_resources_df.cloud_account_friendly_name.alias("root_cloud_account_friendly_name"),
        #     findings_scores_df.finding_id.alias("score_finding_id"),
        #     user_status_df.id.alias("user_status_id"),
        #     user_status_df.actual_status_key,
        #     findings_additional_data_df.finding_id.alias("additional_data_id"),
        #     statuses_df.key.alias("status_key"),
        #     aggregation_groups_df.id.alias("existing_group_id"),
        #     aggregation_groups_df.main_finding_id.alias("existing_main_finding_id"),
        #     aggregation_groups_df.group_identifier.alias("existing_group_identifier"),
        #     aggregation_groups_df.is_locked,
        #     findings_info_df.id.alias("findings_info_id")
        # )
        #
        # print("✓ All joins completed in Spark")
        # print(f"Final result count: {final_df.count():,} rows")
        #
        # return final_df

    def read_small_medium_tables(self, spark):
        # STEP 1: Read SMALL tables first (no partitioning needed)
        print("Reading small tables...")
        statuses_df = spark.read.jdbc(
            url=self.postgres_url,
            table="statuses",
            properties=self.postgres_properties
        ).cache()
        statuses_df.show(5)
        # aggregation_groups_df = spark.read.jdbc(
        #     url=self.postgres_url,
        #     table="aggregation_groups",
        #     properties=self.postgres_properties
        # ).cache()
        # aggregation_rules_excluder_df = spark.read.jdbc(
        #     url=self.postgres_url,
        #     table="aggregation_rules_findings_excluder",
        #     properties=self.postgres_properties
        # ).cache()
        # print("✓ Small tables loaded and cached")
        # # STEP 2: Read MEDIUM tables with hash partitioning
        # print("Reading medium tables with hash partitioning...")
        # # Medium tables - light hash partitioning
        # num_medium_partitions = 4
        # # Finding SLA connections with hash partitioning
        # sla_partitions = []
        # for i in range(num_medium_partitions):
        #     sla_query = f"""
        #     SELECT * FROM finding_sla_rule_connections
        #     WHERE ABS(HASHTEXT(finding_id::text)) % {num_medium_partitions} = {i}
        #     """
        #     sla_partition = spark.read.jdbc(
        #         url=self.postgres_url,
        #         table=f"({sla_query}) as sla_partition_{i}",
        #         properties=self.postgres_properties
        #     )
        #     sla_partitions.append(sla_partition)
        # # Union SLA partitions
        # finding_sla_connections_df = sla_partitions[0]
        # for df in sla_partitions[1:]:
        #     finding_sla_connections_df = finding_sla_connections_df.union(df)
        # # Plain resources - can use regular partitioning as it's medium size
        # plain_resources_df = spark.read.jdbc(
        #     url=self.postgres_url,
        #     table="plain_resources",
        #     properties=self.postgres_properties
        # ).cache()  # Cache for reuse since it's medium size
        # print("plain_resources loaded:")
        # plain_resources_df.show(5)

        print("✓ Medium tables loaded")


        return aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df, plain_resources_df, statuses_df

    def join_tables(self, aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df,
                    findings_additional_data_df, findings_df, findings_info_df, findings_scores_df, plain_resources_df,
                    statuses_df, user_status_df):
        # Start with findings (main table)
        result_df = findings_df
        # LEFT OUTER JOIN finding_sla_rule_connections
        result_df = result_df.join(
            finding_sla_connections_df,
            findings_df.id == finding_sla_connections_df.finding_id,
            "left"
        )
        # JOIN plain_resources (INNER JOIN - broadcast small/medium table)
        result_df = result_df.join(
            broadcast(plain_resources_df),
            findings_df.main_resource_id == plain_resources_df.id,
            "inner"
        )
        # JOIN findings_scores (INNER JOIN)
        result_df = result_df.join(
            findings_scores_df,
            findings_df.id == findings_scores_df.finding_id,
            "inner"
        )
        # JOIN user_status (INNER JOIN)
        result_df = result_df.join(
            user_status_df,
            findings_df.id == user_status_df.id,
            "inner"
        )
        # LEFT OUTER JOIN findings_additional_data
        result_df = result_df.join(
            findings_additional_data_df,
            findings_df.id == findings_additional_data_df.finding_id,
            "left"
        )
        # JOIN statuses (INNER JOIN - broadcast small table)
        result_df = result_df.join(
            broadcast(statuses_df),
            user_status_df.actual_status_key == statuses_df.key,
            "inner"
        )
        # LEFT OUTER JOIN aggregation_groups (broadcast small table)
        result_df = result_df.join(
            broadcast(aggregation_groups_df),
            findings_df.aggregation_group_id == aggregation_groups_df.id,
            "left"
        )
        # LEFT OUTER JOIN findings_info
        result_df = result_df.join(
            findings_info_df,
            findings_df.id == findings_info_df.id,
            "left"
        )
        # LEFT OUTER JOIN aggregation_rules_findings_excluder (broadcast small table)
        result_df = result_df.join(
            broadcast(aggregation_rules_excluder_df),
            findings_df.id == aggregation_rules_excluder_df.finding_id,
            "left"
        )
        return result_df

    def read_large_tables(self, spark):
        num_large_partitions = 8
        # Main findings table with hash partitioning and early filtering
        findings_df = self.read_table_with_hash_partitioning(
            spark, "findings", "id", num_large_partitions
        )
        # Other large tables with hash partitioning
        findings_scores_df = self.read_table_with_hash_partitioning(
            spark, "findings_scores", "finding_id", num_large_partitions
        )
        user_status_df = self.read_table_with_hash_partitioning(
            spark, "user_status", "id", num_large_partitions
        )
        findings_additional_data_df = self.read_table_with_hash_partitioning(
            spark, "findings_additional_data", "finding_id", num_large_partitions
        )
        findings_info_df = self.read_table_with_hash_partitioning(
            spark, "findings_info", "id", num_large_partitions
        )
        print("✓ Large tables loaded with hash partitioning")
        return findings_additional_data_df, findings_df, findings_info_df, findings_scores_df, user_status_df

    # Hash-based partitioning helper function
    def read_table_with_hash_partitioning(self, spark: SparkSession, table_name: str, partition_column: str, num_partitions: int):
        print(f"  Reading {table_name} with {num_partitions} hash partitions...")
        partitions = []

        for i in range(num_partitions):
            hash_query = f"""
                SELECT * FROM {table_name} 
                WHERE ABS(HASHTEXT({partition_column}::text)) % {num_partitions} = {i}
                """

            partition_df = spark.read.jdbc(
                url=self.postgres_url,
                table=f"({hash_query}) as {table_name}_hash_partition_{i}",
                properties=self.postgres_properties
            )
            partitions.append(partition_df)

        # Union all partitions
        result_df = partitions[0]
        for df in partitions[1:]:
            result_df = result_df.union(df)

        return result_df

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

        print("✓ Large tables loaded")

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

