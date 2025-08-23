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
        """Read tables separately based on raw_join_query1.sql structure"""

        print("=== Reading tables separately based on raw_join_query1.sql ===")

        (aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df,
         findings_additional_data_df, findings_df, findings_info_df, findings_scores_df,
         plain_resources_df, statuses_df, user_status_df) = self.read_tables_separately(spark)

        # STEP 4: Perform joins in Spark following raw_join_query1.sql order
        print("Performing joins in Spark following raw_join_query1.sql structure...")

        result_df = self.join_tables(aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df,
                                     findings_additional_data_df, findings_df, findings_info_df, findings_scores_df,
                                     plain_resources_df, statuses_df, user_status_df)

        # STEP 5: Select columns exactly as in raw_join_query1.sql
        final_df = result_df.select(
            findings_df.id.alias("finding_id"),
            findings_df.package_name,
            findings_df.main_resource_id,
            findings_df.aggregation_group_id,
            finding_sla_connections_df.finding_id.alias("sla_connection_id"),
            plain_resources_df.id.alias("resource_id"),
            plain_resources_df.cloud_account.alias("root_cloud_account"),
            plain_resources_df.cloud_account_friendly_name.alias("root_cloud_account_friendly_name"),
            findings_scores_df.finding_id.alias("score_finding_id"),
            user_status_df.id.alias("user_status_id"),
            user_status_df.actual_status_key,
            findings_additional_data_df.finding_id.alias("additional_data_id"),
            statuses_df.key.alias("status_key"),
            aggregation_groups_df.id.alias("existing_group_id"),
            aggregation_groups_df.main_finding_id.alias("existing_main_finding_id"),
            aggregation_groups_df.group_identifier.alias("existing_group_identifier"),
            aggregation_groups_df.is_locked,
            findings_info_df.id.alias("findings_info_id")
        )

        print("✓ All joins completed in Spark")
        print(f"Final result count: {final_df.count():,} rows")

        return final_df

    def read_tables_separately(self, spark):
        # Import required functions
        from pyspark.sql.functions import col
        # STEP 1: Read SMALL tables first (no partitioning needed)
        print("Reading small tables...")
        statuses_df = spark.read.jdbc(
            url=self.postgres_url,
            table="statuses",
            properties=self.postgres_properties
        ).cache()
        aggregation_groups_df = spark.read.jdbc(
            url=self.postgres_url,
            table="aggregation_groups",
            properties=self.postgres_properties
        ).cache()
        aggregation_rules_excluder_df = spark.read.jdbc(
            url=self.postgres_url,
            table="aggregation_rules_findings_excluder",
            properties=self.postgres_properties
        ).cache()
        print("✓ Small tables loaded and cached")
        # STEP 2: Read MEDIUM tables (light partitioning)
        print("Reading medium tables...")
        finding_sla_connections_df = spark.read.jdbc(
            url=self.postgres_url,
            table="finding_sla_rule_connections",
            properties=self.postgres_properties,
            column="finding_id",
            lowerBound=1,
            upperBound=10000000,
            numPartitions=4
        )
        plain_resources_df = spark.read.jdbc(
            url=self.postgres_url,
            table="plain_resources",
            properties=self.postgres_properties,
            column="id",
            lowerBound=1,
            upperBound=1000000,
            numPartitions=4
        ).cache()  # Cache for reuse
        print("✓ Medium tables loaded")
        # STEP 3: Read LARGE tables (aggressive partitioning)
        print("Reading large tables...")
        # Main findings table - filter early
        findings_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings",
            properties=self.postgres_properties,
            column="id",
            lowerBound=1,
            upperBound=10000000,
            numPartitions=8
        ).filter(col("package_name").isNotNull())  # Apply WHERE condition early
        findings_scores_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings_scores",
            properties=self.postgres_properties,
            column="finding_id",
            lowerBound=1,
            upperBound=10000000,
            numPartitions=6
        )
        user_status_df = spark.read.jdbc(
            url=self.postgres_url,
            table="user_status",
            properties=self.postgres_properties,
            column="id",
            lowerBound=1,
            upperBound=10000000,
            numPartitions=6
        )
        findings_additional_data_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings_additional_data",
            properties=self.postgres_properties,
            column="finding_id",
            lowerBound=1,
            upperBound=10000000,
            numPartitions=6
        )
        findings_info_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings_info",
            properties=self.postgres_properties,
            column="id",
            lowerBound=1,
            upperBound=10000000,
            numPartitions=6
        )
        return aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df, findings_additional_data_df, findings_df, findings_info_df, findings_scores_df, plain_resources_df, statuses_df, user_status_df

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

