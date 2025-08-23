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
        """Read tables separately using actual min/max values for partitioning"""

        print("=== Reading tables separately with actual min/max bounds ===")

        # Import required functions
        from pyspark.sql.functions import col, broadcast

        # Helper function to get actual min/max for partitioning
        def get_min_max_bounds(table_name: str, column_name: str):
            bounds_query = f"SELECT MIN({column_name}) as min_val, MAX({column_name}) as max_val FROM {table_name}"
            bounds_df = spark.read.jdbc(
                url=self.postgres_url,
                table=f"({bounds_query}) as bounds",
                properties=self.postgres_properties
            )
            bounds = bounds_df.collect()[0]
            min_val = bounds['min_val'] if bounds['min_val'] is not None else 1
            max_val = bounds['max_val'] if bounds['max_val'] is not None else 1000000
            print(f"  {table_name}.{column_name}: {min_val:,} to {max_val:,}")
            return min_val, max_val

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

        # STEP 2: Read MEDIUM tables with actual bounds
        print("Reading medium tables with actual bounds...")

        # Get actual bounds for medium tables
        sla_min, sla_max = get_min_max_bounds("finding_sla_rule_connections", "finding_id")
        resources_min, resources_max = get_min_max_bounds("plain_resources", "id")

        finding_sla_connections_df = spark.read.jdbc(
            url=self.postgres_url,
            table="finding_sla_rule_connections",
            properties=self.postgres_properties,
            column="finding_id",
            lowerBound=sla_min,
            upperBound=sla_max,
            numPartitions=4
        )

        plain_resources_df = spark.read.jdbc(
            url=self.postgres_url,
            table="plain_resources",
            properties=self.postgres_properties,
            column="id",
            lowerBound=resources_min,
            upperBound=resources_max,
            numPartitions=4
        ).cache()  # Cache for reuse

        print("✓ Medium tables loaded")

        # STEP 3: Read LARGE tables with actual bounds
        print("Reading large tables with actual bounds...")

        # Get actual bounds for large tables
        findings_min, findings_max = get_min_max_bounds("findings", "id")
        scores_min, scores_max = get_min_max_bounds("findings_scores", "finding_id")
        status_min, status_max = get_min_max_bounds("user_status", "id")
        additional_min, additional_max = get_min_max_bounds("findings_additional_data", "finding_id")
        info_min, info_max = get_min_max_bounds("findings_info", "id")

        # Main findings table - filter early
        findings_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings",
            properties=self.postgres_properties,
            column="id",
            lowerBound=findings_min,
            upperBound=findings_max,
            numPartitions=8
        ).filter(col("package_name").isNotNull())  # Apply WHERE condition early

        findings_scores_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings_scores",
            properties=self.postgres_properties,
            column="finding_id",
            lowerBound=scores_min,
            upperBound=scores_max,
            numPartitions=6
        )

        user_status_df = spark.read.jdbc(
            url=self.postgres_url,
            table="user_status",
            properties=self.postgres_properties,
            column="id",
            lowerBound=status_min,
            upperBound=status_max,
            numPartitions=6
        )

        findings_additional_data_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings_additional_data",
            properties=self.postgres_properties,
            column="finding_id",
            lowerBound=additional_min,
            upperBound=additional_max,
            numPartitions=6
        )

        findings_info_df = spark.read.jdbc(
            url=self.postgres_url,
            table="findings_info",
            properties=self.postgres_properties,
            column="id",
            lowerBound=info_min,
            upperBound=info_max,
            numPartitions=6
        )

        print("✓ Large tables loaded")

        # STEP 4: Perform joins in Spark following raw_join_query1.sql order
        print("Performing joins in Spark following raw_join_query1.sql structure...")

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

