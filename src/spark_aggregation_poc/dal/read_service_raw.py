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
        """Read tables separately using sequential batch loading for large tables"""

        print("=== Reading tables separately with sequential batching for large tables ===")

        # STEP 1 & 2: Read small and medium tables
        aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df, plain_resources_df, statuses_df = self.read_small_medium_tables(
            spark)

        # STEP 3: Read LARGE tables with sequential batching
        print("Reading large tables with sequential batching...")

        findings_additional_data_df, findings_df, findings_info_df, findings_scores_df, user_status_df = self.read_large_tables(
            spark)

        # # STEP 4: Perform joins in Spark following raw_join_query1.sql order
        # print("Performing joins in Spark following raw_join_query1.sql structure...")

        result_df = self.join_tables(aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df,
                                     findings_additional_data_df, findings_df, findings_info_df, findings_scores_df,
                                     plain_resources_df, statuses_df, user_status_df)
        print("joined result_df")
        return result_df

        # # STEP 5: Select columns exactly as in raw_join_query1.sql
        # final_df = result_df.select(
        #     findings_df["id"].alias("finding_id"),
        #     findings_df["package_name"],
        #     findings_df["main_resource_id"],
        #     findings_df["aggregation_group_id"],
        #     finding_sla_connections_df["finding_id"].alias("sla_connection_id"),
        #     plain_resources_df["id"].alias("resource_id"),
        #     plain_resources_df["cloud_account"].alias("root_cloud_account"),
        #     plain_resources_df["cloud_account_friendly_name"].alias("root_cloud_account_friendly_name"),
        #     findings_scores_df["finding_id"].alias("score_finding_id"),
        #     user_status_df["id"].alias("user_status_id"),
        #     user_status_df["actual_status_key"],
        #     findings_additional_data_df["finding_id"].alias("additional_data_id"),
        #     statuses_df["key"].alias("status_key"),
        #     aggregation_groups_df["id"].alias("existing_group_id"),
        #     aggregation_groups_df["main_finding_id"].alias("existing_main_finding_id"),
        #     aggregation_groups_df["group_identifier"].alias("existing_group_identifier"),
        #     aggregation_groups_df["is_locked"],
        #     findings_info_df["id"].alias("findings_info_id")
        # )
        #
        # print("✓ All joins completed in Spark")
        # final_count = final_df.count()
        # print(f"Final result count: {final_count:,} rows")
        #
        # return final_df

    def read_small_medium_tables(self, spark):
        """Read small and medium tables"""

        print("Reading small tables...")

        # Import required functions
        from pyspark.sql.functions import col, broadcast

        # STEP 1: Read SMALL tables first (no partitioning needed)
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

        # STEP 2: Read MEDIUM tables
        print("Reading medium tables...")

        finding_sla_connections_df = spark.read.jdbc(
            url=self.postgres_url,
            table="finding_sla_rule_connections",
            properties=self.postgres_properties
        )

        plain_resources_df = spark.read.jdbc(
            url=self.postgres_url,
            table="plain_resources",
            properties=self.postgres_properties
        ).cache()  # Cache for reuse

        print("✓ Medium tables loaded")

        return aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df, plain_resources_df, statuses_df

    def get_table_bounds(self, spark, table_name, id_column):
        """Get min/max bounds for a table"""
        bounds_query = f"SELECT MIN({id_column}) as min_id, MAX({id_column}) as max_id FROM {table_name}"
        bounds_df = spark.read.jdbc(
            url=self.postgres_url,
            table=f"({bounds_query}) as bounds",
            properties=self.postgres_properties
        )
        bounds = bounds_df.collect()[0]
        min_id = bounds['min_id'] if bounds['min_id'] is not None else 1
        max_id = bounds['max_id'] if bounds['max_id'] is not None else 100000
        return min_id, max_id

    def read_table_in_batches(self, spark, table_name, id_column, batch_size, where_clause="", max_batches=50):
        """Read table in sequential batches"""
        print(f"  Reading {table_name} in sequential batches...")

        # Get the actual bounds
        min_id, max_id = self.get_table_bounds(spark, table_name, id_column)
        print(f"    {table_name} ID range: {min_id:,} to {max_id:,}")

        batches = []
        current_lower = min_id
        batch_num = 1

        while current_lower <= max_id and batch_num <= max_batches:
            current_upper = min(current_lower + batch_size - 1, max_id)

            print(f"    Reading batch {batch_num}: {id_column} {current_lower:,} to {current_upper:,}")

            # Build WHERE clause
            batch_condition = f"{id_column} >= {current_lower} AND {id_column} <= {current_upper}"
            if where_clause:
                full_where = f"WHERE {where_clause} AND {batch_condition}"
            else:
                full_where = f"WHERE {batch_condition}"

            batch_query = f"SELECT * FROM {table_name} {full_where}"

            try:
                batch_df = spark.read.jdbc(
                    url=self.postgres_url,
                    table=f"({batch_query}) as {table_name}_batch_{batch_num}",
                    properties=self.postgres_properties
                )

                # Count rows in this batch
                batch_count = batch_df.count()
                print(f"      Batch {batch_num} loaded: {batch_count:,} rows")

                if batch_count > 0:
                    batches.append(batch_df)

            except Exception as e:
                print(f"      Error reading batch {batch_num}: {e}")
                # Continue with next batch instead of failing completely

            current_lower = current_upper + 1
            batch_num += 1

        # Union all successful batches
        if batches:
            print(f"    Combining {len(batches)} batches for {table_name}...")
            result_df = batches[0]
            for batch_df in batches[1:]:
                result_df = result_df.union(batch_df)

            total_count = result_df.count()
            print(f"    ✓ {table_name} total: {total_count:,} rows from {len(batches)} batches")
            return result_df
        else:
            print(f"    ⚠️  No data loaded for {table_name}")
            # Return empty DataFrame with schema from a small sample
            sample_query = f"SELECT * FROM {table_name} LIMIT 0"
            return spark.read.jdbc(
                url=self.postgres_url,
                table=f"({sample_query}) as empty_{table_name}",
                properties=self.postgres_properties
            )

    def read_large_tables(self, spark):
        """Read large tables using sequential batching"""

        print("Reading large tables with sequential batching...")

        batch_size = 100000  # 100K rows per batch

        try:
            # Findings table with filtering
            findings_df = self.read_table_in_batches(
                spark, "findings", "id", batch_size, "package_name IS NOT NULL", max_batches=30
            )

            # Findings scores
            findings_scores_df = self.read_table_in_batches(
                spark, "findings_scores", "finding_id", batch_size, max_batches=30
            )

            # User status
            user_status_df = self.read_table_in_batches(
                spark, "user_status", "id", batch_size, max_batches=30
            )

            # Findings additional data
            findings_additional_data_df = self.read_table_in_batches(
                spark, "findings_additional_data", "finding_id", batch_size, max_batches=30
            )

            # Findings info
            findings_info_df = self.read_table_in_batches(
                spark, "findings_info", "id", batch_size, max_batches=30
            )

        except Exception as e:
            print(f"Error in sequential batching: {e}")
            import traceback
            traceback.print_exc()
            raise e

        print("✓ Large tables loaded with sequential batching")

        return findings_additional_data_df, findings_df, findings_info_df, findings_scores_df, user_status_df

    def join_tables(self, aggregation_groups_df, aggregation_rules_excluder_df, finding_sla_connections_df,
                    findings_additional_data_df, findings_df, findings_info_df, findings_scores_df,
                    plain_resources_df, statuses_df, user_status_df):
        """Perform joins in Spark following raw_join_query1.sql order"""

        from pyspark.sql.functions import broadcast

        print("Performing joins in Spark following raw_join_query1.sql structure...")

        # Start with findings (main table)
        result_df = findings_df

        # LEFT OUTER JOIN finding_sla_rule_connections
        print("  Joining with finding_sla_rule_connections...")
        result_df = result_df.join(
            finding_sla_connections_df,
            findings_df["id"] == finding_sla_connections_df["finding_id"],
            "left"
        )

        # JOIN plain_resources (INNER JOIN - broadcast small/medium table)
        print("  Joining with plain_resources...")
        result_df = result_df.join(
            broadcast(plain_resources_df),
            findings_df["main_resource_id"] == plain_resources_df["id"],
            "inner"
        )

        # JOIN findings_scores (INNER JOIN)
        print("  Joining with findings_scores...")
        result_df = result_df.join(
            findings_scores_df,
            findings_df["id"] == findings_scores_df["finding_id"],
            "inner"
        )

        # JOIN user_status (INNER JOIN)
        print("  Joining with user_status...")
        result_df = result_df.join(
            user_status_df,
            user_status_df["id"] == findings_df["id"],
            "inner"
        )

        # LEFT OUTER JOIN findings_additional_data
        print("  Joining with findings_additional_data...")
        result_df = result_df.join(
            findings_additional_data_df,
            findings_df["id"] == findings_additional_data_df["finding_id"],
            "left"
        )

        # JOIN statuses (INNER JOIN - broadcast small table)
        print("  Joining with statuses...")
        result_df = result_df.join(
            broadcast(statuses_df),
            user_status_df["actual_status_key"] == statuses_df["key"],
            "inner"
        )

        # LEFT OUTER JOIN aggregation_groups (broadcast small table)
        print("  Joining with aggregation_groups...")
        result_df = result_df.join(
            broadcast(aggregation_groups_df),
            findings_df["aggregation_group_id"] == aggregation_groups_df["id"],
            "left"
        )

        # LEFT OUTER JOIN findings_info
        print("  Joining with findings_info...")
        result_df = result_df.join(
            findings_info_df,
            findings_info_df["id"] == findings_df["id"],
            "left"
        )

        # LEFT OUTER JOIN aggregation_rules_findings_excluder (broadcast small table)
        print("  Joining with aggregation_rules_findings_excluder...")
        result_df = result_df.join(
            broadcast(aggregation_rules_excluder_df),
            findings_df["id"] == aggregation_rules_excluder_df["finding_id"],
            "left"
        )

        return result_df


    def get_join_query(self):
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to spark_aggregation_poc, then into data directory
        sql_file_path = os.path.join(current_dir, "..", "data", "raw_join_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)  # Clean up the path

        with open(sql_file_path, "r") as f:
            content = f.read()

        return content

