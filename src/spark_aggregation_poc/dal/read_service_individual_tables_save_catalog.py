import os
from typing import List, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col, broadcast

from spark_aggregation_poc.config.config import Config


class ReadServiceIndividualTablesSaveCatalog:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession,
                           large_table_batch_size: int = 3200000,
                           connections_per_batch: int = 32,
                           max_id_override: int = 20000000):
        """
        Load tables separately based on size (L/M/S) and join them in Spark.

        Large tables (L): findings, findings_scores, user_status, findings_info, findings_additional_data
        Medium tables (M): finding_sla_rule_connections, plain_resources
        Small tables (S): statuses, aggregation_groups, aggregation_rules_findings_excluder

        Args:
            max_id_override: Optional max ID limit to stop processing before complete large tables
        """

        print("=== Loading Tables Separately by Size and Joining in Spark ===")
        if max_id_override:
            print(f"Using max_id_override: {max_id_override:,}")

        # Define table categories
        large_tables = ["findings", "findings_scores", "user_status", "findings_info", "findings_additional_data", "plain_resources"]
        medium_tables = ["finding_sla_rule_connections"]
        small_tables = ["statuses", "aggregation_groups", "aggregation_rules_findings_excluder"]
        small_tables = ["statuses", "aggregation_groups", "aggregation_rules_findings_excluder"]

        loaded_tables = {}

        # 1. Load Large Tables (L) - Use batched multi-connection approach
        print("\n--- Loading Large Tables (Batched Multi-Connection) ---")
        for table_name in large_tables:
            print(f"\nLoading large table: {table_name}")
            self.load_large_table_batched(spark, table_name, large_table_batch_size, connections_per_batch,
                                               max_id_override)

        # 2. Load Medium Tables (M) - Use simple multi-connection
        print("\n--- Loading Medium Tables (Multi-Connection) ---")
        for table_name in medium_tables:
            print(f"\nLoading medium table: {table_name}")
            df = self.load_medium_table(spark, table_name, max_id_override)
            self.save_to_catalog(df, table_name)

        # 3. Load Small Tables (S) - Use single connection + broadcast
        print("\n--- Loading Small Tables (Single Connection + Broadcast) ---")
        for table_name in small_tables:
            print(f"\nLoading small table: {table_name}")
            df = self.load_small_table(spark, table_name)
            self.save_to_catalog(df, table_name)


    def save_to_catalog(self, df, table_name):
        from datetime import datetime

        df.write \
            .mode("append") \
            .saveAsTable(f"general_data.default.{table_name}")
        save_start_time = datetime.now()
        print(f"✓ {table_name}: loaded and saved to catalog at: {save_start_time.strftime('%H:%M:%S')}")


    def load_large_table_batched(self, spark: SparkSession, table_name: str,
                                 batch_size: int, connections_per_batch: int,
                                 max_id_override: int = None):
        """Load large table using batched multi-connection approach"""

        # Get ID bounds
        id_column = self.get_id_column_for_table(table_name)
        min_id, max_id = self.get_table_id_bounds(spark, table_name, id_column)

        # Apply max_id_override if provided
        if max_id_override is not None:
            original_max_id = max_id
            max_id = min(max_id, max_id_override)
            if max_id < original_max_id:
                print(
                    f"  {table_name} ID range limited by override: {min_id:,} to {max_id:,} (original max: {original_max_id:,})")
            else:
                print(f"  {table_name} ID range: {min_id:,} to {max_id:,} (override {max_id_override:,} not applied)")
        else:
            print(f"  {table_name} ID range: {min_id:,} to {max_id:,}")

        # Calculate batches
        total_range = max_id - min_id + 1
        num_batches = (total_range + batch_size - 1) // batch_size
        print(f"  Loading {table_name} in {num_batches} batches of {batch_size:,} each")

        batches = []

        from datetime import datetime

        for batch_num in range(num_batches):
            start_id = min_id + (batch_num * batch_size)
            end_id = min(start_id + batch_size - 1, max_id)

            print(f"\n--- Batch {batch_num}/{num_batches} ---")
            print(f"    Batch {batch_num + 1}/{num_batches}: {id_column} {start_id:,} to {end_id:,}")
            batch_start_time = datetime.now()
            print(f"🕐 [BATCH START] Batch {batch_num} started at: {batch_start_time.strftime('%H:%M:%S')}")

            batch_df = self.load_table_batch_with_connections(
                spark, table_name, id_column, start_id, end_id, connections_per_batch
            )
            self.save_to_catalog(batch_df, table_name)

            # # # Force materialization to prevent thundering herd
            # # batch_df = batch_df.persist()
            # # batch_count = batch_df.count()
            # print(f"      Loaded: {batch_count:,} rows")
            #
            # if batch_count > 0:
            # batches.append(batch_df)

        # # Union all batches using tree-reduction
        # if len(batches) > 1:
        #     print(f"  Combining {len(batches)} {table_name} batches...")
        #     return self.safe_union_all_batches(batches)
        # elif len(batches) == 1:
        #     return batches[0]
        # else:
        #     return self.get_empty_table_dataframe(spark, table_name)


    def load_medium_table(self, spark: SparkSession, table_name: str,
                          max_id_override: int = None) -> DataFrame:
        """Load medium table using simple multi-connection partitioning"""

        id_column = self.get_id_column_for_table(table_name)
        min_id, max_id = self.get_table_id_bounds(spark, table_name, id_column)

        # Apply max_id_override if provided
        if max_id_override is not None:
            original_max_id = max_id
            max_id = min(max_id, max_id_override)
            if max_id < original_max_id:
                print(f"  {table_name} ID range limited by override: {min_id:,} to {max_id:,} (original max: {original_max_id:,})")

        return spark.read.jdbc(
            url=self.postgres_url,
            table=table_name,
            column=id_column,
            lowerBound=min_id,
            upperBound=max_id,
            numPartitions=4,  # Use 4 connections
            properties=self.get_optimized_properties()
        )


    def load_small_table(self, spark: SparkSession, table_name: str) -> DataFrame:
        """Load small table using single connection"""

        return spark.read.jdbc(
            url=self.postgres_url,
            table=table_name,
            properties=self.get_optimized_properties()
        )


    def load_table_batch_with_connections(self, spark: SparkSession, table_name: str,
                                          id_column: str, start_id: int, end_id: int,
                                          num_connections: int) -> DataFrame:
        """Load a specific ID range using multiple connections"""

        query = f"(SELECT * FROM {table_name} WHERE {id_column} BETWEEN {start_id} AND {end_id}) as batch"

        return spark.read.jdbc(
            url=self.postgres_url,
            table=query,
            column=id_column,
            lowerBound=start_id,
            upperBound=end_id,
            numPartitions=num_connections,
            properties=self.get_optimized_properties()
        )

    def join_all_tables(self, tables: Dict[str, DataFrame]) -> DataFrame:
        """Join all loaded tables in optimal order"""

        # Start with findings (main table) and apply base filter
        result_df = tables["findings"].select(
            col("id").alias("finding_id"),
            col("package_name"),
            col("main_resource_id"),
            col("aggregation_group_id")
        ).filter(col("package_name").isNotNull())

        print(f"  Starting with findings: {result_df.count():,} rows")

        # Create aliased plain_resources DataFrame first
        plain_resources_df = tables["plain_resources"].select(
            col("id").alias("resource_id"),
            col("cloud_account").alias("root_cloud_account"),
            col("cloud_account_friendly_name").alias("root_cloud_account_friendly_name")
        )

        # Join with plain_resources (INNER JOIN) - use col() for both sides
        result_df = result_df.join(
            plain_resources_df,
            col("main_resource_id") == col("resource_id"),
            "inner"
        )
        print(f"  After plain_resources join: {result_df.count():,} rows")

        # Create aliased findings_scores DataFrame first
        findings_scores_df = tables["findings_scores"].select(
            col("finding_id").alias("score_finding_id"),
            col("severity")
        )

        # Join with findings_scores (INNER JOIN)
        result_df = result_df.join(
            findings_scores_df,
            col("finding_id") == col("score_finding_id"),
            "inner"
        )
        print(f"  After findings_scores join: {result_df.count():,} rows")

        # Create aliased user_status DataFrame first
        user_status_df = tables["user_status"].select(
            col("id").alias("user_status_id"),
            col("actual_status_key")
        )

        # Join with user_status (INNER JOIN)
        result_df = result_df.join(
            user_status_df,
            col("finding_id") == col("user_status_id"),
            "inner"
        )
        print(f"  After user_status join: {result_df.count():,} rows")

        # Create aliased statuses DataFrame first
        statuses_df = broadcast(tables["statuses"].select(
            col("key").alias("status_key")
        ))

        # Join with statuses (INNER JOIN) - broadcast small table
        result_df = result_df.join(
            statuses_df,
            col("actual_status_key") == col("status_key"),
            "inner"
        )
        print(f"  After statuses join: {result_df.count():,} rows")

        # Left outer joins for optional tables
        sla_connections_df = tables["finding_sla_rule_connections"].select(
            col("finding_id").alias("sla_connection_id")
        )

        result_df = result_df.join(
            sla_connections_df,
            col("finding_id") == col("sla_connection_id"),
            "left_outer"
        )

        additional_data_df = tables["findings_additional_data"].select(
            col("finding_id").alias("additional_data_id")
        )

        result_df = result_df.join(
            additional_data_df,
            col("finding_id") == col("additional_data_id"),
            "left_outer"
        )

        aggregation_groups_df = broadcast(tables["aggregation_groups"].select(
            col("id").alias("existing_group_id"),
            col("main_finding_id").alias("existing_main_finding_id"),
            col("group_identifier").alias("existing_group_identifier"),
            col("is_locked")
        ))

        result_df = result_df.join(
            aggregation_groups_df,
            col("aggregation_group_id") == col("existing_group_id"),
            "left_outer"
        )

        findings_info_df = tables["findings_info"].select(
            col("id").alias("findings_info_id")
        )

        result_df = result_df.join(
            findings_info_df,
            col("finding_id") == col("findings_info_id"),
            "left_outer"
        )

        return result_df


    def get_id_column_for_table(self, table_name: str) -> str:
        """Get the ID column name for each table"""
        id_columns = {
            "findings": "id",
            "findings_scores": "finding_id",
            "user_status": "id",
            "findings_info": "id",
            "findings_additional_data": "finding_id",
            "finding_sla_rule_connections": "finding_id",
            "plain_resources": "id",
            "statuses": "key",  # Different for statuses
            "aggregation_groups": "id",
            "aggregation_rules_findings_excluder": "finding_id"
        }
        return id_columns.get(table_name, "id")


    def get_table_id_bounds(self, spark: SparkSession, table_name: str, id_column: str) -> Tuple[int, int]:
        """Get min and max ID for a table"""

        bounds_query = f"(SELECT MIN({id_column}) as min_id, MAX({id_column}) as max_id FROM {table_name}) as bounds"

        bounds_df = spark.read.jdbc(
            url=self.postgres_url,
            table=bounds_query,
            properties=self.postgres_properties
        )

        row = bounds_df.collect()[0]
        return int(row['min_id']), int(row['max_id'])


    def get_empty_table_dataframe(self, spark: SparkSession, table_name: str) -> DataFrame:
        """Return empty DataFrame for a specific table"""
        empty_query = f"(SELECT * FROM {table_name} LIMIT 0) as empty_{table_name}"
        return spark.read.jdbc(
            url=self.postgres_url,
            table=empty_query,
            properties=self.postgres_properties
        )








    def safe_union_all_batches(self, batches: List[DataFrame]) -> DataFrame:
        """Safely union all batches using tree-reduction approach"""
        if not batches:
            return None

        if len(batches) == 1:
            return batches[0]

        try:
            print(f"    Using tree-reduction approach for {len(batches)} batches")

            # Tree-reduction: pair-wise union in stages to avoid deep recursion
            current_level: list[DataFrame] = batches[:]
            stage = 1

            while len(current_level) > 1:
                next_level = []
                pairs_count = (len(current_level) + 1) // 2
                print(f"      Stage {stage}: Reducing {len(current_level)} → {pairs_count} DataFrames")

                # Pair up DataFrames and union them
                for i in range(0, len(current_level), 2):
                    if i + 1 < len(current_level):
                        # Union two DataFrames
                        try:
                            combined = current_level[i].union(current_level[i + 1])
                            next_level.append(combined)
                        except Exception as e:
                            print(f"        Failed to union pair {i},{i + 1}: {e}")
                            # If union fails, keep first DataFrame
                            next_level.append(current_level[i])
                    else:
                        # Odd one out, carry forward
                        next_level.append(current_level[i])

                current_level = next_level
                stage += 1

                # Persist intermediate results for first few stages only
                if stage <= 4 and len(current_level) > 1:
                    print(f"        Persisting {len(current_level)} DataFrames at stage {stage}")
                    for df in current_level:
                        df.persist()

            print(f"    ✓ Tree-reduction completed in {stage - 1} stages")
            final_result = current_level[0]

            # Force evaluation to ensure everything is materialized
            final_count = final_result.count()
            print(f"    ✓ Final result: {final_count:,} rows")

            return final_result

        except Exception as e:
            print(f"    ❌ Tree-reduction failed: {e}")
            # Fallback: return first non-empty batch
            for batch in batches:
                try:
                    if batch.count() > 0:
                        print(f"    Fallback: returning first non-empty batch")
                        return batch
                except:
                    continue
            return None


    def get_id_bounds(self, spark: SparkSession) -> Tuple[int, int]:
        """Get min and max finding IDs"""
        bounds_query = "SELECT MIN(id) as min_id, MAX(id) as max_id FROM findings WHERE package_name IS NOT NULL"
        bounds_df = spark.read.jdbc(
            url=self.postgres_url,
            table=f"({bounds_query}) as bounds",
            properties=self.postgres_properties
        )

        bounds_row = bounds_df.collect()[0]
        return bounds_row["min_id"], bounds_row["max_id"]


    def get_optimized_properties(self) -> dict:
        """Get JDBC properties optimized for multi-connection batching"""
        optimized_properties = self.postgres_properties.copy()
        optimized_properties.update({
            "fetchsize": "50000",  # Fetch size for each connection
            "queryTimeout": "1800",  # 30 minute query timeout
            "loginTimeout": "120",  # 2 minute login timeout
            "socketTimeout": "1800",  # 30 minute socket timeout
            "tcpKeepAlive": "true",  # Keep connections alive
            "batchsize": "50000",  # Batch operations
            "stringtype": "unspecified"  # Handle PostgreSQL strings
        })
        return optimized_properties


    def get_join_query(self) -> str:
        """Load the raw join query from file"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = os.path.join(current_dir, "..", "data", "raw_join_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)

        with open(sql_file_path, "r") as f:
            content = f.read().strip()

        return content


    def get_empty_dataframe(self, spark: SparkSession) -> DataFrame:
        """Return empty DataFrame with correct schema"""
        empty_query = f"({self.get_join_query()}) as empty_result LIMIT 0"
        return spark.read.jdbc(
            url=self.postgres_url,
            table=empty_query,
            properties=self.postgres_properties
        )


