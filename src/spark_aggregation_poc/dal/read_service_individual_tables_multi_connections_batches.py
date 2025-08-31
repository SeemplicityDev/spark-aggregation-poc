import os
from typing import List, Tuple, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col, broadcast

from spark_aggregation_poc.config.config import Config


class ReadServiceIndividualTablesMultiConnectionBatches:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url


    def read_findings_data(self, spark: SparkSession,
                                        large_table_batch_size: int = 3200000,
                                        connections_per_batch: int = 32) -> DataFrame:
        """
        Load tables separately based on size (L/M/S) and join them in Spark.

        Large tables (L): findings, findings_scores, user_status, findings_info, findings_additional_data
        Medium tables (M): finding_sla_rule_connections, plain_resources
        Small tables (S): statuses, aggregation_groups, aggregation_rules_findings_excluder
        """

        print("=== Loading Tables Separately by Size and Joining in Spark ===")

        # Define table categories
        large_tables = ["findings", "findings_scores", "user_status", "findings_info", "findings_additional_data"]
        medium_tables = ["finding_sla_rule_connections", "plain_resources"]
        small_tables = ["statuses", "aggregation_groups", "aggregation_rules_findings_excluder"]

        loaded_tables = {}

        # 1. Load Large Tables (L) - Use batched multi-connection approach
        print("\n--- Loading Large Tables (Batched Multi-Connection) ---")
        for table_name in large_tables:
            print(f"\nLoading large table: {table_name}")
            df = self.load_large_table_batched(spark, table_name, large_table_batch_size, connections_per_batch)
            df = df.persist()
            count = df.count()
            print(f"✓ {table_name}: {count:,} rows loaded and persisted")
            loaded_tables[table_name] = df

        # 2. Load Medium Tables (M) - Use simple multi-connection
        print("\n--- Loading Medium Tables (Multi-Connection) ---")
        for table_name in medium_tables:
            print(f"\nLoading medium table: {table_name}")
            df = self.load_medium_table(spark, table_name)
            df = df.persist()
            count = df.count()
            print(f"✓ {table_name}: {count:,} rows loaded and persisted")
            loaded_tables[table_name] = df

        # 3. Load Small Tables (S) - Use single connection + broadcast
        print("\n--- Loading Small Tables (Single Connection + Broadcast) ---")
        for table_name in small_tables:
            print(f"\nLoading small table: {table_name}")
            df = self.load_small_table(spark, table_name)
            df = df.persist()
            count = df.count()
            print(f"✓ {table_name}: {count:,} rows loaded and persisted")
            loaded_tables[table_name] = df

        # 4. Perform Spark joins in optimal order
        print("\n--- Performing Spark Joins ---")
        result_df = self.join_all_tables(loaded_tables)

        # Final persistence and count
        result_df = result_df.persist()
        final_count = result_df.count()
        print(f"✓ Final joined result: {final_count:,} rows")

        return result_df


    def load_large_table_batched(self, spark: SparkSession, table_name: str,
                                 batch_size: int, connections_per_batch: int) -> DataFrame:
        """Load large table using batched multi-connection approach"""

        # Get ID bounds
        id_column = self.get_id_column_for_table(table_name)
        min_id, max_id = self.get_table_id_bounds(spark, table_name, id_column)

        print(f"  {table_name} ID range: {min_id:,} to {max_id:,}")

        # Calculate batches
        total_range = max_id - min_id + 1
        num_batches = (total_range + batch_size - 1) // batch_size
        print(f"  Loading {table_name} in {num_batches} batches of {batch_size:,} each")

        batches = []

        for batch_num in range(num_batches):
            start_id = min_id + (batch_num * batch_size)
            end_id = min(start_id + batch_size - 1, max_id)

            print(f"    Batch {batch_num + 1}/{num_batches}: {id_column} {start_id:,} to {end_id:,}")

            batch_df = self.load_table_batch_with_connections(
                spark, table_name, id_column, start_id, end_id, connections_per_batch
            )

            # Force materialization to prevent thundering herd
            batch_df = batch_df.persist()
            batch_count = batch_df.count()
            print(f"      Loaded: {batch_count:,} rows")

            if batch_count > 0:
                batches.append(batch_df)

        # Union all batches using tree-reduction
        if len(batches) > 1:
            print(f"  Combining {len(batches)} {table_name} batches...")
            return self.safe_union_all_batches(batches)
        elif len(batches) == 1:
            return batches[0]
        else:
            return self.get_empty_table_dataframe(spark, table_name)


    def load_medium_table(self, spark: SparkSession, table_name: str) -> DataFrame:
        """Load medium table using simple multi-connection partitioning"""

        id_column = self.get_id_column_for_table(table_name)
        min_id, max_id = self.get_table_id_bounds(spark, table_name, id_column)

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

        # Join with plain_resources (INNER JOIN)
        result_df = result_df.join(
            tables["plain_resources"].select(
                col("id").alias("resource_id"),
                col("cloud_account").alias("root_cloud_account"),
                col("cloud_account_friendly_name").alias("root_cloud_account_friendly_name")
            ),
            result_df.main_resource_id == tables["plain_resources"].id,
            "inner"
        )
        print(f"  After plain_resources join: {result_df.count():,} rows")

        # Join with findings_scores (INNER JOIN)
        result_df = result_df.join(
            tables["findings_scores"].select(
                col("finding_id").alias("score_finding_id"),
                col("severity")
            ),
            result_df.finding_id == tables["findings_scores"].finding_id,
            "inner"
        )
        print(f"  After findings_scores join: {result_df.count():,} rows")

        # Join with user_status (INNER JOIN)
        result_df = result_df.join(
            tables["user_status"].select(
                col("id").alias("user_status_id"),
                col("actual_status_key")
            ),
            result_df.finding_id == tables["user_status"].id,
            "inner"
        )
        print(f"  After user_status join: {result_df.count():,} rows")

        # Join with statuses (INNER JOIN) - broadcast small table
        result_df = result_df.join(
            broadcast(tables["statuses"].select(
                col("key").alias("status_key")
            )),
            result_df.actual_status_key == tables["statuses"].key,
            "inner"
        )
        print(f"  After statuses join: {result_df.count():,} rows")

        # Left outer joins for optional tables
        result_df = result_df.join(
            tables["finding_sla_rule_connections"].select(
                col("finding_id").alias("sla_connection_id")
            ),
            result_df.finding_id == tables["finding_sla_rule_connections"].finding_id,
            "left_outer"
        )

        result_df = result_df.join(
            tables["findings_additional_data"].select(
                col("finding_id").alias("additional_data_id")
            ),
            result_df.finding_id == tables["findings_additional_data"].finding_id,
            "left_outer"
        )

        result_df = result_df.join(
            broadcast(tables["aggregation_groups"].select(
                col("id").alias("existing_group_id"),
                col("main_finding_id").alias("existing_main_finding_id"),
                col("group_identifier").alias("existing_group_identifier"),
                col("is_locked")
            )),
            result_df.aggregation_group_id == tables["aggregation_groups"].id,
            "left_outer"
        )

        result_df = result_df.join(
            tables["findings_info"].select(
                col("id").alias("findings_info_id")
            ),
            result_df.finding_id == tables["findings_info"].id,
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


    def read_findings_data(self, spark: SparkSession,
                           batch_size: int = 3200000,  # Total batch size across 4 connections
                           connections_per_batch: int = 32,
                           consolidation_frequency: int = 2000,
                           min_id_override: int = None) -> DataFrame:
        """
        Read data using PostgreSQL join query with multi-connection batching.
        Uses safe union methods to handle thousands of batches without recursion issues.
        """

        print(f"=== Reading data using multi-connection batching ===")
        print(f"Batch size: {batch_size:,} IDs per batch")
        print(f"Connections per batch: {connections_per_batch}")
        print(f"Consolidation frequency: every {consolidation_frequency} batches")

        # Get the raw join query
        raw_query = self.get_join_query()
        print("Base join query loaded from raw_join_query1.sql")

        # Get findings ID bounds for batching
        print("Getting findings ID bounds for batching...")
        min_id, max_id = self.get_id_bounds(spark)

        # Override min_id if specified for testing
        if min_id_override is not None:
            min_id = max(min_id, min_id_override)
            print(f"Testing mode: starting min ID from {min_id:,} to {max_id:,}")

        total_range = max_id - min_id + 1
        estimated_batches = (total_range + batch_size - 1) // batch_size

        print(f"Findings ID range: {min_id:,} to {max_id:,} ({total_range:,} IDs)")
        print(f"Estimated batches: {estimated_batches}")

        # Get optimized properties
        optimized_properties = self.get_optimized_properties()

        # Read in batches with safe consolidation
        batches = []
        current_lower = min_id
        batch_num = 1

        while current_lower <= max_id:
            current_upper = min(current_lower + batch_size - 1, max_id)

            print(f"  Reading batch {batch_num}: findings.id {current_lower:,} to {current_upper:,}")

            # Load this batch using multi-connection approach
            batch_df = self.load_batch_with_connections(
                spark, current_lower, current_upper, connections_per_batch,
                batch_num, raw_query, optimized_properties
            )

            if batch_df is not None:
                # batch_df = batch_df.persist()
                batch_count = batch_df.count()
                print(f"    Batch {batch_num} loaded: {batch_count:,} rows")

                if batch_count > 0:
                    batches.append(batch_df)
            else:
                print(f"    Batch {batch_num} failed, skipping")

            current_lower = current_upper + 1
            batch_num += 1

            # CRITICAL: Consolidate periodically to prevent recursion issues
            if len(batches) >= consolidation_frequency:
                print(f"    Consolidating {len(batches)} batches to prevent recursion...")
                consolidated_df = self.safe_union_all_batches(batches)  # ← Use tree-reduction
                if consolidated_df is not None:
                    batches = [consolidated_df]
                    print(f"    ✓ Consolidated to 1 batch")
                else:
                    print(f"    ⚠️ Consolidation failed, continuing with individual batches")

        # Final union of all batches using safe method
        if batches:
            print(f"  Safely combining {len(batches)} final batches...")
            result_df = self.safe_union_all_batches(batches)

            if result_df is not None:
                # Final optimization
                result_df = result_df.repartition(16, "package_name").cache()

                total_count = result_df.count()
                print(f"  ✓ Total joined data: {total_count:,} rows from PostgreSQL join query")

                # Show sample
                print("  Sample of joined data:")
                result_df.persist()
                result_df.show(5)

                return result_df
            else:
                print("  ❌ Failed to combine batches")
                return self.get_empty_dataframe(spark)
        else:
            print("  ⚠️  No data loaded")
            return self.get_empty_dataframe(spark)


    def load_batch_with_connections(self, spark: SparkSession, start_id: int, end_id: int,
                                    num_connections: int, batch_num: int, raw_query: str,
                                    properties: dict) -> DataFrame:
        """Load a single batch using JDBC partitioning for multiple connections"""

        try:
            # Create batched version of the join query
            batch_condition = f"findings.id BETWEEN {start_id} AND {end_id}"

            if "WHERE" in raw_query:
                batched_query = raw_query.replace(
                    "WHERE findings.package_name IS NOT NULL",
                    f"WHERE findings.package_name IS NOT NULL AND {batch_condition}"
                )
            else:
                batched_query = f"{raw_query} WHERE {batch_condition}"

            # Use JDBC partitioning to create multiple parallel connections
            batch_df = spark.read.jdbc(
                url=self.postgres_url,
                table=f"({batched_query}) as batch_{batch_num}",
                properties=properties,
                column="finding_id",
                lowerBound=start_id,
                upperBound=end_id,
                numPartitions=num_connections
            )

            return batch_df

        except Exception as e:
            print(f"    Error reading batch {batch_num}: {e}")

            # Try with smaller batch size on error
            if (end_id - start_id) > 50000:
                print(f"    Retrying batch {batch_num} with smaller size...")
                try:
                    smaller_batch_df = self.retry_with_smaller_batch(
                        spark, raw_query, start_id, end_id, batch_num, properties
                    )
                    return smaller_batch_df
                except Exception as retry_error:
                    print(f"    Retry also failed: {retry_error}")

            return None


    def retry_with_smaller_batch(self, spark: SparkSession, raw_query: str,
                                 start_id: int, end_id: int, batch_num: int,
                                 properties: dict) -> DataFrame:
        """Retry failed batch with smaller size"""
        smaller_size = (end_id - start_id) // 2
        smaller_end = start_id + smaller_size

        batch_condition = f"findings.id BETWEEN {start_id} AND {smaller_end}"

        if "WHERE" in raw_query:
            smaller_query = raw_query.replace(
                "WHERE findings.package_name IS NOT NULL",
                f"WHERE findings.package_name IS NOT NULL AND {batch_condition}"
            )
        else:
            smaller_query = f"{raw_query} WHERE {batch_condition}"

        return spark.read.jdbc(
            url=self.postgres_url,
            table=f"({smaller_query}) as batch_{batch_num}_retry",
            properties=properties
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


