import os
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from spark_aggregation_poc.config.config import Config


class ReadServiceRawJoinMultiConnectionBatches:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession,
                           batch_size: int = 400000,  # Total batch size across 4 connections
                           connections_per_batch: int = 8,
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

    def safe_consolidate_batches(self, batches: List[DataFrame]) -> DataFrame:
        """Safely consolidate batches using iterative chunking approach"""
        if not batches:
            return None

        if len(batches) == 1:
            return batches[0]

        try:
            print(f"      Consolidating {len(batches)} batches iteratively...")

            # Process in small chunks to avoid deep recursion
            chunk_size = 8  # Small chunk size to be safe
            result: DataFrame = batches[0]

            for i in range(1, len(batches), chunk_size):
                chunk_end: int = min(i + chunk_size, len(batches))
                chunk: list[DataFrame] = batches[i:chunk_end]

                print(f"        Processing chunk {i}-{chunk_end - 1} ({len(chunk)} DataFrames)")

                # Union this chunk iteratively
                chunk_result: DataFrame = chunk[0]
                for j, batch_df in enumerate(chunk[1:], 1):
                    chunk_result = chunk_result.union(batch_df)

                # Union chunk result with main result
                result = result.union(chunk_result)

                # Persist every few chunks to break lineage and avoid memory buildup
                if i % (chunk_size * 3) == 1:  # Every ~24 batches
                    result = result.persist()
                    # Force evaluation to materialize
                    _ = result.count()
                    print(f"        Persisted intermediate result at chunk {i}")

            # Final persist and evaluation
            result = result.persist()
            final_count = result.count()
            print(f"      ✓ Consolidated to single DataFrame with {final_count:,} rows")

            return result

        except Exception as e:
            print(f"      ❌ Consolidation failed: {e}")
            return None

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



