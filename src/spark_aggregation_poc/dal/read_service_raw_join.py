import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import spark_partition_id, count, min as spark_min, max as spark_max, coalesce, collect_list, \
    when, col, array

from spark_aggregation_poc.config.config import Config


class ReadServiceRawJoin:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession, batch_size: int = 50000) -> DataFrame:
        """Read data using PostgreSQL join query in batches to avoid disk space issues"""

        print("=== Reading data using PostgreSQL join query in batches ===")

        # Get the raw join query
        raw_query = self.get_join_query()
        print("Base join query loaded from raw_join_query1.sql")

        # Get findings ID bounds for batching
        print("Getting findings ID bounds for batching...")
        bounds_query = "SELECT MIN(id) as min_id, MAX(id) as max_id FROM findings WHERE package_name IS NOT NULL"
        bounds_df = spark.read.jdbc(
            url=self.postgres_url,
            table=f"({bounds_query}) as bounds",
            properties=self.postgres_properties
        )

        bounds_row = bounds_df.collect()[0]
        min_id = bounds_row["min_id"]
        max_id = bounds_row["max_id"]

        print(f"Findings ID range: {min_id:,} to {max_id:,}")

        # Read in batches
        batches = []
        current_lower = min_id
        batch_num = 1

        while current_lower <= max_id:
            current_upper = min(current_lower + batch_size - 1, max_id)

            print(f"  Reading batch {batch_num}: findings.id {current_lower:,} to {current_upper:,}")

            # Create batched version of the join query
            # Add findings.id filter to the existing WHERE clause
            batch_condition = f"findings.id BETWEEN {current_lower} AND {current_upper}"

            # Replace the existing WHERE clause to include the batch condition
            if "WHERE" in raw_query:
                # Insert the batch condition with the existing WHERE clause
                batched_query = raw_query.replace(
                    "WHERE findings.package_name IS NOT NULL",
                    f"WHERE findings.package_name IS NOT NULL AND {batch_condition}"
                )
            else:
                # Add WHERE clause if it doesn't exist
                batched_query = f"{raw_query} WHERE {batch_condition}"

            try:
                # Read the batch with optimized properties
                # batch_properties = self.get_optimized_batch_properties()

                batch_df = spark.read.jdbc(
                    url=self.postgres_url,
                    table=f"({batched_query}) as batch_{batch_num}",
                    properties=self.postgres_properties
                )

                # Force execution and count
                batch_count = batch_df.count()
                print(f"    Batch {batch_num} loaded: {batch_count:,} rows")

                if batch_count > 0:
                    batches.append(batch_df)

                # Always move to next batch
                current_lower = current_upper + 1

            except Exception as e:
                print(f"    Error reading batch {batch_num}: {e}")
                # Try smaller batch on error
                if batch_size > 10000:
                    print(f"    Retrying with smaller batch size...")
                    try:
                        smaller_batch_df = self.retry_with_smaller_batch(
                            spark, raw_query, current_lower, current_upper, batch_num, batch_size // 2
                        )
                        if smaller_batch_df is not None:
                            smaller_count = smaller_batch_df.count()
                            print(f"    Retry batch {batch_num} loaded: {smaller_count:,} rows")
                            if smaller_count > 0:
                                batches.append(smaller_batch_df)
                    except Exception as retry_error:
                        print(f"    Retry also failed: {retry_error}")

                current_lower = current_upper + 1

            batch_num += 1

            # Memory management - consolidate every 10 batches
            if len(batches) > 0 and len(batches) % 10 == 0:
                print(f"    Consolidating {len(batches)} batches to manage memory...")
                batches = [self.consolidate_batches(batches)]

        # Union all batches
        if batches:
            print(f"  Combining {len(batches)} final batches...")
            result_df = batches[0]
            for batch_df in batches[1:]:
                result_df = result_df.union(batch_df)

            # Final optimization
            result_df = result_df.repartition(16, "package_name").cache()

            total_count = result_df.count()
            print(f"  ✓ Total joined data: {total_count:,} rows from PostgreSQL join query")

            # Show sample
            print("  Sample of joined data:")
            result_df.show(5, truncate=False)

            return result_df
        else:
            print("  ⚠️  No data loaded")
            # Return empty DataFrame with correct schema
            empty_query = f"({raw_query}) as empty_result LIMIT 0"
            return spark.read.jdbc(
                url=self.postgres_url,
                table=empty_query,
                properties=self.postgres_properties
            )

    def get_optimized_batch_properties(self):
        """Get optimized JDBC properties for batched reads"""
        batch_properties = self.postgres_properties.copy()
        batch_properties.update({
            "fetchsize": "50000",  # Larger fetch for complex joins
            "queryTimeout": "1800",  # 30 minute timeout
            "loginTimeout": "120",
            "tcpKeepAlive": "true",
            "socketTimeout": "1800",
            "batchsize": "50000",
            "stringtype": "unspecified"  # Handle PostgreSQL string types better
        })
        return batch_properties

    def retry_with_smaller_batch(self, spark, raw_query, current_lower, current_upper, batch_num, smaller_batch_size):
        """Retry failed batch with smaller batch size"""
        smaller_upper = min(current_lower + smaller_batch_size - 1, current_upper)

        smaller_batch_condition = f"findings.id BETWEEN {current_lower} AND {smaller_upper}"
        smaller_batched_query = raw_query.replace(
            "WHERE findings.package_name IS NOT NULL",
            f"WHERE findings.package_name IS NOT NULL AND {smaller_batch_condition}"
        )

        return spark.read.jdbc(
            url=self.postgres_url,
            table=f"({smaller_batched_query}) as batch_{batch_num}_retry",
            properties=self.postgres_properties
        )

    def consolidate_batches(self, batches):
        """Consolidate multiple batches into one to manage memory"""
        consolidated_df = batches[0]
        for batch_df in batches[1:]:
            consolidated_df = consolidated_df.union(batch_df)

        return consolidated_df.cache()

    def get_table_bounds(self, spark, table_name, id_column):
        """Get the min and max values for the given column in the table"""
        bounds_query = f"SELECT MIN({id_column}) as min_id, MAX({id_column}) as max_id FROM {table_name}"
        bounds_df = spark.read.jdbc(
            url=self.postgres_url,
            table=f"({bounds_query}) as bounds",
            properties=self.postgres_properties
        )

        bounds_row = bounds_df.collect()[0]
        return bounds_row["min_id"], bounds_row["max_id"]

    def read_table_in_batches(self, spark, table_name, id_column, batch_size, where_clause=""):
        """Read table in batches sequentially without skipping any ranges"""
        print(f"  Reading {table_name} with complete sequential batching...")

        # Get the actual bounds
        min_id, max_id = self.get_table_bounds(spark, table_name, id_column)
        print(f"    {table_name} ID range: {min_id:,} to {max_id:,}")

        batches = []
        current_lower = min_id
        batch_num = 1

        # Process every batch sequentially - NO SKIPPING
        while current_lower <= max_id:
            current_upper = min(current_lower + batch_size - 1, max_id)

            print(f"    Reading batch {batch_num}: {id_column} {current_lower:,} to {current_upper:,}")

            # Build WHERE clause using BETWEEN
            batch_condition = f"{id_column} BETWEEN {current_lower} AND {current_upper}"
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

                batch_count = batch_df.count()
                print(f"      Batch {batch_num} loaded: {batch_count:,} rows")

                # Always add batch to list, even if empty (for completeness)
                if batch_count > 0:
                    batches.append(batch_df)

                # ALWAYS move to next batch - no skipping logic
                current_lower = current_upper + 1

            except Exception as e:
                print(f"      Error reading batch {batch_num}: {e}")
                # Even on error, move to next batch - no skipping
                current_lower = current_upper + 1

            batch_num += 1

            # Optional: Progress logging for very large ranges
            if batch_num % 100 == 0:
                progress_pct = ((current_lower - min_id) / (max_id - min_id)) * 100
                print(f"    Progress: {progress_pct:.1f}% complete ({batch_num - 1} batches processed)")

        # Union all batches
        if batches:
            print(f"    Combining {len(batches)} batches for {table_name}...")
            result_df = batches[0]
            for batch_df in batches[1:]:
                result_df = result_df.union(batch_df)

            total_count = result_df.count()
            print(f"    ✓ {table_name} total: {total_count:,} rows from {len(batches)} batches")
            print(f"    ✓ Processed {batch_num - 1} total batches (including empty ones)")
            return result_df
        else:
            print(f"    ⚠️  No data loaded for {table_name} (all {batch_num - 1} batches were empty)")
            sample_query = f"SELECT * FROM {table_name} LIMIT 0"
            return spark.read.jdbc(
                url=self.postgres_url,
                table=f"({sample_query}) as empty_{table_name}",
                properties=self.postgres_properties
            )


    def get_join_query(self):
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to spark_aggregation_poc, then into data directory
        sql_file_path = os.path.join(current_dir, "..", "data", "raw_join_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)  # Clean up the path

        with open(sql_file_path, "r") as f:
            content = f.read()

        return content

