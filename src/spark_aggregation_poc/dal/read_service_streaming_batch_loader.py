from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


class StreamingBatchLoader:

    def __init__(self, config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data_streaming(self, spark: SparkSession,
                                     batch_size: int = 400000,
                                     max_batches_in_memory: int = 5) -> DataFrame:
        """
        Load PostgreSQL data using streaming to avoid memory accumulation.
        This solves your GC pressure issue by processing data incrementally.
        """

        print("=== Loading PostgreSQL data using streaming approach ===")

        # Get ID bounds
        min_id, max_id = self.get_id_bounds(spark)
        total_range = max_id - min_id + 1
        total_batches = (total_range + batch_size - 1) // batch_size

        print(f"ID range: {min_id:,} to {max_id:,}")
        print(f"Total batches: {total_batches}")
        print(f"Max batches in memory: {max_batches_in_memory}")

        # Create batch metadata for streaming
        batch_metadata = self.create_batch_metadata(spark, min_id, max_id, batch_size)

        # Convert to streaming source
        streaming_batches = spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", 1) \
            .load() \
            .withColumn("batch_index", col("value") % total_batches) \
            .join(batch_metadata, "batch_index") \
            .drop("timestamp", "value")

        # Process each batch using foreachBatch to control memory
        final_result_path = "/tmp/streaming_results"
        self.cleanup_result_path(final_result_path)

        def process_batch_metadata(batch_df: DataFrame, batch_id: int):
            """Process each batch and write results immediately"""
            self.process_single_batch_streaming(batch_df, batch_id, final_result_path)

        # Start streaming with controlled batch processing
        query = streaming_batches.writeStream \
            .foreachBatch(process_batch_metadata) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/streaming_checkpoint") \
            .trigger(processingTime="10 seconds") \
            .start()

        # Wait for completion
        query.awaitTermination(timeout=3600)  # 1 hour max
        query.stop()

        # Read final results
        final_df = spark.read.parquet(final_result_path)

        total_count = final_df.count()
        print(f"✓ Streaming load completed: {total_count:,} total rows")

        return final_df

    def process_single_batch_streaming(self, batch_metadata_df: DataFrame, batch_id: int, output_path: str):
        """Process a single batch and write immediately to avoid memory accumulation"""

        if batch_metadata_df.count() == 0:
            return

        # Get batch parameters from metadata
        batch_info = batch_metadata_df.collect()[0]
        start_id = batch_info["start_id"]
        end_id = batch_info["end_id"]
        batch_index = batch_info["batch_index"]

        print(f"  Processing batch {batch_index}: IDs {start_id:,} to {end_id:,}")

        try:
            # Load this batch from PostgreSQL (distributed)
            batch_df = self.load_single_batch_from_postgres(start_id, end_id, batch_index)

            if batch_df is not None:
                batch_count = batch_df.count()
                print(f"    Loaded {batch_count:,} rows")

                if batch_count > 0:
                    # Write immediately to disk (no memory accumulation)
                    batch_df.write \
                        .mode("append") \
                        .parquet(f"{output_path}/batch_{batch_index}")

                    print(f"    ✓ Batch {batch_index} written to disk")

                    # Force cleanup
                    batch_df.unpersist()
                else:
                    print(f"    Batch {batch_index} was empty")
            else:
                print(f"    ✗ Batch {batch_index} failed to load")

        except Exception as e:
            print(f"    ✗ Error processing batch {batch_index}: {e}")

    def load_single_batch_from_postgres(self, start_id: int, end_id: int, batch_index: int) -> DataFrame:
        """Load a single batch from PostgreSQL using distributed connections"""

        # Get the join query
        raw_query = self.get_join_query()

        # Add batch condition
        batch_condition = f"findings.id BETWEEN {start_id} AND {end_id}"
        if "WHERE" in raw_query:
            batched_query = raw_query.replace(
                "WHERE findings.package_name IS NOT NULL",
                f"WHERE findings.package_name IS NOT NULL AND {batch_condition}"
            )
        else:
            batched_query = f"{raw_query} WHERE {batch_condition}"

        # Get Spark session
        spark = SparkSession.getActiveSession()

        # Load with distributed JDBC connections
        try:
            batch_df = spark.read.jdbc(
                url=self.postgres_url,
                table=f"({batched_query}) as batch_{batch_index}",
                properties=self.get_optimized_properties(),
                column="finding_id",
                lowerBound=start_id,
                upperBound=end_id,
                numPartitions=4  # 4 parallel connections per batch
            )

            return batch_df

        except Exception as e:
            print(f"    Error loading batch {batch_index}: {e}")
            return None

    def create_batch_metadata(self, spark: SparkSession, min_id: int, max_id: int, batch_size: int) -> DataFrame:
        """Create metadata DataFrame for all batches"""

        batches_info = []
        current_start = min_id
        batch_index = 0

        while current_start <= max_id:
            current_end = min(current_start + batch_size - 1, max_id)

            batches_info.append((
                batch_index,
                current_start,
                current_end,
                current_end - current_start + 1
            ))

            current_start = current_end + 1
            batch_index += 1

        # Create DataFrame with batch metadata
        schema = StructType([
            StructField("batch_index", LongType()),
            StructField("start_id", LongType()),
            StructField("end_id", LongType()),
            StructField("range_size", LongType())
        ])

        return spark.createDataFrame(batches_info, schema)

    def read_findings_data_alternative_streaming(self, spark: SparkSession,
                                                 batch_size: int = 400000) -> DataFrame:
        """
        Alternative: Direct streaming without rate source.
        Process batches sequentially with immediate disk writes.
        """

        print("=== Alternative: Sequential batch processing with streaming writes ===")

        min_id, max_id = self.get_id_bounds(spark)
        output_path = "/tmp/sequential_results"
        self.cleanup_result_path(output_path)

        current_start = min_id
        batch_num = 1
        processed_batches = 0

        while current_start <= max_id:
            current_end = min(current_start + batch_size - 1, max_id)

            print(f"  Processing batch {batch_num}: IDs {current_start:,} to {current_end:,}")

            # Load single batch (distributed)
            batch_df = self.load_single_batch_from_postgres(current_start, current_end, batch_num)

            if batch_df is not None:
                batch_count = batch_df.count()
                print(f"    Loaded {batch_count:,} rows")

                if batch_count > 0:
                    # Write immediately (no memory accumulation)
                    batch_df.coalesce(4).write \
                        .mode("append") \
                        .parquet(f"{output_path}/batch_{batch_num}")

                    processed_batches += 1
                    print(f"    ✓ Written to disk ({processed_batches} batches completed)")

                    # Explicit cleanup
                    batch_df.unpersist()

                    # Force garbage collection hint every 10 batches
                    if batch_num % 10 == 0:
                        import gc
                        gc.collect()
                        print(f"    Memory cleanup performed at batch {batch_num}")

            current_start = current_end + 1
            batch_num += 1

        # Read all results from disk
        print(f"Reading {processed_batches} batch files from disk...")
        final_df = spark.read.parquet(output_path)

        total_count = final_df.count()
        print(f"✓ Sequential processing completed: {total_count:,} total rows")

        return final_df

    def get_optimized_properties(self) -> dict:
        """JDBC properties optimized for memory efficiency"""
        props = self.postgres_properties.copy()
        props.update({
            "fetchsize": "25000",  # Smaller fetch to reduce memory
            "queryTimeout": "900",  # 15 minute timeout
            "batchsize": "25000"  # Smaller batch operations
        })
        return props

    def cleanup_result_path(self, path: str):
        """Clean up result directory"""
        try:
            import shutil
            shutil.rmtree(path, ignore_errors=True)
            print(f"✓ Cleaned up {path}")
        except:
            pass

    def get_id_bounds(self, spark: SparkSession):
        """Get min/max finding IDs"""
        bounds_query = "SELECT MIN(id) as min_id, MAX(id) as max_id FROM findings WHERE package_name IS NOT NULL"
        bounds_df = spark.read.jdbc(
            url=self.postgres_url,
            table=f"({bounds_query}) as bounds",
            properties=self.postgres_properties
        )

        bounds_row = bounds_df.collect()[0]
        return bounds_row["min_id"], bounds_row["max_id"]

    def get_join_query(self) -> str:
        """Load raw join query"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = os.path.join(current_dir, "..", "data", "raw_join_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)

        with open(sql_file_path, "r") as f:
            return f.read().strip()