import os
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from spark_aggregation_poc.config.config import Config


class ReadServiceRawJoinMultiConnection:
   

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def read_findings_data(self, spark: SparkSession, batch_size: int = 100000, num_connections: int = 4) -> DataFrame:
        """Read data using DataFrame JDBC partitioning for parallel connections"""

        print(f"=== Reading data using {num_connections} parallel DataFrame connections ===")

        # Get findings ID bounds
        print("Getting findings ID bounds...")
        min_id, max_id = self.get_id_bounds(spark)
        print(f"Findings ID range: {min_id:,} to {max_id:,}")

        # Create connection ranges
        connection_ranges = self.create_connection_ranges(min_id, max_id, num_connections)

        # Process each connection range as separate DataFrame operations
        all_dataframes = []

        for conn_id, (start_id, end_id) in enumerate(connection_ranges):
            print(f"Processing connection {conn_id}: IDs {start_id:,} to {end_id:,}")

            # Create DataFrame for this connection's range using JDBC partitioning
            connection_df = self.read_connection_range_with_partitioning(
                spark, conn_id, start_id, end_id, batch_size
            )

            if connection_df is not None:
                all_dataframes.append(connection_df)

        # Union all connection DataFrames
        if all_dataframes:
            print(f"Combining {len(all_dataframes)} connection DataFrames...")
            result_df = all_dataframes[0]
            for df in all_dataframes[1:]:
                result_df = result_df.union(df)

            # Final optimization
            result_df = result_df.repartition(16, "package_name").cache()

            total_count = result_df.count()
            print(f"✓ Total data loaded: {total_count:,} rows from {num_connections} parallel connections")

            return result_df
        else:
            print("⚠️ No data loaded from any connection")
            return self.get_empty_dataframe(spark)

    def read_connection_range_with_partitioning(self, spark: SparkSession, conn_id: int,
                                                start_id: int, end_id: int, batch_size: int) -> DataFrame:
        """Read a connection's ID range using JDBC partitioning for parallel execution"""

        try:
            # Create the query for this connection's range
            base_query = self.get_join_query()

            # Add the ID range filter
            range_condition = f"findings.id BETWEEN {start_id} AND {end_id}"
            if "WHERE" in base_query:
                connection_query = base_query.replace(
                    "WHERE findings.package_name IS NOT NULL",
                    f"WHERE findings.package_name IS NOT NULL AND {range_condition}"
                )
            else:
                connection_query = f"{base_query} WHERE {range_condition}"

            # Calculate number of partitions for this range
            range_size = end_id - start_id + 1
            num_partitions = max(1, min(8, range_size // batch_size))  # Cap at 8 partitions per connection

            print(f"  Connection {conn_id}: Using {num_partitions} partitions for range {start_id:,}-{end_id:,}")

            # Use JDBC partitioning to read this range in parallel
            connection_df = spark.read.jdbc(
                url=self.postgres_url,
                table=f"({connection_query}) as connection_{conn_id}_data",
                properties=self.postgres_properties,
                column="finding_id",  # Partition on finding_id
                lowerBound=start_id,
                upperBound=end_id,
                numPartitions=num_partitions
            )

            # Add metadata to track which connection this came from
            connection_df = connection_df.withColumn("_connection_id", lit(conn_id))

            # Force evaluation to ensure the query works
            row_count = connection_df.count()
            print(f"  Connection {conn_id}: Loaded {row_count:,} rows")

            return connection_df

        except Exception as e:
            print(f"  Connection {conn_id} failed: {e}")
            return None

    def read_findings_data_alternative(self, spark: SparkSession, batch_size: int = 50000,
                                       num_connections: int = 4) -> DataFrame:
        """Alternative approach using union of multiple JDBC reads"""

        print(f"=== Alternative: Reading data using {num_connections} union operations ===")

        # Get findings ID bounds
        min_id, max_id = self.get_id_bounds(spark)
        print(f"Findings ID range: {min_id:,} to {max_id:,}")

        # Create connection ranges
        connection_ranges = self.create_connection_ranges(min_id, max_id, num_connections)

        # Create a DataFrame for each connection range
        connection_dataframes = []

        for conn_id, (start_id, end_id) in enumerate(connection_ranges):
            print(f"Creating DataFrame for connection {conn_id}: IDs {start_id:,} to {end_id:,}")

            # Create query for this range
            base_query = self.get_join_query()
            range_condition = f"findings.id BETWEEN {start_id} AND {end_id}"

            if "WHERE" in base_query:
                range_query = base_query.replace(
                    "WHERE findings.package_name IS NOT NULL",
                    f"WHERE findings.package_name IS NOT NULL AND {range_condition}"
                )
            else:
                range_query = f"{base_query} WHERE {range_condition}"

            # Create DataFrame for this range (will be executed in parallel during union)
            range_df = spark.read.jdbc(
                url=self.postgres_url,
                table=f"({range_query}) as range_{conn_id}",
                properties=self.postgres_properties
            ).withColumn("_connection_id", lit(conn_id))

            connection_dataframes.append(range_df)

        # Union all DataFrames (Spark will execute them in parallel)
        print(f"Creating union of {len(connection_dataframes)} DataFrames...")
        result_df = connection_dataframes[0]
        for df in connection_dataframes[1:]:
            result_df = result_df.union(df)

        # Cache and force evaluation
        result_df = result_df.cache()
        total_count = result_df.count()
        print(f"✓ Total data loaded: {total_count:,} rows")

        return result_df

    def create_connection_ranges(self, min_id: int, max_id: int, num_connections: int) -> List[Tuple[int, int]]:
        """Create evenly distributed ID ranges for each connection"""
        ranges = []
        total_range = max_id - min_id + 1
        range_per_connection = total_range // num_connections
        remainder = total_range % num_connections

        current_start = min_id

        for i in range(num_connections):
            # Add one extra ID to first 'remainder' connections to distribute remainder evenly
            extra = 1 if i < remainder else 0
            range_size = range_per_connection + extra

            current_end = current_start + range_size - 1

            # Ensure last connection goes to actual max_id
            if i == num_connections - 1:
                current_end = max_id

            ranges.append((current_start, current_end))
            current_start = current_end + 1

        return ranges

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

    def get_join_query(self):
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to spark_aggregation_poc, then into data directory
        sql_file_path = os.path.join(current_dir, "..", "data", "raw_join_query1.sql")
        sql_file_path = os.path.normpath(sql_file_path)  # Clean up the path

        with open(sql_file_path, "r") as f:
            content = f.read()

        return content

