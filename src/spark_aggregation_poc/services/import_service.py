from datetime import datetime
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import FindingsImporterInterface, CatalogDalInterface, \
    RelationalDalInterface
from spark_aggregation_poc.schemas.schemas import Schemas


class ImportService(FindingsImporterInterface):
    _allow_init = False

    @classmethod
    def create_import_service(cls, config: Config, relational_dal:RelationalDalInterface, catalog_dal: CatalogDalInterface):
        cls._allow_init = True
        result = ImportService(config, relational_dal, catalog_dal)
        cls._allow_init = False

        return result

    def __init__(self, config: Config, relational_dal:RelationalDalInterface, catalog_dal: CatalogDalInterface):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url
        self.config = config
        self.relational_dal = relational_dal
        self.catalog_dal = catalog_dal

    def import_findings_data(self, spark: SparkSession,
                             large_table_batch_size: int = 3200000,
                             connections_per_batch: int = 32,
                             max_id_override: int = None) -> None:
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
        small_tables = ["statuses", "aggregation_groups", "aggregation_rules", "aggregation_rules_findings_excluder", "scoring_rules", "selection_rules",
                        "resource_to_scopes", "scope_groups", "finding_ticket_associations", "tickets", "user_sla"]

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
            self.catalog_dal.save_to_catalog(df, table_name)

        # 3. Load Small Tables (S) - Use single connection + broadcast
        print("\n--- Loading Small Tables (Single Connection + Broadcast) ---")
        for table_name in small_tables:
            print(f"\nLoading small table: {table_name}")
            df = self.load_small_table(spark, table_name)
            self.catalog_dal.save_to_catalog(df, table_name)





    def load_large_table_batched(self, spark: SparkSession, table_name: str,
                                 batch_size: int, connections_per_batch: int,
                                 max_id_override: int = None):
        """Load large table using batched multi-connection approach"""

        # Get ID bounds
        id_column = Schemas.get_id_column_for_table(table_name)
        min_id, max_id = self.get_table_id_bounds(spark, table_name, id_column)

        # Handle empty tables
        if min_id == 0 and max_id == 0:
            print(f"  {table_name} is empty, skipping...")
            return

        max_id = self.apply_max_id_override(max_id, max_id_override, min_id, table_name)

        # Calculate batches
        total_range = max_id - min_id + 1
        num_batches = (total_range + batch_size - 1) // batch_size
        print(f"  Loading {table_name} in {num_batches} batches of {batch_size:,} each")

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
            self.catalog_dal.save_to_catalog(batch_df, table_name)


    def apply_max_id_override(self, max_id, max_id_override, min_id, table_name):
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
        return max_id

    def load_medium_table(self, spark: SparkSession, table_name: str,
                          max_id_override: int = None) -> DataFrame:
        """Load medium table using simple multi-connection partitioning"""

        id_column = Schemas.get_id_column_for_table(table_name)
        min_id, max_id = self.get_table_id_bounds(spark, table_name, id_column)

        # Handle empty tables
        if min_id == 0 and max_id == 0:
            print(f"  {table_name} is empty, returning empty DataFrame...")
            return self.get_empty_table_dataframe(spark, table_name)

        # Apply max_id_override if provided
        if max_id_override is not None:
            original_max_id = max_id
            max_id = min(max_id, max_id_override)
            if max_id < original_max_id:
                print(f"  {table_name} ID range limited by override: {min_id:,} to {max_id:,} (original max: {original_max_id:,})")

        return self.relational_dal.query_with_multiple_connections(
            spark,
            num_connections=4,
            query=table_name,
            id_column=id_column,
            start_id=min_id,
            end_id=max_id
        )


    def load_small_table(self, spark: SparkSession, table_name: str) -> DataFrame:
        """Load small table using single connection"""
        return self.relational_dal.query(spark, table_name)


    def load_table_batch_with_connections(self, spark: SparkSession, table_name: str,
                                          id_column: str, start_id: int, end_id: int,
                                          num_connections: int) -> DataFrame:
        """Load a specific ID range using multiple connections"""

        query = f"(SELECT * FROM {table_name} WHERE {id_column} BETWEEN {start_id} AND {end_id}) as batch"

        return self.query_with_multiple_connections(spark=spark, num_connections=num_connections, query=query, id_column=id_column, start_id=start_id, end_id=end_id)

    def query_with_multiple_connections(self, spark: SparkSession, num_connections: int, query: str, id_column: str, start_id: int, end_id: int) -> DataFrame:
        return self.relational_dal.query_with_multiple_connections(
            spark,
            num_connections=num_connections,
            query=query,
            id_column=id_column,
            start_id=start_id,
            end_id=end_id
        )



    def get_table_id_bounds(self, spark: SparkSession, table_name: str, id_column: str) -> Tuple[int, int]:
        """Get min and max ID for a table"""

        bounds_query = f"(SELECT MIN({id_column}) as min_id, MAX({id_column}) as max_id FROM {table_name}) as bounds"

        bounds_df = self.relational_dal.query(spark=spark, query=bounds_query)

        row = bounds_df.collect()[0]
        min_id = row['min_id']
        max_id = row['max_id']

        # Handle empty tables (MIN/MAX return NULL)
        if min_id is None or max_id is None:
            print(f"⚠️  Table {table_name} appears to be empty (min_id={min_id}, max_id={max_id})")
            return 0, 0  # Return default values for empty tables

        return int(min_id), int(max_id)



    def get_empty_table_dataframe(self, spark: SparkSession, table_name: str) -> DataFrame:
        """Return empty DataFrame for a specific table"""
        empty_query = f"(SELECT * FROM {table_name} LIMIT 0) as empty_{table_name}"
        return self.relational_dal.query(spark=spark, query=empty_query)







