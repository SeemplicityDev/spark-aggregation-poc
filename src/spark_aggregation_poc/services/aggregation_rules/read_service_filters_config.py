import os
from typing import List, Tuple, Set

from pyspark.sql import DataFrame, SparkSession

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.services.aggregation_rules.rule_loader import RuleLoader


class ReadServiceFiltersConfig():
   

    def __init__(self, config: Config, rule_loader: RuleLoader):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url
        self._customer = config.customer
        self.rule_loader = rule_loader

    def extract_columns_from_filters_config(self, spark: SparkSession) -> Set[str]:
        """Extract all column names referenced in FiltersConfig from all rules"""
        try:
            # Load aggregation rules
            rules_df = self.rule_loader.load_aggregation_rules(spark)
            spark_rules = self.rule_loader.parse_rules_to_spark_format(rules_df)

            referenced_columns = set()

            for rule in spark_rules:
                if rule.filters_config:
                    # Extract column names from filters_config
                    columns = self._extract_columns_from_filter_obj(rule.filters_config)
                    referenced_columns.update(columns)

                # Also extract from group_by fields
                if rule.group_by:
                    for group_field in rule.group_by:
                        # Remove table prefix (e.g., "findings.source" -> "source")
                        clean_field = group_field.split('.')[-1]
                        referenced_columns.add(clean_field)

            print(f"📋 Columns needed from FiltersConfig: {sorted(referenced_columns)}")
            return referenced_columns

        except Exception as e:
            print(f"⚠️ Could not extract columns from FiltersConfig: {e}")
            print("📋 Using default column set")
            # Return default columns if extraction fails
            return {"source", "category", "rule_family", "rule_id", "severity", "finding_type_str"}

    def _extract_columns_from_filter_obj(self, filter_obj: dict) -> Set[str]:
        """Recursively extract field names from a filter object"""
        columns = set()

        if isinstance(filter_obj, dict):
            # Check if this is a leaf filter condition
            if "field" in filter_obj:
                columns.add(filter_obj["field"])

            # Check for nested operands
            if "operands" in filter_obj and isinstance(filter_obj["operands"], list):
                for operand in filter_obj["operands"]:
                    columns.update(self._extract_columns_from_filter_obj(operand))

            # Check filtersjson
            if "filtersjson" in filter_obj:
                columns.update(self._extract_columns_from_filter_obj(filter_obj["filtersjson"]))

        return columns

    def build_dynamic_join_query(self, spark: SparkSession) -> str:
        """Build join query with columns dynamically determined from FiltersConfig"""

        # Get columns needed from FiltersConfig
        dynamic_columns = self.extract_columns_from_filters_config(spark)

        # Base columns that are always needed
        base_columns = [
            "findings.id as finding_id",
            "findings.package_name",
            "findings.main_resource_id",
            "findings.aggregation_group_id",
            "finding_sla_rule_connections.finding_id as sla_connection_id",
            "plain_resources.id as resource_id",
            "plain_resources.cloud_account as root_cloud_account",
            "plain_resources.cloud_account_friendly_name as root_cloud_account_friendly_name",
            "findings_scores.finding_id as score_finding_id",
            "user_status.id as user_status_id",
            "user_status.actual_status_key",
            "findings_additional_data.finding_id as additional_data_id",
            "statuses.key as status_key",
            "aggregation_groups.id as existing_group_id",
            "aggregation_groups.main_finding_id as existing_main_finding_id",
            "aggregation_groups.group_identifier as existing_group_identifier",
            "aggregation_groups.is_locked",
            "findings_info.id as findings_info_id"
        ]

        # Map dynamic columns to their SQL expressions
        column_mappings = {
            "source": "findings.source as source",
            "category": "findings.category as category",
            "rule_family": "findings.rule_family as rule_family",
            "rule_id": "findings.rule_id as rule_id",
            "severity": "findings_scores.severity as severity",
            "finding_type_str": "findings.finding_type_str as finding_type_str",
            "fix_subtype": "findings.fix_subtype as fix_subtype",
            "fix_id": "findings.fix_id as fix_id",
            "fix_type": "findings.fix_type as fix_type",
            "cve": "findings_additional_data.cve[1] as cve",
            "score": "findings_scores.score as score",
            "user_score": "findings_scores.user_score as user_score",
            "normalized_score": "findings_scores.normalized_score as normalized_score",
            "factorized_score": "findings_scores.factorized_score as factorized_score",
            "original_score": "findings.original_score as original_score",
            "last_reported_time": "findings.last_reported_time as last_reported_time",
            "discovered_time": "findings.discovered_time as discovered_time",
            "created_time": "findings.created_time as created_time"
        }

        # Add dynamic columns based on FiltersConfig needs
        dynamic_column_expressions = []
        for col_name in dynamic_columns:
            if col_name in column_mappings:
                dynamic_column_expressions.append(column_mappings[col_name])
            else:
                # Fallback: try to guess the table
                if col_name in ["score", "severity", "user_score", "normalized_score", "factorized_score"]:
                    dynamic_column_expressions.append(f"findings_scores.{col_name} as {col_name}")
                elif col_name in ["cve", "package_version", "fixed_versions"]:
                    dynamic_column_expressions.append(f"findings_additional_data.{col_name} as {col_name}")
                else:
                    # Default to findings table
                    dynamic_column_expressions.append(f"findings.{col_name} as {col_name}")

        # Combine all columns
        all_columns = base_columns + dynamic_column_expressions

        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in all_columns:
            col_alias = col.split(" as ")[-1] if " as " in col else col
            if col_alias not in seen:
                seen.add(col_alias)
                unique_columns.append(col)

        print(f"📊 Generated query with {len(unique_columns)} columns")
        print(f"🔧 Dynamic columns added: {sorted([col.split(' as ')[-1] for col in dynamic_column_expressions])}")

        # Build the complete query - fix f-string backslash issue
        column_separator = ',\n    '  # Move the separator outside the f-string
        columns_joined = column_separator.join(unique_columns)

        query = f"""
        SELECT
            {columns_joined}
        FROM findings
        LEFT OUTER JOIN finding_sla_rule_connections ON
            findings.id = finding_sla_rule_connections.finding_id
        JOIN plain_resources ON
            findings.main_resource_id = plain_resources.id
        JOIN findings_scores ON
            findings.id = findings_scores.finding_id
        JOIN user_status ON
            user_status.id = findings.id
        LEFT OUTER JOIN findings_additional_data ON
            findings.id = findings_additional_data.finding_id
        JOIN statuses ON
            statuses.key = user_status.actual_status_key
        LEFT OUTER JOIN aggregation_groups ON
            findings.aggregation_group_id = aggregation_groups.id
        LEFT OUTER JOIN findings_info ON
            findings_info.id = findings.id
        LEFT OUTER JOIN aggregation_rules_findings_excluder ON
            findings.id = aggregation_rules_findings_excluder.finding_id
        WHERE findings.package_name IS NOT NULL
        AND (findings.id <> aggregation_groups.main_finding_id
        OR findings.aggregation_group_id is null)"""

        return query.strip()

    def get_join_query(self, spark: SparkSession = None) -> str:
        """Load the join query - either from file or build dynamically"""

        # If spark session is provided, build dynamic query
        if spark is not None:
            try:
                return self.build_dynamic_join_query(spark)
            except Exception as e:
                print(f"⚠️ Could not build dynamic query: {e}")
                print("📁 Falling back to static query file")

        # Fallback: load from static file
        path: str = "raw_join_query1.sql"
        if self._customer == "Carlsberg":
            path: str = "raw_join_query1_carlsberg.sql"
        if self._customer == "Unilever":
            path: str = "raw_join_query1_unilever.sql"

        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = os.path.join(current_dir, "..", "data", path)
        sql_file_path = os.path.normpath(sql_file_path)

        with open(sql_file_path, "r") as f:
            content = f.read().strip()

        return content

    def read_findings_data(self, spark: SparkSession,
                           batch_size: int = 3200000,
                           connections_per_batch: int = 32,
                           min_id_override: int = None,
                           max_id_override: int = 50000000) -> DataFrame:
        """Read data using PostgreSQL join query with multi-connection batching."""
        from datetime import datetime

        start_time = datetime.now()
        print(f"🚀 Starting data load at: {start_time.strftime('%H:%M:%S')}")

        # Get ID bounds and apply overrides
        min_id, max_id = self.get_id_bounds(spark)

        if min_id_override:
            min_id = min_id_override
            print(f"🔧 Overriding min_id to: {min_id:,}")

        if max_id_override:
            original_max_id = max_id
            max_id = min(max_id, max_id_override)
            print(f"🔧 Overriding max_id to: {max_id:,} (original: {original_max_id:,})")

            if max_id_override < original_max_id:
                print(f"⚠️  Stopping early - will only process up to finding_id {max_id:,}")

        print(f"📊 Processing ID range: {min_id:,} to {max_id:,}")

        # Validate range
        if min_id > max_id:
            print(f"❌ Invalid ID range: min_id ({min_id:,}) > max_id ({max_id:,})")
            return spark.createDataFrame([], schema=None)

        # Load raw query with dynamic columns based on FiltersConfig
        print("🔄 Building dynamic join query based on FiltersConfig...")
        raw_query = self.get_join_query(spark)  # Pass spark session for dynamic query

        # Calculate batches based on the (possibly limited) range
        total_ids = max_id - min_id + 1
        total_batches = (total_ids + batch_size - 1) // batch_size
        print(f"📦 Total batches to process: {total_batches}")
        print(f"📈 Expected total records: ~{total_ids:,} IDs")

        # Process batches
        all_batches = []

        for batch_num in range(1, total_batches + 1):
            batch_start_time = datetime.now()

            start_id = min_id + (batch_num - 1) * batch_size
            end_id = min(start_id + batch_size - 1, max_id)

            print(f"\n--- Batch {batch_num}/{total_batches} ---")
            print(f"📥 Reading batch {batch_num}: findings.id {start_id:,} to {end_id:,}")

            # Load batch
            batch_df = self.load_batch_with_connections(
                spark, start_id, end_id, connections_per_batch, batch_num, raw_query, self.postgres_properties
            )

            if batch_df is not None:
                batch_count = batch_df.count()

                print(f"✅ Batch {batch_num} loaded and cached: {batch_count:,} rows")
                all_batches.append(batch_df)

                # Check if we've reached the max_id limit
                if end_id >= max_id:
                    print(f"🎯 Reached maximum finding_id limit ({max_id:,}) - stopping")
                    break

            else:
                print(f"⚠️ Batch {batch_num} returned no data")

        # Final union
        print(f"\n🔗 Combining {len(all_batches)} batches...")
        combine_start_time = datetime.now()

        if all_batches:
            final_df = self.safe_union_all_batches(all_batches)
            combine_duration = (datetime.now() - combine_start_time).total_seconds()

            total_duration = (datetime.now() - start_time).total_seconds()
            final_count = final_df.count()

            print(f"✅ Data loading completed at: {datetime.now().strftime('%H:%M:%S')}")
            print(f"⏱️ Total time: {total_duration / 60:.1f} minutes, Union time: {combine_duration:.1f}s")
            print(f"📊 Final dataset: {final_count:,} rows from {len(all_batches)} batches")

            if max_id_override:
                print(f"🎯 Limited to findings.id ≤ {max_id:,} (stopped early)")

            return final_df
        else:
            print("❌ No batches loaded successfully")
            return spark.createDataFrame([], schema=None)


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

            # CRITICAL FIX: Force immediate materialization to prevent thundering herd
            batch_df = batch_df.persist()
            actual_count = batch_df.count()  # This forces execution now, not later
            print(f"    Batch {batch_num} materialized: {actual_count:,} rows")

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


    def get_empty_dataframe(self, spark: SparkSession) -> DataFrame:
        """Return empty DataFrame with correct schema"""
        empty_query = f"({self.get_join_query()}) as empty_result LIMIT 0"
        return spark.read.jdbc(
            url=self.postgres_url,
            table=empty_query,
            properties=self.postgres_properties
        )



