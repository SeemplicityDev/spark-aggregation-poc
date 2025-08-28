import json
import os
from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, collect_list, count, lit, concat_ws, coalesce, explode
from spark_aggregation_poc.config.config import Config


class AggregationServiceMultiRules():

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url
        self.groups_table = "group_finding_relation"
        self.group_cloud_accounts_table = "group_cloud_accounts"

    def aggregate(self, df: DataFrame) -> DataFrame:
        """
        Process rules from unilever_rules.json in order, saving results to PostgreSQL after each rule
        """
        print("=== Unilever PostgreSQL-Backed Aggregation ===")

        # Initialize tables
        self.initialize_tables(df.sparkSession)

        # Load rules
        rules = self.load_rules()
        print(f"Processing {len(rules)} rules")

        remaining_data = df
        total_groups_created = 0

        for rule_idx, rule in enumerate(rules, 1):
            print(f"\n--- Rule {rule_idx}/{len(rules)} ---")

            # 1. Exclude already processed finding_ids from PostgreSQL
            remaining_data = self.exclude_processed_findings(remaining_data)
            remaining_count = remaining_data.count()
            print(f"Available data after exclusions: {remaining_count:,} rows")

            if remaining_count == 0:
                print(f"Rule {rule_idx}: No remaining data to process")
                continue

            # 2. Apply finding_filter
            filtered_data = self.apply_sql_filter(remaining_data, rule['finding_filter'], rule_idx)
            filtered_count = filtered_data.count()
            print(f"Data after rule filter: {filtered_count:,} rows")

            if filtered_count == 0:
                print(f"Rule {rule_idx}: No data matches finding_filter")
                continue

            # 3. Group by specified columns
            group_columns = self.extract_group_columns(rule['group_by'])
            grouped_result = self.create_groups(filtered_data, group_columns, rule_idx)

            group_count = grouped_result.count()
            if group_count > 0:
                print(f"Rule {rule_idx}: Created {group_count} groups")

                # 4. Save groups and cloud accounts to separate tables
                self.save_groups_and_cloud_accounts_to_tables(grouped_result, rule_idx)

                total_groups_created += group_count
                print(f"Rule {rule_idx}: ✓ Saved groups and cloud accounts to PostgreSQL")

                # Show sample
                grouped_result.select("group_id", "count", "rule_number").show(3, truncate=False)
            else:
                print(f"Rule {rule_idx}: No groups created")

        # 5. Return final aggregated view from PostgreSQL
        print(f"\n=== Final Results ===")
        print(f"Total groups created across all rules: {total_groups_created}")

        return self.get_final_aggregated_view(df.sparkSession)

    def initialize_tables(self, spark: SparkSession):
        """Initialize/clear the tables"""
        try:
            print("Initializing PostgreSQL tables...")
            """
            # CREATE TABLE group_finding_relation (
            # finding_id BIGINT,
            # group_id VARCHAR(500),
            # rule_number INTEGER,
            # created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            # PRIMARY KEY (finding_id)
            # );
            """
            """
            # CREATE TABLE group_cloud_accounts (
            # group_id VARCHAR(500) PRIMARY KEY,
            # cloud_accounts TEXT[],  -- Array/list of cloud accounts
            # rule_number INTEGER,
            # created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            # FOREIGN KEY (group_id) REFERENCES group_finding_relation(group_id)
            # );
            """
            print("✓ Tables will be created automatically on first write")

        except Exception as e:
            print(f"⚠️ Table initialization warning: {e}")

    def exclude_processed_findings(self, df: DataFrame) -> DataFrame:
        """Exclude finding_ids that are already in the group_finding_relation table"""
        try:
            # Read processed finding_ids from PostgreSQL
            processed_findings_query = f"(SELECT DISTINCT finding_id FROM {self.groups_table}) as processed"

            processed_df = df.sparkSession.read.jdbc(
                url=self.postgres_url,
                table=processed_findings_query,
                properties=self.postgres_properties
            )

            processed_count = processed_df.count()
            if processed_count == 0:
                print("No previously processed findings found")
                return df

            print(f"Excluding {processed_count:,} previously processed findings")

            # Anti-join to exclude processed findings
            return df.join(processed_df, df.finding_id == processed_df.finding_id, "left_anti")

        except Exception as e:
            print(f"⚠️ Could not read processed findings (table might not exist yet): {e}")
            return df

    def load_rules(self) -> List[Dict]:
        """Load rules from JSON file"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        rules_path = os.path.join(current_dir, "..", "data", "unilever_rules.json")

        with open(os.path.normpath(rules_path), 'r') as f:
            return json.load(f)

    def apply_sql_filter(self, df: DataFrame, filter_condition: str, rule_idx: int) -> DataFrame:
        """Apply SQL WHERE condition"""
        try:
            temp_view = f"rule_{rule_idx}_data"
            df.createOrReplaceTempView(temp_view)

            query = f"SELECT * FROM {temp_view} WHERE {filter_condition}"
            return df.sparkSession.sql(query)
        except Exception as e:
            print(f"⚠️ Filter failed: {e}")
            return df.limit(0)

    def extract_group_columns(self, group_by_fields: List[str]) -> List[str]:
        """Convert JSON field names to DataFrame column names"""
        columns = []
        for field in group_by_fields:
            # Remove "findings." prefix
            clean_field = field.replace("findings.", "")
            columns.append(clean_field)
        return columns

    def create_groups(self, df: DataFrame, group_columns: List[str], rule_idx: int) -> DataFrame:
        """Group by columns and aggregate finding_ids + cloud_accounts as lists"""

        # Validate columns exist
        valid_columns = [col for col in group_columns if col in df.columns]

        if not valid_columns:
            print(f"No valid group columns: {group_columns}")
            return df.limit(0)

        print(f"Grouping by: {valid_columns}")

        # Group and aggregate
        return df.groupBy(*valid_columns).agg(
            collect_list("finding_id").alias("finding_ids"),
            collect_list("root_cloud_account").alias("cloud_accounts"),  # Aggregated as list
            count("finding_id").alias("count"),
            lit(rule_idx).alias("rule_number")
        ).withColumn(
            "group_id",
            concat_ws("_", *[coalesce(col(c).cast("string"), lit("null")) for c in valid_columns])
        )

    def save_groups_and_cloud_accounts_to_tables(self, grouped_df: DataFrame, rule_idx: int):
        """
        Save groups to group_finding_relation table and cloud accounts to separate table with group_id FK
        """

        # 1. Save group_finding_relation (flattened by finding_id)
        groups_flattened = grouped_df.select(
            explode("finding_ids").alias("finding_id"),
            col("group_id"),
            col("rule_number")
        )

        print(f"  Saving {groups_flattened.count():,} group mappings...")
        groups_flattened.write \
            .mode("append") \
            .option("createTableIfNotExists", "true") \
            .jdbc(
            url=self.postgres_url,
            table=self.groups_table,
            properties=self.postgres_properties
        )

        # 2. Save group_cloud_accounts (one row per group with cloud_accounts as array/list)
        group_cloud_accounts = grouped_df.select(
            col("group_id"),
            col("cloud_accounts"),  # This is already aggregated as a list
            col("rule_number")
        )

        print(f"  Saving {group_cloud_accounts.count():,} group cloud account lists...")
        group_cloud_accounts.write \
            .mode("append") \
            .option("createTableIfNotExists", "true") \
            .jdbc(
            url=self.postgres_url,
            table=self.group_cloud_accounts_table,
            properties=self.postgres_properties
        )

        print("  ✓ Successfully saved both tables to PostgreSQL")

    def get_final_aggregated_view(self, spark: SparkSession) -> DataFrame:
        """Get final aggregated view from PostgreSQL tables with JOIN"""
        try:
            print("Reading final results from PostgreSQL...")

            # Join both tables to get complete view
            final_query = f"""
            (SELECT 
                g.group_id,
                g.rule_number,
                COUNT(g.finding_id) as finding_count,
                ARRAY_AGG(g.finding_id) as finding_ids,
                c.cloud_accounts
             FROM {self.groups_table} g
             LEFT JOIN {self.group_cloud_accounts_table} c ON g.group_id = c.group_id
             GROUP BY g.group_id, g.rule_number, c.cloud_accounts
             ORDER BY g.rule_number, g.group_id) as final_groups
            """

            result_df = spark.read.jdbc(
                url=self.postgres_url,
                table=final_query,
                properties=self.postgres_properties
            )

            total_groups = result_df.count()
            if total_groups > 0:
                total_findings = result_df.agg({"finding_count": "sum"}).collect()[0][0]
                print(f"✓ Final aggregated view: {total_groups:,} groups containing {total_findings:,} findings")
            else:
                print("✓ No groups found in final results")

            return result_df

        except Exception as e:
            print(f"❌ Failed to read final results: {e}")
            return spark.sql("SELECT 1 as error").limit(0)