import json
import os
from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, collect_list, count, lit, concat_ws, coalesce, expr
)


class AggregationServiceMultiRulesNoWrite():

    def __init__(self):
        pass

    def aggregate(self, df: DataFrame) -> DataFrame:
        """
        Process rules from carlsberg_rules.json in order without filtering processed IDs
        """
        print("=== Carlsberg Rules-Based Aggregation (No ID Filtering) ===")

        # Load rules
        rules = self.load_rules()
        print(f"Processing {len(rules)} Carlsberg rules")

        all_results = []

        for rule_idx, rule in enumerate(rules, 1):
            print(f"\n--- Carlsberg Rule {rule_idx}/{len(rules)} ---")

            data_count = df.count()
            print(f"Processing full dataset: {data_count:,} rows")

            # 1. Apply finding_filter
            filtered_data = self.apply_sql_filter(df, rule['finding_filter'], rule_idx)
            filtered_count = filtered_data.count()
            print(f"Data after rule filter: {filtered_count:,} rows")

            if filtered_count == 0:
                print(f"Rule {rule_idx}: No data matches finding_filter")
                continue

            # 2. Group by specified columns
            group_columns = self.extract_group_columns(rule['group_by'])
            grouped_result = self.create_groups(filtered_data, group_columns, rule_idx)

            group_count = grouped_result.count()
            if group_count > 0:
                print(f"Rule {rule_idx}: Created {group_count} groups")

                # 3. Add to results (no ID filtering)
                all_results.append(grouped_result)

                # Show sample
                grouped_result.select("group_id", "count", "rule_number").show(3, truncate=False)
            else:
                print(f"Rule {rule_idx}: No groups created")

        # 4. Combine all results
        if all_results:
            print(f"\n=== Final Carlsberg Results ===")
            print(f"Combining results from {len(all_results)} rules")

            # Union all rule results
            final_result = all_results[0]
            for result in all_results[1:]:
                final_result = final_result.union(result)

            total_groups = final_result.count()
            total_findings = final_result.agg({"count": "sum"}).collect()[0][0]
            print(f"✓ Final result: {total_groups:,} groups containing {total_findings:,} findings")

            return final_result
        else:
            print("❌ No groups created by any rule")
            return df.limit(0)

    def load_rules(self) -> List[Dict]:
        """Load rules from carlsberg_rules.json file"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        rules_path = os.path.join(current_dir, "..", "data", "carlsberg_rules.json")

        with open(os.path.normpath(rules_path), 'r') as f:
            return json.load(f)

    def apply_sql_filter(self, df: DataFrame, filter_condition: str, rule_idx: int) -> DataFrame:
        """Apply SQL WHERE condition using expr()"""
        try:
            print(f"  Applying filter: {filter_condition[:100]}..." if len(
                filter_condition) > 100 else f"  Applying filter: {filter_condition}")

            # Use expr() to evaluate SQL expression directly on DataFrame
            filtered_df = df.filter(expr(filter_condition))

            rows_before = df.count()
            rows_after = filtered_df.count()
            print(f"  Filter result: {rows_before:,} → {rows_after:,} rows")

            return filtered_df

        except Exception as e:
            print(f"  ❌ Filter failed: {e}")
            print(f"  Using original data without filter")
            return df

    def extract_group_columns(self, group_by_fields: List[str]) -> List[str]:
        """Convert JSON field names to DataFrame column names"""
        columns = []
        for field in group_by_fields:
            # Remove "findings." prefix and map to actual column names
            clean_field = field.replace("findings.", "")

            # Map specific Carlsberg fields to actual DataFrame columns
            if clean_field == "cloud_account":
                columns.append("root_cloud_account")  # Assuming this is the actual column name
            elif clean_field == "main_resource_id":
                columns.append("main_resource_id")
            elif clean_field == "rule_family":
                columns.append("rule_family")
            elif clean_field == "rule_id":
                columns.append("rule_id")
            elif clean_field == "package_name":
                columns.append("package_name")
            else:
                # Try to use the field as-is
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
            collect_list("root_cloud_account").alias("cloud_accounts"),
            count("finding_id").alias("count"),
            lit(rule_idx).alias("rule_number")
        ).withColumn(
            "group_id",
            concat_ws("_", *[coalesce(col(c).cast("string"), lit("null")) for c in valid_columns])
        )