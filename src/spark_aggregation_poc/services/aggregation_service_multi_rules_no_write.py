import json
import os
from datetime import datetime
from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, concat_ws, coalesce, expr
)

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.services.column_aggregation_util import ColumnAggregationUtil


class AggregationServiceMultiRulesNoWrite():

    def __init__(self, config: Config):
        self.config = config
        self._customer = config.customer
        pass

    def aggregate(self, df: DataFrame) -> DataFrame:
        """
        Process rules from unilever_rules.json in order without filtering processed IDs
        """
        print("=== Rules-Based Aggregation (No ID Filtering) ===")

        # Load rules
        rules = self.load_rules()
        print(f"Processing {len(rules)} Unilever rules")

        all_results = []

        for rule_idx, rule in enumerate(rules, 1):
            rule_start_time = datetime.now()
            print(f"🕐 [RULE START] Rule {rule_idx} started at: {rule_start_time.strftime('%H:%M:%S')}")

            print(f"\n--- Rule {rule_idx}/{len(rules)} ---")

            data_count = df.count()
            print(f"Processing full dataset: {data_count:,} rows")

            # 1. Apply finding_filter
            filtered_data = self.apply_sql_filter(df, rule, rule_idx)
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
            print(f"\n=== Final Results ===")
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
        """Load rules from unilever_rules.json file"""
        path: str = "unilever_rules.json"
        if self._customer == "Carlsberg":
            path: str = "carlsberg_rules.json"
        if self._customer == "Unilever":
            path: str = "unilever_rules.json"
        if self.config.is_databricks is False:
            path: str = "rules_local.json"

        current_dir = os.path.dirname(os.path.abspath(__file__))
        rules_path = os.path.join(current_dir, "..", "data", path)

        with open(os.path.normpath(rules_path), 'r') as f:
            return json.load(f)

    def apply_sql_filter(self, df: DataFrame, rule: dict, rule_idx: int) -> DataFrame:
        """Apply SQL WHERE condition using expr() after removing table prefixes"""
        try:
            if "finding_filter" not in rule:
                return df
            filter_condition: str = rule['finding_filter']
            # Remove table prefixes from the filter condition
            cleaned_filter = self.clean_filter_condition(filter_condition)

            print(f"  Original filter: {filter_condition[:100]}..." if len(
                filter_condition) > 100 else f"  Original filter: {filter_condition}")
            print(f"  Cleaned filter: {cleaned_filter[:100]}..." if len(
                cleaned_filter) > 100 else f"  Cleaned filter: {cleaned_filter}")

            # Use expr() to evaluate SQL expression directly on DataFrame
            filtered_df = df.filter(expr(cleaned_filter))

            rows_before = df.count()
            rows_after = filtered_df.count()
            print(f"  Filter result: {rows_before:,} → {rows_after:,} rows")

            return filtered_df

        except Exception as e:
            print(f"  ❌ Filter failed: {e}")
            print(f"  Using original data without filter")
            return df

    def clean_filter_condition(self, filter_condition: str) -> str:
        """
        Remove table prefixes from SQL filter condition - supports both Carlsberg and Unilever datasets

        Examples:
        - "findings.finding_type_str in ('Wiz Vulnerabilities')" -> "finding_type_str in ('Wiz Vulnerabilities')"
        - "findings_additional_Data.cve[1]" -> "cve"
        """
        cleaned = filter_condition

        # Handle special PostgreSQL syntax first (before removing table prefixes)
        special_cases = [
            # PostgreSQL array access with typo -> Spark column reference
            ('findings_additional_Data.cve[1]', 'cve'),
            ('findings_additional_data.cve[1]', 'cve'),
            # PostgreSQL JSON access -> Spark column reference
            ("findings_info.remediation ->> 'text'", 'remediation'),
            ('findings_info.remediation->>', 'remediation'),
            # Add other special PostgreSQL syntax cases as needed
        ]

        # Apply special case mappings first
        for old_syntax, new_syntax in special_cases:
            cleaned = cleaned.replace(old_syntax, new_syntax)

        # Define table prefixes to remove (supports both datasets)
        table_prefixes = [
            'findings.',
            'findings_scores.',
            'findings_additional_data.',
            'findings_additional_Data.',  # Handle the typo case too
            'plain_resources.',
            'user_status.',
            'statuses.',
            'aggregation_groups.',
            'findings_info.',
            'finding_sla_rule_connections.',
        ]

        # Remove table prefixes
        for prefix in table_prefixes:
            cleaned = cleaned.replace(prefix, '')

        # Handle column name mappings for both datasets
        column_mappings = [
            ('cloud_account=', 'root_cloud_account='),
            ('cloud_account ', 'root_cloud_account '),
            ('cloud_account,', 'root_cloud_account,'),
            ('cloud_account)', 'root_cloud_account)'),
            ('cloud_account_friendly_name=', 'root_cloud_account_friendly_name='),
            ('cloud_account_friendly_name ', 'root_cloud_account_friendly_name '),
            ('cloud_account_friendly_name,', 'root_cloud_account_friendly_name,'),
            ('cloud_account_friendly_name)', 'root_cloud_account_friendly_name)'),
        ]

        # Apply column mappings
        for old_pattern, new_pattern in column_mappings:
            cleaned = cleaned.replace(old_pattern, new_pattern)

        return cleaned

    def extract_group_columns(self, group_by_fields: List[str]) -> List[str]:
        """Convert JSON field names to DataFrame column names - supports both Carlsberg and Unilever fields"""
        columns = []
        for field in group_by_fields:
            # Remove "findings." prefix and map to actual column names
            clean_field = field.replace("findings.", "")

            # Map fields to actual DataFrame columns (supports both datasets)
            if clean_field == "cloud_account":
                columns.append("root_cloud_account")  # Common mapping for both datasets
            elif clean_field == "main_resource_id":
                columns.append("main_resource_id")
            elif clean_field == "rule_family":
                columns.append("rule_family")  # Available in Carlsberg SQL, may need handling for Unilever
            elif clean_field == "rule_id":
                columns.append("rule_id")  # Available in Carlsberg SQL, may need handling for Unilever
            elif clean_field == "package_name":
                columns.append("package_name")
            elif clean_field == "source":
                columns.append("source")  # Available in Carlsberg SQL
            elif clean_field == "severity":
                columns.append("severity")  # Available in Carlsberg SQL (findings_scores.severity)
            elif clean_field == "category":
                columns.append("category")  # Available in Carlsberg SQL (findings.category)
            else:
                # Try to use the field as-is for any other fields
                columns.append(clean_field)

        return columns

    def create_groups(self, df: DataFrame, group_columns: List[str], rule_idx: int) -> DataFrame:
        """Group by columns and aggregate finding_ids + cloud_accounts as lists"""

        # Validate columns exist
        valid_columns = [column for column in group_columns if column in df.columns]

        if not valid_columns:
            print(f"No valid group columns: {group_columns}")
            return df.limit(0)

        print(f"Grouping by: {valid_columns}")

        # Group and aggregate
        # all_aggregations = ColumnAggregationUtil.get_all_aggregations(df, rule_idx)
        all_aggregations = ColumnAggregationUtil.get_basic_aggregations(df, rule_idx)

        result: DataFrame = df.groupBy(*valid_columns).agg(
            *all_aggregations
            # collect_list("finding_id").alias("finding_ids"),
            # collect_list("root_cloud_account").alias("cloud_accounts"),
            # count("finding_id").alias("count"),
            # lit(rule_idx).alias("rule_number")
        ).withColumn(
            "group_id",
            concat_ws("_", *[coalesce(col(column).cast("string"), lit("null")) for column in valid_columns])
        )

        return result