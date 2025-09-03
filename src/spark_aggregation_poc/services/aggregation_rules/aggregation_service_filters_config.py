from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, collect_list, count, lit, concat_ws, coalesce

from spark_aggregation_poc.services.aggregation_rules.rule_loader import RuleLoader
from spark_aggregation_poc.services.aggregation_rules.spark_filters_config_processor import FiltersConfigProcessor


# Usage Example
class AggregationServiceFiltersConfig:
    """
    Your main aggregation service using engine rules
    """

    def __init__(self,  rule_loader: RuleLoader, filters_config_processor: FiltersConfigProcessor):
        self.rule_loader = rule_loader
        self.filters_config_processor = filters_config_processor

    def aggregate(self, spark:SparkSession, findings_df: DataFrame,
                                           customer_id: Optional[int] = None) -> DataFrame:
        """
        Process findings using engine aggregation rules

        Args:
            findings_df: DataFrame with findings data
            customer_id: Optional customer filter

        Returns:
            Aggregated findings DataFrame
        """
        # Load rules from database
        rules_df = self.rule_loader.load_aggregation_rules(spark, customer_id)
        print("loaded rules from DB")
        rules_df.show()
        spark_rules = self.rule_loader.parse_rules_to_spark_format(rules_df)
        print(f"transformed rules to spark format:\n {spark_rules}")

        print(f"Loaded {len(spark_rules)} aggregation rules")

        aggregated_results = []

        for rule_idx, rule in enumerate(spark_rules):
            print(f"Processing rule {rule_idx + 1}: ID={rule.id}, Type={rule.rule_type}")

            # Apply filters_config
            filtered_df = self.filters_config_processor.apply_filters_config_to_dataframe(
                findings_df,
                rule.filters_config
            )

            if rule.group_by:
                # Apply grouping and aggregation
                grouped_df = self.create_groups_with_filters_config(
                    filtered_df,
                    rule.group_by,
                    rule_idx,
                    rule.filters_config
                )
                aggregated_results.append(grouped_df)

        # Union all results
        if aggregated_results:
            if len(aggregated_results) == 1:
                return aggregated_results[0]
            else:
                # Chain unionByName operations - each call only takes one DataFrame
                result = aggregated_results[0]
                for df in aggregated_results[1:]:
                    result = result.unionByName(df)
                return result
        else:
            return findings_df.limit(0)  # Empty DataFrame

    def create_groups_with_filters_config(self, df: DataFrame, group_columns: List[str], rule_idx: int,
                                          filters_config: Dict[str, Any]) -> DataFrame:
        """
        Your existing create_groups method enhanced with filters_config
        """
        # Clean and validate group columns
        valid_columns = self.validate_and_clean_group_columns(df, group_columns)

        if not valid_columns:
            print(f"No valid group columns: {group_columns}")
            return df.limit(0)

        print(f"Grouping by: {valid_columns}")

        # Group and aggregate using your EngineAggregationCalculator
        # (This would use the class we discussed earlier)
        return df.groupBy(*valid_columns).agg(
            collect_list("finding_id").alias("finding_ids"),
            collect_list("root_cloud_account").alias("cloud_accounts"),
            count("finding_id").alias("count"),
            lit(rule_idx).alias("rule_number")
        ).withColumn(
            "group_id",
            concat_ws("_", *[coalesce(col(column).cast("string"), lit("null")) for column in valid_columns])
        )

    def clean_group_columns(self, group_columns: List[str]) -> List[str]:
        """
        Remove table prefixes from group column names

        Args:
            group_columns: List of column names with potential table prefixes

        Returns:
            List of cleaned column names
        """
        cleaned_columns = []

        for column in group_columns:
            # Remove table prefixes (e.g., "findings.package_name" -> "package_name")
            if '.' in column:
                cleaned_column = column.split('.')[-1]  # Take the part after the last dot
            else:
                cleaned_column = column

            cleaned_columns.append(cleaned_column)

        return cleaned_columns

    def validate_and_clean_group_columns(self, df: DataFrame, group_columns: List[str]) -> List[str]:
        """
        Clean group columns and validate they exist in DataFrame

        Args:
            df: Input DataFrame
            group_columns: Raw group columns from engine rules

        Returns:
            List of valid, cleaned column names
        """
        # Clean the column names first
        cleaned_columns = self.clean_group_columns(group_columns)

        # Validate columns exist in DataFrame
        df_columns = set(df.columns)
        valid_columns = [col for col in cleaned_columns if col in df_columns]

        if len(valid_columns) != len(cleaned_columns):
            missing_columns = set(cleaned_columns) - set(valid_columns)
            print(f"Missing group columns after cleaning: {missing_columns}")
            print(f"Original group columns: {group_columns}")
            print(f"Cleaned group columns: {cleaned_columns}")
            print(f"Available DataFrame columns: {sorted(df_columns)}")

        return valid_columns