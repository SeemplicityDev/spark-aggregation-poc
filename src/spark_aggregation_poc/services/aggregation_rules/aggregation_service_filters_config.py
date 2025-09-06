from datetime import datetime
from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, concat_ws, coalesce, explode

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.services.aggregation_rules.rule_loader import RuleLoader, SparkAggregationRule
from spark_aggregation_poc.services.aggregation_rules.spark_filters_config_processor import FiltersConfigProcessor
from spark_aggregation_poc.services.column_aggregation_util import ColumnAggregationUtil


# Usage Example
class AggregationServiceFiltersConfig:
    """
    Your main aggregation service using engine rules
    """

    def __init__(self, config: Config,  rule_loader: RuleLoader, filters_config_processor: FiltersConfigProcessor):
        self.config = config
        self.rule_loader = rule_loader
        self.filters_config_processor = filters_config_processor

    def aggregate(self, spark:SparkSession, findings_df: DataFrame = None, customer_id: Optional[int] = None) -> tuple[DataFrame, DataFrame]:
        if findings_df is None:
            findings_df = self.create_base_df(spark)
        # Load rules from database
        rules_df = self.rule_loader.load_aggregation_rules(spark, customer_id)
        print("loaded rules from DB")
        rules_df.show()
        spark_rules: list[SparkAggregationRule] = self.rule_loader.parse_rules_to_spark_format(rules_df)
        # print(f"transformed rules to spark format:\n {json.dumps(spark_rules, indent=4, default=str)}")

        print(f"Loaded {len(spark_rules)} aggregation rules")

        all_group_agg_columns: List[DataFrame] = []
        all_finding_group_relation: List[DataFrame] = []

        for rule_idx, rule in enumerate(spark_rules):
            start_time = datetime.now()
            print(f"Processing rule {rule_idx + 1}: ID={rule.id}, Order={rule.order}, Type={rule.rule_type} {start_time.strftime('%H:%M:%S')}")

            # Apply filters_config
            filtered_df = self.filters_config_processor.apply_filters_config_to_dataframe(
                findings_df,
                rule.filters_config
            )
            filtered_count = filtered_df.count()
            print(f"Data after rule filter: {filtered_count:,} rows")

            if filtered_count == 0:
                print(f"Rule {rule_idx}: No data matches finding_filter")
                continue

            if rule.group_by:
                # Apply grouping and aggregation
                df_group_agg_columns, df_finding_group_relation = self.create_groups_with_filters_config(
                    filtered_df,
                    rule.group_by,
                    rule_idx,
                    rule.filters_config
                )
                group_count = df_group_agg_columns.count()
                if group_count > 0:
                    print(f"Rule {rule_idx + 1}: Created {group_count} groups")

                    # 3. Add to results (no ID filtering)
                    all_group_agg_columns.append(df_group_agg_columns)
                    all_finding_group_relation.append(df_finding_group_relation)

                    # Show sample
                    df_group_agg_columns.show(3)
                    df_finding_group_relation.show(3)
                else:
                    print(f"Rule {rule_idx + 1}: No groups created")

        df_final_group_agg_columns =  self.union_group_agg_columns(all_group_agg_columns, findings_df)
        df_final_finding_group_relation = self.union_finding_group_relation(all_finding_group_relation, findings_df)

        return df_final_group_agg_columns, df_final_finding_group_relation


    def union_group_agg_columns(self, all_group_agg_columns, findings_df) -> DataFrame:
        if all_group_agg_columns:
            print(f"Combining results from {len(all_group_agg_columns)} rules")

            # Union all rule results
            final_result = all_group_agg_columns[0]
            for result in all_group_agg_columns[1:]:
                final_result = final_result.union(result)

            total_groups = final_result.count()
            total_findings = final_result.agg({"count": "sum"}).collect()[0][0]
            print(f"✓ Final result: {total_groups:,} groups containing {total_findings:,} findings")

            return final_result
        else:
            print("❌ No group aggregations created by any rule")
            return findings_df.limit(0)


    def union_finding_group_relation(self, all_finding_group_relation: List[DataFrame], findings_df: DataFrame) -> DataFrame:
        if all_finding_group_relation:
            print(f"Combining results from {len(all_finding_group_relation)} rules")

            # Union all rule results
            final_result = all_finding_group_relation[0]
            for result in all_finding_group_relation[1:]:
                final_result = final_result.union(result)

            total_findings = final_result.count()
            print(f"✓ Final result: {total_findings:,} findings")

            return final_result
        else:
            print("❌ No finding group relation created by any rule")
            return findings_df.limit(0)


    def create_groups_with_filters_config(self, df: DataFrame, group_columns: List[str], rule_idx: int,
                                          filters_config: Dict[str, Any]) -> tuple[DataFrame, DataFrame]:
        """
        Your existing create_groups method enhanced with filters_config
        """
        # Clean and validate group columns
        valid_columns = self.validate_and_clean_group_columns(df, group_columns)

        if not valid_columns:
            print(f"No valid group columns: {group_columns}")
            return df.limit(0)

        print(f"Grouping by: {valid_columns}")

        # # Group and aggregate using your EngineAggregationCalculator
        # # (This would use the class we discussed earlier)
        # return df.groupBy(*valid_columns).agg(
        #     collect_list("finding_id").alias("finding_ids"),
        #     collect_list("root_cloud_account").alias("cloud_accounts"),
        #     count("finding_id").alias("count"),
        #     lit(rule_idx).alias("rule_number")
        # ).withColumn(
        #     "group_id",
        #     concat_ws("_", *[coalesce(col(column).cast("string"), lit("null")) for column in valid_columns])
        # )

        all_aggregations = ColumnAggregationUtil.get_basic_aggregations(df, rule_idx)

        df_group_agg_columns: DataFrame = df.groupBy(*valid_columns).agg(
            *all_aggregations
        ).withColumn(
            "group_id",
            concat_ws("_", *[coalesce(col(column).cast("string"), lit("null")) for column in valid_columns])
        )

        df_finding_group_relation: DataFrame = self.create_finding_group_relation(df_group_agg_columns)

        return df_group_agg_columns, df_finding_group_relation


    def create_finding_group_relation(self, df: DataFrame) -> DataFrame:
        result: DataFrame = df.select(
            col("group_id").alias("group_id"),
            explode("finding_ids").alias("finding_id")
        )

        return result



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


    def create_base_df(self, spark: SparkSession) -> DataFrame:
        # After tables are saved to catalog, create a view from the raw join
        print("Creating base findings view from catalog tables...")
        # Create a temporary view using the raw_join_query1_unilever_bak.sql logic
        sql_str: str = """
               CREATE OR REPLACE TEMPORARY VIEW base_findings_view AS
               SELECT
                   findings.id as finding_id,
                   findings.package_name as package_name,
                   findings.main_resource_id,
                   findings.aggregation_group_id,
                   finding_sla_rule_connections.finding_id as sla_connection_id,
                   plain_resources.id as resource_id,
                   plain_resources.cloud_account as root_cloud_account,
                   plain_resources.cloud_account_friendly_name as root_cloud_account_friendly_name,
                   plain_resources.r1_resource_type as resource_type,
                   plain_resources.tags_values as tags_values,
                   plain_resources.tags_key_values as tags_key_values,
                   plain_resources.cloud_provider as cloud_provider,
                   findings_scores.finding_id as score_finding_id,
                   user_status.id as user_status_id,
                   user_status.actual_status_key,
                   findings_additional_data.finding_id as additional_data_id,
                   statuses.key as status_key,
                   aggregation_groups.id as existing_group_id,
                   aggregation_groups.main_finding_id as existing_main_finding_id,
                   aggregation_groups.group_identifier as existing_group_identifier,
                   aggregation_groups.is_locked,   
                   findings_info.id as findings_info_id,
                   findings.finding_type_str as finding_type,
                   findings.fix_subtype as fix_subtype,
                   statuses.category as category,
                   findings.fix_id as fix_id,
                   findings_additional_data.cve[1] as cve,
                   findings.fix_type as fix_type
               FROM general_data.default.findings
               LEFT OUTER JOIN general_data.default.finding_sla_rule_connections ON
                    findings.id = finding_sla_rule_connections.finding_id
               JOIN general_data.default.plain_resources ON
                   findings.main_resource_id = plain_resources.id
               JOIN general_data.default.findings_scores ON
                   findings.id = findings_scores.finding_id
               JOIN general_data.default.user_status ON
                   user_status.id = findings.id
               LEFT OUTER JOIN general_data.default.findings_additional_data ON
                   findings.id = findings_additional_data.finding_id
               JOIN general_data.default.statuses ON
                   statuses.key = user_status.actual_status_key
               LEFT OUTER JOIN general_data.default.aggregation_groups ON
                   findings.aggregation_group_id = aggregation_groups.id
               LEFT OUTER JOIN general_data.default.findings_info ON
                   findings_info.id = findings.id
               WHERE findings.package_name IS NOT NULL
               AND (findings.id <> aggregation_groups.main_finding_id
               OR findings.aggregation_group_id is null)
               """
        if self.config.is_databricks is False:
            sql_str = sql_str.replace("general_data.default.", "seemplicitytest.")

        spark.sql(sql_str)

        # Now read the view as a DataFrame
        df = spark.table("base_findings_view")
        return df