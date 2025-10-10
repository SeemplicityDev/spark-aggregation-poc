from datetime import datetime
from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import expr, col as spark_col, explode, concat_ws, coalesce, lit

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import FindingsAggregatorInterface, RuleLoaderInterface, \
    FilterConfigParserInterface, CatalogDalInterface
from spark_aggregation_poc.services.aggregation.rollup_util import RollupUtil
from spark_aggregation_poc.utils.aggregation_rules.rule_loader import AggregationRule


# Usage Example
class AggregationService(FindingsAggregatorInterface):
    """
    Your main aggregation service using engine rules
    """
    _allow_init = False

    @classmethod
    def create_aggregation_service(cls, config: Config, catalog_dal: CatalogDalInterface, rule_loader: RuleLoaderInterface, filters_config_parser: FilterConfigParserInterface):
        cls._allow_init = True
        result = AggregationService(config=config, catalog_dal=catalog_dal, rule_loader=rule_loader, filters_config_parser=filters_config_parser)
        cls._allow_init = False

        return result


    def __init__(self, config: Config, catalog_dal:CatalogDalInterface, rule_loader: RuleLoaderInterface, filters_config_parser: FilterConfigParserInterface):
        self.config = config
        self.catalog_dal = catalog_dal
        self.rule_loader = rule_loader
        self.filters_config_parser = filters_config_parser


    def aggregate_findings(self, spark:SparkSession, customer_id: Optional[int] = None) -> tuple[DataFrame, DataFrame]:
        findings_df = self.catalog_dal.read_base_findings(spark)

        spark_rules: list[AggregationRule] = self.rule_loader.load_aggregation_rules(spark, customer_id)
        print(f"Loaded {len(spark_rules)} aggregation rules")

        all_finding_group_rollup: List[DataFrame] = []
        all_finding_group_association: List[DataFrame] = []

        for rule_idx, rule in enumerate(spark_rules):
            start_time = datetime.now()
            print(f"Processing rule {rule_idx + 1}: ID={rule.id}, Order={rule.order}, Type={rule.rule_type} {start_time.strftime('%H:%M:%S')}")

            # Apply filters_config
            filtered_df = self.apply_filters_config_to_dataframe(
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
                df_finding_group_rollup, df_finding_group_association = self.create_groups(
                    filtered_df,
                    rule.group_by,
                    rule_idx
                )
                group_count = df_finding_group_rollup.count()
                if group_count > 0:
                    print(f"Rule {rule_idx + 1}: Created {group_count} groups")

                    all_finding_group_rollup.append(df_finding_group_rollup)
                    all_finding_group_association.append(df_finding_group_association)

                    # Show sample
                    df_finding_group_rollup.show(3)
                    df_finding_group_association.show(3)
                else:
                    print(f"Rule {rule_idx + 1}: No groups created")

        # Union all rules into single Dataframe (one for rollup and one for association)
        df_final_finding_group_rollup =  self.union_finding_group_rollup(all_finding_group_rollup, findings_df)
        df_final_finding_group_association = self.union_finding_group_association(all_finding_group_association, findings_df)

        return df_final_finding_group_rollup, df_final_finding_group_association

    def write_aggregated_findings(self, spark:SparkSession, df_final_finding_group_association: DataFrame, df_final_finding_group_rollup: DataFrame):
        self.catalog_dal.save_to_catalog(df_final_finding_group_association, "finding_group_association")
        self.catalog_dal.save_to_catalog(df_final_finding_group_rollup, "finding_group_rollup")


    def union_finding_group_rollup(self, all_finding_group_rollup: List[DataFrame], findings_df: DataFrame) -> DataFrame:
        if all_finding_group_rollup:
            print(f"Combining rollup results from {len(all_finding_group_rollup)} rules")

            # Union all rule results
            final_result = all_finding_group_rollup[0]
            for result in all_finding_group_rollup[1:]:
                #  allowMissingColumns=True because rollup df schema includes group by columns (which differ from rule to rule)
                final_result = final_result.unionByName(result,True)

            total_groups = final_result.count()
            total_findings = final_result.agg({"findings_count": "sum"}).collect()[0][0]
            print(f"✓ Final result: {total_groups:,} groups containing {total_findings:,} findings")

            return final_result
        else:
            print("❌ No group aggregations created by any rule")
            return findings_df.limit(0)


    def union_finding_group_association(self, all_finding_group_association: List[DataFrame], findings_df: DataFrame) -> DataFrame:
        if all_finding_group_association:
            print(f"Combining association results from {len(all_finding_group_association)} rules")

            # Union all rule results
            final_result = all_finding_group_association[0]
            for result in all_finding_group_association[1:]:
                final_result = final_result.union(result)

            total_findings = final_result.count()
            print(f"✓ Final result: {total_findings:,} findings")

            return final_result
        else:
            print("❌ No finding group association created by any rule")
            return findings_df.limit(0)

    def create_groups(self, df: DataFrame, group_columns: List[str], rule_idx: int) -> tuple[DataFrame, DataFrame]:
        """
        Your existing create_groups method enhanced with debugging
        """
        print(f"🔍 Rule {rule_idx} - Input DataFrame columns: {df.columns}")
        print(f"🔍 Rule {rule_idx} - Requested group_columns: {group_columns}")

        # Clean and validate group columns
        valid_columns = self.validate_and_clean_group_columns(df, group_columns)

        if not valid_columns:
            print(f"No valid group columns: {group_columns}")
            return df.limit(0), df.limit(0)

        print(f"🔍 Rule {rule_idx} - Valid columns after validation: {valid_columns}")

        # Check if all valid_columns exist in the DataFrame
        missing_columns = set(valid_columns) - set(df.columns)
        if missing_columns:
            print(f"❌ Rule {rule_idx} - Missing columns in DataFrame: {missing_columns}")
            print(f"Available columns: {df.columns}")
            raise ValueError(f"Missing columns: {missing_columns}")

        all_rollups: list[Column] = RollupUtil.get_basic_rollup(df, rule_idx)
        print(f"🔍 Rule {rule_idx} - Rollup columns: {[str(col) for col in all_rollups]}")

        try:
            df_finding_group_rollup: DataFrame = df.groupBy(*valid_columns).agg(
                *all_rollups
            ).withColumn(
                "group_id",
                concat_ws("_", *[coalesce(spark_col(column).cast("string"), lit("null")) for column in valid_columns])
            )

            print(f"✅ Rule {rule_idx} - Successfully created rollup with schema: {df_finding_group_rollup.columns}")

        except Exception as e:
            print(f"❌ Rule {rule_idx} - Error in groupBy/agg: {e}")
            print(f"DataFrame schema: {df.schema}")
            print(f"Valid columns: {valid_columns}")
            raise

        df_finding_group_association: DataFrame = self.create_finding_group_association(df_finding_group_rollup)

        return df_finding_group_rollup, df_finding_group_association


    def create_finding_group_association(self, df: DataFrame) -> DataFrame:
        result: DataFrame = df.select(
            spark_col("group_id").alias("group_id"),
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



    def apply_filters_config_to_dataframe(self, df: DataFrame, filters_config: Dict[str, Any]) -> DataFrame:
        """
        Apply filters_config directly to DataFrame

        Args:
            df: Input DataFrame
            filters_config: Filters configuration

        Returns:
            Filtered DataFrame
        """
        filter_condition = self.filters_config_parser.generate_filter_condition(filters_config)

        if filter_condition:
            print(f"Applying filters_config condition: {filter_condition}")
            return df.filter(expr(filter_condition))

        return df