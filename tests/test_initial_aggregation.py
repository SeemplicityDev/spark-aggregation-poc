"""Tests for initial aggregation functionality using SchemaRegistry and AggregationOutput"""
import hashlib

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as spark_sum, size, md5
from pyspark.sql.types import StructType

from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsAggregatorInterface
from spark_aggregation_poc.models.aggregation_output import AggregationOutput
from spark_aggregation_poc.schemas.schema_registry import (
    ColumnNames, TableNames, SchemaRegistry
)
from tests.base.test_aggregation_base import TestAggregationBase


class TestInitialAggregation(TestAggregationBase):
    """
    Test class for initial aggregation functionality.
    Inherits all data setup methods from TestAggregationBase.
    """

    # ==================== Explicitly Request Base Fixture ====================

    @pytest.fixture(scope="class", autouse=True)
    def setup_class_with_validation(self, validated_test_environment):
        """
        Explicitly request the base class fixture.
        This ensures validated_test_environment runs for this test class.
        """
        # validated_test_environment fixture runs here
        yield
        # Cleanup happens after all tests


    def test_initial_aggregation(self, spark, test_config):
        print("\n=== Running Initial Aggregation Test ===")
        self.setup_all_temp_views(spark)

        # Execute aggregation - returns AggregationOutput
        output: AggregationOutput = self._run_aggregation(spark, test_config)

        # Validate data integrity (only if we have data)
        if output.finding_group_association.count() > 0:
            self._validate_columns_schema(output.finding_group_association, set(SchemaRegistry.get_schema_for_table(TableNames.FINDING_GROUP_ASSOCIATION).names))
            self._validate_columns_schema(output.finding_group_rollup, set(SchemaRegistry.get_schema_for_table(TableNames.FINDING_GROUP_ROLLUP).names))
            self._validate_output_consistency(spark, output)
            self._validate_correct_group_association(spark, output)


    def _validate_correct_group_association(self, spark: SparkSession, output: AggregationOutput):
        """
        Direct complete DataFrame comparison using subtract().

        Since group_id is deterministic (concat of group_by fields),
        we can hardcode the expected DataFrame with exact group_ids.
        """
        print("\n=== Complete DataFrame Subtract ===")

        # Run aggregation
        actual_df = output.finding_group_association

        # Hardcoded expected DataFrame with exact group_ids
        # group_id format: {group_by_columns joined by underscore}
        expected_data = [
            # Rule 0 (rule_idx=0): Group by package_name only
            (self.calculate_group_id(0, "pkg0"), "pkg0", 1),
            (self.calculate_group_id(0, "pkg1"), "pkg1", 2),
            (self.calculate_group_id(0, "pkg1"), "pkg1", 3),
            (self.calculate_group_id(0, "pkg2"), "pkg2", 4),
            (self.calculate_group_id(0, "pkg2"), "pkg2", 5),
            (self.calculate_group_id(0, "pkg2"), "pkg2", 6),
            (self.calculate_group_id(0, "pkg3"), "pkg3", 7),
            (self.calculate_group_id(0, "pkg3"), "pkg3", 8),
            (self.calculate_group_id(0, "pkg3"), "pkg3", 9),
            (self.calculate_group_id(0, "pkg3"), "pkg3", 10),

            # Rule 1 (rule_idx=1): Group by package_name + cloud_account
            (self.calculate_group_id(1, "pkg0-koko_account"), "pkg0_koko_account", 1),
            (self.calculate_group_id(1, "pkg1-koko_account"), "pkg1_koko_account", 2),
            (self.calculate_group_id(1, "pkg1-koko_account"), "pkg1_koko_account", 3),
            (self.calculate_group_id(1, "pkg2-koko_account"), "pkg2_koko_account", 4),
            (self.calculate_group_id(1, "pkg2-koko_account"), "pkg2_koko_account", 5),
            (self.calculate_group_id(1, "pkg2-koko_account"), "pkg2_koko_account", 6),
            (self.calculate_group_id(1, "pkg3-koko_account"), "pkg3_koko_account", 7),
            (self.calculate_group_id(1, "pkg3-koko_account"), "pkg3_koko_account", 8),
            (self.calculate_group_id(1, "pkg3-koko_account"), "pkg3_koko_account", 9),
            (self.calculate_group_id(1, "pkg3-koko_account"), "pkg3_koko_account", 10),
        ]


        # Create expected DataFrame with exact same schema
        expected_df = spark.createDataFrame(
            expected_data,
            schema=SchemaRegistry.finding_group_association_schema()
        )

        print(f"\n📊 Expected: {expected_df.count()} rows")
        print(f"📊 Actual: {actual_df.count()} rows")

        print("\n📋 Expected DataFrame:")
        expected_df.orderBy(ColumnNames.GROUP_IDENTIFIER, ColumnNames.FINDING_ID).show(50, truncate=False)

        print("\n📋 Actual DataFrame:")
        actual_df.orderBy(ColumnNames.GROUP_IDENTIFIER, ColumnNames.FINDING_ID).show(50, truncate=False)

        # Direct subtract - comparing COMPLETE DataFrames (both columns)!
        missing = expected_df.subtract(actual_df)
        extra = actual_df.subtract(expected_df)

        missing_count = missing.count()
        extra_count = extra.count()

        if missing_count > 0:
            print("\n❌ MISSING rows (in expected but NOT in actual):")
            missing.orderBy(ColumnNames.GROUP_IDENTIFIER, ColumnNames.FINDING_ID).show(truncate=False)

        if extra_count > 0:
            print("\n❌ EXTRA rows (in actual but NOT in expected):")
            extra.orderBy(ColumnNames.GROUP_IDENTIFIER, ColumnNames.FINDING_ID).show(truncate=False)

        assert missing_count == 0, f"Missing {missing_count} expected rows"
        assert extra_count == 0, f"Found {extra_count} unexpected rows"

        print("\n✅ PERFECT MATCH! Complete DataFrames are identical.")


    def calculate_group_id(self, rule_idx: int, *values: str) -> str:
        """
        Calculate group_id the same way as the aggregation service.
        Formula: md5(concat_ws("-", rule_idx, value1, value2, ...))
        """
        # Join values with "-" separator
        concatenated = "-".join([str(rule_idx)] + list(values))
        # Calculate MD5 hash
        return hashlib.md5(concatenated.encode()).hexdigest()



    def _run_aggregation(
            self,
            spark: SparkSession,
            test_config
    ) -> AggregationOutput:
        """
        Helper: Run aggregation with mocked rule loader.

        Returns:
            AggregationOutput: Typed output container with both dataframes
        """
        aggregation_service: FindingsAggregatorInterface = Factory.create_aggregator(test_config)
        aggregation_service.rule_loader = self.create_mock_rule_loader()

        return aggregation_service.aggregate_findings(spark=spark)


    def _validate_columns_schema(self, df: DataFrame, expected_columns: set[str]) -> None:
        actual_cols = set(df.columns)

        assert actual_cols == expected_columns



    def _validate_output_consistency(
            self,
            spark: SparkSession,
            output: AggregationOutput
    ) -> None:
        """
        Helper: Validate that association and rollup are consistent.

        Checks:
        1. Total findings match between association and rollup
        2. Each group in rollup has corresponding associations
        """
        print("\n🔍 Validating output consistency...")

        # Create temp views for SQL validation
        output.finding_group_association.createOrReplaceTempView("temp_association")
        output.finding_group_rollup.createOrReplaceTempView("temp_rollup")

        # Verify each group's findings_count matches actual associations
        mismatch_query = f"""
            SELECT r.{ColumnNames.GROUP_IDENTIFIER}, 
                   r.{ColumnNames.FINDINGS_COUNT} as expected_count,
                   COUNT(a.{ColumnNames.FINDING_ID}) as actual_count
            FROM temp_rollup r
            LEFT JOIN temp_association a 
                ON r.{ColumnNames.GROUP_IDENTIFIER} = a.{ColumnNames.GROUP_IDENTIFIER}
            GROUP BY r.{ColumnNames.GROUP_IDENTIFIER}, r.{ColumnNames.FINDINGS_COUNT}
            HAVING r.{ColumnNames.FINDINGS_COUNT} != COUNT(a.{ColumnNames.FINDING_ID})
        """

        mismatches = spark.sql(mismatch_query)
        mismatch_count = mismatches.count()

        if mismatch_count > 0:
            print("⚠️  Found mismatches between rollup and association:")
            mismatches.show()

        assert mismatch_count == 0, \
            f"Found {mismatch_count} groups where findings_count doesn't match actual associations"

        print("✅ Output consistency validated")

