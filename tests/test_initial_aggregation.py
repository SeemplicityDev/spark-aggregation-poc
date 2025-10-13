"""Tests for initial aggregation functionality using SchemaRegistry and AggregationOutput"""
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import sum as spark_sum, size

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

    # ==================== Helper Methods ====================

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


    def _validate_association_columns(self, df: DataFrame) -> None:
        """Helper: Validate association dataframe has expected columns"""
        expected_columns = [
            ColumnNames.GROUP_ID,
            ColumnNames.FINDING_ID
        ]
        actual_cols = set(df.columns)

        for col in expected_columns:
            assert col in actual_cols, \
                f"Expected column '{col}' in association results. Found: {actual_cols}"

    def _validate_rollup_columns(self, df: DataFrame) -> None:
        """Helper: Validate rollup dataframe has expected columns"""
        expected_columns = [
            ColumnNames.GROUP_ID,
            ColumnNames.FINDING_IDS,
            ColumnNames.FINDINGS_COUNT,
            ColumnNames.CLOUD_ACCOUNTS,
            ColumnNames.RULE_NUMBER
        ]
        actual_cols = set(df.columns)

        for col in expected_columns:
            assert col in actual_cols, \
                f"Expected column '{col}' in rollup results. Found: {actual_cols}"




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
            SELECT r.{ColumnNames.GROUP_ID}, 
                   r.{ColumnNames.FINDINGS_COUNT} as expected_count,
                   COUNT(a.{ColumnNames.FINDING_ID}) as actual_count
            FROM temp_rollup r
            LEFT JOIN temp_association a 
                ON r.{ColumnNames.GROUP_ID} = a.{ColumnNames.GROUP_ID}
            GROUP BY r.{ColumnNames.GROUP_ID}, r.{ColumnNames.FINDINGS_COUNT}
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



    def test_initial_aggregation(self, spark, test_config):
        print("\n=== Running Initial Aggregation Test ===")
        self.setup_all_temp_views(spark)

        # Execute aggregation - returns AggregationOutput
        output: AggregationOutput = self._run_aggregation(spark, test_config)

        # Validate data integrity (only if we have data)
        if output.finding_group_association.count() > 0:
            self._validate_association_columns(output.finding_group_association)
            self._validate_rollup_columns(output.finding_group_rollup)
            self._validate_output_consistency(spark, output)

