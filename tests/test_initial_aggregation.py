"""Tests for initial aggregation functionality using SchemaRegistry and AggregationOutput"""
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

    def _assert_output_exists(self, output: AggregationOutput) -> None:
        """Helper: Assert that output exists and is correct type"""
        assert output is not None, "Expected AggregationOutput to be returned"
        assert isinstance(output, AggregationOutput), \
            f"Expected AggregationOutput, got {type(output)}"

    def _get_output_counts(self, output: AggregationOutput) -> tuple[int, int]:
        """
        Helper: Get counts from AggregationOutput.

        Returns:
            Tuple of (association_count, rollup_count)
        """
        association_count = output.get_association_count()
        rollup_count = output.get_rollup_count()

        print(f"📊 Association result count: {association_count}")
        print(f"📊 Rollup result count: {rollup_count}")

        return association_count, rollup_count

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

    def _validate_no_null_values(self, df: DataFrame, column_name: str) -> None:
        """Helper: Validate that a column has no null values"""
        null_count = df.filter(df[column_name].isNull()).count()
        assert null_count == 0, \
            f"Found {null_count} null values in column '{column_name}'"

    def _validate_positive_values(self, df: DataFrame, column_name: str) -> None:
        """Helper: Validate that numeric column has only positive values"""
        invalid_count = df.filter(df[column_name] <= 0).count()
        assert invalid_count == 0, \
            f"Found {invalid_count} non-positive values in column '{column_name}'"

    def _validate_non_empty_arrays(self, df: DataFrame, column_name: str) -> None:
        """Helper: Validate that array column has no empty arrays"""
        empty_count = df.filter(size(df[column_name]) == 0).count()
        assert empty_count == 0, \
            f"Found {empty_count} empty arrays in column '{column_name}'"

    def _validate_association_integrity(self, df: DataFrame) -> None:
        """Helper: Run all association-specific validations"""
        print("\n🔍 Validating association integrity...")

        self._validate_association_columns(df)
        self._validate_no_null_values(df, ColumnNames.GROUP_ID)
        self._validate_no_null_values(df, ColumnNames.FINDING_ID)

        print("✅ Association validation passed")

    def _validate_rollup_integrity(self, df: DataFrame) -> None:
        """Helper: Run all rollup-specific validations"""
        print("\n🔍 Validating rollup integrity...")

        self._validate_rollup_columns(df)
        self._validate_no_null_values(df, ColumnNames.GROUP_ID)
        self._validate_positive_values(df, ColumnNames.FINDINGS_COUNT)
        self._validate_non_empty_arrays(df, ColumnNames.FINDING_IDS)

        print("✅ Rollup validation passed")

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

        association_count = output.get_association_count()
        total_in_rollup = output.get_total_findings()

        assert association_count == total_in_rollup, \
            f"Association count ({association_count}) should equal sum of findings " \
            f"in rollup ({total_in_rollup})"

        # Create temp views for SQL validation
        output.create_temp_views("temp_association", "temp_rollup")

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

    def _validate_business_rules(
            self,
            rollup_count: int,
            expected_min_groups: int = 1
    ) -> None:
        """Helper: Validate high-level business rules"""
        print("\n🔍 Validating business rules...")

        assert rollup_count >= expected_min_groups, \
            f"Expected at least {expected_min_groups} aggregation groups, got {rollup_count}"

        print(f"✅ Business rules validated: {rollup_count} groups created")

    def _print_test_summary(
            self,
            output: AggregationOutput
    ) -> None:
        """Helper: Print final test summary"""
        rollup_count = output.get_rollup_count()
        association_count = output.get_association_count()

        print("\n" + "=" * 70)
        print("✅ Initial aggregation test passed successfully!")
        print("=" * 70)
        print(f"   📊 Created {rollup_count} aggregation groups")
        print(f"   📊 Associated {association_count} findings to groups")
        print(f"   📊 Total findings: {output.get_total_findings()}")
        print(f"   ✓  All validations using SchemaRegistry constants passed")
        print(f"   ✓  AggregationOutput type safety verified")
        print("=" * 70)

    # ==================== Main Test Methods ====================

    def test_initial_aggregation(self, spark, test_config):
        """
        Test the complete aggregation flow with AggregationOutput.

        This is the main integration test that validates:
        1. Aggregation execution returns AggregationOutput
        2. Output dataframe structure
        3. Data integrity
        4. Business rules
        5. Built-in validations
        """
        print("\n=== Running Initial Aggregation Test ===")
        self.setup_all_temp_views(spark)

        # Execute aggregation - returns AggregationOutput
        output: AggregationOutput = self._run_aggregation(spark, test_config)

        # Basic validations
        self._assert_output_exists(output)
        association_count, rollup_count = self._get_output_counts(output)

        # Use AggregationOutput convenience methods
        print("\n📊 Using AggregationOutput methods:")
        output.show_summary(verbose=True)
        output.show_sample_data(num_rows=5, truncate=False)

        # Validate data integrity (only if we have data)
        if not output.is_empty():
            self._validate_association_integrity(output.association)
            self._validate_rollup_integrity(output.rollup)
            self._validate_output_consistency(spark, output)

        # Validate business rules
        self._validate_business_rules(rollup_count, expected_min_groups=1)

        # Test built-in validations
        print("\n🔍 Testing built-in AggregationOutput validations...")
        try:
            output.validate_schemas()
            print("✅ Schema validation passed")
        except ValueError as e:
            raise AssertionError(f"Schema validation failed: {e}")

        try:
            output.validate_integrity()
            print("✅ Integrity validation passed")
        except ValueError as e:
            raise AssertionError(f"Integrity validation failed: {e}")

        # Print summary
        self._print_test_summary(output)

    def test_aggregation_output_properties(self, spark, test_config):
        """
        Test AggregationOutput properties and convenience methods.
        """
        print("\n=== Testing AggregationOutput Properties ===")
        self.setup_all_temp_views(spark)

        output: AggregationOutput = self._run_aggregation(spark, test_config)

        # Test property accessors
        assert output.association is not None
        assert output.rollup is not None
        assert output.finding_group_association is not None  # Alias
        assert output.finding_group_rollup is not None  # Alias

        # Test count methods
        assoc_count = output.get_association_count()
        rollup_count = output.get_rollup_count()
        total_findings = output.get_total_findings()

        print(f"📊 Counts via methods:")
        print(f"   - Associations: {assoc_count}")
        print(f"   - Groups: {rollup_count}")
        print(f"   - Total findings: {total_findings}")

        # Test is_empty
        is_empty = output.is_empty()
        assert isinstance(is_empty, bool)
        print(f"   - Is empty: {is_empty}")

        # Test to_dict
        output_dict = output.to_dict()
        assert isinstance(output_dict, dict)
        assert "rollup_count" in output_dict
        assert "association_count" in output_dict
        assert "total_findings" in output_dict
        assert "is_empty" in output_dict

        print(f"\n📋 Output as dict: {output_dict}")

        # Test __repr__
        repr_str = repr(output)
        assert "AggregationOutput" in repr_str
        print(f"\n📝 Representation: {repr_str}")

        print("\n✅ All AggregationOutput properties working correctly")

    def test_aggregation_output_temp_views(self, spark, test_config):
        """
        Test AggregationOutput temp view creation.
        """
        print("\n=== Testing AggregationOutput Temp View Creation ===")
        self.setup_all_temp_views(spark)

        output: AggregationOutput = self._run_aggregation(spark, test_config)

        # Create temp views with custom names
        output.create_temp_views("test_assoc_view", "test_rollup_view")

        # Verify views were created and are accessible
        assoc_df = spark.table("test_assoc_view")
        rollup_df = spark.table("test_rollup_view")

        assert assoc_df.count() == output.get_association_count()
        assert rollup_df.count() == output.get_rollup_count()

        print("✅ Temp views created and validated successfully")

    def test_aggregation_with_package_grouping(self, spark, test_config):
        """
        Test that package_name grouping works correctly using AggregationOutput.
        """
        print("\n=== Testing Package Grouping ===")
        self.setup_all_temp_views(spark)

        output: AggregationOutput = self._run_aggregation(spark, test_config)

        self._validate_package_grouping(output.rollup)

    def _validate_package_grouping(self, df_rollup: DataFrame) -> None:
        """Helper: Validate package-based grouping"""
        if ColumnNames.PACKAGE_NAME not in df_rollup.columns:
            print("⚠️  No package_name column found in rollup (might be from different rule)")
            return

        package_groups = df_rollup.filter(
            df_rollup[ColumnNames.PACKAGE_NAME].isNotNull()
        )

        if package_groups.count() == 0:
            print("⚠️  No groups with package_name found")
            return

        print(f"\n📦 Groups by package_name:")
        package_groups.select(
            ColumnNames.PACKAGE_NAME,
            ColumnNames.FINDINGS_COUNT
        ).show()

        distinct_packages = package_groups.select(
            ColumnNames.PACKAGE_NAME
        ).distinct().count()

        # We have pkg0, pkg1, pkg2, pkg3 = 4 packages in test data
        expected_packages = 4
        assert distinct_packages == expected_packages, \
            f"Expected {expected_packages} distinct packages, got {distinct_packages}"

        print(f"✅ Package grouping validated: {distinct_packages} distinct packages")

    def test_aggregation_output_schema_compliance(self, spark, test_config):
        """
        Test that AggregationOutput validates schema compliance.
        """
        print("\n=== Testing Schema Compliance ===")
        self.setup_all_temp_views(spark)

        output: AggregationOutput = self._run_aggregation(spark, test_config)

        # Schema validation should pass (was done in __init__)
        assert output.validate_schemas()

        print("✅ Schema compliance validated through AggregationOutput")

    def test_aggregation_no_duplicate_findings(self, spark, test_config):
        """
        Test that no finding appears in multiple groups.
        """
        print("\n=== Testing No Duplicate Findings ===")
        self.setup_all_temp_views(spark)

        output: AggregationOutput = self._run_aggregation(spark, test_config)

        self._validate_no_duplicate_findings(output.association)

    def _validate_no_duplicate_findings(self, df_association: DataFrame) -> None:
        """Helper: Validate no finding appears in multiple groups"""
        if df_association.count() == 0:
            print("⚠️  No associations to validate")
            return

        # Group by finding_id and count occurrences
        duplicates = df_association.groupBy(ColumnNames.FINDING_ID).count() \
            .filter("count > 1")

        duplicate_count = duplicates.count()

        if duplicate_count > 0:
            print("⚠️  Found findings in multiple groups:")
            duplicates.show()

        assert duplicate_count == 0, \
            f"Found {duplicate_count} findings that appear in multiple groups"

        print("✅ No duplicate findings validated")

    def test_aggregation_all_findings_assigned(self, spark, test_config):
        """
        Test that all eligible findings are assigned to a group.
        """
        print("\n=== Testing All Findings Assigned ===")
        self.setup_all_temp_views(spark)

        output: AggregationOutput = self._run_aggregation(spark, test_config)

        self._validate_all_findings_assigned(spark, output)

    def _validate_all_findings_assigned(
            self,
            spark: SparkSession,
            output: AggregationOutput
    ) -> None:
        """Helper: Validate all eligible findings are assigned"""
        # Get total eligible findings from base data
        findings_df = spark.table(TableNames.FINDINGS.value)

        # Filter for findings that should be aggregated (package_name is not null)
        eligible_findings = findings_df.filter(
            findings_df[ColumnNames.PACKAGE_NAME].isNotNull()
        ).count()

        assigned_findings = output.get_association_count()

        print(f"📊 Eligible findings: {eligible_findings}")
        print(f"📊 Assigned findings: {assigned_findings}")

        # All eligible findings should be assigned (based on our test rules)
        assert assigned_findings == eligible_findings, \
            f"Expected all {eligible_findings} eligible findings to be assigned, " \
            f"but only {assigned_findings} were assigned"

        print("✅ All eligible findings assigned to groups")

    def test_aggregation_output_with_validation_flags(self, spark, test_config):
        """
        Test AggregationOutput initialization with different validation flags.
        """
        print("\n=== Testing Validation Flags ===")
        self.setup_all_temp_views(spark)

        # Get raw dataframes
        aggregation_service = Factory.create_aggregator(test_config)
        aggregation_service.rule_loader = self.create_mock_rule_loader()

        # Note: This would need to be adjusted if aggregate_findings always returns AggregationOutput
        # For testing purposes, you might need to access the internal dataframes
        output_full = aggregation_service.aggregate_findings(spark=spark)

        # Test with validation disabled
        output_no_validation = AggregationOutput(
            finding_group_association=output_full.association,
            finding_group_rollup=output_full.rollup,
            validate_schema=False,
            validate_integrity=False
        )

        assert output_no_validation is not None
        print("✅ AggregationOutput created without automatic validation")

        # Now manually validate
        assert output_no_validation.validate_schemas()
        assert output_no_validation.validate_integrity()
        print("✅ Manual validation successful")

    def test_aggregation_empty_output(self, spark, test_config):
        """
        Test AggregationOutput behavior with empty results.
        """
        print("\n=== Testing Empty Output Handling ===")

        # Create empty temp views with correct schemas
        for table_name in [TableNames.FINDINGS, TableNames.PLAIN_RESOURCES,
                           TableNames.FINDINGS_SCORES, TableNames.USER_STATUS,
                           TableNames.STATUSES, TableNames.AGGREGATION_GROUPS,
                           TableNames.FINDING_SLA_RULE_CONNECTIONS,
                           TableNames.FINDINGS_ADDITIONAL_DATA,
                           TableNames.FINDINGS_INFO, TableNames.SCORING_RULES,
                           TableNames.SELECTION_RULES, TableNames.AGGREGATION_RULES]:
            schema = SchemaRegistry.get_schema_for_table(table_name)
            empty_df = spark.createDataFrame([], schema)
            empty_df.createOrReplaceTempView(table_name.value)

        output: AggregationOutput = self._run_aggregation(spark, test_config)

        # Should return valid AggregationOutput, not None
        self._assert_output_exists(output)

        # Should be empty
        assert output.is_empty(), "Expected empty output"
        assert output.get_association_count() == 0
        assert output.get_rollup_count() == 0
        assert output.get_total_findings() == 0

        # Show methods should handle empty gracefully
        output.show_summary()
        output.show_sample_data()

        # Schemas should still be valid
        assert output.validate_schemas()

        print("✅ Empty output handled gracefully with AggregationOutput")