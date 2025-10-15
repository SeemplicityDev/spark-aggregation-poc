"""Tests for initial aggregation functionality using SchemaRegistry and AggregationOutput"""
import json
from unittest.mock import Mock

from pyspark.sql import SparkSession

from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsAggregatorInterface
from spark_aggregation_poc.models.aggregation_output import AggregationOutput
from spark_aggregation_poc.models.spark_aggregation_rules import AggregationRule
from spark_aggregation_poc.schemas.schema_registry import (
    ColumnNames, TableNames, SchemaRegistry
)
from tests.base.test_aggregation_base import TestAggregationBase


class TestInitialAggregation(TestAggregationBase):

    def test_initial_aggregation(self, spark, test_config):
        print("\n=== Running Initial Aggregation Test ===")

        output: AggregationOutput = self.run_aggregation(spark, test_config)

        # Validate data integrity (only if we have data)
        if output.finding_group_association.count() > 0:
            super().validate_columns_schema(output.finding_group_association, set(SchemaRegistry.get_schema_for_table(TableNames.FINDING_GROUP_ASSOCIATION).names))
            super().validate_columns_schema(output.finding_group_rollup, set(SchemaRegistry.get_schema_for_table(TableNames.FINDING_GROUP_ROLLUP).names))
            super().validate_output_consistency(spark, output)
            self._validate_correct_group_association(spark, output)

    def run_aggregation(self,spark: SparkSession,test_config) -> AggregationOutput:
        aggregation_service: FindingsAggregatorInterface = Factory.create_aggregator(test_config)
        aggregation_service.rule_loader = self._create_mock_rule_loader()

        return aggregation_service.aggregate_findings(spark=spark)


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
            (super().calculate_group_id(0, "pkg0"), "pkg0", 1),
            (super().calculate_group_id(0, "pkg1"), "pkg1", 2),
            (super().calculate_group_id(0, "pkg1"), "pkg1", 3),
            (super().calculate_group_id(0, "pkg2"), "pkg2", 4),
            (super().calculate_group_id(0, "pkg2"), "pkg2", 5),
            (super().calculate_group_id(0, "pkg2"), "pkg2", 6),
            (super().calculate_group_id(0, "pkg3"), "pkg3", 7),
            (super().calculate_group_id(0, "pkg3"), "pkg3", 8),
            (super().calculate_group_id(0, "pkg3"), "pkg3", 9),
            (super().calculate_group_id(0, "pkg3"), "pkg3", 10),

            # Rule 1 (rule_idx=1): Group by package_name + cloud_account
            (super().calculate_group_id(1, "pkg0-koko_account"), "pkg0_koko_account", 1),
            (super().calculate_group_id(1, "pkg1-koko_account"), "pkg1_koko_account", 2),
            (super().calculate_group_id(1, "pkg1-koko_account"), "pkg1_koko_account", 3),
            (super().calculate_group_id(1, "pkg2-koko_account"), "pkg2_koko_account", 4),
            (super().calculate_group_id(1, "pkg2-koko_account"), "pkg2_koko_account", 5),
            (super().calculate_group_id(1, "pkg2-koko_account"), "pkg2_koko_account", 6),
            (super().calculate_group_id(1, "pkg3-koko_account"), "pkg3_koko_account", 7),
            (super().calculate_group_id(1, "pkg3-koko_account"), "pkg3_koko_account", 8),
            (super().calculate_group_id(1, "pkg3-koko_account"), "pkg3_koko_account", 9),
            (super().calculate_group_id(1, "pkg3-koko_account"), "pkg3_koko_account", 10),
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



    def _create_mock_rule_loader(self):
        """Create mock rule loader with realistic aggregation rules"""
        mock_rule_loader = Mock()
        mock_rule_loader.load_aggregation_rules.return_value = [
            AggregationRule(
                id=4,
                order=1003,
                group_by=[ColumnNames.PACKAGE_NAME],  # Use ColumnNames
                filters_config={},
                field_calculation=json.dumps({}),
                rule_type="AGG"
            ),
            AggregationRule(
                id=15,
                order=1013,
                group_by=[ColumnNames.PACKAGE_NAME, ColumnNames.CLOUD_ACCOUNT],  # Use ColumnNames
                filters_config={
                    "filtersjson": {
                        "value": ["koko"],
                        "field": ColumnNames.SOURCE,  # Use ColumnNames
                        "condition": "in"
                    }
                },
                field_calculation=json.dumps({}),
                rule_type="AGG"
            )
        ]
        return mock_rule_loader

    def create_findings_data(self, spark):
        print("\n=== Creating Findings Data from Base===")
        """Create findings test data using SchemaRegistry"""
        schema = SchemaRegistry.findings_schema()

        # Schema has 32 fields in this order:
        # id, datasource_id, datasource_definition_id, title, source, finding_id_str,
        # original_finding_id, created_time, discovered_time, due_date, last_collected_time,
        # last_reported_time, original_status, time_to_remediate, category_field, sub_category,
        # rule_id, resource_reported_not_exist, aggregation_group_id, main_resource_id,
        # package_name, image_id, scan_id, editable, reopen_date, finding_type_str,
        # fix_id, fix_vendor_id, fix_type, fix_subtype, rule_type, rule_family

        findings_data = [
            # 32 fields matching schema exactly
            (
                1,  # id
                0,  # datasource_id
                0,  # datasource_definition_id
                "9d50933c11ca4ff69c5fd8401c8982d2",  # title
                "koko",  # source
                "9d50933c11ca4ff69c5fd8401c8982d2",  # finding_id_str
                "9d50933c11ca4ff69c5fd8401c8982d2",  # original_finding_id
                "2025-08-08T08:24:06.201Z",  # created_time
                "2025-08-08T08:24:06.201Z",  # discovered_time
                None,  # due_date
                "2025-08-08T08:24:06.201Z",  # last_collected_time
                "2025-08-08T08:24:06.201Z",  # last_reported_time
                "",  # original_status
                None,  # time_to_remediate
                "",  # category_field
                "",  # sub_category
                "rule_001",  # rule_id
                "",  # resource_reported_not_exist
                None,  # aggregation_group_id
                4,  # main_resource_id
                "pkg0",  # package_name
                None,  # image_id
                "koko_account",  # scan_id
                False,  # editable
                None,  # reopen_date
                None,  # finding_type_str
                None,  # fix_id
                None,  # fix_vendor_id
                None,  # fix_type
                None,  # fix_subtype
                None,  # rule_type
                None  # rule_family
            ),
            (
                2, 0, 0, "c9e27257d5dc47d3b6f9fb0c688b3640", "koko",
                "c9e27257d5dc47d3b6f9fb0c688b3640", "c9e27257d5dc47d3b6f9fb0c688b3640",
                "2025-08-08T08:24:06.309Z", "2025-08-08T08:24:06.309Z", None,
                "2025-08-08T08:24:06.309Z", "2025-08-08T08:24:06.309Z", "", None, "", "",
                "rule_001", "", None, 4, "pkg1", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
            (
                3, 0, 0, "3a8ff87e9c474c8cbef0a4aa0f71f217", "koko",
                "3a8ff87e9c474c8cbef0a4aa0f71f217", "3a8ff87e9c474c8cbef0a4aa0f71f217",
                "2025-08-08T08:24:06.349Z", "2025-08-08T08:24:06.349Z", None,
                "2025-08-08T08:24:06.349Z", "2025-08-08T08:24:06.349Z", "", None, "", "",
                "rule_001", "", None, 4, "pkg1", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
            (
                4, 0, 0, "ab2b1886120f4e0fa6e88d9f68ebb86c", "koko",
                "ab2b1886120f4e0fa6e88d9f68ebb86c", "ab2b1886120f4e0fa6e88d9f68ebb86c",
                "2025-08-08T08:24:06.388Z", "2025-08-08T08:24:06.388Z", None,
                "2025-08-08T08:24:06.388Z", "2025-08-08T08:24:06.388Z", "", None, "", "",
                "rule_002", "", None, 4, "pkg2", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
            (
                5, 0, 0, "0f778268e5ae45fbbceef510d9c18640", "koko",
                "0f778268e5ae45fbbceef510d9c18640", "0f778268e5ae45fbbceef510d9c18640",
                "2025-08-08T08:24:06.428Z", "2025-08-08T08:24:06.428Z", None,
                "2025-08-08T08:24:06.428Z", "2025-08-08T08:24:06.428Z", "", None, "", "",
                "rule_002", "", None, 4, "pkg2", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
            (
                6, 0, 0, "17e155826363430482881365ecfc36b8", "koko",
                "17e155826363430482881365ecfc36b8", "17e155826363430482881365ecfc36b8",
                "2025-08-08T08:24:06.468Z", "2025-08-08T08:24:06.468Z", None,
                "2025-08-08T08:24:06.468Z", "2025-08-08T08:24:06.468Z", "", None, "", "",
                "rule_002", "", None, 4, "pkg2", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
            (
                7, 0, 0, "fbfd29ee76cf4d85a36b2b5476169041", "koko",
                "fbfd29ee76cf4d85a36b2b5476169041", "fbfd29ee76cf4d85a36b2b5476169041",
                "2025-08-08T08:24:06.509Z", "2025-08-08T08:24:06.509Z", None,
                "2025-08-08T08:24:06.509Z", "2025-08-08T08:24:06.509Z", "", None, "", "",
                "rule_003", "", None, 4, "pkg3", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
            (
                8, 0, 0, "78b0bf19e90e4f24ba29e218bff34354", "koko",
                "78b0bf19e90e4f24ba29e218bff34354", "78b0bf19e90e4f24ba29e218bff34354",
                "2025-08-08T08:24:06.550Z", "2025-08-08T08:24:06.550Z", None,
                "2025-08-08T08:24:06.550Z", "2025-08-08T08:24:06.550Z", "", None, "", "",
                "rule_003", "", None, 4, "pkg3", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
            (
                9, 0, 0, "10bc668c14984b228f8ba65267f9da5e", "koko",
                "10bc668c14984b228f8ba65267f9da5e", "10bc668c14984b228f8ba65267f9da5e",
                "2025-08-08T08:24:06.594Z", "2025-08-08T08:24:06.594Z", None,
                "2025-08-08T08:24:06.594Z", "2025-08-08T08:24:06.594Z", "", None, "", "",
                "rule_003", "", None, 4, "pkg3", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
            (
                10, 0, 0, "b379db5e4dcc4eb5b1e911c22eefda7d", "koko",
                "b379db5e4dcc4eb5b1e911c22eefda7d", "b379db5e4dcc4eb5b1e911c22eefda7d",
                "2025-08-08T08:24:06.642Z", "2025-08-08T08:24:06.642Z", None,
                "2025-08-08T08:24:06.642Z", "2025-08-08T08:24:06.642Z", "", None, "", "",
                "rule_003", "", None, 4, "pkg3", None, "koko_account", False, None,
                None, None, None, None, None, None, None
            ),
        ]

        df = spark.createDataFrame(findings_data, schema)
        df.createOrReplaceTempView(TableNames.FINDINGS.value)
        return df







