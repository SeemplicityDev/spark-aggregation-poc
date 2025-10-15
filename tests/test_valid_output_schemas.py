"""Tests for initial aggregation functionality using SchemaRegistry and AggregationOutput"""
import json
from unittest.mock import Mock

from pyspark.sql import SparkSession

from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsAggregatorInterface
from spark_aggregation_poc.models.aggregation_output import AggregationOutput
from spark_aggregation_poc.models.spark_aggregation_rules import AggregationRule
from spark_aggregation_poc.schemas.schemas import (
    ColumnNames, TableNames, Schemas
)
from tests.base.test_aggregation_base import TestAggregationBase


class TestValidOutputSchemas(TestAggregationBase):

    def test_valid_output_schemas(self, spark, test_config):
        print("\n=== Running Initial Aggregation Test ===")

        output: AggregationOutput = self.run_aggregation(spark, test_config)

        if output.finding_group_association.count() > 0:
            super().validate_columns_schema(output.finding_group_association, set(Schemas.get_schema_for_table(TableNames.FINDING_GROUP_ASSOCIATION).names))
            super().validate_columns_schema(output.finding_group_rollup, set(Schemas.get_schema_for_table(TableNames.FINDING_GROUP_ROLLUP).names))


    def run_aggregation(self,spark: SparkSession,test_config) -> AggregationOutput:
        aggregation_service: FindingsAggregatorInterface = Factory.create_aggregator(test_config)
        aggregation_service.rule_loader = self.create_mock_rule_loader()

        return aggregation_service.aggregate_findings(spark=spark)



    def create_mock_rule_loader(self) -> Mock:
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
        schema = Schemas.findings_schema()

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

    def create_plain_resources_data(self, spark):
        """Create plain_resources using SchemaRegistry"""
        schema = Schemas.plain_resources_schema()

        plain_resources_data = [
            (4, "resource_type", "resource_name", "resource_id", "region", "koko_region",
             "koko_account_koko_region", "cloud_account", "koko_account", "koko_account",
             "cloud_provider", "koko_provider", "koko_provider", "koko_provider",
             "koko_account", "koko_account", "{}", "{}", "{}", "{}",
             "2025-08-08T08:24:06.000Z", "2025-08-08T08:24:06.000Z"),
            (3, "region", "koko_region", "koko_account_koko_region", "cloud_account",
             "koko_account", "koko_account", "cloud_provider", "koko_provider", "koko_provider",
             None, None, None, "koko_provider", "koko_account", "koko_account",
             "{}", "{}", "{}", "{}", "2025-08-08T08:24:06.000Z", "2025-08-08T08:24:06.000Z"),
            (2, "cloud_account", "koko_account", "koko_account", "cloud_provider",
             "koko_provider", "koko_provider", None, None, None,
             None, None, None, "koko_provider", "koko_account", "koko_account",
             "{}", "{}", "{}", "{}", "2025-08-08T08:24:06.000Z", "2025-08-08T08:24:06.000Z"),
            (1, "cloud_provider", "koko_provider", "koko_provider", None, None, None,
             None, None, None, None, None, None,
             "koko_provider", None, None, "{}", "{}", "{}", "{}",
             "2025-08-08T08:24:06.000Z", "2025-08-08T08:24:06.000Z"),
        ]

        df = spark.createDataFrame(plain_resources_data, schema)
        df.createOrReplaceTempView(TableNames.PLAIN_RESOURCES.value)
        return df

    def create_findings_scores_data(self, spark):
        """Create findings_scores using SchemaRegistry"""
        schema = Schemas.findings_scores_schema()

        findings_scores_data = [
            (finding_id, "1", 1.0, None, None, None, None, None, 1.0, 3, None)
            for finding_id in range(1, 11)
        ]

        df = spark.createDataFrame(findings_scores_data, schema)
        df.createOrReplaceTempView(TableNames.FINDINGS_SCORES.value)
        return df

    def create_user_status_data(self, spark):
        """Create user_status using SchemaRegistry"""
        schema = Schemas.user_status_schema()

        user_status_data = [
            (i, 10000, None, 10000, None)
            for i in range(1, 11)
        ]

        df = spark.createDataFrame(user_status_data, schema)
        df.createOrReplaceTempView(TableNames.USER_STATUS.value)
        return df

    def create_statuses_data(self, spark):
        """Create statuses using SchemaRegistry"""
        schema = Schemas.statuses_schema()

        statuses_data = [
            (10000, "OPEN", "New1", "SYSTEM", "some reason", "some description",
             False, True, False, None, None),
            (10001, "FIXED", "Resolved1", "SYSTEM", "some reason", "some description",
             False, True, False, None, None),
            (10002, "OPEN", "New1", "USER", "some reason", "some description",
             False, True, False, None, None),
            (11, "OPEN", "New", "SYSTEM", "Found by data source",
             "Finding was identified by data source", False, True, False, None, None),
            (21, "FIXED", "Resolved", "SYSTEM", "Finding was reported fixed by data source",
             "Finding was reported fixed by the data source", False, True, False, None, None),
        ]

        df = spark.createDataFrame(statuses_data, schema)
        df.createOrReplaceTempView(TableNames.STATUSES.value)
        return df




