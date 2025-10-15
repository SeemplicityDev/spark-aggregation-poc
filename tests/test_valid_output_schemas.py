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

        findings_data = [
            # 32 fields matching schema exactly
            (
                1,  # id
                0,  # datasource_id
                0,  # datasource_definition_id
                None,  # title
                "koko",  # source
                None,  # finding_id_str
                None,  # original_finding_id
                None,  # created_time
                None,  # discovered_time
                None,  # due_date
                None,  # last_collected_time
                None,  # last_reported_time
                "",  # original_status
                None,  # time_to_remediate
                "",  # category_field
                "",  # sub_category
                None,  # rule_id ← CHANGED
                "",  # resource_reported_not_exist
                None,  # aggregation_group_id
                4,  # main_resource_id
                "pkg0",  # package_name
                None,  # image_id
                None,  # scan_id ← CHANGED
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
                2, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg1", None, None, False, None,
                None, None, None, None, None, None, None
            ),
            (
                3, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg1", None, None, False, None,
                None, None, None, None, None, None, None
            ),
            (
                4, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg2", None, None, False, None,
                None, None, None, None, None, None, None
            ),
            (
                5, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg2", None, None, False, None,
                None, None, None, None, None, None, None
            ),
            (
                6, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg2", None, None, False, None,
                None, None, None, None, None, None, None
            ),
            (
                7, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg3", None, None, False, None,
                None, None, None, None, None, None, None
            ),
            (
                8, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg3", None, None, False, None,
                None, None, None, None, None, None, None
            ),
            (
                9, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg3", None, None, False, None,
                None, None, None, None, None, None, None
            ),
            (
                10, 0, 0, None, "koko",
                None, None, None, None, None,
                None, None, "", None, "", "",
                None, "", None, 4, "pkg3", None, None, False, None,
                None, None, None, None, None, None, None
            ),
        ]

        df = spark.createDataFrame(findings_data, schema)
        df.createOrReplaceTempView(TableNames.FINDINGS.value)
        return df

    def create_plain_resources_data(self, spark):
        """Create plain_resources with only required columns"""
        schema = Schemas.plain_resources_schema()

        plain_resources_data = [
            (
                4,  # id - REQUIRED (used in JOIN)
                "VM",  # r1_resource_type - REQUIRED (used as resource_type)
                None,  # r1_resource_name
                None,  # r1_resource_id
                None,  # r2_resource_type
                None,  # r2_resource_name
                None,  # r2_resource_id
                None,  # r3_resource_type
                None,  # r3_resource_name
                None,  # r3_resource_id
                None,  # r4_resource_type
                None,  # r4_resource_name
                None,  # r4_resource_id
                "koko_provider",  # cloud_provider - REQUIRED
                "koko_account",  # cloud_account - REQUIRED
                "koko_account",  # cloud_account_friendly_name - REQUIRED
                "[]",  # tags_values - REQUIRED
                None,  # seem_tags_values
                "[]",  # tags_key_values - REQUIRED
                None,  # seem_tags_key_values
                None,  # last_seen
                None  # first_seen
            ),
        ]

        df = spark.createDataFrame(plain_resources_data, schema)
        df.createOrReplaceTempView(TableNames.PLAIN_RESOURCES.value)
        return df

    def create_statuses_data(self, spark):
        """Create statuses with only required columns"""
        schema = Schemas.statuses_schema()

        statuses_data = [
            (
                10000,  # key - REQUIRED (used in JOIN)
                "OPEN",  # category - REQUIRED (used in SELECT)
                None,  # sub_status
                None,  # type
                None,  # reason
                None,  # description
                None,  # is_extended
                None,  # enabled
                None,  # editable
                None,  # created_by_user_id
                None  # updated_by_user_id
            ),
        ]

        df = spark.createDataFrame(statuses_data, schema)
        df.createOrReplaceTempView(TableNames.STATUSES.value)
        return df

    def create_findings_scores_data(self, spark):
        """Create findings_scores using SchemaRegistry"""
        schema = Schemas.findings_scores_schema()

        findings_scores_data = [
            (finding_id, None, None, None, None, None, None, None, None, 3, None)
            for finding_id in range(1, 11)
        ]

        df = spark.createDataFrame(findings_scores_data, schema)
        df.createOrReplaceTempView(TableNames.FINDINGS_SCORES.value)
        return df

    def create_user_status_data(self, spark):
        """Create user_status using SchemaRegistry"""
        schema = Schemas.user_status_schema()

        user_status_data = [
            (i, None, None, 10000, None)
            for i in range(1, 11)
        ]

        df = spark.createDataFrame(user_status_data, schema)
        df.createOrReplaceTempView(TableNames.USER_STATUS.value)
        return df




