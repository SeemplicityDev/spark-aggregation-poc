import json
from unittest.mock import Mock

from pyspark.sql import SparkSession

from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsAggregatorInterface
from spark_aggregation_poc.models.aggregation_output import AggregationOutput
from spark_aggregation_poc.models.spark_aggregation_rules import AggregationRule
from spark_aggregation_poc.schemas.schemas import Schemas, TableNames, ColumnNames
from tests.base.test_aggregation_base import TestAggregationBase


class TestCorrectAssociation(TestAggregationBase):

    def test_correct_association(self, spark, test_config):
        print("\n=== Running Initial Aggregation Test ===")

        output: AggregationOutput = self.run_aggregation(spark, test_config)

        # Validate data integrity (only if we have data)
        if output.finding_group_association.count() > 0:
            super().validate_columns_schema(output.finding_group_association, set(Schemas.get_schema_for_table(
                TableNames.FINDING_GROUP_ASSOCIATION).names))
            super().validate_columns_schema(output.finding_group_rollup, set(Schemas.get_schema_for_table(
                TableNames.FINDING_GROUP_ROLLUP).names))
            super().validate_output_consistency(spark, output)
            self._validate_correct_group_association(spark, output)


    def run_aggregation(self, spark: SparkSession, test_config) -> AggregationOutput:
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
            schema=Schemas.finding_group_association_schema()
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
                    "scope_group": 1,
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

    def create_resource_to_scopes_data(self, spark):
        """Create user_status using SchemaRegistry"""
        schema = Schemas.resource_to_scopes_schema()

        resource_to_scopes_data = [
            (4, [1])
        ]

        df = spark.createDataFrame(resource_to_scopes_data, schema)
        df.createOrReplaceTempView(TableNames.RESOURCE_TO_SCOPES.value)
        return df


    def create_scope_groups_data(self, spark):
        """Create user_status using SchemaRegistry"""
        schema = Schemas.scope_groups_schema()

        scope_groups_data = [
            (1, "fffff", False, None, True)
        ]

        df = spark.createDataFrame(scope_groups_data, schema)
        df.createOrReplaceTempView(TableNames.SCOPE_GROUPS.value)
        return df

