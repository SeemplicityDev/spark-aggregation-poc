import json
import os
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession
from spark_aggregation_poc.schemas.schema_registry import (
    SchemaRegistry, ColumnNames, TableNames
)

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.models.spark_aggregation_rules import AggregationRule


class TestAggregationBase:
    """
    Sanity tests for aggregation service with hardcoded test data.
    Data is created in code rather than loaded from JSON files.
    """

    @pytest.fixture
    def spark(self):
        return SparkSession.builder \
            .appName("PostgreSQLSparkApp") \
            .master("local[*]") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", self.get_local_warehouse_path()) \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .getOrCreate()

    @pytest.fixture
    def test_config(self):
        """Configuration for component testing"""
        return Config(
            postgres_url="",
            postgres_properties={},
            catalog_table_prefix="",
            customer=""
        )

    def create_findings_data(self, spark):
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


    def create_plain_resources_data(self, spark):
        """Create plain_resources using SchemaRegistry"""
        schema = SchemaRegistry.plain_resources_schema()

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
        schema = SchemaRegistry.findings_scores_schema()

        findings_scores_data = [
            (finding_id, "1", 1.0, None, None, None, None, None, 1.0, 3, None)
            for finding_id in range(1, 11)
        ]

        df = spark.createDataFrame(findings_scores_data, schema)
        df.createOrReplaceTempView(TableNames.FINDINGS_SCORES.value)
        return df

    def create_user_status_data(self, spark):
        """Create user_status using SchemaRegistry"""
        schema = SchemaRegistry.user_status_schema()

        user_status_data = [
            (i, 10000, None, 10000, None)
            for i in range(1, 11)
        ]

        df = spark.createDataFrame(user_status_data, schema)
        df.createOrReplaceTempView(TableNames.USER_STATUS.value)
        return df

    def create_statuses_data(self, spark):
        """Create statuses using SchemaRegistry"""
        schema = SchemaRegistry.statuses_schema()

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

    def create_aggregation_groups_data(self, spark):
        """Create empty aggregation_groups using SchemaRegistry"""
        schema = SchemaRegistry.aggregation_groups_schema()
        df = spark.createDataFrame([], schema)
        df.createOrReplaceTempView(TableNames.AGGREGATION_GROUPS.value)
        return df

    def create_finding_sla_rule_connections_data(self, spark):
        """Create empty finding_sla_rule_connections using SchemaRegistry"""
        schema = SchemaRegistry.finding_sla_rule_connections_schema()
        df = spark.createDataFrame([], schema)
        df.createOrReplaceTempView(TableNames.FINDING_SLA_RULE_CONNECTIONS.value)
        return df

    def create_findings_additional_data(self, spark):
        """Create empty findings_additional_data using SchemaRegistry"""
        schema = SchemaRegistry.findings_additional_data_schema()
        df = spark.createDataFrame([], schema)
        df.createOrReplaceTempView(TableNames.FINDINGS_ADDITIONAL_DATA.value)
        return df

    def create_findings_info_data(self, spark):
        """Create empty findings_info using SchemaRegistry"""
        schema = SchemaRegistry.findings_info_schema()
        df = spark.createDataFrame([], schema)
        df.createOrReplaceTempView(TableNames.FINDINGS_INFO.value)
        return df

    def create_scoring_rules_data(self, spark):
        """Create empty scoring_rules using SchemaRegistry"""
        schema = SchemaRegistry.scoring_rules_schema()
        df = spark.createDataFrame([], schema)
        df.createOrReplaceTempView(TableNames.SCORING_RULES.value)
        return df

    def create_selection_rules_data(self, spark):
        """Create empty selection_rules using SchemaRegistry"""
        schema = SchemaRegistry.selection_rules_schema()
        df = spark.createDataFrame([], schema)
        df.createOrReplaceTempView(TableNames.SELECTION_RULES.value)
        return df

    def create_aggregation_rules_data(self, spark):
        """Create aggregation_rules using SchemaRegistry"""
        schema = SchemaRegistry.aggregation_rules_schema()

        aggregation_rules_data = [
            (4, 1003, "AGG", '{"group_by":["findings.package_name"]}', "{}",
             False, False, 3, None, None, "seemplicity", "2025-08-08T11:24:06.194Z",
             "seemplicity", "2025-08-08T08:24:06.194Z", None, None, None),
            (15, 1013, "AGG",
             '{"group_by":["findings.package_name", "plain_resources.cloud_account"],"filters_config":{"scopesid":null,"scopesjson":null,"scopes_fields_to_exclude":null,"scopes_fields_to_include":null,"scopes_fields_to_override":null,"scope_group":1,"filtersid":null,"filtersjson":{"value":["koko"],"field":"source","condition":"in"},"filters_fields_to_exclude":null,"filters_fields_to_include":null,"filters_fields_to_override":null}}',
             "{}", False, False, 3, None, None, "seemplicity", "2025-08-08T11:24:06.194Z",
             "seemplicity", "2025-08-08T11:24:06.194Z", None, None, None),
        ]

        df = spark.createDataFrame(aggregation_rules_data, schema)
        df.createOrReplaceTempView(TableNames.AGGREGATION_RULES.value)
        return df

    def setup_all_temp_views(self, spark):
        """Create all temporary views with hardcoded data using SchemaRegistry"""
        print("=== Creating Temporary Views with Explicit Schemas ===")

        self.create_findings_data(spark)
        print(f"✅ {TableNames.FINDINGS.value}: 10 rows")

        self.create_plain_resources_data(spark)
        print(f"✅ {TableNames.PLAIN_RESOURCES.value}: 4 rows")

        self.create_findings_scores_data(spark)
        print(f"✅ {TableNames.FINDINGS_SCORES.value}: 10 rows")

        self.create_user_status_data(spark)
        print(f"✅ {TableNames.USER_STATUS.value}: 10 rows")

        self.create_statuses_data(spark)
        print(f"✅ {TableNames.STATUSES.value}: 5 rows")

        self.create_aggregation_groups_data(spark)
        print(f"✅ {TableNames.AGGREGATION_GROUPS.value}: 0 rows (empty)")

        self.create_finding_sla_rule_connections_data(spark)
        print(f"✅ {TableNames.FINDING_SLA_RULE_CONNECTIONS.value}: 0 rows (empty)")

        self.create_findings_additional_data(spark)
        print(f"✅ {TableNames.FINDINGS_ADDITIONAL_DATA.value}: 0 rows (empty)")

        self.create_findings_info_data(spark)
        print(f"✅ {TableNames.FINDINGS_INFO.value}: 0 rows (empty)")

        self.create_scoring_rules_data(spark)
        print(f"✅ {TableNames.SCORING_RULES.value}: 0 rows (empty)")

        self.create_selection_rules_data(spark)
        print(f"✅ {TableNames.SELECTION_RULES.value}: 0 rows (empty)")

        self.create_aggregation_rules_data(spark)
        print(f"✅ {TableNames.AGGREGATION_RULES.value}: 2 rows")

        print("\n=== All Temporary Views Created with SchemaRegistry ===")

    def create_mock_rule_loader(self):
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

    def test_sanity_basic_data_creation(self, spark):
        """Sanity test: Verify all temp views are created correctly"""
        self.setup_all_temp_views(spark)

        # Verify findings using TableNames
        findings_df = spark.table(TableNames.FINDINGS.value)
        assert findings_df.count() == 10, "Expected 10 findings"

        # Verify plain_resources
        resources_df = spark.table(TableNames.PLAIN_RESOURCES.value)
        assert resources_df.count() == 4, "Expected 4 plain_resources"

        # Verify aggregation_rules
        rules_df = spark.table(TableNames.AGGREGATION_RULES.value)
        assert rules_df.count() == 2, "Expected 2 aggregation_rules"

        print("✅ Sanity test passed: All data created with SchemaRegistry")

    def test_sanity_findings_by_package(self, spark):
        """Sanity test: Verify findings grouped by package_name using ColumnNames"""
        self.setup_all_temp_views(spark)

        # Use ColumnNames constants
        findings_df = spark.sql(f"""
            SELECT {ColumnNames.PACKAGE_NAME}, COUNT(*) as count
            FROM {TableNames.FINDINGS.value}
            GROUP BY {ColumnNames.PACKAGE_NAME}
            ORDER BY {ColumnNames.PACKAGE_NAME}
        """)

        findings_df.show()

        results = findings_df.collect()
        assert len(results) == 4, "Expected 4 distinct packages"
        assert results[0][ColumnNames.PACKAGE_NAME] == "pkg0" and results[0]["count"] == 1
        assert results[1][ColumnNames.PACKAGE_NAME] == "pkg1" and results[1]["count"] == 2
        assert results[2][ColumnNames.PACKAGE_NAME] == "pkg2" and results[2]["count"] == 3
        assert results[3][ColumnNames.PACKAGE_NAME] == "pkg3" and results[3]["count"] == 4

        print("✅ Sanity test passed: Findings grouped by package using ColumnNames")

    def test_sanity_join_findings_with_resources(self, spark):
        """Sanity test: Verify join using ColumnNames constants"""
        self.setup_all_temp_views(spark)

        # Use ColumnNames constants
        joined_df = spark.sql(f"""
            SELECT f.{ColumnNames.ID}, f.{ColumnNames.PACKAGE_NAME}, 
                   p.{ColumnNames.CLOUD_ACCOUNT}, p.{ColumnNames.CLOUD_PROVIDER}
            FROM {TableNames.FINDINGS.value} f
            JOIN {TableNames.PLAIN_RESOURCES.value} p 
                ON f.{ColumnNames.MAIN_RESOURCE_ID} = p.{ColumnNames.ID}
            ORDER BY f.{ColumnNames.ID}
        """)

        joined_df.show(5)

        assert joined_df.count() == 10, "Expected 10 joined records"

        first_row = joined_df.first()
        assert first_row[ColumnNames.CLOUD_ACCOUNT] == "koko_account"
        assert first_row[ColumnNames.CLOUD_PROVIDER] == "koko_provider"

        print("✅ Sanity test passed: Join using ColumnNames constants")

    def get_local_warehouse_path(self):
        current_file = os.path.abspath(__file__)
        # Go up from src/spark_aggregation_poc/ to project root
        project_root = os.path.dirname(os.path.dirname(current_file))
        warehouse_path = os.path.join(project_root, "local-catalog")
        print("local_warehouse_path:", warehouse_path)
        return warehouse_path