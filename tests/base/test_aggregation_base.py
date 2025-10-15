import hashlib
import json
import os
from abc import abstractmethod
from typing import final
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession, DataFrame

from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsAggregatorInterface
from spark_aggregation_poc.models.aggregation_output import AggregationOutput
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

    @pytest.fixture(scope="class")
    def spark(self):
        spark_session = SparkSession.builder \
            .appName("PostgreSQLSparkApp") \
            .master("local[*]") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", self.get_local_warehouse_path()) \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .getOrCreate()

        yield spark_session  # ← ADDED yield

        # Cleanup after all tests
        print("\n🧹 Stopping Spark session...")
        spark_session.stop()  # ← ADDED cleanup


    @pytest.fixture(scope="class")
    def test_config(self):
        """Configuration for component testing"""
        return Config(
            postgres_url="",
            postgres_properties={},
            catalog_table_prefix="",
            customer=""
        )

    @pytest.fixture(scope="class", autouse=True)
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

        # self.create_aggregation_rules_data(spark)
        # print(f"✅ {TableNames.AGGREGATION_RULES.value}: 2 rows")

        print("\n=== All Temporary Views Created with SchemaRegistry ===")

    @abstractmethod
    def run_aggregation(self,spark: SparkSession,test_config) -> AggregationOutput:
       pass


    def create_findings_data(self, spark):
        schema = SchemaRegistry.findings_schema()

        findings_data = []

        df = spark.createDataFrame(findings_data, schema)
        df.createOrReplaceTempView(TableNames.USER_STATUS.value)
        return df

    def create_plain_resources_data(self, spark):
        schema = SchemaRegistry.plain_resources_schema()

        plain_resources_data = []

        df = spark.createDataFrame(plain_resources_data, schema)
        df.createOrReplaceTempView(TableNames.USER_STATUS.value)
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


    def validate_columns_schema(self, df: DataFrame, expected_columns: set[str]) -> None:
        actual_cols = set(df.columns)

        assert actual_cols == expected_columns

    def validate_output_consistency(
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


    def get_local_warehouse_path(self):
        current_file = os.path.abspath(__file__)
        # Go up from src/spark_aggregation_poc/ to project root
        project_root = os.path.dirname(os.path.dirname(current_file))
        warehouse_path = os.path.join(project_root, "local-catalog")
        print("local_warehouse_path:", warehouse_path)
        return warehouse_path

    def calculate_group_id(self, rule_idx: int, *values: str) -> str:
        """
        Calculate group_id the same way as the aggregation service.
        Formula: md5(concat_ws("-", rule_idx, value1, value2, ...))
        """
        # Join values with "-" separator
        concatenated = "-".join([str(rule_idx)] + list(values))
        # Calculate MD5 hash
        return hashlib.md5(concatenated.encode()).hexdigest()


    def create_plain_resources_data(self, spark):
        schema = SchemaRegistry.plain_resources_schema()

        plain_resources_data = []

        df = spark.createDataFrame(plain_resources_data, schema)
        df.createOrReplaceTempView(TableNames.PLAIN_RESOURCES.value)
        return df

    def create_findings_scores_data(self, spark):
        """Create findings_scores using SchemaRegistry"""
        schema = SchemaRegistry.findings_scores_schema()

        findings_scores_data = []

        df = spark.createDataFrame(findings_scores_data, schema)
        df.createOrReplaceTempView(TableNames.FINDINGS_SCORES.value)
        return df

    def create_user_status_data(self, spark):
        """Create user_status using SchemaRegistry"""
        schema = SchemaRegistry.user_status_schema()

        user_status_data = []

        df = spark.createDataFrame(user_status_data, schema)
        df.createOrReplaceTempView(TableNames.USER_STATUS.value)
        return df

    def create_statuses_data(self, spark):
        """Create statuses using SchemaRegistry"""
        schema = SchemaRegistry.statuses_schema()

        statuses_data = []

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