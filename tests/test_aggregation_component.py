import json
import os
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsAggregatorInterface
from spark_aggregation_poc.models.spark_aggregation_rules import AggregationRule
from spark_aggregation_poc.services.aggregation.aggregation_service import AggregationService

"""
    Component tests for the aggregation service.
    Tests the complete aggregation flow from input data to S3 output,
    but stops before the downstream PostgreSQL write process.
    """

class TestAggregationComponent:

    def get_local_warehouse_path(self):
        current_file = os.path.abspath(__file__)
        # Go up from src/spark_aggregation_poc/ to project root
        project_root = os.path.dirname(os.path.dirname(current_file))
        warehouse_path = os.path.join(project_root, "local-catalog")
        print("local_warehouse_path:", warehouse_path)
        return warehouse_path

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
        # return SparkSession.builder \
        #     .appName("component_test") \
        #     .master("local[2]") \
        #     .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        #     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        #     .getOrCreate()


    @pytest.fixture
    def sample_findings_data(self, spark):
        """Create realistic sample data for component testing"""
        findings_data = [
            (1, "package1", "account1", "rule1", "OPEN", "HIGH", "fix1"),
            (2, "package1", "account1", "rule1", "OPEN", "HIGH", "fix1"),  # Same group
            (3, "package2", "account2", "rule2", "FIXED", "MEDIUM", "fix2"),
            (4, "package1", "account3", "rule1", "OPEN", "HIGH", "fix1"),  # Different account
            (5, "package3", "account1", "rule3", "IGNORED", "LOW", "fix3")
        ]

        findings_df = spark.createDataFrame(
            findings_data,
            ["finding_id", "package_name", "cloud_account", "rule_id", "status", "severity", "fix_id"]
        )

        # Save to catalog as the aggregation service expects
        findings_df.write.mode("overwrite").saveAsTable("spark_catalog.default.findings")
        return findings_df


    @pytest.fixture
    def test_config(self):
        """Configuration for component testing"""
        return Config(
            postgres_url="jdbc:postgresql://localhost:5432/test_db",
            postgres_properties={
                "user": "test_user",
                "password": "test_password",
                "driver": "org.postgresql.Driver"
            },
            catalog_table_prefix="",
            customer="test_customer"
        )

    def create_mock_rule_loader(self):
        # Create mock rule loader with realistic rules
        mock_rule_loader = Mock()
        mock_rule_loader.load_aggregation_rules.return_value = [
            AggregationRule(
                id=1,
                order=1,
                group_by=["package_name", "cloud_account"],
                filters_config={"filtersjson": {
                    "field": "package_name",
                    "condition": "is_not_null",
                    "value": None
                }},
                field_calculation=json.dumps({"status": ["OPEN"]}),
                rule_type="AGG"
            ),
            AggregationRule(
                id=2,
                order=2,
                group_by=["rule_id", "cloud_account"],
                filters_config={"filtersjson": {
                    "field": "rule_id",
                    "condition": "is_not_null",
                    "value": None
                }},
                field_calculation=json.dumps({"severity": [1, 2, 3]}),
                rule_type="AGG"
            )
        ]

        return mock_rule_loader

    def create_temp_views_from_json(self, spark):
        """
        Create temporary views from JSON test data files.
        Enhanced to handle empty JSON files properly.
        """

        print("=== Creating Temporary Views from JSON Test Data ===")

        test_data_path = self.get_test_data_path()

        # Define all tables we need for testing
        test_tables = [
            "findings",
            "plain_resources",
            "finding_sla_rule_connections",
            "findings_scores",
            "user_status",
            "findings_additional_data",
            "statuses",
            "aggregation_groups",
            "findings_info",
            "scoring_rules",
            "selection_rules",
            "aggregation_rules"
        ]

        created_views = []
        failed_views = []
        total_rows = 0

        for table_name in test_tables:
            json_file_path = f"{test_data_path}/{table_name}.json"

            try:
                print(f"📁 Loading {table_name} from JSON...")

                import os
                if not os.path.exists(json_file_path):
                    print(f"⚠️  JSON file not found: {json_file_path}")
                    df = self._create_empty_dataframe_for_table(spark, table_name)
                    record_count = 0
                else:
                    # Check if file is empty or contains empty array
                    with open(json_file_path, 'r') as f:
                        content = f.read().strip()

                    if not content or content == "[]" or content == "":
                        print(f"⚠️  JSON file is empty: {json_file_path}")
                        df = self._create_empty_dataframe_for_table(spark, table_name)
                        record_count = 0
                    else:
                        # Read JSON file into DataFrame
                        df = spark.read.option("multiline", "true").json(json_file_path)
                        record_count = df.count()

                total_rows += record_count

                # Create temporary view (single-part name only)
                df.createOrReplaceTempView(table_name)

                # Verify the view is accessible
                test_df = spark.table(table_name)
                verify_count = test_df.count()

                if verify_count == record_count:
                    created_views.append(table_name)
                    if record_count > 0:
                        print(f"✅ {table_name}: {record_count:,} rows → temp view created")
                    else:
                        print(f"✅ {table_name}: empty table → temp view created with proper schema")

                    # Show sample data and schema for key tables (only if not empty)
                    if table_name in ['findings', 'plain_resources'] and record_count > 0:
                        print(f"   📋 Sample data from {table_name}:")
                        test_df.show(2, truncate=True)

                    # Always show schema for debugging
                    if record_count == 0:
                        print(f"   📊 Schema for empty {table_name}: {', '.join(test_df.columns)}")

                else:
                    failed_views.append(table_name)
                    print(f"❌ {table_name}: Row count mismatch - expected {record_count}, got {verify_count}")

            except Exception as e:
                failed_views.append(table_name)
                print(f"❌ {table_name}: Failed to create temp view - {e}")

        # Summary
        print(f"\n=== JSON Test Data Loading Summary ===")
        print(f"✅ Successfully created: {len(created_views)} views")
        print(f"❌ Failed to create: {len(failed_views)} views")
        print(f"📊 Total test data loaded: {total_rows:,} rows")

        return created_views, failed_views



    def get_test_data_path(self):
        """Get the path to the test data directory"""
        import os
        current_file = os.path.abspath(__file__)
        test_dir = os.path.dirname(current_file)
        test_data_path = os.path.join(test_dir, "data")

        print(f"🔍 Test data path: {test_data_path}")

        # Create test_data directory if it doesn't exist
        if not os.path.exists(test_data_path):
            os.makedirs(test_data_path)
            print(f"📁 Created test_data directory: {test_data_path}")

        return test_data_path

    def _create_empty_dataframe_for_table(self, spark, table_name: str):
        """Create empty DataFrame with appropriate schema for missing tables"""
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, BooleanType

        # Define minimal schemas for each table
        schemas = {
            "findings": StructType([
                StructField("id", IntegerType(), True),
                StructField("package_name", StringType(), True),
                StructField("main_resource_id", IntegerType(), True),
                StructField("aggregation_group_id", IntegerType(), True),
                StructField("source", StringType(), True),
                StructField("rule_family", StringType(), True),
                StructField("rule_id", StringType(), True),
                StructField("finding_type_str", StringType(), True),
                StructField("fix_subtype", StringType(), True),
                StructField("fix_id", StringType(), True),
                StructField("fix_type", StringType(), True)
            ]),
            "plain_resources": StructType([
                StructField("id", IntegerType(), True),
                StructField("cloud_account", StringType(), True),
                StructField("cloud_account_friendly_name", StringType(), True),
                StructField("r1_resource_type", StringType(), True),
                StructField("tags_values", ArrayType(StringType()), True),
                StructField("tags_key_values", ArrayType(StringType()), True),
                StructField("cloud_provider", StringType(), True)
            ]),
            "finding_sla_rule_connections": StructType([
                StructField("finding_id", IntegerType(), True)
            ]),
            "findings_scores": StructType([
                StructField("finding_id", IntegerType(), True),
                StructField("severity", IntegerType(), True),
                StructField("scoring_rule_id", IntegerType(), True)
            ]),
            "user_status": StructType([
                StructField("id", IntegerType(), True),
                StructField("actual_status_key", IntegerType(), True)
            ]),
            "findings_additional_data": StructType([
                StructField("finding_id", IntegerType(), True),
                StructField("cve", ArrayType(StringType()), True)
            ]),
            "statuses": StructType([
                StructField("key", IntegerType(), True),
                StructField("category", StringType(), True)
            ]),
            "aggregation_groups": StructType([
                StructField("id", IntegerType(), True),
                StructField("main_finding_id", IntegerType(), True),
                StructField("group_identifier", StringType(), True),
                StructField("is_locked", BooleanType(), True)
            ]),
            "findings_info": StructType([
                StructField("id", IntegerType(), True)
            ]),
            "scoring_rules": StructType([
                StructField("id", IntegerType(), True),
                StructField("selection_rule_id", IntegerType(), True)
            ]),
            "selection_rules": StructType([
                StructField("id", IntegerType(), True),
                StructField("scope_group", IntegerType(), True)
            ]),
            "aggregation_rules": StructType([
                StructField("id", IntegerType(), True),
                StructField("rule_order", IntegerType(), True),
                StructField("aggregation_query", StringType(), True),
                StructField("rule_type", StringType(), True),
                StructField("field_calculation", StringType(), True)
            ])
        }

        schema = schemas.get(table_name, StructType([StructField("id", IntegerType(), True)]))
        return spark.createDataFrame([], schema)





    def test_aggregate_findings_success(self, test_config, spark):
        self.create_temp_views_from_json(spark)

        aggregation_service: FindingsAggregatorInterface = Factory.create_aggregator(test_config)
        aggregation_service.rule_loader = self.create_mock_rule_loader()

        df_final_finding_group_rollup, df_final_finding_group_association = aggregation_service.aggregate_findings(spark=spark)

        assert df_final_finding_group_rollup is not None




