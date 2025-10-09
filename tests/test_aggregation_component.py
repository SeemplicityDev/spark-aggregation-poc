import json
import os
from typing import Dict, Any
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from spark_aggregation_poc.config.config import Config, RunMode
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
            run_mode = RunMode.Test,
            customer="test_customer"
        )


    def test_aggregation_service_with_mock_loader(self, spark, test_config):
        """Test AggregationService with mocked rule loader"""

        print("=== Registering Existing Delta Tables ===")

        warehouse_path = self.get_local_warehouse_path()

        # List of tables that create_base_df expects
        required_tables = [
            "findings", "plain_resources", "finding_sla_rule_connections",
            "findings_scores", "user_status", "findings_additional_data",
            "statuses", "aggregation_groups", "findings_info",
            "scoring_rules", "selection_rules"
        ]

        # Register each existing Delta table in Spark's catalog
        for table_name in required_tables:
            table_path = f"{warehouse_path}/{table_name}"
            catalog_table = f"spark_catalog.default.{table_name}"

            try:
                # This is the same as what WriteUtil.save_to_catalog() does,
                # but for existing tables instead of new DataFrames
                spark.sql(f"""
                        CREATE OR REPLACE TABLE {catalog_table}
                        USING DELTA
                        LOCATION '{table_path}'
                    """)

                # Verify the table is accessible
                df = spark.table(catalog_table)
                count = df.count()
                print(f"✅ Registered {table_name}: {count:,} rows")

            except Exception as e:
                print(f"❌ Failed to register {table_name}: {e}")
                # Continue with other tables

        print("=== Running Aggregation Test ===")


        # Create mock rule loader
        mock_rule_loader = Mock()
        mock_rule_loader.load_aggregation_rules.return_value = [
            AggregationRule(
                id=1,
                order = 1,
                group_by=["package_name", "cloud_account"],
                filters_config ={"filtersjson": {
                    "field": "status",
                    "condition": "in",
                    "value": ["OPEN", "FIXED"]
                }},
                field_calculation={"status": ["OPEN"]},  # Mock filters
                rule_type = "AGG"
            )
        ]

        # Create mock filter parser
        mock_filter_parser = Mock()
        mock_filter_parser.generate_filter_condition.return_value = "status = 'OPEN'"

        # Create aggregation service with mocked dependencies
        aggregation_service = AggregationService.create_aggregation_service(
            config=test_config,
            rule_loader=mock_rule_loader,
            filters_config_parser=mock_filter_parser
        )

        # # Setup test data
        # test_data = [
        #     (1, "package1", "account1", "OPEN"),
        #     (2, "package1", "account1", "FIXED"),
        #     (3, "package2", "account2", "OPEN")
        # ]
        # df = spark.createDataFrame(test_data, ["finding_id", "package_name", "cloud_account", "status"])

        # Run aggregation
        result_agg, result_assoc = aggregation_service.aggregate_findings(spark)

        # Verify mocks were called
        mock_rule_loader.load_aggregation_rules.assert_called_once()
        mock_filter_parser.generate_filter_condition.assert_called()

        # Verify results
        assert result_agg.count() > 0
        assert result_assoc.count() > 0

    def create_temp_views_from_local_catalog(self, spark):
        """
        Create temporary views from existing local catalog data.
        This method reads the actual data from local Delta tables and creates temp views
        that can be used for testing without relying on catalog registration.

        Excludes result tables: finding_group_association, finding_group_rollup
        """

        print("=== Creating Temp Views from Local Catalog Data ===")

        warehouse_path = self.get_local_warehouse_path()

        # Define all source tables (excluding result tables)
        source_tables = [
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
            "aggregation_rules",
            "aggregation_rules_findings_excluder"
        ]

        created_views = []
        failed_views = []

        for table_name in source_tables:
            table_path = f"{warehouse_path}/{table_name}"
            # Use single-part name for temp views
            view_name = table_name  # Just the table name, not spark_catalog.default.table_name

            try:
                # Read the actual Delta table data
                df = spark.read.format("delta").load(table_path)

                # Get record count for verification
                record_count = df.count()

                # Create temporary view with single-part name
                df.createOrReplaceTempView(view_name)

                # Verify the view is accessible
                test_df = spark.table(view_name)
                verify_count = test_df.count()

                if verify_count == record_count:
                    created_views.append(table_name)
                    print(f"✅ Created temp view {table_name}: {record_count:,} rows")
                else:
                    failed_views.append(table_name)
                    print(f"❌ View verification failed for {table_name}: expected {record_count}, got {verify_count}")

            except Exception as e:
                failed_views.append(table_name)
                print(f"❌ Failed to create temp view for {table_name}: {e}")

                # For debugging, try to read just the schema
                try:
                    df_schema = spark.read.format("delta").load(table_path).schema
                    print(f"   📋 Schema available: {len(df_schema.fields)} fields")
                except Exception as schema_error:
                    print(f"   📋 Cannot read schema: {schema_error}")

        # Summary
        print(f"\n=== Temp Views Creation Summary ===")
        print(f"✅ Successfully created: {len(created_views)} views")
        print(f"❌ Failed to create: {len(failed_views)} views")

        if created_views:
            print(f"Created views: {', '.join(created_views)}")

        if failed_views:
            print(f"Failed views: {', '.join(failed_views)}")

        # Verify we have the minimum required tables for create_base_df
        required_for_aggregation = [
            "findings", "plain_resources", "finding_sla_rule_connections",
            "findings_scores", "user_status", "findings_additional_data",
            "statuses", "aggregation_groups", "findings_info",
            "scoring_rules", "selection_rules"
        ]

        missing_required = [table for table in required_for_aggregation if table not in created_views]

        if missing_required:
            print(f"⚠️  Missing required tables: {', '.join(missing_required)}")
            print("   Aggregation tests may fail without these tables")
        else:
            print("✅ All required tables for aggregation are available")

        return created_views, failed_views

    def test_aggregation_service_with_temp_views(self, spark, test_config):
        """Test AggregationService using temp views created from local catalog data"""

        # Create temp views from existing local catalog data
        created_views, failed_views = self.create_temp_views_from_local_catalog(spark)

        # Verify we have enough tables to proceed
        if len(failed_views) > len(created_views):
            pytest.fail(f"Too many tables failed to load: {failed_views}")

        print("=== Running Aggregation Test with Temp Views ===")

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

        # Create mock filter parser
        mock_filter_parser = Mock()
        mock_filter_parser.generate_filter_condition.return_value = "package_name IS NOT NULL"

        # Create aggregation service
        aggregation_service = AggregationService.create_aggregation_service(
            config=test_config,
            rule_loader=mock_rule_loader,
            filters_config_parser=mock_filter_parser
        )

        # Run aggregation - this should now work with temp views!
        try:
            result_agg, result_assoc = aggregation_service.aggregate_findings(spark)

            # Verify results
            agg_count = result_agg.count()
            assoc_count = result_assoc.count()

            print(f"✅ Aggregation completed successfully!")
            print(f"   📊 Aggregation results: {agg_count:,} groups")
            print(f"   📊 Association results: {assoc_count:,} associations")

            # Show sample results
            if agg_count > 0:
                print("\n📋 Sample aggregation results:")
                result_agg.show(5, truncate=False)

            if assoc_count > 0:
                print("\n📋 Sample association results:")
                result_assoc.show(5, truncate=False)

            # Verify mocks were called
            mock_rule_loader.load_aggregation_rules.assert_called_once()
            mock_filter_parser.generate_filter_condition.assert_called()

            # Basic assertions
            assert agg_count > 0, "Should have aggregation results"
            assert assoc_count > 0, "Should have association results"

            print("✅ All test assertions passed!")

        except Exception as e:
            print(f"❌ Aggregation failed: {e}")

            # Debug information
            print("\n🔍 Debug Information:")
            print("Available tables:")
            spark.sql("SHOW TABLES").show()

            # Try to show sample data from key tables
            try:
                findings_sample = spark.table("spark_catalog.default.findings")
                print(f"Findings table sample ({findings_sample.count()} total rows):")
                findings_sample.show(3)
            except Exception as debug_error:
                print(f"Cannot access findings table: {debug_error}")

            raise

