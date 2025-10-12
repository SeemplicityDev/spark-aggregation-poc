"""Catalog Data Access Layer with strong typing"""
from typing import Final
from pyspark.sql import DataFrame, SparkSession

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import CatalogDalInterface
from spark_aggregation_poc.schemas.schema_registry import ColumnNames, TableNames


class CatalogDal(CatalogDalInterface):
    """Data access layer for catalog operations with type safety"""

    _allow_init: bool = False

    @classmethod
    def create_catalog_dal(cls, config: Config) -> 'CatalogDal':
        """
        Factory method to create CatalogDal instance.

        Args:
            config: Application configuration

        Returns:
            CatalogDal instance
        """
        cls._allow_init = True
        result = CatalogDal(config=config)
        cls._allow_init = False
        return result

    def __init__(self, config: Config) -> None:
        """
        Initialize CatalogDal.

        Args:
            config: Application configuration with catalog settings
        """
        if not self._allow_init:
            raise RuntimeError(
                "CatalogDal must be created using create_catalog_dal() factory method"
            )
        self.catalog_table_prefix: str = config.catalog_table_prefix

    def read_base_findings(self, spark: SparkSession) -> DataFrame:
        """
        Create and read base findings view with all necessary joins.
        Uses ColumnNames and TableNames constants for type safety.

        This method:
        1. Creates a temporary view joining all necessary tables
        2. Filters for aggregatable findings (non-null package_name)
        3. Excludes findings that are already main findings in groups

        Args:
            spark: Active SparkSession

        Returns:
            DataFrame with base findings data including all joined information

        Schema includes columns:
            - finding_id (from findings.id)
            - package_name
            - main_resource_id
            - aggregation_group_id
            - source
            - rule_family
            - rule_id
            - sla_connection_id
            - resource_id
            - cloud_account
            - root_cloud_account_friendly_name
            - resource_type
            - tags_values
            - tags_key_values
            - cloud_provider
            - score_finding_id
            - severity
            - user_status_id
            - actual_status_key
            - additional_data_id
            - status_key
            - existing_group_id
            - existing_main_finding_id
            - existing_group_identifier
            - is_locked
            - findings_info_id
            - finding_type
            - fix_subtype
            - category
            - fix_id
            - cve
            - fix_type
            - scope_group
        """
        print("Creating base findings view from catalog tables...")

        # Use constants for table and column names
        view_name: Final[str] = TableNames.BASE_FINDINGS_VIEW.value

        # Build SQL using ColumnNames constants for type safety
        sql_str: str = f"""
            CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
            SELECT
                findings.{ColumnNames.ID} as {ColumnNames.FINDING_ID},
                findings.{ColumnNames.PACKAGE_NAME} as {ColumnNames.PACKAGE_NAME},
                findings.{ColumnNames.MAIN_RESOURCE_ID},
                findings.{ColumnNames.AGGREGATION_GROUP_ID},
                findings.{ColumnNames.SOURCE},
                findings.{ColumnNames.RULE_FAMILY},
                findings.{ColumnNames.RULE_ID},
                {TableNames.FINDING_SLA_RULE_CONNECTIONS.value}.{ColumnNames.FINDING_ID} as sla_connection_id,
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.ID} as {ColumnNames.RESOURCE_ID},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.CLOUD_ACCOUNT},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.CLOUD_ACCOUNT_FRIENDLY_NAME} as root_cloud_account_friendly_name,
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.R1_RESOURCE_TYPE} as {ColumnNames.RESOURCE_TYPE},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.TAGS_VALUES} as {ColumnNames.TAGS_VALUES},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.TAGS_KEY_VALUES} as {ColumnNames.TAGS_KEY_VALUES},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.CLOUD_PROVIDER} as {ColumnNames.CLOUD_PROVIDER},
                {TableNames.FINDINGS_SCORES.value}.{ColumnNames.FINDING_ID} as score_finding_id,
                {TableNames.FINDINGS_SCORES.value}.{ColumnNames.SEVERITY},
                {TableNames.USER_STATUS.value}.{ColumnNames.ID} as user_status_id,
                {TableNames.USER_STATUS.value}.{ColumnNames.ACTUAL_STATUS_KEY},
                {TableNames.FINDINGS_ADDITIONAL_DATA.value}.{ColumnNames.FINDING_ID} as additional_data_id,
                {TableNames.STATUSES.value}.{ColumnNames.KEY} as status_key,
                {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.ID} as existing_group_id,
                {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.MAIN_FINDING_ID} as existing_main_finding_id,
                {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.GROUP_IDENTIFIER} as existing_group_identifier,
                {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.IS_LOCKED},
                {TableNames.FINDINGS_INFO.value}.{ColumnNames.ID} as findings_info_id,
                findings.{ColumnNames.FINDING_TYPE_STR} as finding_type,
                findings.{ColumnNames.FIX_SUBTYPE} as {ColumnNames.FIX_SUBTYPE},
                {TableNames.STATUSES.value}.{ColumnNames.CATEGORY} as {ColumnNames.CATEGORY},
                findings.{ColumnNames.FIX_ID} as {ColumnNames.FIX_ID},
                {TableNames.FINDINGS_ADDITIONAL_DATA.value}.{ColumnNames.CVE}[1] as {ColumnNames.CVE},
                findings.{ColumnNames.FIX_TYPE} as {ColumnNames.FIX_TYPE},
                {TableNames.SELECTION_RULES.value}.{ColumnNames.SCOPE_GROUP} as {ColumnNames.SCOPE_GROUP}
            FROM {self.catalog_table_prefix}{TableNames.FINDINGS.value}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.FINDING_SLA_RULE_CONNECTIONS.value} ON
                findings.{ColumnNames.ID} = {TableNames.FINDING_SLA_RULE_CONNECTIONS.value}.{ColumnNames.FINDING_ID}
            JOIN {self.catalog_table_prefix}{TableNames.PLAIN_RESOURCES.value} ON
                findings.{ColumnNames.MAIN_RESOURCE_ID} = {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.ID}
            JOIN {self.catalog_table_prefix}{TableNames.FINDINGS_SCORES.value} ON
                findings.{ColumnNames.ID} = {TableNames.FINDINGS_SCORES.value}.{ColumnNames.FINDING_ID}
            JOIN {self.catalog_table_prefix}{TableNames.USER_STATUS.value} ON
                {TableNames.USER_STATUS.value}.{ColumnNames.ID} = findings.{ColumnNames.ID}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.FINDINGS_ADDITIONAL_DATA.value} ON
                findings.{ColumnNames.ID} = {TableNames.FINDINGS_ADDITIONAL_DATA.value}.{ColumnNames.FINDING_ID}
            JOIN {self.catalog_table_prefix}{TableNames.STATUSES.value} ON
                {TableNames.STATUSES.value}.{ColumnNames.KEY} = {TableNames.USER_STATUS.value}.{ColumnNames.ACTUAL_STATUS_KEY}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.AGGREGATION_GROUPS.value} ON
                findings.{ColumnNames.AGGREGATION_GROUP_ID} = {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.ID}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.FINDINGS_INFO.value} ON
                {TableNames.FINDINGS_INFO.value}.{ColumnNames.ID} = findings.{ColumnNames.ID}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.SCORING_RULES.value} ON
                {TableNames.FINDINGS_SCORES.value}.{ColumnNames.SCORING_RULE_ID} = {TableNames.SCORING_RULES.value}.{ColumnNames.ID}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.SELECTION_RULES.value} ON
                {TableNames.SCORING_RULES.value}.{ColumnNames.SELECTION_RULE_ID} = {TableNames.SELECTION_RULES.value}.{ColumnNames.ID}
            WHERE findings.{ColumnNames.PACKAGE_NAME} IS NOT NULL
            AND (findings.{ColumnNames.ID} <> {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.MAIN_FINDING_ID}
            OR findings.{ColumnNames.AGGREGATION_GROUP_ID} is null)
        """

        # Execute view creation
        spark.sql(sql_str)
        print(f"✅ Created temporary view: {view_name}")

        # Read the view as a DataFrame
        df: DataFrame = spark.table(view_name)

        # Log row count for debugging
        row_count: int = df.count()
        print(f"📊 Base findings view contains {row_count:,} rows")

        return df

    def save_to_catalog(self, df: DataFrame, table_name: str) -> None:
        """
        Save DataFrame to catalog table.

        Args:
            df: DataFrame to save
            table_name: Name of the target table (without prefix)

        Note:
            Uses Delta format for ACID transactions and time travel
        """
        full_table_name: str = f"{self.catalog_table_prefix}{table_name}"

        print(f"💾 Saving to catalog table: {full_table_name}")

        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(full_table_name)

        saved_count: int = df.count()
        print(f"✅ Saved {saved_count:,} rows to {full_table_name}")