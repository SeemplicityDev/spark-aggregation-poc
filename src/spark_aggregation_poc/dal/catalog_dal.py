"""Catalog Data Access Layer with strong typing"""
from typing import Final
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import CatalogDalInterface
from spark_aggregation_poc.schemas.schemas import ColumnNames, TableNames, Schemas


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
        print("Creating base findings view from catalog tables...")

        # Use constants for table and column names
        view_name: Final[str] = TableNames.BASE_FINDINGS_VIEW.value

        # Build SQL using ColumnNames constants for type safety
        sql_str: str = f"""
            CREATE OR REPLACE TEMPORARY VIEW {view_name} AS
            SELECT
                {self.column_with_alias(TableNames.FINDINGS.value, ColumnNames.ID, Schemas.findings_schema())},
                {self.column_with_alias(TableNames.FINDINGS.value, ColumnNames.PACKAGE_NAME, Schemas.findings_schema())},
                {TableNames.FINDINGS.value}.{ColumnNames.MAIN_RESOURCE_ID},
                {TableNames.FINDINGS.value}.{ColumnNames.AGGREGATION_GROUP_ID},
                {TableNames.FINDINGS.value}.{ColumnNames.SOURCE},
                {TableNames.FINDINGS.value}.{ColumnNames.RULE_FAMILY},
                {TableNames.FINDINGS.value}.{ColumnNames.RULE_ID},
                {self.column_with_alias(TableNames.FINDINGS.value, ColumnNames.FINDING_TYPE_STR, Schemas.findings_schema())},
                {TableNames.FINDINGS.value}.{ColumnNames.FIX_SUBTYPE},
                {TableNames.FINDINGS.value}.{ColumnNames.FIX_TYPE},
                {TableNames.FINDINGS.value}.{ColumnNames.FIX_ID},
                {self.column_with_alias(TableNames.FINDING_SLA_RULE_CONNECTIONS.value, ColumnNames.FINDING_ID, Schemas.finding_sla_rule_connections_schema())},
                {self.column_with_alias(TableNames.PLAIN_RESOURCES.value, ColumnNames.ID, Schemas.plain_resources_schema())},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.CLOUD_ACCOUNT},
                {self.column_with_alias(TableNames.PLAIN_RESOURCES.value, ColumnNames.CLOUD_ACCOUNT_FRIENDLY_NAME, Schemas.plain_resources_schema())},
                {self.column_with_alias(TableNames.PLAIN_RESOURCES.value, ColumnNames.R1_RESOURCE_TYPE, Schemas.plain_resources_schema())},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.TAGS_VALUES},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.TAGS_KEY_VALUES},
                {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.CLOUD_PROVIDER},
                {self.column_with_alias(TableNames.FINDINGS_SCORES.value, ColumnNames.FINDING_ID, Schemas.findings_scores_schema())},
                {TableNames.FINDINGS_SCORES.value}.{ColumnNames.SEVERITY},
                {self.column_with_alias(TableNames.USER_STATUS.value, ColumnNames.ID, Schemas.user_status_schema())},
                {TableNames.USER_STATUS.value}.{ColumnNames.ACTUAL_STATUS_KEY},
                {self.column_with_alias(TableNames.FINDINGS_ADDITIONAL_DATA.value, ColumnNames.FINDING_ID, Schemas.findings_additional_data_schema())},
                {self.column_with_alias(TableNames.STATUSES.value, ColumnNames.KEY, Schemas.statuses_schema())},
                {self.column_with_alias(TableNames.AGGREGATION_GROUPS.value, ColumnNames.ID, Schemas.aggregation_groups_schema())},
                {self.column_with_alias(TableNames.AGGREGATION_GROUPS.value, ColumnNames.MAIN_FINDING_ID, Schemas.aggregation_groups_schema())},
                {self.column_with_alias(TableNames.AGGREGATION_GROUPS.value, ColumnNames.GROUP_IDENTIFIER, Schemas.aggregation_groups_schema())},
                {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.IS_LOCKED},
                {self.column_with_alias(TableNames.FINDINGS_INFO.value, ColumnNames.ID, Schemas.findings_info_schema())},
                {TableNames.STATUSES.value}.{ColumnNames.CATEGORY},
                {TableNames.FINDINGS_ADDITIONAL_DATA.value}.{ColumnNames.CVE}[1] as {ColumnNames.CVE},
                {TableNames.SELECTION_RULES.value}.{ColumnNames.SCOPE_GROUP}
            FROM {self.catalog_table_prefix}{TableNames.FINDINGS.value}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.FINDING_SLA_RULE_CONNECTIONS.value} ON
                {TableNames.FINDINGS.value}.{ColumnNames.ID} = {TableNames.FINDING_SLA_RULE_CONNECTIONS.value}.{ColumnNames.FINDING_ID}
            JOIN {self.catalog_table_prefix}{TableNames.PLAIN_RESOURCES.value} ON
                {TableNames.FINDINGS.value}.{ColumnNames.MAIN_RESOURCE_ID} = {TableNames.PLAIN_RESOURCES.value}.{ColumnNames.ID}
            JOIN {self.catalog_table_prefix}{TableNames.FINDINGS_SCORES.value} ON
                {TableNames.FINDINGS.value}.{ColumnNames.ID} = {TableNames.FINDINGS_SCORES.value}.{ColumnNames.FINDING_ID}
            JOIN {self.catalog_table_prefix}{TableNames.USER_STATUS.value} ON
                {TableNames.USER_STATUS.value}.{ColumnNames.ID} = {TableNames.FINDINGS.value}.{ColumnNames.ID}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.FINDINGS_ADDITIONAL_DATA.value} ON
                {TableNames.FINDINGS.value}.{ColumnNames.ID} = {TableNames.FINDINGS_ADDITIONAL_DATA.value}.{ColumnNames.FINDING_ID}
            JOIN {self.catalog_table_prefix}{TableNames.STATUSES.value} ON
                {TableNames.STATUSES.value}.{ColumnNames.KEY} = {TableNames.USER_STATUS.value}.{ColumnNames.ACTUAL_STATUS_KEY}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.AGGREGATION_GROUPS.value} ON
                {TableNames.FINDINGS.value}.{ColumnNames.AGGREGATION_GROUP_ID} = {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.ID}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.FINDINGS_INFO.value} ON
                {TableNames.FINDINGS_INFO.value}.{ColumnNames.ID} = {TableNames.FINDINGS.value}.{ColumnNames.ID}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.SCORING_RULES.value} ON
                {TableNames.FINDINGS_SCORES.value}.{ColumnNames.SCORING_RULE_ID} = {TableNames.SCORING_RULES.value}.{ColumnNames.ID}
            LEFT OUTER JOIN {self.catalog_table_prefix}{TableNames.SELECTION_RULES.value} ON
                {TableNames.SCORING_RULES.value}.{ColumnNames.SELECTION_RULE_ID} = {TableNames.SELECTION_RULES.value}.{ColumnNames.ID}
            WHERE {TableNames.FINDINGS.value}.{ColumnNames.PACKAGE_NAME} IS NOT NULL
            AND ({TableNames.FINDINGS.value}.{ColumnNames.ID} <> {TableNames.AGGREGATION_GROUPS.value}.{ColumnNames.MAIN_FINDING_ID}
            OR {TableNames.FINDINGS.value}.{ColumnNames.AGGREGATION_GROUP_ID} is null)
        """

        # todo -> also join the following tables:
        #  resource_to_scopes,
        #  scope_groups,
        #  finding_ticket_associations,
        #  tickets,
        #  user_sla


        # Execute view creation
        spark.sql(sql_str)
        print(f"✅ Created temporary view: {view_name}")

        # Read the view as a DataFrame
        df: DataFrame = spark.table(view_name)

        # Log row count for debugging
        row_count: int = df.count()
        print(f"📊 Base findings view contains {row_count:,} rows")

        return df

    def column_with_alias(self, table: str, col_name: str, schema: StructType) -> str:
        alias = Schemas.get_alias_for_field(schema, col_name)
        table_col = f"{table}.{col_name}"
        return f"{table_col} as {alias}" if alias != col_name else table_col

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