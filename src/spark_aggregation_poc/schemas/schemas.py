"""
Centralized schema definitions for all dataframes.
No schema inference - all schemas explicitly defined.
"""
from enum import Enum
from typing import Dict, Union
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, DoubleType, ArrayType, LongType
)


class ColumnNames:
    """Column name constants - use instead of magic strings"""

    # Findings columns
    ID = "id"
    FINDING_ID = "finding_id"
    FINDING_ID_STR = "finding_id_str"  # Original finding_id as string
    TITLE = "title"
    SOURCE = "source"
    ORIGINAL_FINDING_ID = "original_finding_id"
    PACKAGE_NAME = "package_name"
    MAIN_RESOURCE_ID = "main_resource_id"
    AGGREGATION_GROUP_ID = "aggregation_group_id"
    RULE_ID = "rule_id"
    RULE_FAMILY = "rule_family"
    RULE_TYPE = "rule_type"
    FIX_ID = "fix_id"
    FIX_TYPE = "fix_type"
    FIX_SUBTYPE = "fix_subtype"
    FINDING_TYPE_STR = "finding_type_str"
    CREATED_TIME = "created_time"
    DISCOVERED_TIME = "discovered_time"
    LAST_COLLECTED_TIME = "last_collected_time"
    LAST_REPORTED_TIME = "last_reported_time"
    SCAN_ID = "scan_id"
    IMAGE_ID = "image_id"
    DATASOURCE_ID = "datasource_id"
    DATASOURCE_DEFINITION_ID = "datasource_definition_id"
    ORIGINAL_STATUS = "original_status"
    DUE_DATE = "due_date"
    TIME_TO_REMEDIATE = "time_to_remediate"
    CATEGORY_FIELD = "category"
    SUB_CATEGORY = "sub_category"
    RESOURCE_REPORTED_NOT_EXIST = "resource_reported_not_exist"
    EDITABLE = "editable"
    REOPEN_DATE = "reopen_date"
    FIX_VENDOR_ID = "fix_vendor_id"

    # Resource columns
    RESOURCE_ID = "resource_id"
    R1_RESOURCE_TYPE = "r1_resource_type"
    R1_RESOURCE_NAME = "r1_resource_name"
    R1_RESOURCE_ID = "r1_resource_id"
    R2_RESOURCE_TYPE = "r2_resource_type"
    R2_RESOURCE_NAME = "r2_resource_name"
    R2_RESOURCE_ID = "r2_resource_id"
    R3_RESOURCE_TYPE = "r3_resource_type"
    R3_RESOURCE_NAME = "r3_resource_name"
    R3_RESOURCE_ID = "r3_resource_id"
    R4_RESOURCE_TYPE = "r4_resource_type"
    R4_RESOURCE_NAME = "r4_resource_name"
    R4_RESOURCE_ID = "r4_resource_id"
    CLOUD_ACCOUNT = "cloud_account"
    CLOUD_ACCOUNT_FRIENDLY_NAME = "cloud_account_friendly_name"
    CLOUD_PROVIDER = "cloud_provider"
    RESOURCE_TYPE = "resource_type"
    TAGS_VALUES = "tags_values"
    SEEM_TAGS_VALUES = "seem_tags_values"
    TAGS_KEY_VALUES = "tags_key_values"
    SEEM_TAGS_KEY_VALUES = "seem_tags_key_values"
    LAST_SEEN = "last_seen"
    FIRST_SEEN = "first_seen"

    # Score columns
    SEVERITY = "severity"
    SCORE = "score"
    ORIGINAL_SCORE = "original_score"
    NORMALIZED_SCORE = "normalized_score"
    SCORING_RULE_ID = "scoring_rule_id"
    FACTORIZED_SCORE = "factorized_score"
    USER_SCORE = "user_score"
    USER_SCORE_SET_BY = "user_score_set_by"
    USER_SCORE_UPDATED_TIME = "user_score_updated_time"

    # Status columns
    KEY = "key"
    STATUS_KEY = "status_key"
    ACTUAL_STATUS_KEY = "actual_status_key"
    SYSTEM_STATUS_KEY = "system_status_key"
    USER_STATUS_KEY = "user_status_key"
    CATEGORY = "category"
    SUB_STATUS = "sub_status"
    TYPE = "type"
    REASON = "reason"
    DESCRIPTION = "description"
    IS_EXTENDED = "is_extended"
    ENABLED = "enabled"
    CREATED_BY_USER_ID = "created_by_user_id"
    UPDATED_BY_USER_ID = "updated_by_user_id"
    UPDATED_TIME = "updated_time"

    # Aggregation output columns
    GROUP_ID = "group_id"
    GROUP_IDENTIFIER = "group_identifier"
    GROUP_IDENTIFIER_READABLE = "group_identifier_readable"
    FINDING_IDS = "finding_ids"
    CLOUD_ACCOUNTS = "cloud_accounts"
    FINDINGS_COUNT = "findings_count"
    RULE_NUMBER = "rule_number"
    MAIN_FINDING_ID = "main_finding_id"
    IS_LOCKED = "is_locked"

    # Additional data columns
    CVE = "cve"

    # Aggregation rules
    ORDER = "order"
    AGGREGATION_QUERY = "aggregation_query"
    FIELD_CALCULATION = "field_calculation"
    DISABLED = "disabled"
    IS_DELETED = "is_deleted"
    VERSION = "version"
    LAST_SUCCESSFUL_RUN = "last_successful_run"
    LAST_NUMBER_OF_GENERATED_GROUPS = "last_number_of_generated_groups"
    CREATED_BY = "created_by"
    CREATION_TIME = "creation_time"
    UPDATED_BY = "updated_by"
    CUSTOMER_DESCRIPTION = "customer_description"
    INTERNAL_DESCRIPTION = "internal_description"
    AGGREGATION_RULE_DEFINITION_ID = "aggregation_rule_definition_id"

    # Other
    SELECTION_RULE_ID = "selection_rule_id"
    SCOPE_GROUP = "scope_group"


class TableNames(str, Enum):
    """Table name constants"""
    FINDINGS = "findings"
    PLAIN_RESOURCES = "plain_resources"
    FINDINGS_SCORES = "findings_scores"
    USER_STATUS = "user_status"
    STATUSES = "statuses"
    AGGREGATION_GROUPS = "aggregation_groups"
    FINDING_SLA_RULE_CONNECTIONS = "finding_sla_rule_connections"
    FINDINGS_ADDITIONAL_DATA = "findings_additional_data"
    FINDINGS_INFO = "findings_info"
    SCORING_RULES = "scoring_rules"
    SELECTION_RULES = "selection_rules"
    AGGREGATION_RULES = "aggregation_rules"
    FINDING_GROUP_ASSOCIATION = "finding_group_association"
    FINDING_GROUP_ROLLUP = "finding_group_rollup"
    BASE_FINDINGS_VIEW = "base_findings_view"

class MetadataKeys:
    ID_COLUMN = "id_column"
    ALIAS = "alias"

class Schemas:
    """Centralized registry of all schemas"""

    @classmethod
    def findings_schema(cls) -> StructType:
        """Schema for findings table"""
        return StructType([
            StructField(ColumnNames.ID, IntegerType(), nullable=False,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "finding_id"
                        }),
            StructField(ColumnNames.DATASOURCE_ID, IntegerType(), nullable=True),
            StructField(ColumnNames.DATASOURCE_DEFINITION_ID, IntegerType(), nullable=True),
            StructField(ColumnNames.TITLE, StringType(), nullable=True),
            StructField(ColumnNames.SOURCE, StringType(), nullable=True),
            StructField(ColumnNames.FINDING_ID_STR, StringType(), nullable=True),
            StructField(ColumnNames.ORIGINAL_FINDING_ID, StringType(), nullable=True),
            StructField(ColumnNames.CREATED_TIME, StringType(), nullable=True),
            StructField(ColumnNames.DISCOVERED_TIME, StringType(), nullable=True),
            StructField(ColumnNames.DUE_DATE, StringType(), nullable=True),
            StructField(ColumnNames.LAST_COLLECTED_TIME, StringType(), nullable=True),
            StructField(ColumnNames.LAST_REPORTED_TIME, StringType(), nullable=True),
            StructField(ColumnNames.ORIGINAL_STATUS, StringType(), nullable=True),
            StructField(ColumnNames.TIME_TO_REMEDIATE, StringType(), nullable=True),
            StructField(ColumnNames.CATEGORY_FIELD, StringType(), nullable=True),
            StructField(ColumnNames.SUB_CATEGORY, StringType(), nullable=True),
            StructField(ColumnNames.RULE_ID, StringType(), nullable=True),
            StructField(ColumnNames.RESOURCE_REPORTED_NOT_EXIST, StringType(), nullable=True),
            StructField(ColumnNames.AGGREGATION_GROUP_ID, IntegerType(), nullable=True),
            StructField(ColumnNames.MAIN_RESOURCE_ID, IntegerType(), nullable=True),
            StructField(ColumnNames.PACKAGE_NAME, StringType(), nullable=True),
            StructField(ColumnNames.IMAGE_ID, StringType(), nullable=True),
            StructField(ColumnNames.SCAN_ID, StringType(), nullable=True),
            StructField(ColumnNames.EDITABLE, BooleanType(), nullable=True),
            StructField(ColumnNames.REOPEN_DATE, StringType(), nullable=True),
            StructField(ColumnNames.FINDING_TYPE_STR, StringType(), nullable=True,
                        metadata={MetadataKeys.ALIAS: "finding_type"}),
            StructField(ColumnNames.FIX_ID, StringType(), nullable=True),
            StructField(ColumnNames.FIX_VENDOR_ID, StringType(), nullable=True),
            StructField(ColumnNames.FIX_TYPE, StringType(), nullable=True),
            StructField(ColumnNames.FIX_SUBTYPE, StringType(), nullable=True),
            StructField(ColumnNames.RULE_TYPE, StringType(), nullable=True),
            StructField(ColumnNames.RULE_FAMILY, StringType(), nullable=True),
        ])

    @classmethod
    def plain_resources_schema(cls) -> StructType:
        """Schema for plain_resources table"""
        return StructType([
            StructField(ColumnNames.ID, IntegerType(), nullable=False,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "resource_id"  # Used as 'resource_id' in output
                        }),
            StructField(ColumnNames.R1_RESOURCE_TYPE, StringType(), nullable=True,
                        metadata={MetadataKeys.ALIAS: "resource_type"}),
            StructField(ColumnNames.R1_RESOURCE_NAME, StringType(), nullable=True),
            StructField(ColumnNames.R1_RESOURCE_ID, StringType(), nullable=True),
            StructField(ColumnNames.R2_RESOURCE_TYPE, StringType(), nullable=True),
            StructField(ColumnNames.R2_RESOURCE_NAME, StringType(), nullable=True),
            StructField(ColumnNames.R2_RESOURCE_ID, StringType(), nullable=True),
            StructField(ColumnNames.R3_RESOURCE_TYPE, StringType(), nullable=True),
            StructField(ColumnNames.R3_RESOURCE_NAME, StringType(), nullable=True),
            StructField(ColumnNames.R3_RESOURCE_ID, StringType(), nullable=True),
            StructField(ColumnNames.R4_RESOURCE_TYPE, StringType(), nullable=True),
            StructField(ColumnNames.R4_RESOURCE_NAME, StringType(), nullable=True),
            StructField(ColumnNames.R4_RESOURCE_ID, StringType(), nullable=True),
            StructField(ColumnNames.CLOUD_PROVIDER, StringType(), nullable=True),
            StructField(ColumnNames.CLOUD_ACCOUNT, StringType(), nullable=True),
            StructField(ColumnNames.CLOUD_ACCOUNT_FRIENDLY_NAME, StringType(), nullable=True,
                        metadata={MetadataKeys.ALIAS: "root_cloud_account_friendly_name"}),
            StructField(ColumnNames.TAGS_VALUES, StringType(), nullable=True),
            StructField(ColumnNames.SEEM_TAGS_VALUES, StringType(), nullable=True),
            StructField(ColumnNames.TAGS_KEY_VALUES, StringType(), nullable=True),
            StructField(ColumnNames.SEEM_TAGS_KEY_VALUES, StringType(), nullable=True),
            StructField(ColumnNames.LAST_SEEN, StringType(), nullable=True),
            StructField(ColumnNames.FIRST_SEEN, StringType(), nullable=True),
        ])

    @classmethod
    def findings_scores_schema(cls) -> StructType:
        """Schema for findings_scores table"""
        return StructType([
            StructField(ColumnNames.FINDING_ID, IntegerType(), nullable=False,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "score_finding_id"
                        }),
            StructField(ColumnNames.ORIGINAL_SCORE, StringType(), nullable=True),
            StructField(ColumnNames.NORMALIZED_SCORE, DoubleType(), nullable=True),
            StructField(ColumnNames.SCORING_RULE_ID, IntegerType(), nullable=True),
            StructField(ColumnNames.FACTORIZED_SCORE, DoubleType(), nullable=True),
            StructField(ColumnNames.USER_SCORE, DoubleType(), nullable=True),
            StructField(ColumnNames.USER_SCORE_SET_BY, StringType(), nullable=True),
            StructField(ColumnNames.USER_SCORE_UPDATED_TIME, StringType(), nullable=True),
            StructField(ColumnNames.SCORE, DoubleType(), nullable=True),
            StructField(ColumnNames.SEVERITY, IntegerType(), nullable=True),
            StructField(ColumnNames.UPDATED_TIME, StringType(), nullable=True),
        ])

    @classmethod
    def user_status_schema(cls) -> StructType:
        """Schema for user_status table"""
        return StructType([
            StructField(ColumnNames.ID, IntegerType(), nullable=False,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "user_status_id"
                        }),
            StructField(ColumnNames.SYSTEM_STATUS_KEY, IntegerType(), nullable=True),
            StructField(ColumnNames.USER_STATUS_KEY, IntegerType(), nullable=True),
            StructField(ColumnNames.ACTUAL_STATUS_KEY, IntegerType(), nullable=True),
            StructField(ColumnNames.UPDATED_TIME, StringType(), nullable=True),
        ])

    @classmethod
    def statuses_schema(cls) -> StructType:
        """Schema for statuses table"""
        return StructType([
            StructField(ColumnNames.KEY, IntegerType(), nullable=False,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "status_key"
                        }),
            StructField(ColumnNames.CATEGORY, StringType(), nullable=True),
            StructField(ColumnNames.SUB_STATUS, StringType(), nullable=True),
            StructField(ColumnNames.TYPE, StringType(), nullable=True),
            StructField(ColumnNames.REASON, StringType(), nullable=True),
            StructField(ColumnNames.DESCRIPTION, StringType(), nullable=True),
            StructField(ColumnNames.IS_EXTENDED, BooleanType(), nullable=True),
            StructField(ColumnNames.ENABLED, BooleanType(), nullable=True),
            StructField(ColumnNames.EDITABLE, BooleanType(), nullable=True),
            StructField(ColumnNames.CREATED_BY_USER_ID, IntegerType(), nullable=True),
            StructField(ColumnNames.UPDATED_BY_USER_ID, IntegerType(), nullable=True),
        ])

    @classmethod
    def aggregation_groups_schema(cls) -> StructType:
        """Schema for aggregation_groups table"""
        return StructType([
            StructField(ColumnNames.ID, IntegerType(), nullable=True,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "existing_group_id"
                        }),
            StructField(ColumnNames.MAIN_FINDING_ID, IntegerType(), nullable=True),
            StructField(ColumnNames.GROUP_IDENTIFIER, StringType(), nullable=True),
            StructField(ColumnNames.IS_LOCKED, BooleanType(), nullable=True),
        ])

    @classmethod
    def finding_sla_rule_connections_schema(cls) -> StructType:
        """Schema for finding_sla_rule_connections table"""
        return StructType([
            StructField(ColumnNames.FINDING_ID, IntegerType(), nullable=True,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "sla_connection_id"
                        }),
        ])

    @classmethod
    def findings_additional_data_schema(cls) -> StructType:
        """Schema for findings_additional_data table"""
        return StructType([
            StructField(ColumnNames.FINDING_ID, IntegerType(), nullable=True,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "additional_data_id"
                        }),
            StructField(ColumnNames.CVE, ArrayType(StringType()), nullable=True),
        ])

    @classmethod
    def findings_info_schema(cls) -> StructType:
        """Schema for findings_info table"""
        return StructType([
            StructField(ColumnNames.ID, IntegerType(), nullable=True,
                        metadata={
                            MetadataKeys.ID_COLUMN: True,
                            MetadataKeys.ALIAS: "findings_info_id"
                        }),
        ])

    @classmethod
    def scoring_rules_schema(cls) -> StructType:
        """Schema for scoring_rules table"""
        return StructType([
            StructField(ColumnNames.ID, IntegerType(), nullable=True),
            StructField(ColumnNames.SELECTION_RULE_ID, IntegerType(), nullable=True),
        ])

    @classmethod
    def selection_rules_schema(cls) -> StructType:
        """Schema for selection_rules table"""
        return StructType([
            StructField(ColumnNames.ID, IntegerType(), nullable=True),
            StructField(ColumnNames.SCOPE_GROUP, IntegerType(), nullable=True),
        ])

    @classmethod
    def aggregation_rules_schema(cls) -> StructType:
        """Schema for aggregation_rules table"""
        return StructType([
            StructField(ColumnNames.ID, IntegerType(), nullable=False),
            StructField(ColumnNames.ORDER, IntegerType(), nullable=True),
            StructField(ColumnNames.TYPE, StringType(), nullable=True),
            StructField(ColumnNames.AGGREGATION_QUERY, StringType(), nullable=True),
            StructField(ColumnNames.FIELD_CALCULATION, StringType(), nullable=True),
            StructField(ColumnNames.DISABLED, BooleanType(), nullable=True),
            StructField(ColumnNames.IS_DELETED, BooleanType(), nullable=True),
            StructField(ColumnNames.VERSION, IntegerType(), nullable=True),
            StructField(ColumnNames.LAST_SUCCESSFUL_RUN, StringType(), nullable=True),
            StructField(ColumnNames.LAST_NUMBER_OF_GENERATED_GROUPS, IntegerType(), nullable=True),
            StructField(ColumnNames.CREATED_BY, StringType(), nullable=True),
            StructField(ColumnNames.CREATION_TIME, StringType(), nullable=True),
            StructField(ColumnNames.UPDATED_BY, StringType(), nullable=True),
            StructField(ColumnNames.UPDATED_TIME, StringType(), nullable=True),
            StructField(ColumnNames.CUSTOMER_DESCRIPTION, StringType(), nullable=True),
            StructField(ColumnNames.INTERNAL_DESCRIPTION, StringType(), nullable=True),
            StructField(ColumnNames.AGGREGATION_RULE_DEFINITION_ID, IntegerType(), nullable=True),
        ])

    @classmethod
    def finding_group_association_schema(cls) -> StructType:
        """Schema for finding_group_association output"""
        return StructType([
            StructField(ColumnNames.GROUP_IDENTIFIER, StringType(), nullable=False),
            StructField(ColumnNames.GROUP_IDENTIFIER_READABLE, StringType(), nullable=False),
            StructField(ColumnNames.FINDING_ID, IntegerType(), nullable=False),
        ])

    @classmethod
    def finding_group_rollup_base_schema(cls) -> StructType:
        """Base schema for finding_group_rollup (without dynamic group_by columns)"""
        return StructType([
            StructField(ColumnNames.GROUP_IDENTIFIER, StringType(), nullable=False),
            StructField(ColumnNames.GROUP_IDENTIFIER_READABLE, StringType(), nullable=False),
            StructField(ColumnNames.FINDING_IDS, ArrayType(IntegerType()), nullable=False),
            StructField(ColumnNames.CLOUD_ACCOUNTS, ArrayType(StringType()), nullable=False),
            StructField(ColumnNames.FINDINGS_COUNT, LongType(), nullable=False),
            StructField(ColumnNames.RULE_NUMBER, IntegerType(), nullable=False),
        ])

    @classmethod
    def get_schema_for_table(cls, table_name: Union[str, TableNames]) -> StructType:
        if isinstance(table_name, TableNames):
            table_name = table_name.value

        schema_map: Dict[str, StructType] = {
            TableNames.FINDINGS.value: Schemas.findings_schema(),
            TableNames.PLAIN_RESOURCES.value: Schemas.plain_resources_schema(),
            TableNames.FINDINGS_SCORES.value: Schemas.findings_scores_schema(),
            TableNames.USER_STATUS.value: Schemas.user_status_schema(),
            TableNames.STATUSES.value: Schemas.statuses_schema(),
            TableNames.AGGREGATION_GROUPS.value: Schemas.aggregation_groups_schema(),
            TableNames.FINDING_SLA_RULE_CONNECTIONS.value: Schemas.finding_sla_rule_connections_schema(),
            TableNames.FINDINGS_ADDITIONAL_DATA.value: Schemas.findings_additional_data_schema(),
            TableNames.FINDINGS_INFO.value: Schemas.findings_info_schema(),
            TableNames.SCORING_RULES.value: Schemas.scoring_rules_schema(),
            TableNames.SELECTION_RULES.value: Schemas.selection_rules_schema(),
            TableNames.AGGREGATION_RULES.value: Schemas.aggregation_rules_schema(),
            TableNames.FINDING_GROUP_ASSOCIATION.value: Schemas.finding_group_association_schema(),
            TableNames.FINDING_GROUP_ROLLUP.value: Schemas.finding_group_rollup_base_schema(),
        }
        return schema_map[table_name]


    @classmethod
    def get_id_column_for_table(cls, table_name: Union[str, TableNames]) -> str:
        schema = Schemas.get_schema_for_table(table_name)

        # Find field with ID_COLUMN metadata
        for field in schema.fields:
            if field.metadata.get(MetadataKeys.ID_COLUMN, False):
                return field.name

        # Fallback to "id" if no metadata found
        return ColumnNames.ID

    @classmethod
    def get_alias_for_field(cls, schema: StructType, field_name: str) -> str:
        """
        Get the alias for a field from its metadata.
        Returns the field name itself if no alias is defined.
        """
        for field in schema.fields:
            if field.name == field_name:
                return field.metadata.get(MetadataKeys.ALIAS, field_name)
        return field_name