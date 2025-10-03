from typing import List

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import (
    col, when, coalesce, lit,
    collect_list, count,
    max as spark_max, min as spark_min,
    first, bool_or
)


class RollupUtil:
    """
    Separate class that handles all the complex engine-compatible aggregations
    """

    # Status categories
    STATUS_OPEN = "OPEN"
    STATUS_FIXED = "FIXED"
    STATUS_IGNORED = "IGNORED"
    DUMMY_OPEN_STATUS = -1

    @classmethod
    def get_basic_rollup(cls, df: DataFrame, rule_idx: int) -> list[Column]:
        """
        Returns all aggregation expressions for the .agg() method
        Combines basic fields with engine-compatible calculated fields
        """

        # Basic aggregations (your original 4 lines)
        basic_aggs: list[Column] = [
            collect_list("finding_id").alias("finding_ids"),
            collect_list("cloud_account").alias("cloud_accounts"),
            count("finding_id").alias("count"),
            lit(rule_idx).alias("rule_number")
        ]

        return basic_aggs

    @classmethod
    def get_all_rollups(cls, df: DataFrame, rule_idx: int) -> List:
        """
        Returns all aggregation expressions for the .agg() method
        Returns calculated fields as done in engine repo
        """

        # Engine-compatible calculated fields
        engine_aggs = [
            # ValueBySortedField with OPEN filter and mode() default
            coalesce(
                cls._first_by_priority("category", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("category")
            ).alias("category"),

            coalesce(
                cls._first_by_priority("sub_category", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("sub_category")
            ).alias("sub_category"),

            coalesce(
                cls._first_by_priority("rule_id", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("rule_id")
            ).alias("rule_id"),

            coalesce(
                cls._first_by_priority("fix_type", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("fix_type")
            ).alias("fix_type"),

            coalesce(
                cls._first_by_priority("fix_subtype", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("fix_subtype")
            ).alias("fix_subtype"),

            coalesce(
                cls._first_by_priority("fix_vendor_id", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("fix_vendor_id")
            ).alias("fix_vendor_id"),

            coalesce(
                cls._first_by_priority("rule_family", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("rule_family")
            ).alias("rule_family"),

            coalesce(
                cls._first_by_priority("rule_type", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("rule_type")
            ).alias("rule_type"),

            coalesce(
                cls._first_by_priority("finding_type_str", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("finding_type_str")
            ).alias("finding_type_str"),

            coalesce(
                cls._first_by_priority("source", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("source")
            ).alias("source"),

            coalesce(
                cls._first_by_priority("datasource_id", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("datasource_id")
            ).alias("datasource_id"),

            coalesce(
                cls._first_by_priority("datasource_definition_id", "status_category", cls.STATUS_OPEN),
                cls._most_common_value("datasource_definition_id")
            ).alias("datasource_definition_id"),

            # Simple calculated fields
            spark_max("fix_id").alias("fix_id"),
            cls._most_common_value("package_name").alias("package_name"),

            # SameOrDefaultCalculatedField
            when(
                spark_min("image_id") == spark_max("image_id"),
                spark_min("image_id")
            ).otherwise(lit("")).alias("image_id"),

            when(
                spark_min("scan_id") == spark_max("scan_id"),
                spark_min("scan_id")
            ).otherwise(lit("")).alias("scan_id"),

            when(
                spark_min("resource_reported_not_exist") == spark_max("resource_reported_not_exist"),
                spark_min("resource_reported_not_exist")
            ).otherwise(lit("")).alias("resource_reported_not_exist"),

            # EmptyCalculatedField
            lit("").alias("original_finding_id"),

            # Time-based fields
            coalesce(
                cls._first_by_priority("last_reported_time", "status_category", cls.STATUS_OPEN),
                spark_max("last_reported_time")
            ).alias("last_reported_time"),

            coalesce(
                cls._first_by_priority("discovered_time", "status_category", cls.STATUS_OPEN),
                spark_min("discovered_time")
            ).alias("discovered_time"),

            coalesce(
                cls._first_by_priority("created_time", "status_category", cls.STATUS_OPEN),
                spark_min("created_time")
            ).alias("created_time"),

            # Status key calculations
            spark_min(
                when(col("status_category") == cls.STATUS_OPEN, col("system_status_key"))
            ).alias("open_system_status_key"),

            spark_min(
                when(col("status_category") == cls.STATUS_OPEN, col("user_status_key"))
            ).alias("open_user_status_key"),

            spark_min(
                when(col("status_category") == cls.STATUS_FIXED, col("system_status_key"))
            ).alias("fixed_system_status_key"),

            spark_min(
                when(col("status_category") == cls.STATUS_FIXED, col("user_status_key"))
            ).alias("fixed_user_status_key"),

            spark_min(
                when(col("status_category") == cls.STATUS_IGNORED, col("system_status_key"))
            ).alias("ignored_system_status_key"),

            spark_min(
                when(col("status_category") == cls.STATUS_IGNORED, col("user_status_key"))
            ).alias("ignored_user_status_key"),

            # Complex status calculations
            spark_min(
                when(
                    (col("status_category").isin([cls.STATUS_FIXED, cls.STATUS_IGNORED])) &
                    (col("user_status_key").isNotNull()),
                    col("user_status_key")
                ).otherwise(lit(cls.DUMMY_OPEN_STATUS))
            ).alias("min_closed_user_status"),

            spark_min(
                when(
                    (col("status_category") == cls.STATUS_OPEN) & (col("user_status_key").isNotNull()),
                    col("user_status_key")
                ).when(
                    (col("status_category") == cls.STATUS_OPEN) & (col("user_status_key").isNull()),
                    lit(cls.DUMMY_OPEN_STATUS)
                )
            ).alias("min_open_user_status"),

            # Score and severity calculations - Open findings only
            coalesce(
                cls._first_by_priority("score", "status_category", cls.STATUS_OPEN),
                spark_max(when(col("status_category") == cls.STATUS_OPEN, col("score")))
            ).alias("max_open_score"),

            coalesce(
                cls._first_by_priority("severity", "status_category", cls.STATUS_OPEN),
                spark_max(when(col("status_category") == cls.STATUS_OPEN, col("severity")))
            ).alias("max_open_severity"),

            coalesce(
                cls._first_by_priority("user_score", "status_category", cls.STATUS_OPEN),
                spark_max(when(col("status_category") == cls.STATUS_OPEN, col("user_score")))
            ).alias("max_open_user_score"),

            coalesce(
                cls._first_by_priority("normalized_score", "status_category", cls.STATUS_OPEN),
                spark_max(when(col("status_category") == cls.STATUS_OPEN, col("normalized_score")))
            ).alias("max_open_normalized_score"),

            coalesce(
                cls._first_by_priority("factorized_score", "status_category", cls.STATUS_OPEN),
                spark_max(when(col("status_category") == cls.STATUS_OPEN, col("factorized_score")))
            ).alias("max_open_factorized_score"),

            # All findings (no status filter)
            spark_max("score").alias("max_score"),
            spark_max("severity").alias("max_severity"),

            # main_resource_id
            coalesce(
                cls._first_by_priority("main_resource_id", "status_category", cls.STATUS_OPEN),
                spark_min("main_resource_id")
            ).alias("main_resource_id"),

            # Original score and status with open filter
            coalesce(
                cls._first_by_priority("original_score", "status_category", cls.STATUS_OPEN),
                lit(None)
            ).alias("original_score"),

            coalesce(
                cls._first_by_priority("original_status", "status_category", cls.STATUS_OPEN),
                lit("")
            ).alias("original_status"),
        ]

        # Add conditional aggregations
        conditional_aggs = cls._conditional_aggregations(df)

        # Return combined list
        return engine_aggs + conditional_aggs

    @classmethod
    def _first_by_priority(cls, field_name: str, filter_col: str, filter_value: str):
        """Get first value from highest priority record matching filter"""
        return first(
            when(col(filter_col) == filter_value, col(field_name)),
            ignorenulls=True
        )

    @classmethod
    def _most_common_value(cls, field_name: str):
        """Approximate most common value"""
        return first(col(field_name), ignorenulls=True)

    @classmethod
    def _conditional_aggregations(cls, df: DataFrame):
        """Return aggregations for optional columns that may or may not exist"""
        aggregations = []

        # CISA KEV
        if "cisa_kev" in df.columns:
            aggregations.append(
                coalesce(
                    cls._first_by_priority("cisa_kev", "status_category", cls.STATUS_OPEN),
                    bool_or("cisa_kev")
                ).alias("cisa_kev")
            )
        else:
            aggregations.append(lit(False).alias("cisa_kev"))

        # VulnCheck KEV
        if "vulncheck_kev" in df.columns:
            aggregations.append(
                coalesce(
                    cls._first_by_priority("vulncheck_kev", "status_category", cls.STATUS_OPEN),
                    bool_or("vulncheck_kev")
                ).alias("vulncheck_kev")
            )
        else:
            aggregations.append(lit(False).alias("vulncheck_kev"))

        # Is Fixable
        if "is_fixable" in df.columns:
            aggregations.append(
                coalesce(
                    cls._first_by_priority("is_fixable", "status_category", cls.STATUS_OPEN),
                    bool_or("is_fixable")
                ).alias("is_fixable")
            )
        else:
            aggregations.append(lit(False).alias("is_fixable"))

        # Has Exploit
        if "has_exploit" in df.columns:
            aggregations.append(
                coalesce(
                    cls._first_by_priority("has_exploit", "status_category", cls.STATUS_OPEN),
                    bool_or("has_exploit")
                ).alias("has_exploit")
            )
        else:
            aggregations.append(lit(False).alias("has_exploit"))

        # Vulnerability reported date
        if "vulnerability_reported_date" in df.columns:
            aggregations.append(
                spark_min("vulnerability_reported_date").alias("vulnerability_reported_date")
            )
        else:
            aggregations.append(lit(None).alias("vulnerability_reported_date"))

        return aggregations