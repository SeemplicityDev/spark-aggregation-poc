from pyspark.sql import DataFrame, SparkSession

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import CatalogDalInterface


class CatalogDal(CatalogDalInterface):
    _allow_init = False

    @classmethod
    def create_catalog_dal(cls, config):
        cls._allow_init = True
        result = CatalogDal(config=config)
        cls._allow_init = False

        return result

    def __init__(self, config: Config):
        self.catalog_table_prefix = config.catalog_table_prefix

    def read_base_findings(self, spark: SparkSession) -> DataFrame:
        # After tables are saved to catalog, create a view from the raw join
        print("Creating base findings view from catalog tables...")

        sql_str = f"""
               CREATE OR REPLACE TEMPORARY VIEW base_findings_view AS
               SELECT
                   findings.id as finding_id,
                   findings.package_name as package_name,
                   findings.main_resource_id,
                   findings.aggregation_group_id,
                   findings.source,
                   findings.rule_family,
                   findings.rule_id,
                   finding_sla_rule_connections.finding_id as sla_connection_id,
                   plain_resources.id as resource_id,
                   plain_resources.cloud_account,
                   plain_resources.cloud_account_friendly_name as root_cloud_account_friendly_name,
                   plain_resources.r1_resource_type as resource_type,
                   plain_resources.tags_values as tags_values,
                   plain_resources.tags_key_values as tags_key_values,
                   plain_resources.cloud_provider as cloud_provider,
                   findings_scores.finding_id as score_finding_id,
                   findings_scores.severity,
                   user_status.id as user_status_id,
                   user_status.actual_status_key,
                   findings_additional_data.finding_id as additional_data_id,
                   statuses.key as status_key,
                   aggregation_groups.id as existing_group_id,
                   aggregation_groups.main_finding_id as existing_main_finding_id,
                   aggregation_groups.group_identifier as existing_group_identifier,
                   aggregation_groups.is_locked,
                   findings_info.id as findings_info_id,
                   findings.finding_type_str as finding_type,
                   findings.fix_subtype as fix_subtype,
                   statuses.category as category,
                   findings.fix_id as fix_id,
                   findings_additional_data.cve[1] as cve,
                   findings.fix_type as fix_type,
                   selection_rules.scope_group as scope_group
               FROM {self.catalog_table_prefix}findings
               LEFT OUTER JOIN {self.catalog_table_prefix}finding_sla_rule_connections ON
                    findings.id = finding_sla_rule_connections.finding_id
               JOIN {self.catalog_table_prefix}plain_resources ON
                   findings.main_resource_id = plain_resources.id
               JOIN {self.catalog_table_prefix}findings_scores ON
                   findings.id = findings_scores.finding_id
               JOIN {self.catalog_table_prefix}user_status ON
                   user_status.id = findings.id
               LEFT OUTER JOIN {self.catalog_table_prefix}findings_additional_data ON
                   findings.id = findings_additional_data.finding_id
               JOIN {self.catalog_table_prefix}statuses ON
                   statuses.key = user_status.actual_status_key
               LEFT OUTER JOIN {self.catalog_table_prefix}aggregation_groups ON
                   findings.aggregation_group_id = aggregation_groups.id
               LEFT OUTER JOIN {self.catalog_table_prefix}findings_info ON
                   findings_info.id = findings.id
               LEFT OUTER JOIN {self.catalog_table_prefix}scoring_rules ON
                    findings_scores.scoring_rule_id = scoring_rules.id
               LEFT OUTER JOIN {self.catalog_table_prefix}selection_rules ON
                    scoring_rules.selection_rule_id = selection_rules.id
               WHERE findings.package_name IS NOT NULL
               AND (findings.id <> aggregation_groups.main_finding_id
               OR findings.aggregation_group_id is null)
               """

        spark.sql(sql_str)

        # Now read the view as a DataFrame
        df = spark.table("base_findings_view")
        return df


    def save_to_catalog(self, df: DataFrame, table_name: str):
        from datetime import datetime

        # Determine the catalog and table reference based on environment
        catalog_table = f"{self.catalog_table_prefix}{table_name}"

        print(f"Saving {table_name}")

        try:
            # Unified approach: use saveAsTable for both environments
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(catalog_table)
            print(f"✓ {table_name}: saved to {catalog_table}")

        except Exception as e:
            print(f"❌ Failed to save {table_name} to catalog: {e}")

        save_start_time = datetime.now()
        print(f"Save completed at: {save_start_time.strftime('%H:%M:%S')}")