from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config


class WriteUtil:
    @classmethod
    def save_to_catalog(cls, config: Config, df: DataFrame, table_name: str):
        from datetime import datetime

        # Determine the catalog and table reference based on environment
        if config.is_databricks:
            catalog_table = f"general_data.default.{table_name}"
            environment = "Databricks Unity Catalog"
        else:
            catalog_table = f"spark_catalog.default.{table_name}"
            environment = "local spark_catalog"

        print(f"Saving {table_name} to {environment}")

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