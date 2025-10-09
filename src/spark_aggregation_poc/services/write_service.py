from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import AggregatedWriterInterface, CatalogDataInterface


class WriteService(AggregatedWriterInterface):
    _allow_init = False

    @classmethod
    def create_write_service(cls, config: Config, catalog_repository: CatalogDataInterface) -> AggregatedWriterInterface:
        cls._allow_init = True
        result = WriteService(config, catalog_repository)
        cls._allow_init = False

        return result


    def __init__(self, config: Config, catalog_repository: CatalogDataInterface):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url
        self.config = config
        self.catalog_repository = catalog_repository


    def write_finding_group_rollup(self, df: DataFrame):
        # df.write.jdbc(url=self.postgres_url, table="finding_group_rollup", mode="overwrite", properties=self.postgres_properties)
        self.catalog_repository.save_to_catalog(df, "finding_group_rollup")




    def write_finding_group_association(self, df: DataFrame):
        # df.write.jdbc(url=self.postgres_url, table="finding_group_association", mode="overwrite", properties=self.postgres_properties)
        self.catalog_repository.save_to_catalog(df, "finding_group_association")