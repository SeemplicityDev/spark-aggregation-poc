from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import AggregatedFindingsExporterInterface, CatalogDalInterface, \
    FileDalInterface


class ExportService(AggregatedFindingsExporterInterface):

    @classmethod
    def create_export_service(cls, config: Config, catalog_dal: CatalogDalInterface, file_dal: FileDalInterface) -> AggregatedFindingsExporterInterface:
        cls._allow_init = True
        result = ExportService(config, catalog_dal, file_dal)
        cls._allow_init = False

        return result


    def __init__(self, config: Config, catalog_dal: CatalogDalInterface, file_dal: FileDalInterface):
        self.config = config
        self.catalog_dal = catalog_dal
        self.file_dal = file_dal



    def export_aggregated_findings_changes(self, df: DataFrame) -> None:
        pass