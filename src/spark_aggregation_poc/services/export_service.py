from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import AggregatedFindingsExporterInterface


class ExportService(AggregatedFindingsExporterInterface):

    @classmethod
    def create_export_service(cls, config: Config) -> AggregatedFindingsExporterInterface:
        cls._allow_init = True
        result = ExportService(config)
        cls._allow_init = False

        return result


    def __init__(self, config: Config):
        self.config = config



    def export_aggregated_findings_changes(self, df: DataFrame) -> None:
        pass