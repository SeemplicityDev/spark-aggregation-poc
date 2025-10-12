from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import FileDalInterface


class FileDal(FileDalInterface):

    @classmethod
    def create_file_dal(cls, config):
        cls._allow_init = True
        result = FileDal(config=config)
        cls._allow_init = False

        return result

    def __init__(self, config: Config):
        self.catalog_table_prefix = config.catalog_table_prefix



    def save_file(self, df: DataFrame, file_name: str):
        pass