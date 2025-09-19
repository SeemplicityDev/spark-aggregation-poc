from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import IAggregatedWriter


class WriteService(IAggregatedWriter):

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def write_finding_group_aggregation(self, df: DataFrame):
        df.write.jdbc(url=self.postgres_url, table="finding_group_aggregation", mode="overwrite", properties=self.postgres_properties)


    def write_finding_group_association(self, df: DataFrame):
        df.write.jdbc(url=self.postgres_url, table="finding_group_association", mode="overwrite", properties=self.postgres_properties)