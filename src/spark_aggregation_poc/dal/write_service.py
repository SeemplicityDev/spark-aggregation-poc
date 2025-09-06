from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config


class WriteService:

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def write_finding_group_aggregation_columns(self, df: DataFrame):
        df.write.jdbc(url=self.postgres_url, table="finding_group_aggregation_columns", mode="overwrite", properties=self.postgres_properties)


    def write_finding_group_relation(self, df: DataFrame):
        df.write.jdbc(url=self.postgres_url, table="finding_group_relation", mode="overwrite", properties=self.postgres_properties)