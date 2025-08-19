from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config


class WriteService:

    def __init__(self, config: Config):
        self.postgres_properties = config.postgres_properties
        self.postgres_url = config.postgres_url

    def write_groups_to_findings(self, df: DataFrame):
        df.write.jdbc(url=self.postgres_url, table="groups_to_findings", mode="overwrite", properties=self.postgres_properties)