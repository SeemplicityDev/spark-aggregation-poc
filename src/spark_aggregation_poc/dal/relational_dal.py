from pyspark.sql import DataFrame, SparkSession

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import CatalogDalInterface, RelationalDalInterface


class RelationalDal(RelationalDalInterface):
    _allow_init = False

    @classmethod
    def create_relational_dal(cls, config):
        cls._allow_init = True
        result = RelationalDal(config=config)
        cls._allow_init = False

        return result


    def __init__(self, config: Config):
        self.postgres_url = config.postgres_url
        self.postgres_properties = config.postgres_properties


    def query(self, spark: SparkSession, query: str) -> DataFrame:
        return spark.read.jdbc(
            url=self.postgres_url,
            table=query,
            properties=self.postgres_properties
        )

    def query_with_multiple_connections(self, spark: SparkSession, num_connections: int, query: str, id_column: str,
                                        start_id: int, end_id: int) -> DataFrame:
            return spark.read.jdbc(
                url=self.postgres_url,
                table=query,
                column=id_column,
                lowerBound=start_id,
                upperBound=end_id,
                numPartitions=num_connections,
                properties=self._get_optimized_properties()
            )

    def _get_optimized_properties(self) -> dict:
        """Get JDBC properties optimized for multi-connection batching"""
        optimized_properties = self.postgres_properties.copy()
        optimized_properties.update({
            "fetchsize": "50000",  # Fetch size for each connection
            "queryTimeout": "1800",  # 30 minute query timeout
            "loginTimeout": "120",  # 2 minute login timeout
            "socketTimeout": "1800",  # 30 minute socket timeout
            "tcpKeepAlive": "true",  # Keep connections alive
            "batchsize": "50000",  # Batch operations
            "stringtype": "unspecified"  # Handle PostgreSQL strings
        })
        return optimized_properties



