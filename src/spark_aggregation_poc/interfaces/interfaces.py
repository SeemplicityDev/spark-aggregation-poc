from abc import ABC, abstractmethod

from pyspark.sql import SparkSession


class IReadFindings(ABC):

    @abstractmethod
    def read_findings_data(self, spark: SparkSession,
                           large_table_batch_size: int,
                           connections_per_batch: int,
                           max_id_override: int) -> None:
        """Read findings data and save in databricks catalog"""
        pass