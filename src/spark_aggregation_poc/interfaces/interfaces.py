from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import SparkSession, DataFrame


class IFindingsReader(ABC):

    @abstractmethod
    def read_findings_data(self, spark: SparkSession,
                           large_table_batch_size: int,
                           connections_per_batch: int,
                           max_id_override: int) -> None:
        """Read findings data and save in databricks catalog"""
        pass



class IFindingsAggregator(ABC):

    @abstractmethod
    def aggregate_findings(self, spark:SparkSession,
                  findings_df: DataFrame = None,
                  customer_id: Optional[int] = None) -> tuple[DataFrame, DataFrame]:
        """Aggregate findings data and return aggregated data (association-table, groups-table)"""
        pass



class IAggregatedWriter(ABC):

    @abstractmethod
    def write_finding_group_aggregation(self, df: DataFrame) -> None:
        pass

    @abstractmethod
    def write_finding_group_association(self, df: DataFrame) -> None:
        pass