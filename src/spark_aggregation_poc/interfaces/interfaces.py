from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame

from spark_aggregation_poc.models.spark_aggregation_rules import SparkAggregationRule


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



class IRuleLoader(ABC):

    @abstractmethod
    def load_aggregation_rules(self, spark: SparkSession, customer_id: Optional[int] = None) -> list[SparkAggregationRule]:
        pass



class IFilterConfigParser(ABC):

    @abstractmethod
    def generate_filter_condition(self, filters_config: Dict[str, Any]) -> Optional[str]:
        pass



