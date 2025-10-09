from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame

from spark_aggregation_poc.models.spark_aggregation_rules import AggregationRule


class FindingsImportInterface(ABC):

    @abstractmethod
    def import_findings_data(self, spark: SparkSession,
                             large_table_batch_size: int,
                             connections_per_batch: int,
                             max_id_override: int) -> None:
        """Read findings data and save in databricks catalog"""
        pass



class FindingsAggregatorInterface(ABC):

    @abstractmethod
    def aggregate_findings(self, spark:SparkSession,
                  findings_df: DataFrame = None,
                  customer_id: Optional[int] = None) -> tuple[DataFrame, DataFrame]:
        """Aggregate findings data and return aggregated data (association-table, groups-table)"""
        pass



class AggregatedWriterInterface(ABC):

    @abstractmethod
    def write_finding_group_rollup(self, df: DataFrame) -> None:
        pass

    @abstractmethod
    def write_finding_group_association(self, df: DataFrame) -> None:
        pass



class RuleLoaderInterface(ABC):

    @abstractmethod
    def load_aggregation_rules(self, spark: SparkSession, customer_id: Optional[int] = None) -> list[AggregationRule]:
        pass



class FilterConfigParserInterface(ABC):

    @abstractmethod
    def generate_filter_condition(self, filters_config: Dict[str, Any]) -> Optional[str]:
        pass


class PostgresRepositoryInterface(ABC):

    @abstractmethod
    def query(self, spark: SparkSession, query: str) -> DataFrame:
        pass



class CatalogDataInterface(ABC):

    @abstractmethod
    def read_base_findings(self, spark: SparkSession) -> DataFrame:
        pass

    @abstractmethod
    def save_to_catalog(cls, df: DataFrame, table_name: str):
        pass

