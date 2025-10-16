from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame

from spark_aggregation_poc.models.aggregation_output import AggregationOutput
from spark_aggregation_poc.models.spark_aggregation_rules import AggregationRule


class FindingsImporterInterface(ABC):

    @abstractmethod
    def import_findings_data(self, spark: SparkSession,
                             large_table_batch_size: int,
                             connections_per_batch: int,
                             max_id_override: int) -> None:
        """Read findings data and save in databricks catalog"""
        pass



class FindingsAggregatorInterface(ABC):

    @abstractmethod
    def aggregate_findings(self,spark: SparkSession, customer_id: Optional[int] = None) -> AggregationOutput:
        pass

    @abstractmethod
    def write_aggregated_findings(self,spark: SparkSession,aggregation_output: AggregationOutput ):
        pass


class UpdatePlannerInterface(ABC):

    @abstractmethod
    def calculate_aggregation_changes(self, df_final_finding_group_association: DataFrame, df_final_finding_group_rollup: DataFrame) -> None:
        pass


class AggregatedFindingsExporterInterface(ABC):

    @abstractmethod
    def export_aggregated_findings_changes(self, df: DataFrame) -> None:
        pass



class RuleLoaderInterface(ABC):

    @abstractmethod
    def load_aggregation_rules(self, spark: SparkSession, customer_id: Optional[int] = None) -> list[AggregationRule]:
        pass



class FilterConfigParserInterface(ABC):

    @abstractmethod
    def generate_filter_condition(self, filters_config: Dict[str, Any]) -> Optional[str]:
        pass


class RelationalDalInterface(ABC):

    @abstractmethod
    def query(self, spark: SparkSession, query: str) -> DataFrame:
        pass

    @abstractmethod
    def query_with_multiple_connections(self, spark: SparkSession, num_connections: int, query: str, id_column: str, start_id: int, end_id: int) -> DataFrame:
        pass


class CatalogDalInterface(ABC):

    @abstractmethod
    def read_base_findings(self, spark: SparkSession) -> DataFrame:
        pass

    @abstractmethod
    def save_to_catalog(cls, df: DataFrame, table_name: str):
        pass



class FileDalInterface(ABC):

    @abstractmethod
    def save_file(self, df: DataFrame, file_name: str):
        pass