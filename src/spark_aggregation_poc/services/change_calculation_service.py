from pyspark.sql import DataFrame

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import AggregationChangeCalculatorInterface


class ChangeCalculationService(AggregationChangeCalculatorInterface):

    @classmethod
    def create_change_calculation_service(cls, config: Config) -> AggregationChangeCalculatorInterface:
        cls._allow_init = True
        result = ChangeCalculationService(config)
        cls._allow_init = False

        return result


    def __init__(self, config: Config):
        self.config = config


    def calculate_aggregation_changes(self, df_final_finding_group_association: DataFrame,
                                      df_final_finding_group_rollup: DataFrame) -> None:
        pass