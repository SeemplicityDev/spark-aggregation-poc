from dataclasses import dataclass

from spark_aggregation_poc.config.config import Config, ConfigLoader
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsImporterInterface, FindingsAggregatorInterface, \
    AggregatedFindingsExporterInterface, AggregationDeltaCalculatorInterface


@dataclass
class AppContext:
    config: Config
    import_service: FindingsImporterInterface
    aggregation_service: FindingsAggregatorInterface
    delta_calculation_service: AggregationDeltaCalculatorInterface
    export_service: AggregatedFindingsExporterInterface

def build_app_context(config: Config = None) -> AppContext:
    if config is None:
        config: Config = ConfigLoader.load_config()
    print("=== Building AppContext, Config:===")
    print(config)
    import_service: FindingsImporterInterface = Factory.create_importer(config)
    aggregation_service: FindingsAggregatorInterface = Factory.create_aggregator(config)
    change_calculation_service: AggregationDeltaCalculatorInterface = Factory.create_change_calculator(config)
    export_service: AggregatedFindingsExporterInterface = Factory.create_exporter(config)
    return AppContext(config=config,
                      import_service=import_service,
                      aggregation_service=aggregation_service,
                      delta_calculation_service=change_calculation_service,
                      export_service=export_service)