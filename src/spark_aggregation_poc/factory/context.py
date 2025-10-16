from dataclasses import dataclass

from spark_aggregation_poc.config.config import Config, ConfigLoader
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsImporterInterface, FindingsAggregatorInterface, \
    AggregationUpdatesExporterInterface, UpdatePlannerInterface


@dataclass
class AppContext:
    config: Config
    import_service: FindingsImporterInterface
    aggregation_service: FindingsAggregatorInterface
    update_planner_service: UpdatePlannerInterface
    export_service: AggregationUpdatesExporterInterface

def build_app_context(config: Config = None) -> AppContext:
    if config is None:
        config: Config = ConfigLoader.load_config()
    print("=== Building AppContext, Config:===")
    print(config)
    import_service: FindingsImporterInterface = Factory.create_importer(config)
    aggregation_service: FindingsAggregatorInterface = Factory.create_aggregator(config)
    update_planner_service: UpdatePlannerInterface = Factory.create_update_planner(config)
    export_service: AggregationUpdatesExporterInterface = Factory.create_exporter(config)
    return AppContext(config=config,
                      import_service=import_service,
                      aggregation_service=aggregation_service,
                      update_planner_service=update_planner_service,
                      export_service=export_service)