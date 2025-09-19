from dataclasses import dataclass

from spark_aggregation_poc.config.config import Config, ConfigLoader
from spark_aggregation_poc.services.read_service import ReadService
from spark_aggregation_poc.services.write_service import WriteService
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.services.aggregation.aggregation_service import \
    AggregationService


@dataclass
class AppContext:
    config: Config
    # read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches
    read_service: ReadService
    write_service: WriteService
    aggregation_service: AggregationService

def build_app_context(config: Config = None) -> AppContext:
    if config is None:
        config: Config = ConfigLoader.load_config()
    print("=== Building AppContext, Config:===")
    print(config)
    # read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches = Factory.create_read_service_raw_join_multi_connection_batches(config)
    read_service: ReadService = Factory.create_read_service(config)
    write_service: WriteService = Factory.create_write_service(config)
    aggregation_service_filters_config: AggregationService = Factory.create_aggregation_service(config)
    return AppContext(config, read_service, write_service,  aggregation_service_filters_config)