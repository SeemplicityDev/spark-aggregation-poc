from dataclasses import dataclass

from spark_aggregation_poc.config.config import Config, ConfigLoader
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import FindingsImportInterface, FindingsAggregatorInterface, AggregatedWriterInterface


@dataclass
class AppContext:
    config: Config
    # read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches
    read_service: FindingsImportInterface
    aggregation_service: FindingsAggregatorInterface
    write_service: AggregatedWriterInterface

def build_app_context(config: Config = None) -> AppContext:
    if config is None:
        config: Config = ConfigLoader.load_config()
    print("=== Building AppContext, Config:===")
    print(config)
    # read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches = Factory.create_read_service_raw_join_multi_connection_batches(config)
    read_service: FindingsImportInterface = Factory.create_importer(config)
    aggregation_service: FindingsAggregatorInterface = Factory.create_aggregator(config)
    write_service: AggregatedWriterInterface = Factory.create_writer(config)
    return AppContext(config=config, read_service=read_service, aggregation_service=aggregation_service, write_service=write_service)