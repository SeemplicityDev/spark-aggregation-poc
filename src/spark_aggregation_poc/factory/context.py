from dataclasses import dataclass

from spark_aggregation_poc.config.config import Config, ConfigLoader
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.interfaces.interfaces import IFindingsReader, IFindingsAggregator, IAggregatedWriter
from spark_aggregation_poc.services.write_service import WriteService


@dataclass
class AppContext:
    config: Config
    # read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches
    read_service: IFindingsReader
    aggregation_service: IFindingsAggregator
    write_service: IAggregatedWriter

def build_app_context(config: Config = None) -> AppContext:
    if config is None:
        config: Config = ConfigLoader.load_config()
    print("=== Building AppContext, Config:===")
    print(config)
    # read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches = Factory.create_read_service_raw_join_multi_connection_batches(config)
    read_service: IFindingsReader = Factory.create_reader(config)
    aggregation_service: IFindingsAggregator = Factory.create_aggregator(config)
    write_service: IAggregatedWriter = Factory.create_write_service(config)
    return AppContext(config=config, read_service=read_service, aggregation_service=aggregation_service, write_service=write_service)