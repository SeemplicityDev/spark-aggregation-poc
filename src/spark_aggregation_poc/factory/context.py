from dataclasses import dataclass

from spark_aggregation_poc.config.config import Config, ConfigLoader
from spark_aggregation_poc.dal.read_service_pre_partition import ReadServicePrePartition
from spark_aggregation_poc.dal.read_service_raw_join import ReadServiceRawJoin
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.dal.read_service import ReadService
from spark_aggregation_poc.dal.write_service import WriteService
from spark_aggregation_poc.services.aggregation_service import AggregationService
from spark_aggregation_poc.services.aggregation_service_raw_join import AggregationServiceRawJoin


@dataclass
class AppContext:
    config: Config
    read_service: ReadService
    read_service_pre_partition: ReadServicePrePartition
    read_service_raw_join: ReadServiceRawJoin
    write_service: WriteService
    aggregation_service: AggregationService
    aggregation_service_raw_join: AggregationServiceRawJoin

def build_app_context(config: Config = None) -> AppContext:
    if config is None:
        config: Config = ConfigLoader.load_config()
    print("=== Building AppContext, Config:===")
    print(config)
    read_service: ReadService = Factory.create_read_service(config)
    read_service_pre_partition: ReadServicePrePartition = Factory.create_read_service_pre_partition(config)
    read_service_raw_join: ReadServiceRawJoin = Factory.create_read_service_raw_join(config)
    write_service: WriteService = Factory.create_write_service(config)
    aggregation_service: AggregationService = Factory.create_aggregation_service()
    aggregation_service_raw_join: AggregationServiceRawJoin = Factory.create_aggregation_service_raw_join()
    return AppContext(config, read_service, read_service_pre_partition, read_service_raw_join, write_service, aggregation_service, aggregation_service_raw_join)