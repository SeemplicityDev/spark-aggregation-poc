from dataclasses import dataclass

from spark_aggregation_poc.config.config import Config, ConfigLoader
from spark_aggregation_poc.dal.read_service import ReadService
from spark_aggregation_poc.dal.read_service_pre_partition import ReadServicePrePartition
from spark_aggregation_poc.dal.read_service_raw import ReadServiceRaw
from spark_aggregation_poc.dal.read_service_raw_join import ReadServiceRawJoin
from spark_aggregation_poc.dal.read_service_raw_join_multi_connections import ReadServiceRawJoinMultiConnection
from spark_aggregation_poc.dal.read_service_raw_join_multi_connections_batches import \
    ReadServiceRawJoinMultiConnectionBatches
from spark_aggregation_poc.dal.write_service import WriteService
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.services.aggregation_service import AggregationService
from spark_aggregation_poc.services.aggregation_service_multi_rules_no_write import AggregationServiceMultiRulesNoWrite
from spark_aggregation_poc.services.aggregation_service_raw_join import AggregationServiceRawJoin


@dataclass
class AppContext:
    config: Config
    read_service: ReadService
    read_service_pre_partition: ReadServicePrePartition
    read_service_raw_join: ReadServiceRawJoin
    read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches
    read_service_raw: ReadServiceRaw
    write_service: WriteService
    aggregation_service: AggregationService
    aggregation_service_raw_join: AggregationServiceRawJoin
    aggregation_service_multi_rules_no_write: AggregationServiceMultiRulesNoWrite

def build_app_context(config: Config = None) -> AppContext:
    if config is None:
        config: Config = ConfigLoader.load_config()
    print("=== Building AppContext, Config:===")
    print(config)
    read_service: ReadService = Factory.create_read_service(config)
    read_service_pre_partition: ReadServicePrePartition = Factory.create_read_service_pre_partition(config)
    read_service_raw_join: ReadServiceRawJoin = Factory.create_read_service_raw_join(config)
    read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches = Factory.create_read_service_raw_join_multi_connection_batches(config)
    read_service_raw: ReadServiceRaw = Factory.create_read_service_raw(config)
    write_service: WriteService = Factory.create_write_service(config)
    aggregation_service: AggregationService = Factory.create_aggregation_service()
    aggregation_service_raw_join: AggregationServiceRawJoin = Factory.create_aggregation_service_raw_join()
    aggregation_service_multi_rule_no_write: AggregationServiceMultiRulesNoWrite = Factory.create_aggregation_service_multi_rules_no_write()
    return AppContext(config, read_service, read_service_pre_partition, read_service_raw_join, read_service_raw_join_multi_connection_batches,
                      read_service_raw, write_service, aggregation_service, aggregation_service_raw_join, aggregation_service_multi_rule_no_write)