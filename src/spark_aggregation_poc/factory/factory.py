
from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import IFindingsReader, IFindingsAggregator, IAggregatedWriter
from spark_aggregation_poc.services.read_service import ReadService
from spark_aggregation_poc.services.write_service import WriteService
from spark_aggregation_poc.services.aggregation.aggregation_service import \
    AggregationService
from spark_aggregation_poc.utils.aggregation_rules.rule_loader import RuleLoader
from spark_aggregation_poc.utils.aggregation_rules.spark_filters_config_processor import FiltersConfigProcessor


class Factory:

    # @classmethod
    # def create_read_service_raw_join_multi_connection_batches(cls, config: Config) -> ReadServiceRawJoinMultiConnectionBatches:
    #     return ReadServiceRawJoinMultiConnectionBatches(config=config)

    @classmethod
    def create_reader(cls, config: Config) -> IFindingsReader:
        return ReadService(config=config)

    @classmethod
    def create_aggregator(cls, config: Config) -> IFindingsAggregator:
        rule_loader = RuleLoader(config)
        filters_config_processor = FiltersConfigProcessor()
        return AggregationService(config=config, rule_loader=rule_loader, filters_config_processor=filters_config_processor)


    @classmethod
    def create_write_service(cls, config: Config) -> IAggregatedWriter:
        return WriteService(config=config)


