
from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import FindingsReaderInterface, FindingsAggregatorInterface, AggregatedWriterInterface, \
    FilterConfigParserInterface, RuleLoaderInterface
from spark_aggregation_poc.services.aggregation.aggregation_service import \
    AggregationService
from spark_aggregation_poc.services.read_service import ReadService
from spark_aggregation_poc.services.write_service import WriteService
from spark_aggregation_poc.utils.aggregation_rules.rule_loader import RuleLoaderInterface, RuleLoaderService
from spark_aggregation_poc.utils.aggregation_rules.spark_filters_config_processor import FiltersConfigParser


class Factory:

    @classmethod
    def create_reader(cls, config: Config) -> FindingsReaderInterface:
        return ReadService.create_read_service(config=config)

    @classmethod
    def create_aggregator(cls, config: Config) -> FindingsAggregatorInterface:
        filters_config_parser: FilterConfigParserInterface = FiltersConfigParser.create_filters_config_parser()
        rule_loader: RuleLoaderInterface = RuleLoaderService.create_rule_loader(config)
        return AggregationService.create_aggregation_service(config=config, rule_loader=rule_loader, filters_config_parser=filters_config_parser)


    @classmethod
    def create_writer(cls, config: Config) -> AggregatedWriterInterface:
        return WriteService.create_write_service(config=config)


