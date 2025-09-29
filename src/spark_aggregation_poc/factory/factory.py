
from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import IFindingsReader, IFindingsAggregator, IAggregatedWriter, \
    IFilterConfigParser, IRuleLoader
from spark_aggregation_poc.services.aggregation.aggregation_service import \
    AggregationService
from spark_aggregation_poc.services.read_service import ReadService
from spark_aggregation_poc.services.write_service import WriteService
from spark_aggregation_poc.utils.aggregation_rules.rule_loader import RuleLoader
from spark_aggregation_poc.utils.aggregation_rules.spark_filters_config_processor import FiltersConfigParser


class Factory:

    @classmethod
    def create_reader(cls, config: Config) -> IFindingsReader:
        return ReadService.create_read_service(config=config)

    @classmethod
    def create_aggregator(cls, config: Config) -> IFindingsAggregator:
        filters_config_parser: IFilterConfigParser = FiltersConfigParser.create_filters_config_parser()
        rule_loader: IRuleLoader = RuleLoader.create_rule_loader(config)
        return AggregationService.create_aggregation_service(config=config, rule_loader=rule_loader, filters_config_parser=filters_config_parser)


    @classmethod
    def create_writer(cls, config: Config) -> IAggregatedWriter:
        return WriteService.create_write_service(config=config)


