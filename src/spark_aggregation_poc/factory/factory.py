
from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.dal.catalog_repository import CatalogRepository
from spark_aggregation_poc.interfaces.interfaces import FindingsImportInterface, FindingsAggregatorInterface, \
    AggregatedWriterInterface, \
    FilterConfigParserInterface, RuleLoaderInterface, CatalogDataInterface
from spark_aggregation_poc.services.aggregation.aggregation_service import \
    AggregationService
from spark_aggregation_poc.services.import_service import ImportService
from spark_aggregation_poc.services.write_service import WriteService
from spark_aggregation_poc.utils.aggregation_rules.rule_loader import RuleLoaderInterface, RuleLoaderService
from spark_aggregation_poc.utils.aggregation_rules.spark_filters_config_processor import FiltersConfigParser


class Factory:

    @classmethod
    def create_importer(cls, config: Config) -> FindingsImportInterface:
        catalog_repository: CatalogDataInterface = CatalogRepository.create_catalog_repository(config=config)
        return ImportService.create_import_service(config=config, catalog_repository=catalog_repository)

    @classmethod
    def create_aggregator(cls, config: Config) -> FindingsAggregatorInterface:
        filters_config_parser: FilterConfigParserInterface = FiltersConfigParser.create_filters_config_parser()
        rule_loader: RuleLoaderInterface = RuleLoaderService.create_rule_loader(config)
        catalog_repository: CatalogDataInterface = CatalogRepository.create_catalog_repository(config=config)
        return AggregationService.create_aggregation_service(config=config, catalog_repository=catalog_repository, rule_loader=rule_loader, filters_config_parser=filters_config_parser)


    @classmethod
    def create_writer(cls, config: Config) -> AggregatedWriterInterface:
        catalog_repository: CatalogDataInterface = CatalogRepository.create_catalog_repository(config=config)
        return WriteService.create_write_service(config=config, catalog_repository=catalog_repository)


