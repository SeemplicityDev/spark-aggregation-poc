
from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.dal.FileDal import FileDal
from spark_aggregation_poc.dal.catalog_dal import CatalogDal
from spark_aggregation_poc.dal.relational_dal import RelationalDal
from spark_aggregation_poc.interfaces.interfaces import FindingsImporterInterface, FindingsAggregatorInterface, \
    AggregatedFindingsExporterInterface, \
    FilterConfigParserInterface, CatalogDalInterface, AggregationChangeCalculatorInterface, RelationalDalInterface, \
    FileDalInterface
from spark_aggregation_poc.services.aggregation.aggregation_service import \
    AggregationService
from spark_aggregation_poc.services.change_calculation_service import ChangeCalculationService
from spark_aggregation_poc.services.export_service import ExportService
from spark_aggregation_poc.services.import_service import ImportService
from spark_aggregation_poc.utils.aggregation_rules.rule_loader import RuleLoaderInterface, RuleLoaderService
from spark_aggregation_poc.utils.aggregation_rules.spark_filters_config_processor import FiltersConfigParser


class Factory:

    @classmethod
    def create_importer(cls, config: Config) -> FindingsImporterInterface:
        relational_dal: RelationalDalInterface = RelationalDal.create_relational_dal(config)
        catalog_dal: CatalogDalInterface = CatalogDal.create_catalog_dal(config=config)
        return ImportService.create_import_service(config=config, relational_dal=relational_dal, catalog_dal=catalog_dal)

    @classmethod
    def create_aggregator(cls, config: Config) -> FindingsAggregatorInterface:
        filters_config_parser: FilterConfigParserInterface = FiltersConfigParser.create_filters_config_parser()
        rule_loader: RuleLoaderInterface = RuleLoaderService.create_rule_loader(config)
        catalog_dal: CatalogDalInterface = CatalogDal.create_catalog_dal(config=config)
        return AggregationService.create_aggregation_service(config=config, catalog_dal=catalog_dal, rule_loader=rule_loader, filters_config_parser=filters_config_parser)


    @classmethod
    def create_change_calculator(cls, config: Config) -> AggregationChangeCalculatorInterface:
        return ChangeCalculationService.create_change_calculation_service(config=config)


    @classmethod
    def create_exporter(cls, config: Config) -> AggregatedFindingsExporterInterface:
        catalog_dal: CatalogDalInterface = CatalogDal.create_catalog_dal(config=config)
        file_dal: FileDalInterface = FileDal.create_file_dal(config=config)
        return ExportService.create_export_service(config=config, catalog_dal=catalog_dal, file_dal=file_dal)


