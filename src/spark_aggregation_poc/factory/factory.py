from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.dal.read_service import ReadService
from spark_aggregation_poc.dal.read_service_individual_tables_multi_connections_batches import \
    ReadServiceIndividualTablesMultiConnectionBatches
from spark_aggregation_poc.dal.read_service_individual_tables_save_catalog import ReadServiceIndividualTablesSaveCatalog
from spark_aggregation_poc.dal.read_service_pre_partition import ReadServicePrePartition
from spark_aggregation_poc.dal.read_service_raw import ReadServiceRaw
from spark_aggregation_poc.dal.read_service_raw_join import ReadServiceRawJoin
from spark_aggregation_poc.dal.read_service_raw_join_multi_connections import ReadServiceRawJoinMultiConnection
from spark_aggregation_poc.dal.read_service_raw_join_multi_connections_batches import \
    ReadServiceRawJoinMultiConnectionBatches
from spark_aggregation_poc.dal.write_service import WriteService
from spark_aggregation_poc.services.aggregation_rules.aggregation_service_filters_config import \
    AggregationServiceFiltersConfig
from spark_aggregation_poc.services.aggregation_rules.read_service_filters_config import ReadServiceFiltersConfig
from spark_aggregation_poc.services.aggregation_rules.rule_loader import RuleLoader
from spark_aggregation_poc.services.aggregation_rules.spark_filters_config_processor import FiltersConfigProcessor
from spark_aggregation_poc.services.aggregation_service import AggregationService
from spark_aggregation_poc.services.aggregation_service_multi_rules_no_write import AggregationServiceMultiRulesNoWrite
from spark_aggregation_poc.services.aggregation_service_raw_join import AggregationServiceRawJoin


class Factory:
    @classmethod
    def create_read_service(cls, config: Config) -> ReadService:
        return ReadService(config=config)

    @classmethod
    def create_read_service_pre_partition(cls, config: Config) -> ReadServicePrePartition:
        return ReadServicePrePartition(config=config)

    @classmethod
    def create_read_service_raw_join(cls, config: Config) -> ReadServiceRawJoin:
        return ReadServiceRawJoin(config=config)

    @classmethod
    def create_read_service_raw_join_multi_connection_batches(cls, config: Config) -> ReadServiceRawJoinMultiConnectionBatches:
        return ReadServiceRawJoinMultiConnectionBatches(config=config)

    @classmethod
    def create_read_service_individual_tables_multi_connection_batches(cls,
                                                              config: Config) -> ReadServiceIndividualTablesMultiConnectionBatches:
        return ReadServiceIndividualTablesMultiConnectionBatches(config=config)

    @classmethod
    def create_read_service_raw(cls, config: Config) -> ReadServiceRaw:
        return ReadServiceRaw(config=config)

    @classmethod
    def create_read_service_filters_config(cls, config: Config) -> ReadServiceFiltersConfig:
        rule_loader = RuleLoader(config)
        return ReadServiceFiltersConfig(config=config, rule_loader=rule_loader)

    @classmethod
    def create_read_service_individual_tables_save_catalog(cls, config: Config) -> ReadServiceIndividualTablesSaveCatalog:
        return ReadServiceIndividualTablesSaveCatalog(config=config)

    @classmethod
    def create_write_service(cls, config: Config) -> WriteService:
        return WriteService(config=config)

    @classmethod
    def create_aggregation_service(cls) -> AggregationService:
        return AggregationService()

    @classmethod
    def create_aggregation_service_raw_join(cls) -> AggregationServiceRawJoin:
        return AggregationServiceRawJoin()

    @classmethod
    def create_aggregation_service_multi_rules_no_write(cls, config: Config) -> AggregationServiceMultiRulesNoWrite:
        return AggregationServiceMultiRulesNoWrite(config=config)

    @classmethod
    def create_aggregation_service_filters_config(cls, config: Config) -> AggregationServiceFiltersConfig:
        rule_loader = RuleLoader(config)
        filters_config_processor = FiltersConfigProcessor()
        return AggregationServiceFiltersConfig(rule_loader=rule_loader, filters_config_processor=filters_config_processor)
