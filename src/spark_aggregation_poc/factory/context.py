from dataclasses import dataclass

from spark_aggregation_poc.config.config import Config, ConfigLoader
from spark_aggregation_poc.factory.factory import Factory
from spark_aggregation_poc.dal.read_service import ReadService
from spark_aggregation_poc.dal.write_service import WriteService
from spark_aggregation_poc.services.aggregation_service import AggregationService


@dataclass
class AppContext:
    config: Config
    read_service: ReadService
    write_service: WriteService
    transform_service: AggregationService

def build_app_context() -> AppContext:
    config: Config = ConfigLoader.load_config()
    read_service: ReadService = Factory.create_read_service(config)
    write_service: WriteService = Factory.create_write_service(config)
    transform_service: AggregationService = Factory.create_transform_service()
    return AppContext(config, read_service, write_service, transform_service)