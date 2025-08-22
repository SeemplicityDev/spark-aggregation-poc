from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.dal.read_service import ReadService
from spark_aggregation_poc.dal.read_service_pre_partition import ReadServicePrePartition
from spark_aggregation_poc.dal.write_service import WriteService
from spark_aggregation_poc.services.aggregation_service import AggregationService


class Factory:
    @classmethod
    def create_read_service(cls, config: Config) -> ReadService:
        return ReadService(config=config)

    @classmethod
    def create_read_service_pre_partition(cls, config: Config) -> ReadServicePrePartition:
        return ReadServicePrePartition(config=config)

    @classmethod
    def create_write_service(cls, config: Config) -> WriteService:
        return WriteService(config=config)

    @classmethod
    def create_transform_service(cls) -> AggregationService:
        return AggregationService()