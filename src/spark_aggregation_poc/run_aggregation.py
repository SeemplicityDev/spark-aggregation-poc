from pyspark.sql import SparkSession

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.factory.context import build_app_context, AppContext
from spark_aggregation_poc.interfaces.interfaces import FindingsImporterInterface, FindingsAggregatorInterface, \
    AggregatedFindingsExporterInterface, AggregationDeltaCalculatorInterface


def run_aggregation(spark: SparkSession, config: Config = None):
    try:
        app_context: AppContext = build_app_context(config)
        import_service: FindingsImporterInterface = app_context.import_service
        aggregation_service: FindingsAggregatorInterface = app_context.aggregation_service
        delta_calculation_service: AggregationDeltaCalculatorInterface = app_context.delta_calculation_service
        export_service: AggregatedFindingsExporterInterface = app_context.export_service

        from time import time

        # start = time()
        # import_service.import_findings_data(spark=spark)
        # print(f"Read time: {time() - start:.2f} seconds")

        start = time()
        aggregation_output = aggregation_service.aggregate_findings(spark=spark)
        print("\n=== Final Finding Group Association ===")
        aggregation_output.finding_group_association.show()
        print("\n=== Final Group Aggregation Columns ===")
        aggregation_output.finding_group_rollup.show()
        print(f"Rules apply and aggregation time: {time() - start:.2f} seconds")

        # print("\n=== Writing Association table and  Aggregation  table ===")
        # start = time()
        # aggregation_service.write_aggregated_findings(spark, aggregation_output)
        # print(f"Write time: {time() - start:.2f} seconds")
        #
        # print("\n=== Reading Association table and Aggregation table  ===")
        # df = spark.table(f"{config.catalog_table_prefix}finding_group_association")
        # df.show()
        # df = spark.table(f"{config.catalog_table_prefix}finding_group_rollup")
        # df.show()

    except Exception as e:
        print(f"Error aggregating! {e}")
