from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import IFindingsReader, IFindingsAggregator, IAggregatedWriter
from spark_aggregation_poc.services.read_service import ReadService
from spark_aggregation_poc.services.write_service import WriteService
from spark_aggregation_poc.factory.context import build_app_context, AppContext
from spark_aggregation_poc.services.aggregation.aggregation_service import \
    AggregationService
from pyspark.sql import SparkSession



def run_aggregation(spark: SparkSession, config: Config = None):
    try:
        app_context: AppContext = build_app_context(config)
        read_service: IFindingsReader = app_context.read_service
        aggregation_service: IFindingsAggregator = app_context.aggregation_service
        write_service: IAggregatedWriter = app_context.write_service

        from time import time

        start = time()
        read_service.read_findings_data(spark=spark)
        print(f"Read time: {time() - start:.2f} seconds")

        start = time()
        df_final_group_agg_columns, df_final_finding_group_association = aggregation_service.aggregate_findings(spark=spark)
        print("\n=== Final Group Aggregation Columns ===")
        df_final_group_agg_columns.show()
        print("\n=== Final Finding Group Association ===")
        df_final_finding_group_association.show()
        print(f"Rules apply and aggregation time: {time() - start:.2f} seconds")

        # print("\n=== Writing Aggregation table and Association table ===")
        # start = time()
        # write_service.write_finding_group_aggregation(df_final_group_agg_columns)
        # write_service.write_finding_group_association(df_final_finding_group_association)
        # print(f"Write time: {time() - start:.2f} seconds")

    except Exception as e:
        print(f"Error aggregating! {e}")
        # TODO: add rollback functionality (first write to staging_table and validate the expected count (inside driver))
    finally:
        # Stop Spark
        if(config and config.is_databricks is False):
            spark.stop()
