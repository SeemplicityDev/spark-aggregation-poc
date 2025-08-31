import os

from pyspark.sql import SparkSession, DataFrame

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.dal.read_service import ReadService
from spark_aggregation_poc.dal.read_service_individual_tables_multi_connections_batches import \
    ReadServiceIndividualTablesMultiConnectionBatches
from spark_aggregation_poc.dal.read_service_pre_partition import ReadServicePrePartition
from spark_aggregation_poc.dal.read_service_raw import ReadServiceRaw
from spark_aggregation_poc.dal.read_service_raw_join import ReadServiceRawJoin
from spark_aggregation_poc.dal.read_service_raw_join_multi_connections import ReadServiceRawJoinMultiConnection
from spark_aggregation_poc.dal.read_service_raw_join_multi_connections_batches import \
    ReadServiceRawJoinMultiConnectionBatches
from spark_aggregation_poc.dal.write_service import WriteService
from spark_aggregation_poc.factory.context import build_app_context, AppContext
from spark_aggregation_poc.services.aggregation_service import AggregationService
from spark_aggregation_poc.services.aggregation_service_multi_rules_no_write import AggregationServiceMultiRulesNoWrite
from spark_aggregation_poc.services.aggregation_service_raw_join import AggregationServiceRawJoin

"""
TODO:

2. Use aggregation_query1.sql to select from DB (maybe remove 'group_by') 
3. Implement algorithm
"""



def run_aggregation_from_dbx(spark: SparkSession, config: Config = None):
    _run_aggregation(spark, config)

def main():

    jar_path = get_jar_path()

    spark = SparkSession.builder \
        .appName("PostgreSQLSparkApp") \
        .master("local[*]") \
        .config("spark.jars", jar_path) \
        .getOrCreate()

    _run_aggregation(spark)


def get_jar_path():
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)  # src/spark_aggregation_poc/
    jar_path = os.path.join(current_dir, "jars", "postgresql-42.7.1.jar")
    print("jar_path:", jar_path)
    return jar_path


def _run_aggregation(spark: SparkSession, config: Config = None):
    try:
        app_context: AppContext = build_app_context(config)
        read_service: ReadService = app_context.read_service
        read_service_pre_partition: ReadServicePrePartition = app_context.read_service_pre_partition
        read_service_raw_join: ReadServiceRawJoin = app_context.read_service_raw_join
        read_service_raw_join_multi_connection_batches: ReadServiceRawJoinMultiConnectionBatches = app_context.read_service_raw_join_multi_connection_batches
        create_read_service_individual_tables_multi_connection_batches: ReadServiceIndividualTablesMultiConnectionBatches = app_context.read_service_individual_tables_connection_batches
        read_service_raw: ReadServiceRaw = app_context.read_service_raw
        write_service: WriteService = app_context.write_service
        aggregation_service: AggregationService = app_context.aggregation_service
        aggregation_service_raw_join: AggregationServiceRawJoin = app_context.aggregation_service_raw_join
        aggregation_service_multi_rules_no_write: AggregationServiceMultiRulesNoWrite = app_context.aggregation_service_multi_rules_no_write

        from time import time

        start = time()
        df = create_read_service_individual_tables_multi_connection_batches.read_findings_data(spark=spark)
        print(f"Read time: {time() - start:.2f} seconds")
        # df.show()

        df_groups_to_findings: DataFrame = aggregation_service_raw_join.aggregate(df)
        print("\n=== Groups to findings ===")
        start = time()
        df_groups_to_findings.show()
        print(f"Transform time: {time() - start:.2f} seconds")
        #
        # print("\n=== Writing to groups_to_findings table ===")
        # start = time()
        # write_service.write_groups_to_findings(df_transformed)
        # print(f"Write time: {time() - start:.2f} seconds")


    except Exception as e:
        print(f"Error aggregating! {e}")
        # TODO: add rollback functionality (first write to staging_table and validate the expected count (inside driver))
    finally:
        # Stop Spark
        if(config and config.is_databricks is False):
            spark.stop()


if __name__ == "__main__":
    main()


#