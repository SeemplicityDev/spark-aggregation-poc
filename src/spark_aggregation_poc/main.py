import os

from pyspark.sql import SparkSession, DataFrame

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.dal.read_service import ReadService
from spark_aggregation_poc.dal.write_service import WriteService
from spark_aggregation_poc.factory.context import build_app_context, AppContext
from spark_aggregation_poc.services.aggregation_service import AggregationService

"""
TODO:

2. Use aggregation_query1.sql to select from DB (maybe remove 'group_by') 
3. Implement algorithm
"""



def run_aggregation_from_dbx(spark: SparkSession, config: Config = None):
    _run_aggregation(spark, config)
    spark.stop()

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
        write_service: WriteService = app_context.write_service
        transform_service: AggregationService = app_context.transform_service

        from time import time

        start = time()
        df, findings_data = read_service.read_findings_data(spark=spark)
        print(f"Read time: {time() - start:.2f} seconds")

        # # Work with Person objects
        # print("=== All Finding Data as Objects ===")
        # for finding_data in findings_data:
        #     print(f"  {finding_data}")

        df_transformed: DataFrame = transform_service.aggregate(df)
        print("\n=== Groups to findings ===")
        start = time()
        df_transformed.show(10, truncate=False)
        print(f"Transform time: {time() - start:.2f} seconds")

        # print("\n=== Writing to groups_to_findings table ===")
        # start = time()
        # write_service.write_groups_to_findings(df_transformed)
        # print(f"Write time: {time() - start:.2f} seconds")


    except Exception as e:
        print(f"Error aggregating! {e}")
        # TODO: add rollback functionality (first write to staging_table and validate the expected count (inside driver))
    finally:
        # Stop Spark
        spark.stop()


if __name__ == "__main__":
    main()


#