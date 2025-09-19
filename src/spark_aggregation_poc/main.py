import os

from pyspark.sql import SparkSession

from spark_aggregation_poc.run_aggregation import run_aggregation


def main():
    spark = SparkSession.builder \
        .appName("PostgreSQLSparkApp") \
        .master("local[*]") \
        .config("spark.jars", get_postgres_jar_path()) \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", get_local_warehouse_path()) \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .getOrCreate()

    # Debug: Check available catalogs
    print("Available catalogs:")
    try:
        spark.sql("SHOW CATALOGS").show()
    except:
        print("SHOW CATALOGS not supported")

    run_aggregation(spark)





def get_postgres_jar_path():
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)  # src/spark_aggregation_poc/
    jar_path = os.path.join(current_dir, "jars", "postgresql-42.7.1.jar")
    print("jar_path:", jar_path)
    return jar_path


def get_local_warehouse_path():
    current_file = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_file)  # src/spark_aggregation_poc/
    warehouse_path = os.path.join(current_dir, "local-catalog")
    print("local_warehouse_path:", warehouse_path)
    return warehouse_path


if __name__ == "__main__":
    main()


#