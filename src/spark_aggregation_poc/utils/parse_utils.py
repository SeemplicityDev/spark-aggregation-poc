from typing import List

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from spark_aggregation_poc.models.finding_data import FindingData

# TODO: remove class

def row_to_finding_data(row: Row) -> FindingData:
    """Convert a Spark Row directly to FindingData object"""
    return FindingData(
        calculated_group_identifier=row.calculated_group_identifier,
        calculated_finding_id=row.calculated_finding_id,
        finding_ids_without_group=row.finding_ids_without_group,
        group_id=row.group_id,
        main_finding_id=row.main_finding_id
    )

def finding_data_to_dataframe(spark: SparkSession, people: List[FindingData]):
    """Convert list of Person objects to Spark DataFrame"""
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])

    # Convert Person objects to Row objects
    rows = [Row(id=p.id, name=p.name, age=p.age, city=p.city) for p in people]

    # Create DataFrame
    return spark.createDataFrame(rows, schema)