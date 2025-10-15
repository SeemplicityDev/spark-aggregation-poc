from pyspark.sql.types import StructType

from spark_aggregation_poc.schemas.schema_registry import SchemaRegistry

schema: StructType = SchemaRegistry.get_schema_for_table("finding_group_association")
print(schema)