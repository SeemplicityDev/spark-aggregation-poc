from typing import Tuple, Any, Iterable

from pyspark import Row
from pyspark.rdd import PipelinedRDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col


class AggregationService():

    def aggregate(self, df: DataFrame) -> DataFrame:
        df = self.group_and_process(df)
        groups_to_findings: DataFrame = self.create_groups_to_findings(df)
        return groups_to_findings

    def group_and_process(self, df):
        # Repartition for distributed safety
        df = df.repartition("calculated_group_identifier")
        # GroupByKey
        grouped: PipelinedRDD[Tuple[Any, Iterable[Row]]] = df.rdd.map(lambda row: (row["calculated_group_identifier"], row)).groupByKey()
        # Apply mapGroups via rdd - just some processing logic to demonstrate processing each group individually
        processed_df = grouped.map(lambda kv: self.process_group(kv[0], kv[1])).toDF()
        print("=== After final grouping and processing ===")
        processed_df.show(10, truncate=False)
        return df


    def process_group(self, group_id, rows_iter):
        values = [row["calculated_finding_id"] for row in rows_iter]
        return Row(group_id=group_id, count=len(values), concatenated=",".join(values))


    def create_groups_to_findings(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("calculated_group_identifier").alias("group_id"),
            explode("finding_ids_without_group").alias("finding_id")
        )
