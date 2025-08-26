from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import explode, col


class AggregationServiceRawJoin():

    def aggregate(self, df: DataFrame) -> DataFrame:
        df = self.group_and_process(df)
        groups_to_findings: DataFrame = self.create_groups_to_findings(df)
        return groups_to_findings

    def group_and_process(self, df):
        # Repartition for distributed safety
        df = df.repartition("package_name")
        # GroupByKey
        result_df: DataFrame = df.groupBy("package_name").agg(
            collect_list("finding_id").alias("finding_ids"),
        )
        print("=== After final grouping and processing ===")
        result_df.show(10, truncate=False)
        return result_df


    def create_groups_to_findings(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("package_name").alias("group_id"),
            explode("finding_ids").alias("finding_id")
        )
