"""
Strongly-typed container for aggregation output dataframes.
"""
from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class AggregationOutput:
    finding_group_association: DataFrame
    finding_group_rollup: DataFrame