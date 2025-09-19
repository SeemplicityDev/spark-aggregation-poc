import json
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from spark_aggregation_poc.config.config import Config


@dataclass
class SparkAggregationRule:
    """Spark-compatible aggregation rule extracted from engine DB"""
    id: int
    order: int
    group_by: Optional[List[str]]
    filters_config: Optional[Dict[str, Any]]
    field_calculation: Dict[str, Any]
    rule_type: str


class RuleLoader:
    """
    Loads aggregation rules from engine's PostgreSQL using Spark JDBC
    """

    def __init__(self, config: Config):
        self.jdbc_url = config.postgres_url
        self.db_properties = config.postgres_properties

    def load_aggregation_rules(self, spark: SparkSession, customer_id: Optional[int] = None) -> DataFrame:
        """
        Load aggregation rules from PostgreSQL using Spark JDBC

        Args:
            customer_id: Optional customer filter

        Returns:
            DataFrame with aggregation rules
        """
        # Base query to get aggregation rules
        query = """
        (
            SELECT 
                id,
	            "order" as rule_order,
                aggregation_query,
                "type" as rule_type,	
                field_calculation
            FROM aggregation_rules
            WHERE "type" = 'AGG'
        ) as aggregation_rules_query
        """

        # Load rules using Spark JDBC
        rules_df = spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.db_properties
        )

        # Filter by customer if specified
        if customer_id:
            rules_df = rules_df.filter(col("customer_id") == customer_id)

        return rules_df.orderBy("rule_order")

    def parse_rules_to_spark_format(self, rules_df: DataFrame) -> List[SparkAggregationRule]:
        """
        Parse the loaded DataFrame into SparkAggregationRule objects

        Args:
            rules_df: DataFrame loaded from database

        Returns:
            List of SparkAggregationRule objects
        """
        spark_rules = []

        # Collect rules to driver (assuming reasonable number of rules)
        rules_data = rules_df.collect()

        for row in rules_data:
            aggregation_query = json.loads(row.aggregation_query) if row.aggregation_query else {}

            # Extract components from aggregation_query
            group_by = aggregation_query.get("group_by")
            filters_config = aggregation_query.get("filters_config", {})
            field_calculation = aggregation_query.get("field_calculation", {})

            spark_rule = SparkAggregationRule(
                id=row.id,
                order=row.rule_order,
                group_by=group_by,
                filters_config=filters_config,
                field_calculation=field_calculation,
                rule_type=row.rule_type
            )

            spark_rules.append(spark_rule)

        spark_rules.sort(key=lambda x: x.order)

        return spark_rules