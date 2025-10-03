import json
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from spark_aggregation_poc.config.config import Config
from spark_aggregation_poc.interfaces.interfaces import IRuleLoader
from spark_aggregation_poc.models.spark_aggregation_rules import AggregationRule


class RuleLoader(IRuleLoader):
    _allow_init = False

    @classmethod
    def create_rule_loader(cls, config: Config):
        cls._allow_init = True
        result = RuleLoader(config)
        cls._allow_init = False

        return result

    def __init__(self, config: Config):
        self.jdbc_url = config.postgres_url
        self.db_properties = config.postgres_properties

    def load_aggregation_rules(self, spark: SparkSession, customer_id: Optional[int] = None) -> list[AggregationRule]:
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

        rules_df.orderBy("rule_order")

        print("loaded rules from DB")
        rules_df.show(20, False)

        return self.create_aggregation_rules(rules_df)


    def create_aggregation_rules(self, rules_df: DataFrame) -> List[AggregationRule]:
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

            spark_rule = AggregationRule(
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