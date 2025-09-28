from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class SparkAggregationRule:
    """Spark-compatible aggregation rule extracted from engine DB"""
    id: int
    order: int
    group_by: Optional[List[str]]
    filters_config: Optional[Dict[str, Any]]
    field_calculation: Dict[str, Any]
    rule_type: str
