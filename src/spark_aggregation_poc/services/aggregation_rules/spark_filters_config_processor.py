import json
from typing import Dict, Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr


class FiltersConfigProcessor:
    """
    Processes filters_config to generate Spark-compatible filter conditions
    """

    def generate_filter_condition(self, filters_config: Dict[str, Any]) -> Optional[str]:
        """
        Convert filters_config to Spark SQL filter condition

        Args:
            filters_config: The filters configuration from aggregation rule

        Returns:
            Spark SQL WHERE condition string
        """
        if not filters_config:
            return None

        # Check if the filters_config itself is a filter JSON structure
        if "operator" in filters_config and "operands" in filters_config:
            # The entire filters_config is a filter JSON structure
            return self._parse_filter_json(filters_config)

        conditions = []

        for field, values in filters_config.items():
            # Handle scopesjson
            if field == "scopesjson":
                if not values or (isinstance(values, dict) and not values) or (isinstance(values, list) and not values):
                    # Skip empty scopesjson
                    continue
                else:
                    # Parse non-empty scopesjson
                    try:
                        if isinstance(values, str):
                            scope_json = json.loads(values)
                        else:
                            scope_json = values

                        parsed_scope_condition = self._parse_filter_json(scope_json)
                        if parsed_scope_condition:
                            conditions.append(f"({parsed_scope_condition})")
                    except (json.JSONDecodeError, Exception) as e:
                        print(f"Error parsing scopesjson: {e}")
                        continue

            # Handle filtersjson - parse the complex JSON structure
            elif field == "filtersjson" and values:
                try:
                    if isinstance(values, str):
                        filter_json = json.loads(values)
                    else:
                        filter_json = values

                    parsed_condition = self._parse_filter_json(filter_json)
                    if parsed_condition:
                        conditions.append(f"({parsed_condition})")
                except (json.JSONDecodeError, Exception) as e:
                    print(f"Error parsing filtersjson: {e}")
                    continue

            elif field == "custom_sql" and values:
                conditions.append(f"({values})")
            elif isinstance(values, list) and values:
                value_list = "', '".join(str(v) for v in values)
                conditions.append(f"{field} IN ('{value_list}')")
            elif values is not None and values != "":
                # Skip fields that are part of filter JSON structure
                if field not in ["operator", "operands"]:
                    conditions.append(f"{field} = '{values}'")

        return " AND ".join(conditions) if conditions else None

    def _parse_filter_json(self, filter_obj: Dict[str, Any]) -> Optional[str]:
        """
        Parse complex filtersjson structure into SQL conditions
        """
        if not isinstance(filter_obj, dict):
            return None

        # Handle operator-based filters
        if "operator" in filter_obj and "operands" in filter_obj:
            operator = filter_obj["operator"].upper()
            operands = filter_obj["operands"]

            if not operands:
                return None

            parsed_operands = []
            for operand in operands:
                # Handle string operands that might be JSON
                if isinstance(operand, str):
                    try:
                        # Try to parse as JSON first
                        operand = json.loads(operand)
                    except json.JSONDecodeError:
                        # If it's not JSON, skip it or handle as literal
                        print(f"Warning: Could not parse operand as JSON: {operand}")
                        continue

                parsed_operand = self._parse_filter_json(operand)
                if parsed_operand:
                    parsed_operands.append(parsed_operand)

            if parsed_operands:
                if operator == "AND":
                    return f"({' AND '.join(parsed_operands)})"
                elif operator == "OR":
                    return f"({' OR '.join(parsed_operands)})"

        # Handle field-based filters
        elif "field" in filter_obj and "condition" in filter_obj:
            field = filter_obj["field"]
            condition = filter_obj["condition"]
            value = filter_obj.get("value")

            # Skip filters with empty conditions
            if not condition:
                return None

            # Handle nested value objects
            if isinstance(value, dict) and "field" in value:
                # This is a nested filter like the actual_status example
                nested_condition = self._parse_filter_json(value)
                if nested_condition:
                    return nested_condition
                return None

            # Handle different condition types
            if condition == "in" and isinstance(value, list):
                if value:  # Only if list is not empty
                    value_list = "', '".join(str(v) for v in value)
                    return f"{field} IN ('{value_list}')"
            elif condition == "not_in" and isinstance(value, list):
                if value:  # Only if list is not empty
                    value_list = "', '".join(str(v) for v in value)
                    return f"{field} NOT IN ('{value_list}')"
            elif condition == "exists":
                if str(value).lower() == "true":
                    return f"{field} IS NOT NULL"
                else:
                    return f"{field} IS NULL"
            elif condition == "equals":
                return f"{field} = '{value}'"
            elif condition == "not_equals":
                return f"{field} != '{value}'"
            elif condition == "contains":
                return f"{field} LIKE '%{value}%'"
            elif condition == "not_contains":
                return f"{field} NOT LIKE '%{value}%'"
            # Add support for custom conditions
            elif condition == "overlap":
                if isinstance(value, list) and value:
                    # For array overlap, we need to check if the field contains any of the values
                    conditions = []
                    for v in value:
                        conditions.append(f"array_contains({field}, '{v}')")
                    return f"({' OR '.join(conditions)})"
            elif condition == "notoverlapping":
                if isinstance(value, list) and value:
                    # For not overlapping, none of the values should be in the field
                    conditions = []
                    for v in value:
                        conditions.append(f"NOT array_contains({field}, '{v}')")
                    return f"({' AND '.join(conditions)})"
            elif condition == "overlapilikelist":
                if isinstance(value, list) and value:
                    # Case-insensitive overlap
                    conditions = []
                    for v in value:
                        conditions.append(f"exists(transform({field}, x -> lower(x)), x -> x = lower('{v}'))")
                    return f"({' OR '.join(conditions)})"
            elif condition == "notoverlapilikelist":
                if isinstance(value, list) and value:
                    # Case-insensitive not overlap
                    conditions = []
                    for v in value:
                        conditions.append(f"NOT exists(transform({field}, x -> lower(x)), x -> x = lower('{v}'))")
                    return f"({' AND '.join(conditions)})"

        return None

    def apply_filters_config_to_dataframe(self, df: DataFrame, filters_config: Dict[str, Any]) -> DataFrame:
        """
        Apply filters_config directly to DataFrame

        Args:
            df: Input DataFrame
            filters_config: Filters configuration

        Returns:
            Filtered DataFrame
        """
        filter_condition = self.generate_filter_condition(filters_config)

        if filter_condition:
            print(f"Applying filters_config condition: {filter_condition}")
            return df.filter(expr(filter_condition))

        return df