"""
Strongly-typed container for aggregation output dataframes.
"""
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as spark_sum, size

from spark_aggregation_poc.schemas.schema_registry import (
    SchemaRegistry, ColumnNames, TableNames
)


class AggregationOutput:
    """
    Container for the two output dataframes from aggregation.
    Provides type safety, validation, and convenience methods.
    """

    def __init__(
            self,
            finding_group_association: DataFrame,
            finding_group_rollup: DataFrame,
            validate_schema: bool = True,
            validate_integrity: bool = False
    ):
        """
        Initialize aggregation output with optional validation.

        Args:
            finding_group_association: DataFrame with finding-to-group mappings
            finding_group_rollup: DataFrame with aggregated group data
            validate_schema: Whether to validate schemas match SchemaRegistry
            validate_integrity: Whether to validate data integrity rules
        """
        self._association = finding_group_association
        self._rollup = finding_group_rollup

        if validate_schema:
            self.validate_schemas()

        if validate_integrity:
            self.validate_integrity()

    @property
    def association(self) -> DataFrame:
        """Get the finding_group_association dataframe"""
        return self._association

    @property
    def rollup(self) -> DataFrame:
        """Get the finding_group_rollup dataframe"""
        return self._rollup

    # Backwards compatibility aliases
    @property
    def finding_group_association(self) -> DataFrame:
        """Alias for association property"""
        return self._association

    @property
    def finding_group_rollup(self) -> DataFrame:
        """Alias for rollup property"""
        return self._rollup

    def get_association_count(self) -> int:
        """Get count of finding associations"""
        return self._association.count()

    def get_rollup_count(self) -> int:
        """Get count of aggregation groups"""
        return self._rollup.count()

    def get_total_findings(self) -> int:
        """Get total number of findings across all groups"""
        if self.get_rollup_count() == 0:
            return 0

        result = self._rollup.agg(
            spark_sum(ColumnNames.FINDINGS_COUNT).alias("total")
        ).collect()[0]["total"]

        return result if result is not None else 0

    def is_empty(self) -> bool:
        """Check if aggregation produced no results"""
        return self.get_rollup_count() == 0

    def validate_schemas(self) -> bool:
        """
        Validate that dataframes match expected schemas from SchemaRegistry.

        Returns:
            True if schemas are valid

        Raises:
            ValueError if schemas don't match
        """
        # Validate association schema
        expected_association_schema = SchemaRegistry.finding_group_association_schema()
        association_cols = set(self._association.columns)
        expected_association_cols = {field.name for field in expected_association_schema.fields}

        if not expected_association_cols.issubset(association_cols):
            missing = expected_association_cols - association_cols
            raise ValueError(
                f"Association dataframe missing required columns: {missing}. "
                f"Expected: {expected_association_cols}, Got: {association_cols}"
            )

        # Validate rollup has base schema columns
        expected_rollup_schema = SchemaRegistry.finding_group_rollup_base_schema()
        rollup_cols = set(self._rollup.columns)
        expected_rollup_cols = {field.name for field in expected_rollup_schema.fields}

        if not expected_rollup_cols.issubset(rollup_cols):
            missing = expected_rollup_cols - rollup_cols
            raise ValueError(
                f"Rollup dataframe missing required columns: {missing}. "
                f"Expected at least: {expected_rollup_cols}, Got: {rollup_cols}"
            )

        return True

    def validate_integrity(self) -> bool:
        """
        Validate data integrity rules.

        Returns:
            True if all integrity checks pass

        Raises:
            ValueError if integrity checks fail
        """
        if self.is_empty():
            return True

        # Check no null group_ids in association
        null_assoc_groups = self._association.filter(
            self._association[ColumnNames.GROUP_ID].isNull()
        ).count()
        if null_assoc_groups > 0:
            raise ValueError(f"Found {null_assoc_groups} null group_ids in association")

        # Check no null finding_ids in association
        null_assoc_findings = self._association.filter(
            self._association[ColumnNames.FINDING_ID].isNull()
        ).count()
        if null_assoc_findings > 0:
            raise ValueError(f"Found {null_assoc_findings} null finding_ids in association")

        # Check no null group_ids in rollup
        null_rollup_groups = self._rollup.filter(
            self._rollup[ColumnNames.GROUP_ID].isNull()
        ).count()
        if null_rollup_groups > 0:
            raise ValueError(f"Found {null_rollup_groups} null group_ids in rollup")

        # Check all findings_count are positive
        invalid_counts = self._rollup.filter(
            self._rollup[ColumnNames.FINDINGS_COUNT] <= 0
        ).count()
        if invalid_counts > 0:
            raise ValueError(f"Found {invalid_counts} non-positive findings_count values")

        # Check no empty finding_ids arrays
        empty_arrays = self._rollup.filter(
            size(self._rollup[ColumnNames.FINDING_IDS]) == 0
        ).count()
        if empty_arrays > 0:
            raise ValueError(f"Found {empty_arrays} empty finding_ids arrays")

        # Check consistency: association count should equal sum of findings_count
        association_count = self.get_association_count()
        total_in_rollup = self.get_total_findings()

        if association_count != total_in_rollup:
            raise ValueError(
                f"Association count ({association_count}) doesn't match "
                f"sum of findings_count in rollup ({total_in_rollup})"
            )

        return True

    def show_summary(self, verbose: bool = True) -> None:
        """
        Print a summary of the aggregation results.

        Args:
            verbose: If True, show detailed statistics
        """
        rollup_count = self.get_rollup_count()
        association_count = self.get_association_count()

        print("\n" + "=" * 70)
        print("📊 Aggregation Output Summary")
        print("=" * 70)

        if self.is_empty():
            print("  ⚠️  No aggregation groups created")
        else:
            print(f"  Groups created: {rollup_count:,}")
            print(f"  Findings associated: {association_count:,}")
            print(f"  Total findings in groups: {self.get_total_findings():,}")

            if rollup_count > 0:
                avg_findings = self.get_total_findings() / rollup_count
                print(f"  Avg findings per group: {avg_findings:.1f}")

        if verbose and not self.is_empty():
            print("\n  Column Info:")
            print(f"    Association columns: {', '.join(self._association.columns)}")
            print(f"    Rollup columns: {', '.join(self._rollup.columns)}")

        print("=" * 70)

    def show_sample_data(self, num_rows: int = 10, truncate: bool = False) -> None:
        """
        Display sample data from both dataframes.

        Args:
            num_rows: Number of rows to display
            truncate: Whether to truncate long values
        """
        if not self.is_empty():
            print("\n📋 Sample Association Data:")
            self._association.show(num_rows, truncate=truncate)

            print("\n📋 Sample Rollup Data:")
            self._rollup.show(num_rows, truncate=truncate)
        else:
            print("\n⚠️  No data to display - aggregation output is empty")

    def create_temp_views(self, association_view: str = "temp_association",
                          rollup_view: str = "temp_rollup") -> None:
        """
        Create temporary views for SQL queries.

        Args:
            association_view: Name for association temp view
            rollup_view: Name for rollup temp view
        """
        self._association.createOrReplaceTempView(association_view)
        self._rollup.createOrReplaceTempView(rollup_view)

        print(f"✅ Created temp views: '{association_view}' and '{rollup_view}'")

    def to_dict(self) -> dict:
        """
        Convert summary statistics to dictionary.

        Returns:
            Dictionary with key statistics
        """
        return {
            "rollup_count": self.get_rollup_count(),
            "association_count": self.get_association_count(),
            "total_findings": self.get_total_findings(),
            "is_empty": self.is_empty(),
            "association_columns": self._association.columns,
            "rollup_columns": self._rollup.columns
        }

    def __repr__(self) -> str:
        """String representation of AggregationOutput"""
        return (
            f"AggregationOutput("
            f"groups={self.get_rollup_count()}, "
            f"associations={self.get_association_count()}, "
            f"empty={self.is_empty()})"
        )