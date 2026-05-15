"""
Validation rules for DataFrame validation.

This module contains all validation rule classes that can be used to validate
DataFrame data against specific criteria.
"""

from datetime import datetime
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length
from pyspark.sql.types import StructType


class ValidationRule:
    """Base class for DataFrame validation rules"""

    def __init__(self, column: str, description: str):
        self.column = column
        self.description = description

    def validate(self, df: DataFrame) -> list[str]:
        """Validate the rule against the DataFrame. Returns list of error messages."""
        raise NotImplementedError


class IntRangeRule(ValidationRule):
    """Validates that integer column values are within a specified range"""

    def __init__(
        self,
        column: str,
        min_value: int | None = None,
        max_value: int | None = None,
    ):
        description = f"Column '{column}' must be"
        if min_value is not None and max_value is not None:
            description += f" between {min_value} and {max_value}"
        elif min_value is not None:
            description += f" >= {min_value}"
        elif max_value is not None:
            description += f" <= {max_value}"

        super().__init__(column, description)
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, df: DataFrame) -> list[str]:
        errors = []

        if self.column not in df.columns:
            errors.append(f"Column '{self.column}' not found in DataFrame")
            return errors

        # Check min value
        if self.min_value is not None:
            invalid_count = df.filter(col(self.column) < self.min_value).count()
            if invalid_count > 0:
                errors.append(
                    f"Column '{self.column}': {invalid_count} rows have values < {self.min_value}"
                )

        # Check max value
        if self.max_value is not None:
            invalid_count = df.filter(col(self.column) > self.max_value).count()
            if invalid_count > 0:
                errors.append(
                    f"Column '{self.column}': {invalid_count} rows have values > {self.max_value}"
                )

        return errors


class DateTimeRangeRule(ValidationRule):
    """Validates that datetime column values are within a specified range"""

    def __init__(
        self,
        column: str,
        min_date: datetime | None = None,
        max_date: datetime | None = None,
    ):
        description = f"Column '{column}' must be"
        if min_date is not None and max_date is not None:
            description += f" between {min_date} and {max_date}"
        elif min_date is not None:
            description += f" >= {min_date}"
        elif max_date is not None:
            description += f" <= {max_date}"

        super().__init__(column, description)
        self.min_date = min_date
        self.max_date = max_date

    def validate(self, df: DataFrame) -> list[str]:
        errors = []

        if self.column not in df.columns:
            errors.append(f"Column '{self.column}' not found in DataFrame")
            return errors

        # Check min date
        if self.min_date is not None:
            invalid_count = df.filter(col(self.column) < self.min_date).count()
            if invalid_count > 0:
                errors.append(
                    f"Column '{self.column}': {invalid_count} rows have dates < {self.min_date}"
                )

        # Check max date
        if self.max_date is not None:
            invalid_count = df.filter(col(self.column) > self.max_date).count()
            if invalid_count > 0:
                errors.append(
                    f"Column '{self.column}': {invalid_count} rows have dates > {self.max_date}"
                )

        return errors


class NotNullRule(ValidationRule):
    """Validates that a column has no null values"""

    def __init__(self, column: str):
        super().__init__(column, f"Column '{column}' must not contain null values")

    def validate(self, df: DataFrame) -> list[str]:
        errors = []

        if self.column not in df.columns:
            errors.append(f"Column '{self.column}' not found in DataFrame")
            return errors

        null_count = df.filter(col(self.column).isNull()).count()
        if null_count > 0:
            errors.append(
                f"Column '{self.column}': {null_count} rows contain null values"
            )

        return errors


class StringLengthRule(ValidationRule):
    """Validates that string column values meet length requirements"""

    def __init__(
        self,
        column: str,
        min_length: int | None = None,
        max_length: int | None = None,
    ):
        description = f"Column '{column}' length must be"
        if min_length is not None and max_length is not None:
            description += f" between {min_length} and {max_length} characters"
        elif min_length is not None:
            description += f" >= {min_length} characters"
        elif max_length is not None:
            description += f" <= {max_length} characters"

        super().__init__(column, description)
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, df: DataFrame) -> list[str]:
        errors = []

        if self.column not in df.columns:
            errors.append(f"Column '{self.column}' not found in DataFrame")
            return errors

        # Check min length
        if self.min_length is not None:
            invalid_count = df.filter(
                length(col(self.column)) < self.min_length
            ).count()
            if invalid_count > 0:
                errors.append(
                    f"Column '{self.column}': {invalid_count} rows have length < {self.min_length}"
                )

        # Check max length
        if self.max_length is not None:
            invalid_count = df.filter(
                length(col(self.column)) > self.max_length
            ).count()
            if invalid_count > 0:
                errors.append(
                    f"Column '{self.column}': {invalid_count} rows have length > {self.max_length}"
                )

        return errors


class EnumRule(ValidationRule):
    """Validates that column values are valid enum values"""

    def __init__(self, column: str, enum_class: type[Enum]):
        valid_values = [e.value for e in enum_class]
        super().__init__(
            column,
            f"Column '{column}' must contain only valid {enum_class.__name__} values: {valid_values}",
        )
        self.enum_class = enum_class
        self.valid_values = valid_values

    def validate(self, df: DataFrame) -> list[str]:
        errors = []

        if self.column not in df.columns:
            errors.append(f"Column '{self.column}' not found in DataFrame")
            return errors

        # Check for invalid enum values (excluding nulls)
        invalid_count = df.filter(
            col(self.column).isNotNull() & ~col(self.column).isin(self.valid_values)
        ).count()

        if invalid_count > 0:
            # Get sample invalid values for better error reporting
            invalid_values = [
                row[0]
                for row in (
                    df.filter(
                        col(self.column).isNotNull()
                        & ~col(self.column).isin(self.valid_values)
                    )
                    .select(self.column)
                    .distinct()
                    .limit(5)
                    .collect()
                )
            ]

            errors.append(
                f"Column '{self.column}': {invalid_count} rows contain invalid {self.enum_class.__name__} values. "
                f"Valid values: {self.valid_values}. "
                f"Sample invalid values: {invalid_values}"
            )

        return errors


class SchemaValidationRule(ValidationRule):
    """Validates that DataFrame schema matches expected structure"""

    def __init__(self, expected_schema: StructType, allow_extra_columns: bool = False):
        super().__init__("schema", "DataFrame schema must match expected structure")
        self.expected_schema = expected_schema
        self.allow_extra_columns = allow_extra_columns

    def validate(self, df: DataFrame) -> list[str]:
        errors = []

        expected_fields = {field.name: field for field in self.expected_schema.fields}
        actual_fields = {field.name: field for field in df.schema.fields}

        # Check for missing columns
        missing_columns = set(expected_fields.keys()) - set(actual_fields.keys())
        if missing_columns:
            errors.append(f"Missing required columns: {sorted(missing_columns)}")

        # Check for extra columns (if not allowed)
        if not self.allow_extra_columns:
            extra_columns = set(actual_fields.keys()) - set(expected_fields.keys())
            if extra_columns:
                errors.append(f"Unexpected extra columns: {sorted(extra_columns)}")

        # Check data types for existing columns
        for column_name in set(expected_fields.keys()) & set(actual_fields.keys()):
            expected_field = expected_fields[column_name]
            actual_field = actual_fields[column_name]

            # Compare data types (simplified comparison)
            if not isinstance(actual_field.dataType, type(expected_field.dataType)):
                errors.append(
                    f"Column '{column_name}' has incorrect type: "
                    f"expected {expected_field.dataType}, got {actual_field.dataType}"
                )

            # Nullable metadata is intentionally not checked here: Spark marks most
            # output columns as nullable=True regardless of actual data content.
            # @dlt.table(schema=...) enforces nullability at write time.

        return errors
