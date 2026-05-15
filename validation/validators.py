"""
DataFrame validation utilities.

This module contains functions for validating DataFrames against validation rules.
"""

from collections.abc import Callable, Sequence
from functools import wraps
from typing import Any, TypeVar

from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, IntegerType, StructType

from .rules import ValidationRule
from .schema import ValidatedStruct

F = TypeVar("F", bound=Callable[..., DataFrame])


def _find_integer_types_in_schema(schema: StructType, path: str = "") -> list[str]:
    """
    Recursively find all IntegerType fields in a DataFrame schema.

    :param schema: The StructType schema to check
    :param path: The current path for nested fields (used for error reporting)
    :return: List of field paths that contain IntegerType
    """
    integer_fields = []

    for field in schema.fields:
        current_path = f"{path}.{field.name}" if path else field.name

        if isinstance(field.dataType, IntegerType):
            integer_fields.append(current_path)
        elif isinstance(field.dataType, ArrayType):
            if isinstance(field.dataType.elementType, IntegerType):
                integer_fields.append(f"{current_path}[*]")
            elif isinstance(field.dataType.elementType, StructType):
                # Recursively check nested struct in array
                nested_fields = _find_integer_types_in_schema(
                    field.dataType.elementType, f"{current_path}[*]"
                )
                integer_fields.extend(nested_fields)
        elif isinstance(field.dataType, StructType):
            # Recursively check nested struct
            nested_fields = _find_integer_types_in_schema(field.dataType, current_path)
            integer_fields.extend(nested_fields)

    return integer_fields


def validate_no_integer_types(df: DataFrame) -> None:
    """
    Validate that a DataFrame contains no IntegerType fields.

    This function ensures that all integer fields use LongType instead of IntegerType,
    which is required for compatibility with ValidatedStruct schemas.

    :param df: DataFrame to validate
    :raises ValueError: If any IntegerType fields are found
    """
    integer_fields = _find_integer_types_in_schema(df.schema)

    if integer_fields:
        field_list = ", ".join(integer_fields)
        raise ValueError(
            f"DataFrame contains IntegerType fields that should be LongType: {field_list}. "
            f"Use cast_integers_to_long() utility or cast fields to 'long' to fix this. "
            f"ValidatedStruct schemas always use LongType for int fields to ensure compatibility."
        )


def verify(df: DataFrame, rules: Sequence[ValidationRule]) -> dict[str, Any]:
    """
    Verify a DataFrame against a list of validation rules.

    :param df: The DataFrame to validate
    :param rules: List of validation rules to apply
    :return: Dictionary with validation results
    """
    results: dict[str, Any] = {
        "valid": True,
        "total_rules": len(rules),
        "passed_rules": 0,
        "failed_rules": 0,
        "errors": [],
        "rule_results": [],
    }

    for rule in rules:
        rule_errors = rule.validate(df)
        rule_result: dict[str, Any] = {
            "rule": rule.__class__.__name__,
            "column": rule.column,
            "description": rule.description,
            "passed": len(rule_errors) == 0,
            "errors": rule_errors,
        }

        results["rule_results"].append(rule_result)

        if rule_errors:
            results["valid"] = False
            results["failed_rules"] = results["failed_rules"] + 1
            results["errors"] = results["errors"] + rule_errors
        else:
            results["passed_rules"] = results["passed_rules"] + 1

    return results


def verify_with_model(
    model: type[ValidatedStruct],
    include_automatic_rules: bool = True,
) -> Callable[[F], F]:
    """
    Decorator that automatically validates DataFrame results using a ValidatedStruct model.

    This decorator should be used on DLT table functions to ensure automatic validation
    of the returned DataFrame against the specified model's validation rules.

    Additionally, this decorator automatically sets the Spark configuration
    'spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled = true'
    to allow NOT NULL constraints nested within arrays or maps, which is required
    for Delta Lake compatibility with ValidatedStruct schemas.

    :param model: ValidatedStruct model class to use for validation
    :param include_automatic_rules: If True, includes automatic schema and enum validation.
    :return: Decorator function

    Example:
        @dlt.table(name="messages", schema=Message.to_struct())
        @verify_with_model(model=Message)
        def messages_table() -> DataFrame:
            return _run()
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            result_df = func(*args, **kwargs)

            # First, validate that no IntegerType fields are present
            # This is critical for ValidatedStruct compatibility
            validate_no_integer_types(result_df)

            # Then validate the result using the model's verify method
            model.verify(
                result_df,
                strict=True,
                include_automatic_rules=include_automatic_rules,
            )
            return result_df

        return wrapper  # type: ignore

    return decorator
