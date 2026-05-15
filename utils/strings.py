import re

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def get_field_mappings(field_names: list[str]) -> dict[str, str]:
    """
    Public interface to get comprehensive camelCase to snake_case mappings for field names.
    This is the proper way to get field mappings without accessing private functions.

    Args:
        field_names: List of field names to get mappings for

    Returns:
        Dictionary mapping original field names to snake_case equivalents
    """
    return _get_all_camel_to_snake_mappings(field_names)


def default_transformations() -> dict[str, str]:
    """Domain-specific field mappings that override automatic conversion"""
    return {}


def _get_all_camel_to_snake_mappings(field_names: list[str]) -> dict[str, str]:
    """
    Get comprehensive camelCase to snake_case mappings for a list of field names.
    Combines domain-specific mappings with automatic conversion.
    """
    # Start with domain-specific mappings
    mappings = default_transformations()

    # Add automatic conversions for any fields not in domain-specific mappings
    for field_name in field_names:
        if field_name not in mappings:
            snake_case = _camel_to_snake_case(field_name)
            if (
                snake_case != field_name
            ):  # Only add if conversion actually changes the name
                mappings[field_name] = snake_case

    return mappings


def _camel_to_snake_case(camel_str: str) -> str:
    """Convert camelCase string to snake_case using regex"""
    # Insert underscore before uppercase letters that follow lowercase letters or digits
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_str)
    # Insert underscore before uppercase letters that follow lowercase letters
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def short_name(name: str | None) -> str | None:
    """UDF to create short names from full names"""
    if name is None:
        return None
    name_parts = name.split(" ")
    new_name = name_parts[0]
    if len(name_parts) > 1:
        new_name += " " + name_parts[1][0] + "."
    return new_name


# Create the UDF with proper typing
short_name_udf = udf(short_name, returnType=StringType())
