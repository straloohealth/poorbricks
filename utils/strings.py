import re

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def camel_to_snake_case(camel_str: str) -> str:
    """Convert a camelCase or PascalCase string to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_str)
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


short_name_udf = udf(short_name, returnType=StringType())
