from typing import Any

import pyspark.sql.functions as f
from pyspark.sql import Column


def when_mapping(
    column: Column,
    mapping: dict[str, Any],
    invert_dict: bool = False,
    default: Any | None = None,
) -> Column:
    """Create a chained when() expression from a mapping dict.

    Args:
        column: Column to compare against keys.
        mapping: dict mapping keys -> values.
        invert_dict: if True, treat mapping values as keys and vice-versa.
        default: optional default value (or Column) to use in otherwise(). If None,
                 the original column is returned for unmatched cases (backwards compatible).

    Returns:
        A pyspark.sql Column with chained when() expressions and an otherwise().
    """
    if not mapping:
        return column

    when_expr: Column | None = None
    for key, value in mapping.items():
        key_ = key if not invert_dict else value
        value_ = value if not invert_dict else key

        condition = f.when(column == f.lit(key_), f.lit(value_))
        when_expr = (
            condition
            if when_expr is None
            else when_expr.when(column == f.lit(key_), f.lit(value_))
        )

    if when_expr is None:
        return column

    # Determine otherwise value: keep original column if default is None, else use literal or given Column
    if default is None:
        otherwise_value = column
    else:
        otherwise_value = default if isinstance(default, Column) else f.lit(default)

    return when_expr.otherwise(otherwise_value)
