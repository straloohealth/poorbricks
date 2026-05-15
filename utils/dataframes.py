from typing import Any, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DataType, Row, StructType

from utils.strings import get_field_mappings

# Spark Connect (Databricks Serverless / Connect runtime) has its own
# DataFrame class at ``pyspark.sql.connect.DataFrame`` that is NOT a
# subclass of ``pyspark.sql.DataFrame``. ``isinstance`` against the
# classic class returns False, which would force ``create_dataframe``
# down the list-of-rows path and round-trip every row through the
# driver. Bundle both at import time.
try:
    from pyspark.sql.connect.dataframe import DataFrame as _ConnectDataFrame

    _DATAFRAME_TYPES: tuple[type, ...] = (DataFrame, _ConnectDataFrame)
except Exception:
    _DATAFRAME_TYPES = (DataFrame,)


def cast_integers_to_long(df: DataFrame) -> DataFrame:
    """
    Cast all IntegerType columns to LongType to ensure compatibility with ValidatedStruct schemas.

    This function handles:
    - Top-level IntegerType columns
    - IntegerType elements in ArrayType columns
    - Nested IntegerType fields in StructType columns (recursively)

    This function helps resolve schema mismatches where source data has IntegerType
    but ValidatedStruct models expect LongType for all int fields.

    :param df: Input DataFrame that may have IntegerType columns
    :return: DataFrame with IntegerType columns cast to LongType
    """
    from pyspark.sql.functions import col
    from pyspark.sql.types import (
        ArrayType,
        IntegerType,
        LongType,
        StructField,
        StructType,
    )

    def _convert_type_to_long(data_type: DataType) -> DataType:
        """Recursively convert IntegerType to LongType in nested structures."""
        if isinstance(data_type, IntegerType):
            return LongType()
        elif isinstance(data_type, ArrayType):
            # Convert array element type
            new_element_type = _convert_type_to_long(data_type.elementType)
            return ArrayType(new_element_type, data_type.containsNull)
        elif isinstance(data_type, StructType):
            # Convert struct field types
            new_fields = []
            for field in data_type.fields:
                new_field_type = _convert_type_to_long(field.dataType)
                new_fields.append(
                    StructField(
                        field.name, new_field_type, field.nullable, field.metadata
                    )
                )
            return StructType(new_fields)
        else:
            # Return unchanged for other types
            return data_type

    def _needs_casting(data_type: DataType) -> bool:
        """Check if a data type contains any IntegerType that needs casting."""
        if isinstance(data_type, IntegerType):
            return True
        elif isinstance(data_type, ArrayType):
            return _needs_casting(data_type.elementType)
        elif isinstance(data_type, StructType):
            return any(_needs_casting(field.dataType) for field in data_type.fields)
        else:
            return False

    # Check if any casting is needed
    needs_casting = any(_needs_casting(field.dataType) for field in df.schema.fields)

    if not needs_casting:
        return df  # No integer types to cast

    # Build select expressions with proper casting
    select_exprs = []
    for field in df.schema.fields:
        if _needs_casting(field.dataType):
            # Cast the column to the converted schema
            _convert_type_to_long(field.dataType)
            if isinstance(field.dataType, ArrayType) and isinstance(
                field.dataType.elementType, IntegerType
            ):
                # For arrays with integer elements, we need to use transform to cast each element
                from pyspark.sql.functions import transform

                select_exprs.append(
                    transform(col(field.name), lambda x: x.cast("long")).alias(
                        field.name
                    )
                )
            else:
                # For simple integer fields, direct casting works
                select_exprs.append(col(field.name).cast("long").alias(field.name))
        else:
            select_exprs.append(col(field.name))

    return df.select(*select_exprs)


def create_dataframe(
    data: DataFrame | list[dict[str, Any]] | list[Row],
    target_schema: StructType | None = None,
    enforce_nullability: bool = True,
) -> DataFrame:
    """
    Creates a DataFrame with proper schema enforcement.

    :param data: DataFrame, list of dictionaries, or rows containing the data
    :param target_schema: Target schema to enforce
    :param enforce_nullability: If True, filters out rows with NULLs in non-nullable columns
    :return: New DataFrame with proper schema, types, and nullability
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("No active SparkSession found")

    if isinstance(data, _DATAFRAME_TYPES):
        if target_schema is None:
            return cast(DataFrame, data)

        from pyspark.sql import functions as f

        df = cast(DataFrame, data)

        # Cast integers to long
        try:
            df = cast_integers_to_long(df)
        except Exception:
            pass

        select_exprs = []
        non_nullable_cols = []  # Track which columns should NOT be null

        for field in target_schema.fields:
            if field.name in df.columns:
                casted_col = f.col(field.name).cast(field.dataType)
                select_exprs.append(casted_col.alias(field.name))

                # Track non-nullable columns for later filtering
                if enforce_nullability and not field.nullable:
                    non_nullable_cols.append(field.name)
            else:
                select_exprs.append(f.lit(None).cast(field.dataType).alias(field.name))

                if enforce_nullability and not field.nullable:
                    raise ValueError(
                        f"Column '{field.name}' is missing but is non-nullable"
                    )

        # First, select and cast columns
        result_df = df.select(*select_exprs)

        # THEN apply NULL filtering on the transformed DataFrame
        if enforce_nullability and non_nullable_cols:
            from functools import reduce
            from operator import and_

            # Build filter expressions AFTER the select
            null_filters = [
                f.col(col_name).isNotNull() for col_name in non_nullable_cols
            ]
            combined_filter = reduce(and_, null_filters)
            result_df = result_df.filter(combined_filter)

        return result_df

    # Handle list input (existing logic)
    if target_schema is None:
        return spark.createDataFrame(data=cast(list, data))

    list_data = cast(list, data)
    if not list_data:
        return spark.createDataFrame([], target_schema)

    target_columns = [field.name for field in target_schema.fields]

    ordered_data = []
    for row in list_data:
        ordered_row = {col: row[col] for col in target_columns}
        ordered_data.append(ordered_row)

    return spark.createDataFrame(data=ordered_data, schema=target_schema)


def rename_columns(
    df: DataFrame, custom_mapping: dict[str, str] | None = None
) -> DataFrame:
    """
    Rename DataFrame columns using custom mapping and automatic camelCase to snake_case conversion.

    This function applies two types of column renaming:
    1. Custom mappings provided in the custom_mapping parameter
    2. Automatic camelCase to snake_case conversion for remaining columns

    :param df: Input DataFrame to rename columns for
    :param custom_mapping: Optional dictionary mapping old column names to new names
    :return: DataFrame with renamed columns
    """
    if custom_mapping is None:
        custom_mapping = {}

    # Get all column names
    all_columns = df.columns

    # Get automatic field mappings for camelCase to snake_case conversion
    field_mappings = get_field_mappings(all_columns)

    # Combine custom mapping with automatic field mappings
    # Custom mapping takes precedence over automatic conversion
    combined_mapping = {**field_mappings, **custom_mapping}

    # Apply column renaming
    renamed_df = df
    for old_name, new_name in combined_mapping.items():
        if old_name in renamed_df.columns:
            renamed_df = renamed_df.withColumnRenamed(old_name, new_name)

    return renamed_df
