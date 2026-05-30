"""
Schema validation and conversion utilities.

This module contains the ValidatedStruct class and related functions for
converting Pydantic models to PySpark schemas and handling schema validation.
"""

import sys

# Import UnionType for Python 3.10+ union syntax (int | None)
import types
from datetime import date, datetime
from enum import Enum
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _is_optional_type(python_type: Any) -> bool:
    """
    Check if a type is Optional (Union with None).

    Handles both typing.Union[T, None] and Python 3.10+ T | None syntax.

    :param python_type: The type to check
    :return: True if the type is Optional, False otherwise
    """
    origin = get_origin(python_type)

    # Handle typing.Union (e.g., Union[int, None])
    if origin is Union:
        args = get_args(python_type)
        return len(args) == 2 and type(None) in args

    # Handle Python 3.10+ union syntax (e.g., int | None)
    if sys.version_info >= (3, 10) and isinstance(python_type, types.UnionType):
        args = get_args(python_type)
        return len(args) == 2 and type(None) in args

    return False


def _python_type_to_spark_type(python_type: Any) -> DataType:
    """
    Convert Python/Pydantic types to PySpark DataType.

    :param python_type: The Python type to convert
    :return: Corresponding PySpark DataType
    """
    # Handle generic types (List, Optional, etc.)
    origin = get_origin(python_type)
    if origin is not None:
        args = get_args(python_type)

        # Handle List[T] -> ArrayType
        if origin is list:
            if len(args) == 1:
                element_type = args[0]
                element_spark_type = _python_type_to_spark_type(element_type)
                return ArrayType(element_spark_type, containsNull=True)
            else:
                # Fallback for malformed List type
                return ArrayType(StringType(), containsNull=True)

        # Handle Optional[T] (Union[T, None])
        elif len(args) == 2 and type(None) in args:
            # This is Optional[T], get the non-None type
            non_none_type = args[0] if args[1] is type(None) else args[1]
            return _python_type_to_spark_type(non_none_type)

    # Handle Python 3.10+ union syntax (e.g., int | None)
    if sys.version_info >= (3, 10) and isinstance(python_type, types.UnionType):
        args = get_args(python_type)
        if len(args) == 2 and type(None) in args:
            # This is T | None, get the non-None type
            non_none_type = args[0] if args[1] is type(None) else args[1]
            return _python_type_to_spark_type(non_none_type)

    # Handle basic types
    if python_type is str:
        return StringType()
    elif python_type is int:
        return LongType()
    elif python_type is float:
        return DoubleType()
    elif python_type is bool:
        return BooleanType()
    elif python_type is datetime:
        return TimestampType()
    elif python_type is date:
        return DateType()
    else:
        # Handle class types safely
        try:
            # Only call issubclass if python_type is actually a type/class
            if isinstance(python_type, type):
                if issubclass(python_type, Enum):
                    # Enums are stored as strings
                    return StringType()
                elif issubclass(python_type, BaseModel):
                    # Nested Pydantic models are converted to StructType recursively
                    return model_to_struct(python_type)
        except TypeError:
            # python_type is not a class, fall through to default
            pass

        # Default to StringType for unknown types
        return StringType()


def python_type_to_spark_type(python_type: Any) -> DataType:
    """Public wrapper around the private converter.

    Use this from other modules (e.g. the catalog generator) instead of
    importing ``_python_type_to_spark_type`` directly — the underscore form
    is module-private per the project's import-encapsulation rule.
    """
    return _python_type_to_spark_type(python_type)


def model_to_struct(model_class: type[BaseModel]) -> StructType:
    """
    Convert a Pydantic BaseModel class to a PySpark StructType.

    :param model_class: The Pydantic model class to convert
    :return: PySpark StructType representing the model schema
    """
    fields = []
    for field_name, field_info in model_class.model_fields.items():
        # Get the field type annotation
        field_type = field_info.annotation

        # Determine if the field is nullable
        # A field is nullable if it's Optional (Union with None) OR not required
        is_optional = _is_optional_type(field_type)
        is_nullable = is_optional or not field_info.is_required()

        # Convert Python type to Spark type
        spark_type = _python_type_to_spark_type(field_type)

        # Carry the pydantic Field(description=...) into the StructField
        # metadata so it round-trips through ``.jsonValue()`` into the
        # published contract, where cosmo and the Streamlit UI render it.
        metadata = (
            {"description": field_info.description} if field_info.description else None
        )

        # Create StructField
        fields.append(
            StructField(field_name, spark_type, is_nullable, metadata=metadata)
        )

    return StructType(fields)


class ValidatedStruct(BaseModel):
    """
    Base class for Pydantic models that can be converted to PySpark schemas
    and validated against DataFrames.
    """

    @classmethod
    def to_struct(cls) -> StructType:
        """Convert this model to a PySpark StructType."""
        return model_to_struct(cls)

    @classmethod
    def rules(cls) -> list[Any]:
        """
        Define validation rules for this model.
        Override this method to add custom validation rules.

        :return: List of ValidationRule instances
        """
        return []

    @classmethod
    def _get_automatic_rules(cls) -> list[Any]:
        """
        Generate automatic validation rules based on the model definition.
        This includes schema validation and enum validation.
        """
        from .rules import EnumRule, SchemaValidationRule

        automatic_rules: list[Any] = []

        # Add schema validation
        expected_schema = cls.to_struct()
        automatic_rules.append(
            SchemaValidationRule(expected_schema, allow_extra_columns=True)
        )

        # Add enum validation for enum fields
        for field_name, field_info in cls.model_fields.items():
            field_type = field_info.annotation

            # Handle Optional[EnumType] - both typing.Union and Python 3.10+ T | None
            origin = get_origin(field_type)
            is_union = origin is Union or (
                sys.version_info >= (3, 10) and isinstance(field_type, types.UnionType)
            )
            if is_union:
                args = get_args(field_type)
                if len(args) == 2 and type(None) in args:
                    # This is Optional[T], get the non-None type
                    non_none_type = args[0] if args[1] is type(None) else args[1]
                    field_type = non_none_type

            # Check if field is an Enum
            try:
                if isinstance(field_type, type) and issubclass(field_type, Enum):
                    automatic_rules.append(EnumRule(field_name, field_type))
            except TypeError:
                # field_type is not a class, skip
                pass

        return automatic_rules

    @classmethod
    def verify(
        cls, df: Any, strict: bool = True, include_automatic_rules: bool = True
    ) -> Any:
        """
        Verify DataFrame against validation rules.

        :param df: DataFrame to validate
        :param strict: If True, raises exception on validation failure. If False, logs warnings.
        :param include_automatic_rules: If True, includes automatic schema and enum validation.
        :return: The original DataFrame
        """
        from .validators import verify

        # Combine user-defined rules with automatic rules
        all_rules = cls.rules()
        if include_automatic_rules:
            all_rules = cls._get_automatic_rules() + all_rules

        results = verify(df, all_rules)
        if not results["valid"]:
            if strict:
                raise ValueError(
                    f"DataFrame validation failed for {cls.__name__}: {results['errors']}"
                )
            else:
                # Log warnings instead of failing
                print(
                    f"WARNING: DataFrame validation issues for {cls.__name__}: {results['errors']}"
                )
        return df

    @classmethod
    def get_field_names(cls) -> dict[str, str]:
        """
        Get a dictionary mapping field names to themselves.
        Useful for avoiding string literals when referencing columns.

        :return: Dictionary with field names as both keys and values
        """
        return {field_name: field_name for field_name in cls.model_fields.keys()}

    @classmethod
    def create_fields_class(cls) -> type:
        """
        Create a Fields class with field name constants.
        Call this method to get a class with uppercase constants for each field.

        :return: Fields class with uppercase constants for each field
        """
        fields_dict = {}
        for field_name in cls.model_fields.keys():
            # Convert field_name to UPPER_CASE constant name
            const_name = field_name.upper()
            fields_dict[const_name] = field_name

        return type("Fields", (), fields_dict)


def mock_model(
    model_class: type[BaseModel], data: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Mocks a Pydantic model with default values, providing valid enum values when needed.

    :param model_class: The Pydantic model class to mock
    :param data: Optional data to override defaults
    :return: A dictionary with mocked data that can be used to create the model
    """
    if data is None:
        data = {}

    mocked_data = {}

    def get_default_value(field_type: Any, field_info: Any) -> Any:
        """Get a sensible default value for a field type"""
        # Handle Optional types
        origin = get_origin(field_type)
        if origin is not None:
            args = get_args(field_type)

            # Handle List[T]
            if origin is list:
                if len(args) == 1:
                    # Return empty list for now, could be enhanced to generate sample data
                    return []
                return []

            # Handle Optional[T]
            elif len(args) == 2 and type(None) in args:
                # For optional fields, return None unless there's a default
                from pydantic_core import PydanticUndefined

                if field_info.default is not PydanticUndefined:
                    return field_info.default
                return None

        # Handle basic types
        if field_type is str:
            return "test_string"
        elif field_type is int:
            return 42
        elif field_type is float:
            return 3.14
        elif field_type is bool:
            return True
        elif field_type is datetime:
            return datetime.now()
        elif field_type is date:
            return datetime.now().date()
        else:
            # Handle class types
            try:
                if issubclass(field_type, Enum):
                    # Return the first enum value as a string for Spark compatibility
                    return list(field_type)[0].value
                elif issubclass(field_type, BaseModel):
                    # Recursively mock nested models
                    return mock_model(field_type)
            except TypeError:
                pass

            return None

    for field_name, field_info in model_class.model_fields.items():
        field_type = field_info.annotation

        if field_name in data:
            mocked_data[field_name] = data[field_name]
        else:
            # Check if field has a default value (not PydanticUndefined)
            from pydantic_core import PydanticUndefined

            if field_info.default is not PydanticUndefined:
                mocked_data[field_name] = field_info.default
            elif _is_optional_type(field_type):
                # For Optional fields without explicit defaults, use None
                mocked_data[field_name] = None
            else:
                mocked_data[field_name] = get_default_value(field_type, field_info)

    return mocked_data
