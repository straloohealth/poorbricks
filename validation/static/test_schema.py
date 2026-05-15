"""
Tests for schema validation and ValidatedStruct.
"""

from datetime import date, datetime
from enum import Enum
from typing import Any, Optional

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from utils.dataframes import create_dataframe
from validation.rules import NotNullRule, StringLengthRule
from validation.schema import ValidatedStruct, _is_optional_type, model_to_struct


class TestIsOptionalType:
    """Test cases for the _is_optional_type function"""

    def test_is_optional_type_with_typing_union(self) -> None:
        """Test _is_optional_type with typing.Union syntax"""
        from typing import Union

        # Test Union[T, None] syntax — intentionally using legacy Union form as runtime values
        assert _is_optional_type(Union[int, None]) is True  # noqa: UP007
        assert _is_optional_type(Union[str, None]) is True  # noqa: UP007
        assert _is_optional_type(Union[float, None]) is True  # noqa: UP007

        # Test Optional[T] syntax (which is Union[T, None])
        assert _is_optional_type(Optional[int]) is True  # noqa: UP007, UP045
        assert _is_optional_type(Optional[str]) is True  # noqa: UP007, UP045
        assert _is_optional_type(Optional[bool]) is True  # noqa: UP007, UP045

        # Test non-optional Union types
        assert _is_optional_type(Union[int, str]) is False  # noqa: UP007
        assert _is_optional_type(Union[int, str, float]) is False  # noqa: UP007

    def test_is_optional_type_with_union_operator(self) -> None:
        """Test _is_optional_type with Python 3.10+ union operator syntax"""
        # Test T | None syntax
        assert _is_optional_type(int | None) is True
        assert _is_optional_type(str | None) is True
        assert _is_optional_type(float | None) is True
        assert _is_optional_type(bool | None) is True

        # Test non-optional union types
        assert _is_optional_type(int | str) is False

    def test_is_optional_type_with_non_optional_types(self) -> None:
        """Test _is_optional_type with non-optional types"""
        # Test basic types
        assert _is_optional_type(int) is False
        assert _is_optional_type(str) is False
        assert _is_optional_type(float) is False
        assert _is_optional_type(bool) is False
        assert _is_optional_type(datetime) is False
        assert _is_optional_type(date) is False

        # Test complex types
        assert _is_optional_type(list[str]) is False
        assert _is_optional_type(dict[str, int]) is False


class TestModelToStruct:
    """Test cases for the model_to_struct function from schema module"""

    def test_model_to_struct_basic_types(self) -> None:
        """Test model_to_struct with basic Python types"""

        class SimpleModel(ValidatedStruct):
            id: str
            name: str
            age: int
            height: float
            is_active: bool

        result_schema = model_to_struct(SimpleModel)

        # Check that we have the correct number of fields
        assert len(result_schema.fields) == 5

        # Check field names and types
        fields_by_name = {field.name: field for field in result_schema.fields}

        assert "id" in fields_by_name
        assert isinstance(fields_by_name["id"].dataType, StringType)

        assert "name" in fields_by_name
        assert isinstance(fields_by_name["name"].dataType, StringType)

        assert "age" in fields_by_name
        assert isinstance(fields_by_name["age"].dataType, LongType)

        assert "height" in fields_by_name
        assert isinstance(fields_by_name["height"].dataType, DoubleType)

        assert "is_active" in fields_by_name
        assert isinstance(fields_by_name["is_active"].dataType, BooleanType)

    def test_model_to_struct_datetime_types(self) -> None:
        """Test model_to_struct with datetime types"""

        class DateTimeModel(ValidatedStruct):
            created_at: datetime
            birth_date: date

        result_schema = model_to_struct(DateTimeModel)

        fields_by_name = {field.name: field for field in result_schema.fields}

        assert "created_at" in fields_by_name
        assert isinstance(fields_by_name["created_at"].dataType, TimestampType)

        assert "birth_date" in fields_by_name
        assert isinstance(fields_by_name["birth_date"].dataType, DateType)

    def test_model_to_struct_with_enums(self) -> None:
        """Test model_to_struct with enum types"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        class Priority(Enum):
            HIGH = 1
            MEDIUM = 2
            LOW = 3

        class ModelWithEnums(ValidatedStruct):
            status: Status
            priority: Priority

        result_schema = model_to_struct(ModelWithEnums)

        fields_by_name = {field.name: field for field in result_schema.fields}

        # Enums should be converted to StringType
        assert "status" in fields_by_name
        assert isinstance(fields_by_name["status"].dataType, StringType)

        assert "priority" in fields_by_name
        assert isinstance(fields_by_name["priority"].dataType, StringType)

    def test_model_to_struct_with_optional_fields(self) -> None:
        """Test model_to_struct with Optional fields"""

        class ModelWithOptional(ValidatedStruct):
            required_field: str
            optional_field: str | None = None
            optional_with_default: int | None = 42

        result_schema = model_to_struct(ModelWithOptional)

        fields_by_name = {field.name: field for field in result_schema.fields}

        # Required field should not be nullable
        assert "required_field" in fields_by_name
        assert not fields_by_name["required_field"].nullable

        # Optional field should be nullable
        assert "optional_field" in fields_by_name
        assert fields_by_name["optional_field"].nullable

        # Optional field with default should be nullable
        assert "optional_with_default" in fields_by_name
        assert fields_by_name["optional_with_default"].nullable

    def test_model_to_struct_with_union_syntax_optional_fields(self) -> None:
        """Test model_to_struct with Python 3.10+ union syntax (T | None)"""

        class ModelWithUnionOptional(ValidatedStruct):
            required_field: str
            optional_int_field: int | None = None
            optional_str_field: str | None = None
            optional_with_default: float | None = 3.14

        result_schema = model_to_struct(ModelWithUnionOptional)

        fields_by_name = {field.name: field for field in result_schema.fields}

        # Required field should not be nullable
        assert "required_field" in fields_by_name
        assert not fields_by_name["required_field"].nullable

        # Union optional fields should be nullable
        assert "optional_int_field" in fields_by_name
        assert fields_by_name["optional_int_field"].nullable
        assert isinstance(fields_by_name["optional_int_field"].dataType, LongType)

        assert "optional_str_field" in fields_by_name
        assert fields_by_name["optional_str_field"].nullable
        assert isinstance(fields_by_name["optional_str_field"].dataType, StringType)

        # Optional field with default should be nullable
        assert "optional_with_default" in fields_by_name
        assert fields_by_name["optional_with_default"].nullable
        assert isinstance(fields_by_name["optional_with_default"].dataType, DoubleType)

    def test_model_to_struct_message_model(self) -> None:
        """Test model_to_struct with the actual Message model from the project"""
        pytest.skip("legacy module not available")

    def test_model_to_struct_empty_model(self) -> None:
        """Test model_to_struct with an empty model"""

        class EmptyModel(ValidatedStruct):
            pass

        result_schema = model_to_struct(EmptyModel)

        # Should return empty schema
        assert len(result_schema.fields) == 0
        assert isinstance(result_schema, StructType)

    def test_model_to_struct_complex_model(self) -> None:
        """Test model_to_struct with a complex model containing various types"""

        class ComplexModel(ValidatedStruct):
            id: str
            count: int
            rate: float
            enabled: bool
            created_at: datetime
            updated_date: date
            optional_name: str | None = None
            status: str | None = "active"

        result_schema = model_to_struct(ComplexModel)

        # Check that we have the correct number of fields
        assert len(result_schema.fields) == 8

        fields_by_name = {field.name: field for field in result_schema.fields}

        # Check required fields are not nullable
        assert not fields_by_name["id"].nullable
        assert not fields_by_name["count"].nullable
        assert not fields_by_name["rate"].nullable
        assert not fields_by_name["enabled"].nullable
        assert not fields_by_name["created_at"].nullable
        assert not fields_by_name["updated_date"].nullable

        # Check optional fields are nullable
        assert fields_by_name["optional_name"].nullable
        assert fields_by_name["status"].nullable

    def test_model_to_struct_with_nested_models(self) -> None:
        """Test model_to_struct with nested Pydantic models"""

        class Address(ValidatedStruct):
            street: str
            city: str
            zip_code: str
            country: str | None = "USA"

        class Person(ValidatedStruct):
            name: str
            age: int
            address: Address
            created_at: datetime

        result_schema = model_to_struct(Person)

        # Check that we have the correct number of fields
        assert len(result_schema.fields) == 4

        fields_by_name = {field.name: field for field in result_schema.fields}

        # Check basic fields
        assert isinstance(fields_by_name["name"].dataType, StringType)
        assert isinstance(fields_by_name["age"].dataType, LongType)
        assert isinstance(fields_by_name["created_at"].dataType, TimestampType)

        # Check nested model field
        assert "address" in fields_by_name
        address_field = fields_by_name["address"]
        assert isinstance(address_field.dataType, StructType)
        assert not address_field.nullable  # Required nested model

        # Check nested model structure
        nested_fields = {field.name: field for field in address_field.dataType.fields}
        assert len(nested_fields) == 4

        # Check nested field types and nullability
        assert isinstance(nested_fields["street"].dataType, StringType)
        assert not nested_fields["street"].nullable

        assert isinstance(nested_fields["city"].dataType, StringType)
        assert not nested_fields["city"].nullable

        assert isinstance(nested_fields["zip_code"].dataType, StringType)
        assert not nested_fields["zip_code"].nullable

        assert isinstance(nested_fields["country"].dataType, StringType)
        assert nested_fields["country"].nullable  # Optional field with default

    def test_model_to_struct_with_list_of_primitives(self) -> None:
        """Test model_to_struct with lists of primitive types"""

        class ModelWithLists(ValidatedStruct):
            tags: list[str]
            scores: list[int]
            rates: list[float]
            flags: list[bool]

        result_schema = model_to_struct(ModelWithLists)

        fields_by_name = {field.name: field for field in result_schema.fields}

        # Check string list
        tags_field = fields_by_name["tags"]
        assert isinstance(tags_field.dataType, ArrayType)
        assert isinstance(tags_field.dataType.elementType, StringType)
        assert tags_field.dataType.containsNull
        assert not tags_field.nullable  # Required field

        # Check int list
        scores_field = fields_by_name["scores"]
        assert isinstance(scores_field.dataType, ArrayType)
        assert isinstance(scores_field.dataType.elementType, LongType)

        # Check float list
        rates_field = fields_by_name["rates"]
        assert isinstance(rates_field.dataType, ArrayType)
        assert isinstance(rates_field.dataType.elementType, DoubleType)

        # Check bool list
        flags_field = fields_by_name["flags"]
        assert isinstance(flags_field.dataType, ArrayType)
        assert isinstance(flags_field.dataType.elementType, BooleanType)


class TestValidatedStructEnhancedVerify:
    """Test cases for the enhanced ValidatedStruct.verify method with automatic validation"""

    @pytest.mark.spark
    def test_automatic_enum_validation_valid(self, spark: SparkSession) -> None:
        """Test automatic enum validation with valid data"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        class TestModel(ValidatedStruct):
            id: str
            status: Status

        test_data = [
            {"id": "1", "status": "active"},
            {"id": "2", "status": "inactive"},
        ]
        df = create_dataframe(test_data, TestModel.to_struct())

        # Should not raise an exception
        result_df = TestModel.verify(df)
        assert result_df is df

    @pytest.mark.spark
    def test_automatic_enum_validation_invalid(self, spark: SparkSession) -> None:
        """Test automatic enum validation with invalid data"""

        class Priority(Enum):
            HIGH = "high"
            MEDIUM = "medium"
            LOW = "low"

        class TestModel(ValidatedStruct):
            id: str
            priority: Priority

        test_data = [
            {"id": "1", "priority": "high"},  # valid
            {"id": "2", "priority": "invalid"},  # invalid
        ]
        df = create_dataframe(test_data, TestModel.to_struct())

        with pytest.raises(ValueError) as exc_info:
            TestModel.verify(df)

        error_msg = str(exc_info.value)
        assert "DataFrame validation failed for TestModel" in error_msg
        assert "invalid Priority values" in error_msg

    @pytest.mark.spark
    def test_automatic_schema_validation(self, spark: SparkSession) -> None:
        """Test automatic schema validation"""

        class TestModel(ValidatedStruct):
            id: str
            name: str
            age: int

        # Create DataFrame missing required column
        test_data = [{"id": "1", "name": "John"}]  # Missing 'age'
        df = spark.createDataFrame(test_data)

        with pytest.raises(ValueError) as exc_info:
            TestModel.verify(df)

        error_msg = str(exc_info.value)
        assert "DataFrame validation failed for TestModel" in error_msg
        assert "Missing required columns" in error_msg

    @pytest.mark.spark
    def test_automatic_validation_with_optional_enum(self, spark: SparkSession) -> None:
        """Test automatic validation with optional enum fields"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        class TestModel(ValidatedStruct):
            id: str
            status: Status | None = None

        test_data: list[dict[str, Any]] = [
            {"id": "1", "status": "active"},  # valid
            {"id": "2", "status": None},  # valid (null for optional)
            {"id": "3", "status": "invalid"},  # invalid
        ]
        df = create_dataframe(test_data, TestModel.to_struct())

        with pytest.raises(ValueError) as exc_info:
            TestModel.verify(df)

        error_msg = str(exc_info.value)
        assert "invalid Status values" in error_msg
        # Should only report 1 invalid row (not the null one)
        assert "1 rows contain invalid" in error_msg

    @pytest.mark.spark
    def test_combined_automatic_and_custom_rules_enum_error(
        self, spark: SparkSession
    ) -> None:
        """Test combining automatic validation with custom rules - enum error case"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        class TestModel(ValidatedStruct):
            id: str
            name: str
            status: Status

            @classmethod
            def rules(cls) -> list[Any]:
                return [
                    NotNullRule("id"),
                    StringLengthRule("name", min_length=2, max_length=10),
                ]

        # Test data with invalid enum value
        test_data = [
            {"id": "1", "name": "John", "status": "invalid"},  # invalid status
        ]
        df = create_dataframe(test_data, TestModel.to_struct())

        with pytest.raises(ValueError) as exc_info:
            TestModel.verify(df)

        error_msg = str(exc_info.value)
        assert "invalid Status values" in error_msg  # From automatic EnumRule

    @pytest.mark.spark
    def test_combined_automatic_and_custom_rules_custom_error(
        self, spark: SparkSession
    ) -> None:
        """Test combining automatic validation with custom rules - custom rule error case"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        class TestModel(ValidatedStruct):
            id: str
            name: str
            status: Status

            @classmethod
            def rules(cls) -> list[Any]:
                return [
                    NotNullRule("id"),
                    StringLengthRule("name", min_length=2, max_length=10),
                ]

        # Test data with valid enum but invalid string length
        test_data = [
            {"id": "1", "name": "J", "status": "active"},  # name too short
        ]
        df = create_dataframe(test_data, TestModel.to_struct())

        with pytest.raises(ValueError) as exc_info:
            TestModel.verify(df)

        error_msg = str(exc_info.value)
        assert "length <" in error_msg  # From StringLengthRule

    @pytest.mark.spark
    def test_verify_with_non_strict_mode(self, spark: SparkSession) -> None:
        """Test verify method with non-strict mode (warnings instead of exceptions)"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        class TestModel(ValidatedStruct):
            id: str
            status: Status

        test_data = [
            {"id": "1", "status": "invalid"},  # invalid enum value
        ]
        df = create_dataframe(test_data, TestModel.to_struct())

        # Should not raise exception in non-strict mode
        import io
        import sys

        captured_output = io.StringIO()
        sys.stdout = captured_output

        try:
            result_df = TestModel.verify(df, strict=False)
            assert result_df is df
        finally:
            sys.stdout = sys.__stdout__

        output = captured_output.getvalue()
        assert "WARNING: DataFrame validation issues for TestModel" in output

    @pytest.mark.spark
    def test_verify_disable_automatic_rules(self, spark: SparkSession) -> None:
        """Test verify method with automatic rules disabled"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        class TestModel(ValidatedStruct):
            id: str
            status: Status

            @classmethod
            def rules(cls) -> list[Any]:
                return [NotNullRule("id")]

        test_data = [
            {
                "id": "1",
                "status": "invalid",
            },  # invalid enum but automatic rules disabled
        ]
        df = create_dataframe(test_data, TestModel.to_struct())

        # Should not raise exception when automatic rules are disabled
        result_df = TestModel.verify(df, include_automatic_rules=False)
        assert result_df is df

    @pytest.mark.spark
    def test_verify_multiple_enum_fields(self, spark: SparkSession) -> None:
        """Test verify method with multiple enum fields"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        class Priority(Enum):
            HIGH = "high"
            MEDIUM = "medium"
            LOW = "low"

        class TestModel(ValidatedStruct):
            id: str
            status: Status
            priority: Priority

        test_data = [
            {"id": "1", "status": "active", "priority": "high"},  # valid
            {"id": "2", "status": "invalid", "priority": "urgent"},  # both invalid
        ]
        df = create_dataframe(test_data, TestModel.to_struct())

        with pytest.raises(ValueError) as exc_info:
            TestModel.verify(df)

        error_msg = str(exc_info.value)
        # Should have validation errors for both enum fields
        assert "invalid Status values" in error_msg
        assert "invalid Priority values" in error_msg

    @pytest.mark.spark
    def test_verify_with_real_message_model(self, spark: SparkSession) -> None:
        """Test verify method with the real Message model from the project"""
        pytest.skip("legacy module not available")

    @pytest.mark.spark
    def test_verify_with_real_message_model_invalid_data(
        self, spark: SparkSession
    ) -> None:
        """Test verify method with the real Message model with invalid enum values"""
        pytest.skip("legacy module not available")
