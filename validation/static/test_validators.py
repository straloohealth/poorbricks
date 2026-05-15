"""
Tests for validation decorators and utilities.

This module contains tests for the @verify_with_model decorator and related validation functionality.
"""

from datetime import datetime
from enum import Enum

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from utils.dataframes import create_dataframe
from validation import (
    NotNullRule,
    StringLengthRule,
    ValidatedStruct,
    ValidationRule,
    mock_model,
    validate_no_integer_types,
    verify_with_model,
)


class TestSpeaker(Enum):
    PATIENT = "PATIENT"
    NAVIGATOR = "NAVIGATOR"


class TestMessage(ValidatedStruct):
    """Test model for validation decorator tests."""

    id: str
    speaker: TestSpeaker
    created_at: datetime

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [
            NotNullRule(column="id"),
            NotNullRule(column="speaker"),
            NotNullRule(column="created_at"),
            StringLengthRule(column="id", min_length=1, max_length=255),
        ]


class TestMessageWithOptionalFields(ValidatedStruct):
    """Test model with optional fields for testing null validation."""

    id: str | None
    speaker: TestSpeaker
    created_at: datetime

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [
            NotNullRule(column="id"),  # This will fail if id is null
            NotNullRule(column="speaker"),
            NotNullRule(column="created_at"),
        ]


class TestValidationDecorator:
    """Test class for @verify_with_model decorator."""

    @pytest.mark.spark
    def test_verify_decorator_with_valid_data(self, spark: SparkSession) -> None:
        """Test that @verify_with_model decorator passes with valid data."""

        @verify_with_model(model=TestMessage)  # strict=True by default
        def test_function() -> DataFrame:
            test_data = [
                mock_model(TestMessage, {"id": "msg1"}),
                mock_model(TestMessage, {"id": "msg2"}),
            ]
            return create_dataframe(
                data=test_data, target_schema=TestMessage.to_struct()
            )

        # Should not raise any exception
        result_df = test_function()
        assert result_df.count() == 2

        # Verify schema matches
        expected_schema = TestMessage.to_struct()
        assert result_df.schema == expected_schema

    @pytest.mark.spark
    def test_verify_decorator_with_invalid_data_always_strict(
        self, spark: SparkSession
    ) -> None:
        """Test that @verify_with_model decorator always fails with invalid data (always strict)."""

        @verify_with_model(model=TestMessageWithOptionalFields)
        def test_function() -> DataFrame:
            # Create data with null values that violate NotNullRule
            invalid_data = [
                {"id": None, "speaker": "PATIENT", "created_at": datetime.now()},
                {"id": "msg2", "speaker": "PATIENT", "created_at": datetime.now()},
            ]
            return create_dataframe(
                data=invalid_data,
                target_schema=TestMessageWithOptionalFields.to_struct(),
            )

        # Should now raise exception since strict=True is always used
        with pytest.raises(ValueError, match="DataFrame validation failed"):
            test_function()

    @pytest.mark.spark
    def test_verify_decorator_with_invalid_data_strict_duplicate(
        self, spark: SparkSession
    ) -> None:
        """Test that @verify_with_model decorator raises exception (duplicate test, can be removed)."""

        @verify_with_model(model=TestMessageWithOptionalFields)
        def test_function() -> DataFrame:
            # Create data with null values that violate NotNullRule
            invalid_data = [
                {"id": None, "speaker": "PATIENT", "created_at": datetime.now()},
            ]
            return create_dataframe(
                data=invalid_data,
                target_schema=TestMessageWithOptionalFields.to_struct(),
            )

        # Should raise ValueError (validation is always strict now)
        with pytest.raises(ValueError, match="DataFrame validation failed"):
            test_function()

    @pytest.mark.spark
    def test_verify_decorator_preserves_function_metadata(
        self, spark: SparkSession
    ) -> None:
        """Test that @verify_with_model decorator preserves function metadata."""

        @verify_with_model(model=TestMessage)
        def test_function_with_docstring() -> DataFrame:
            """This is a test function with a docstring."""
            test_data = [mock_model(TestMessage)]
            return create_dataframe(
                data=test_data, target_schema=TestMessage.to_struct()
            )

        # Verify that function name and docstring are preserved
        assert test_function_with_docstring.__name__ == "test_function_with_docstring"
        assert (
            test_function_with_docstring.__doc__
            == "This is a test function with a docstring."
        )

    @pytest.mark.spark
    def test_verify_decorator_with_arguments(self, spark: SparkSession) -> None:
        """Test that @verify_with_model decorator works with functions that take arguments."""

        @verify_with_model(model=TestMessage)
        def test_function_with_args(
            message_count: int, prefix: str = "msg"
        ) -> DataFrame:
            test_data = [
                mock_model(TestMessage, {"id": f"{prefix}{i}"})
                for i in range(message_count)
            ]
            return create_dataframe(
                data=test_data, target_schema=TestMessage.to_struct()
            )

        # Test with positional and keyword arguments
        result_df = test_function_with_args(3, prefix="test_")
        assert result_df.count() == 3

        # Verify the data contains our custom prefix
        ids = [row.id for row in result_df.collect()]
        assert all(id_val.startswith("test_") for id_val in ids)

    @pytest.mark.spark
    def test_verify_decorator_without_automatic_rules(
        self, spark: SparkSession
    ) -> None:
        """Test @verify_with_model decorator with automatic rules disabled."""

        @verify_with_model(model=TestMessage, include_automatic_rules=False)
        def test_function() -> DataFrame:
            # Create data that would fail automatic schema validation
            # but should pass because automatic rules are disabled
            test_data = [mock_model(TestMessage)]
            return create_dataframe(
                data=test_data, target_schema=TestMessage.to_struct()
            )

        # Should not raise any exception even with schema mismatches
        # when automatic rules are disabled
        result_df = test_function()
        assert result_df.count() == 1

    @pytest.mark.spark
    def test_verify_decorator_empty_dataframe(self, spark: SparkSession) -> None:
        """Test @verify_with_model decorator with empty DataFrame."""

        @verify_with_model(model=TestMessage)
        def test_function() -> DataFrame:
            return create_dataframe(data=[], target_schema=TestMessage.to_struct())

        # Should handle empty DataFrame gracefully
        result_df = test_function()
        assert result_df.count() == 0
        assert result_df.schema == TestMessage.to_struct()

    def test_verify_decorator_parameters(self) -> None:
        """Test that @verify_with_model decorator accepts correct parameters."""

        # Test with all parameters
        decorator = verify_with_model(model=TestMessage, include_automatic_rules=False)
        assert callable(decorator)

        # Test with minimal parameters
        decorator_minimal = verify_with_model(model=TestMessage)
        assert callable(decorator_minimal)

    @pytest.mark.spark
    def test_verify_decorator_always_strict(self, spark: SparkSession) -> None:
        """Test that @verify_with_model decorator always uses strict validation."""

        @verify_with_model(model=TestMessageWithOptionalFields)
        def test_function() -> DataFrame:
            # Create data with null values that violate NotNullRule
            invalid_data = [
                {"id": None, "speaker": "PATIENT", "created_at": datetime.now()},
            ]
            return create_dataframe(
                data=invalid_data,
                target_schema=TestMessageWithOptionalFields.to_struct(),
            )

        # Should raise ValueError because validation is always strict
        with pytest.raises(ValueError, match="DataFrame validation failed"):
            test_function()

    @pytest.mark.spark
    def test_decorator_integration_with_mock_dlt_table(
        self, spark: SparkSession
    ) -> None:
        """Test integration pattern similar to how it would be used with DLT tables."""

        # Mock the DLT table decorator behavior
        def mock_dlt_table(name: str, schema: StructType, **kwargs):
            def decorator(func):
                # Store metadata like real DLT would
                func._dlt_name = name
                func._dlt_schema = schema
                func._dlt_kwargs = kwargs
                return func

            return decorator

        @verify_with_model(model=TestMessage)
        @mock_dlt_table(
            name="test_messages",
            schema=TestMessage.to_struct(),
            comment="Test messages table",
        )
        def messages_table() -> DataFrame:
            """Test DLT table function."""
            test_data = [
                mock_model(TestMessage, {"id": "msg1"}),
                mock_model(TestMessage, {"id": "msg2"}),
            ]
            return create_dataframe(
                data=test_data, target_schema=TestMessage.to_struct()
            )

        # Test that both decorators work together
        result_df = messages_table()
        assert result_df.count() == 2

        # Verify DLT metadata is preserved
        assert messages_table._dlt_name == "test_messages"
        assert messages_table._dlt_schema == TestMessage.to_struct()
        assert messages_table.__doc__ == "Test DLT table function."


class TestIntegerTypeValidation:
    """Test class for IntegerType validation functionality."""

    @pytest.mark.spark
    def test_validate_no_integer_types_with_valid_schema(
        self, spark: SparkSession
    ) -> None:
        """Test validate_no_integer_types with a schema containing only LongType."""
        from pyspark.sql.types import LongType

        # Create DataFrame with LongType (valid)
        valid_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("count", LongType(), False),
                StructField("optional_count", LongType(), True),
            ]
        )

        test_data = [{"id": "test1", "count": 10, "optional_count": 5}]
        df = spark.createDataFrame(test_data, valid_schema)

        # Should not raise any exception
        validate_no_integer_types(df)

    @pytest.mark.spark
    def test_validate_no_integer_types_with_integer_fields(
        self, spark: SparkSession
    ) -> None:
        """Test validate_no_integer_types with IntegerType fields (should fail)."""
        from pyspark.sql.types import IntegerType

        # Create DataFrame with IntegerType (invalid)
        invalid_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("count", IntegerType(), False),  # This should cause failure
                StructField(
                    "score", IntegerType(), True
                ),  # This should also cause failure
            ]
        )

        test_data = [{"id": "test1", "count": 10, "score": 85}]
        df = spark.createDataFrame(test_data, invalid_schema)

        # Should raise ValueError
        with pytest.raises(ValueError, match="DataFrame contains IntegerType fields"):
            validate_no_integer_types(df)

        # Verify error message contains field names
        try:
            validate_no_integer_types(df)
        except ValueError as e:
            error_msg = str(e)
            assert "count" in error_msg
            assert "score" in error_msg
            assert "cast_integers_to_long()" in error_msg

    @pytest.mark.spark
    def test_validate_no_integer_types_with_array_integer_elements(
        self, spark: SparkSession
    ) -> None:
        """Test validate_no_integer_types with arrays containing IntegerType elements."""
        from pyspark.sql.types import ArrayType, IntegerType

        # Create DataFrame with array of IntegerType (invalid)
        invalid_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField(
                    "scores", ArrayType(IntegerType(), True), False
                ),  # Invalid array elements
            ]
        )

        test_data = [{"id": "test1", "scores": [1, 2, 3]}]
        df = spark.createDataFrame(test_data, invalid_schema)

        # Should raise ValueError
        with pytest.raises(ValueError, match="DataFrame contains IntegerType fields"):
            validate_no_integer_types(df)

        # Verify error message contains array notation
        try:
            validate_no_integer_types(df)
        except ValueError as e:
            error_msg = str(e)
            assert "scores[*]" in error_msg

    @pytest.mark.spark
    def test_validate_no_integer_types_with_valid_array_elements(
        self, spark: SparkSession
    ) -> None:
        """Test validate_no_integer_types with arrays containing LongType elements (valid)."""
        from pyspark.sql.types import ArrayType, LongType

        # Create DataFrame with array of LongType (valid)
        valid_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField(
                    "scores", ArrayType(LongType(), True), False
                ),  # Valid array elements
            ]
        )

        test_data = [{"id": "test1", "scores": [1, 2, 3]}]
        df = spark.createDataFrame(test_data, valid_schema)

        # Should not raise any exception
        validate_no_integer_types(df)

    @pytest.mark.spark
    def test_validate_no_integer_types_with_nested_struct(
        self, spark: SparkSession
    ) -> None:
        """Test validate_no_integer_types with nested structs containing IntegerType."""
        from pyspark.sql.types import IntegerType, LongType

        # Create nested struct with IntegerType (invalid)
        nested_struct = StructType(
            [
                StructField(
                    "nested_count", IntegerType(), False
                ),  # Invalid nested field
                StructField("nested_score", LongType(), False),  # Valid nested field
            ]
        )

        invalid_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("metadata", nested_struct, False),
            ]
        )

        test_data = [
            {"id": "test1", "metadata": {"nested_count": 5, "nested_score": 100}}
        ]
        df = spark.createDataFrame(test_data, invalid_schema)

        # Should raise ValueError
        with pytest.raises(ValueError, match="DataFrame contains IntegerType fields"):
            validate_no_integer_types(df)

        # Verify error message contains nested path
        try:
            validate_no_integer_types(df)
        except ValueError as e:
            error_msg = str(e)
            assert "metadata.nested_count" in error_msg

    @pytest.mark.spark
    def test_validate_no_integer_types_with_array_of_nested_structs(
        self, spark: SparkSession
    ) -> None:
        """Test validate_no_integer_types with arrays of structs containing IntegerType."""
        from pyspark.sql.types import ArrayType, IntegerType, LongType

        # Create array of structs with IntegerType (invalid)
        nested_struct = StructType(
            [
                StructField(
                    "item_count", IntegerType(), False
                ),  # Invalid nested field in array
                StructField(
                    "item_score", LongType(), False
                ),  # Valid nested field in array
            ]
        )

        invalid_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("items", ArrayType(nested_struct, True), False),
            ]
        )

        test_data = [{"id": "test1", "items": [{"item_count": 1, "item_score": 100}]}]
        df = spark.createDataFrame(test_data, invalid_schema)

        # Should raise ValueError
        with pytest.raises(ValueError, match="DataFrame contains IntegerType fields"):
            validate_no_integer_types(df)

        # Verify error message contains nested array path
        try:
            validate_no_integer_types(df)
        except ValueError as e:
            error_msg = str(e)
            assert "items[*].item_count" in error_msg

    @pytest.mark.spark
    def test_verify_with_model_decorator_catches_integer_types(
        self, spark: SparkSession
    ) -> None:
        """Test that @verify_with_model decorator automatically catches IntegerType usage."""
        from pyspark.sql.types import IntegerType

        @verify_with_model(model=TestMessage)
        def test_function_with_integer_type() -> DataFrame:
            # Create data with IntegerType schema (this should fail)
            problematic_schema = StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("speaker", StringType(), False),
                    StructField("created_at", TimestampType(), False),
                    StructField(
                        "bad_count", IntegerType(), False
                    ),  # This should cause failure
                ]
            )

            test_data = [
                {
                    "id": "msg1",
                    "speaker": "PATIENT",
                    "created_at": datetime.now(),
                    "bad_count": 42,
                }
            ]
            return spark.createDataFrame(test_data, problematic_schema)

        # Should raise ValueError due to IntegerType validation
        with pytest.raises(ValueError, match="DataFrame contains IntegerType fields"):
            test_function_with_integer_type()

    @pytest.mark.spark
    def test_verify_with_model_decorator_passes_with_long_types(
        self, spark: SparkSession
    ) -> None:
        """Test that @verify_with_model decorator passes when using LongType."""

        @verify_with_model(model=TestMessage)
        def test_function_with_long_type() -> DataFrame:
            # Create valid data using create_dataframe (which ensures proper types)
            test_data = [mock_model(TestMessage, {"id": "msg1"})]
            return create_dataframe(
                data=test_data, target_schema=TestMessage.to_struct()
            )

        # Should not raise any exception
        result_df = test_function_with_long_type()
        assert result_df.count() == 1

    @pytest.mark.spark
    def test_validate_no_integer_types_comprehensive_error_message(
        self, spark: SparkSession
    ) -> None:
        """Test that validate_no_integer_types provides comprehensive error messages."""
        from pyspark.sql.types import ArrayType, IntegerType, LongType

        # Create a complex schema with multiple IntegerType violations
        nested_struct = StructType(
            [
                StructField("nested_int", IntegerType(), False),
            ]
        )

        complex_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("simple_int", IntegerType(), False),
                StructField("int_array", ArrayType(IntegerType(), True), False),
                StructField("struct_with_int", nested_struct, False),
                StructField("valid_long", LongType(), False),  # This should be fine
            ]
        )

        test_data = [
            {
                "id": "test1",
                "simple_int": 1,
                "int_array": [1, 2, 3],
                "struct_with_int": {"nested_int": 4},
                "valid_long": 5,
            }
        ]
        df = spark.createDataFrame(test_data, complex_schema)

        # Should raise ValueError with all problematic fields
        try:
            validate_no_integer_types(df)
            assert False, "Expected ValueError to be raised"
        except ValueError as e:
            error_msg = str(e)
            # Check that all problematic fields are mentioned
            assert "simple_int" in error_msg
            assert "int_array[*]" in error_msg
            assert "struct_with_int.nested_int" in error_msg
            # Check that the valid field is not mentioned
            assert "valid_long" not in error_msg
            # Check that helpful guidance is provided
            assert "cast_integers_to_long()" in error_msg
            assert "ValidatedStruct" in error_msg
