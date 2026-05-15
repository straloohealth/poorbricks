"""
Tests for validation rules.
"""

from datetime import datetime
from enum import Enum

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from validation.rules import (
    DateTimeRangeRule,
    EnumRule,
    IntRangeRule,
    NotNullRule,
    SchemaValidationRule,
    StringLengthRule,
)
from validation.validators import verify


class TestVerifyFunction:
    """Test cases for the verify function and validation rules"""

    def test_int_range_rule_valid_data(self, spark: SparkSession) -> None:
        """Test IntRangeRule with valid data"""
        test_data = [{"age": 25}, {"age": 30}, {"age": 35}]
        df = spark.createDataFrame(test_data)

        rules = [IntRangeRule("age", min_value=18, max_value=65)]
        result = verify(df, rules)

        assert result["valid"] is True
        assert result["passed_rules"] == 1
        assert result["failed_rules"] == 0
        assert len(result["errors"]) == 0

    def test_int_range_rule_invalid_data(self, spark: SparkSession) -> None:
        """Test IntRangeRule with invalid data"""
        test_data = [{"age": 15}, {"age": 25}, {"age": 70}]
        df = spark.createDataFrame(test_data)

        rules = [IntRangeRule("age", min_value=18, max_value=65)]
        result = verify(df, rules)

        assert result["valid"] is False
        assert result["passed_rules"] == 0
        assert result["failed_rules"] == 1
        assert len(result["errors"]) == 2  # One for min, one for max

    def test_datetime_range_rule(self, spark: SparkSession) -> None:
        """Test DateTimeRangeRule"""
        min_date = datetime(2020, 1, 1)
        max_date = datetime(2023, 12, 31)

        test_data = [
            {"created_at": datetime(2021, 6, 15)},
            {"created_at": datetime(2022, 3, 10)},
        ]

        schema = StructType([StructField("created_at", TimestampType(), False)])

        df = spark.createDataFrame(test_data, schema)

        rules = [DateTimeRangeRule("created_at", min_date=min_date, max_date=max_date)]
        result = verify(df, rules)

        assert result["valid"] is True
        assert result["passed_rules"] == 1

    def test_not_null_rule(self, spark: SparkSession) -> None:
        """Test NotNullRule"""
        test_data = [{"name": "John"}, {"name": None}, {"name": "Jane"}]
        df = spark.createDataFrame(test_data)

        rules = [NotNullRule("name")]
        result = verify(df, rules)

        assert result["valid"] is False
        assert result["failed_rules"] == 1
        assert "1 rows contain null values" in result["errors"][0]

    def test_string_length_rule(self, spark: SparkSession) -> None:
        """Test StringLengthRule"""
        test_data = [{"name": "Jo"}, {"name": "John"}, {"name": "VeryLongName"}]
        df = spark.createDataFrame(test_data)

        rules = [StringLengthRule("name", min_length=3, max_length=8)]
        result = verify(df, rules)

        assert result["valid"] is False
        assert result["failed_rules"] == 1
        # Should have errors for "Jo" (too short) and "VeryLongName" (too long)
        assert len([e for e in result["errors"] if "length <" in e]) == 1
        assert len([e for e in result["errors"] if "length >" in e]) == 1

    def test_multiple_rules_all_pass(self, spark: SparkSession) -> None:
        """Test multiple validation rules that all pass"""
        test_data = [
            {"name": "John", "age": 25},
            {"name": "Jane", "age": 30},
        ]
        df = spark.createDataFrame(test_data)

        rules = [
            NotNullRule("name"),
            StringLengthRule("name", min_length=2, max_length=10),
            IntRangeRule("age", min_value=18, max_value=65),
        ]

        result = verify(df, rules)

        assert result["valid"] is True
        assert result["total_rules"] == 3
        assert result["passed_rules"] == 3
        assert result["failed_rules"] == 0

    def test_multiple_rules_some_fail(self, spark: SparkSession) -> None:
        """Test multiple validation rules where some fail"""
        test_data = [
            {"name": "Jo", "age": 15},  # name too short, age too low
            {"name": "Jane", "age": 30},  # valid
        ]
        df = spark.createDataFrame(test_data)

        rules = [
            StringLengthRule("name", min_length=3, max_length=10),
            IntRangeRule("age", min_value=18, max_value=65),
        ]

        result = verify(df, rules)

        assert result["valid"] is False
        assert result["total_rules"] == 2
        assert result["passed_rules"] == 0  # Both rules have violations
        assert result["failed_rules"] == 2

    def test_rule_with_missing_column(self, spark: SparkSession) -> None:
        """Test validation rule with missing column"""
        test_data = [{"name": "John"}]
        df = spark.createDataFrame(test_data)

        rules = [IntRangeRule("age", min_value=18, max_value=65)]
        result = verify(df, rules)

        assert result["valid"] is False
        assert "not found in DataFrame" in result["errors"][0]


class TestEnumRule:
    """Test cases for the EnumRule validation"""

    @pytest.mark.spark
    def test_enum_rule_valid_values(self, spark: SparkSession) -> None:
        """Test EnumRule with all valid enum values"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"
            PENDING = "pending"

        test_data = [
            {"status": "active"},
            {"status": "inactive"},
            {"status": "pending"},
        ]
        df = spark.createDataFrame(test_data)

        rule = EnumRule("status", Status)
        errors = rule.validate(df)

        assert len(errors) == 0

    @pytest.mark.spark
    def test_enum_rule_invalid_values(self, spark: SparkSession) -> None:
        """Test EnumRule with invalid enum values"""

        class Priority(Enum):
            HIGH = "high"
            MEDIUM = "medium"
            LOW = "low"

        test_data = [
            {"priority": "high"},  # valid
            {"priority": "invalid"},  # invalid
            {"priority": "urgent"},  # invalid
            {"priority": "low"},  # valid
        ]
        df = spark.createDataFrame(test_data)

        rule = EnumRule("priority", Priority)
        errors = rule.validate(df)

        assert len(errors) == 1
        error_msg = errors[0]
        assert "2 rows contain invalid Priority values" in error_msg
        assert "Valid values: ['high', 'medium', 'low']" in error_msg
        assert "Sample invalid values: ['invalid', 'urgent']" in error_msg

    @pytest.mark.spark
    def test_enum_rule_with_nulls(self, spark: SparkSession) -> None:
        """Test EnumRule ignores null values"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        test_data = [
            {"status": "active"},
            {"status": None},  # null should be ignored
            {"status": "invalid"},  # invalid
        ]
        df = spark.createDataFrame(test_data)

        rule = EnumRule("status", Status)
        errors = rule.validate(df)

        assert len(errors) == 1
        assert "1 rows contain invalid Status values" in errors[0]
        # Should not complain about null values
        assert "null" not in errors[0].lower()

    @pytest.mark.spark
    def test_enum_rule_missing_column(self, spark: SparkSession) -> None:
        """Test EnumRule with missing column"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        test_data = [{"name": "test"}]
        df = spark.createDataFrame(test_data)

        rule = EnumRule("status", Status)
        errors = rule.validate(df)

        assert len(errors) == 1
        assert "Column 'status' not found in DataFrame" in errors[0]

    @pytest.mark.spark
    def test_enum_rule_numeric_enum(self, spark: SparkSession) -> None:
        """Test EnumRule with numeric enum values"""

        class Priority(Enum):
            HIGH = 1
            MEDIUM = 2
            LOW = 3

        test_data = [
            {"priority": 1},  # valid
            {"priority": 2},  # valid
            {"priority": 99},  # invalid
        ]
        df = spark.createDataFrame(test_data)

        rule = EnumRule("priority", Priority)
        errors = rule.validate(df)

        assert len(errors) == 1
        assert "1 rows contain invalid Priority values" in errors[0]
        assert "Valid values: [1, 2, 3]" in errors[0]


class TestSchemaValidationRule:
    """Test cases for the SchemaValidationRule validation"""

    @pytest.mark.spark
    def test_schema_validation_exact_match(self, spark: SparkSession) -> None:
        """Test SchemaValidationRule with exact schema match"""
        expected_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("age", IntegerType(), False),
            ]
        )

        test_data = [{"id": "1", "name": "John", "age": 25}]
        df = spark.createDataFrame(test_data, expected_schema)

        rule = SchemaValidationRule(expected_schema, allow_extra_columns=False)
        errors = rule.validate(df)

        assert len(errors) == 0

    @pytest.mark.spark
    def test_schema_validation_missing_columns(self, spark: SparkSession) -> None:
        """Test SchemaValidationRule with missing columns"""
        expected_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("age", IntegerType(), False),
            ]
        )

        # DataFrame missing 'age' column
        test_data = [{"id": "1", "name": "John"}]
        df = spark.createDataFrame(test_data)

        rule = SchemaValidationRule(expected_schema, allow_extra_columns=False)
        errors = rule.validate(df)

        assert len(errors) >= 1
        assert any("Missing required columns: ['age']" in error for error in errors)

    @pytest.mark.spark
    def test_schema_validation_extra_columns_not_allowed(
        self, spark: SparkSession
    ) -> None:
        """Test SchemaValidationRule with extra columns not allowed"""
        expected_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
            ]
        )

        # DataFrame has extra 'age' column
        test_data = [{"id": "1", "name": "John", "age": 25}]
        df = spark.createDataFrame(test_data)

        rule = SchemaValidationRule(expected_schema, allow_extra_columns=False)
        errors = rule.validate(df)

        assert len(errors) >= 1
        assert any("Unexpected extra columns: ['age']" in error for error in errors)

    @pytest.mark.spark
    def test_schema_validation_extra_columns_allowed(self, spark: SparkSession) -> None:
        """Test SchemaValidationRule with extra columns allowed"""
        expected_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
            ]
        )

        # DataFrame has extra 'age' column
        test_data = [{"id": "1", "name": "John", "age": 25}]
        df = spark.createDataFrame(test_data)

        rule = SchemaValidationRule(expected_schema, allow_extra_columns=True)
        errors = rule.validate(df)

        # Should not complain about extra columns, but may have nullability issues
        assert not any("extra columns" in error.lower() for error in errors)

    @pytest.mark.spark
    def test_schema_validation_wrong_data_types(self, spark: SparkSession) -> None:
        """Test SchemaValidationRule with incorrect data types"""
        expected_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("age", IntegerType(), False),
            ]
        )

        # Create DataFrame with age as string instead of int
        actual_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("age", StringType(), False),  # Wrong type
            ]
        )

        test_data = [{"id": "1", "age": "25"}]
        df = spark.createDataFrame(test_data, actual_schema)

        rule = SchemaValidationRule(expected_schema, allow_extra_columns=False)
        errors = rule.validate(df)

        assert len(errors) == 1
        assert "Column 'age' has incorrect type" in errors[0]
        assert "expected IntegerType" in errors[0]
        assert "got StringType" in errors[0]

    @pytest.mark.spark
    def test_schema_validation_nullability_mismatch(self, spark: SparkSession) -> None:
        """Test that nullability mismatch is intentionally ignored.

        Spark marks most output columns as nullable=True regardless of actual
        data content.  @dlt.table(schema=...) enforces nullability at write
        time, so SchemaValidationRule skips this check.
        """
        expected_schema = StructType(
            [
                StructField("id", StringType(), False),
            ]
        )

        actual_schema = StructType(
            [
                StructField("id", StringType(), True),
            ]
        )

        test_data = [{"id": "1"}]
        df = spark.createDataFrame(test_data, actual_schema)

        rule = SchemaValidationRule(expected_schema, allow_extra_columns=False)
        errors = rule.validate(df)

        assert len(errors) == 0
