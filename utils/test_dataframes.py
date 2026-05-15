from datetime import date, datetime
from enum import Enum
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from utils.dataframes import (
    cast_integers_to_long,
    create_dataframe,
    rename_columns,
)
from validation import ValidatedStruct, mock_model


class TestSchemaMismatchIssues:
    """Test cases for schema mismatch issues between LongType and IntegerType"""

    @pytest.mark.spark
    def test_databricks_integer_to_long_casting(self, spark: SparkSession) -> None:
        """Test solution for IntegerType to LongType casting in data pipelines"""
        from datetime import date

        from pyspark.sql.functions import col

        # Define a model similar to PatientMessages
        class TestModel(ValidatedStruct):
            patient_id: str
            computed_date: date
            messages_total_count: int
            days_since_last_message: int | None

        # Simulate the problematic scenario: source data with IntegerType schema
        problematic_source_schema = StructType(
            [
                StructField("patient_id", StringType(), False),
                StructField("computed_date", DateType(), False),
                StructField(
                    "messages_total_count", IntegerType(), False
                ),  # Problem: IntegerType
                StructField(
                    "days_since_last_message", IntegerType(), True
                ),  # Problem: IntegerType
            ]
        )

        # Create source data with IntegerType schema (simulating external system)
        source_data = [
            ("patient1", date(2023, 1, 1), 10, 5),
            ("patient2", date(2023, 1, 2), 20, None),
        ]

        source_df = spark.createDataFrame(source_data, problematic_source_schema)

        print("\n=== PROBLEMATIC SOURCE SCHEMA ===")
        source_df.printSchema()

        # Get our target schema (uses LongType)
        target_schema = TestModel.to_struct()
        print("\n=== TARGET SCHEMA (ValidatedStruct) ===")
        for field in target_schema.fields:
            print(f" |-- {field.name}: {field.dataType} (nullable = {field.nullable})")

        # This is the solution: Cast IntegerType fields to LongType
        casted_df = source_df.select(
            col("patient_id"),
            col("computed_date"),
            col("messages_total_count").cast("long").alias("messages_total_count"),
            col("days_since_last_message")
            .cast("long")
            .alias("days_since_last_message"),
        )

        print("\n=== AFTER CASTING TO LONG ===")
        casted_df.printSchema()

        # Verify the casting worked
        casted_fields = {f.name: f.dataType for f in casted_df.schema.fields}
        target_fields = {f.name: f.dataType for f in target_schema.fields}

        # Now the schemas should match
        for field_name in ["messages_total_count", "days_since_last_message"]:
            assert isinstance(casted_fields[field_name], LongType), (
                f"{field_name} should be LongType after casting"
            )
            assert isinstance(
                casted_fields[field_name], type(target_fields[field_name])
            ), f"{field_name} types should match"

        print("\n✅ Schema casting solution works!")
        print("This is what should be applied in your pipeline transformations.")

    @pytest.mark.spark
    def test_cast_integers_to_long_utility(self, spark: SparkSession) -> None:
        """Test the cast_integers_to_long utility function"""

        # Create a DataFrame with IntegerType columns
        problematic_schema = StructType(
            [
                StructField("name", StringType(), False),
                StructField("age", IntegerType(), False),
                StructField("score", IntegerType(), True),
                StructField(
                    "height", DoubleType(), False
                ),  # This should remain unchanged
            ]
        )

        test_data = [("Alice", 25, 95, 5.6), ("Bob", 30, None, 6.0)]

        df_with_integers = spark.createDataFrame(test_data, problematic_schema)

        print("\n=== BEFORE CASTING ===")
        df_with_integers.printSchema()

        # Apply the utility function
        df_casted = cast_integers_to_long(df_with_integers)

        print("\n=== AFTER CASTING ===")
        df_casted.printSchema()

        # Verify the results
        casted_fields = {f.name: f.dataType for f in df_casted.schema.fields}

        # Integer fields should now be LongType
        assert isinstance(casted_fields["age"], LongType)
        assert isinstance(casted_fields["score"], LongType)

        # Non-integer fields should remain unchanged
        assert isinstance(casted_fields["name"], StringType)
        assert isinstance(casted_fields["height"], DoubleType)

        print("\n✅ cast_integers_to_long utility function works correctly!")

    @pytest.mark.spark
    def test_longtype_vs_integertype_mismatch(self, spark: SparkSession) -> None:
        """Test schema mismatch between declared LongType and inferred IntegerType"""
        from datetime import date

        # Define a model similar to PatientMessages with Optional[int] fields
        class TestModel(ValidatedStruct):
            patient_id: str
            computed_date: date
            messages_total_count: int
            days_since_last_message: int | None

        # Get the declared schema (should use LongType for int fields)
        declared_schema = TestModel.to_struct()

        # Create test data with small integer values that Spark will infer as IntegerType
        test_data = [
            {
                "patient_id": "patient1",
                "computed_date": date(2023, 1, 1),
                "messages_total_count": 10,  # Small int - Spark infers as IntegerType
                "days_since_last_message": 5,  # Small int - Spark infers as IntegerType
            },
            {
                "patient_id": "patient2",
                "computed_date": date(2023, 1, 2),
                "messages_total_count": 20,
                "days_since_last_message": None,  # Optional field
            },
        ]

        # Create DataFrame with Spark's schema inference (will use IntegerType for small ints)
        inferred_df = spark.createDataFrame(test_data)

        # Also test with larger values that might force LongType
        large_test_data = [
            {
                "patient_id": "patient1",
                "computed_date": date(2023, 1, 1),
                "messages_total_count": 2147483648,  # > max int32, forces LongType
                "days_since_last_message": 2147483648,
            }
        ]
        large_inferred_df = spark.createDataFrame(large_test_data)

        # Print schemas for debugging
        print("\n=== DECLARED SCHEMA (from ValidatedStruct) ===")
        for field in declared_schema.fields:
            print(f" |-- {field.name}: {field.dataType} (nullable = {field.nullable})")

        print("\n=== INFERRED SCHEMA (small integers) ===")
        inferred_df.printSchema()

        print("\n=== INFERRED SCHEMA (large integers) ===")
        large_inferred_df.printSchema()

        # Check the type differences
        declared_fields = {f.name: f.dataType for f in declared_schema.fields}
        inferred_fields = {f.name: f.dataType for f in inferred_df.schema.fields}
        large_inferred_fields = {
            f.name: f.dataType for f in large_inferred_df.schema.fields
        }

        # Verify that our declared schema uses LongType for int fields
        assert isinstance(declared_fields["messages_total_count"], LongType)
        assert isinstance(declared_fields["days_since_last_message"], LongType)

        # Show the schema mismatch problem
        print(f"\n=== SCHEMA MISMATCH ANALYSIS ===")
        print(
            f"Declared messages_total_count: {declared_fields['messages_total_count']} ({type(declared_fields['messages_total_count']).__name__})"
        )
        print(
            f"Inferred messages_total_count: {inferred_fields['messages_total_count']} ({type(inferred_fields['messages_total_count']).__name__})"
        )
        print(
            f"Large inferred messages_total_count: {large_inferred_fields['messages_total_count']} ({type(large_inferred_fields['messages_total_count']).__name__})"
        )

        print(
            f"\nDeclared days_since_last_message: {declared_fields['days_since_last_message']} ({type(declared_fields['days_since_last_message']).__name__})"
        )
        print(
            f"Inferred days_since_last_message: {inferred_fields['days_since_last_message']} ({type(inferred_fields['days_since_last_message']).__name__})"
        )

        # This demonstrates the mismatch: Declared=LongType, Inferred=IntegerType
        mismatch_exists = not isinstance(
            inferred_fields["messages_total_count"], LongType
        ) or not isinstance(
            inferred_fields["days_since_last_message"], IntegerType
        )  # Note: this might be IntegerType

        if mismatch_exists:
            print("\n⚠️  SCHEMA MISMATCH DETECTED!")
            print("This is the same issue you're seeing in Databricks:")
            print("- Declared schema expects LongType")
            print("- Inferred schema from data uses IntegerType for small values")
            print("- This causes Delta table merge failures")

        # Try to create a DataFrame with the declared schema - this should work
        try:
            df_with_declared_schema = create_dataframe(
                data=test_data, target_schema=declared_schema
            )
            print(f"\n✅ Successfully created DataFrame with declared LongType schema")
            print("Final schema:")
            df_with_declared_schema.printSchema()
        except Exception as e:
            print(f"\n❌ Error creating DataFrame with declared schema: {e}")
            raise


class TestRenameColumns:
    """Test cases for the rename_columns function from dataframes module"""

    def test_rename_columns(self, spark: SparkSession) -> None:
        """Test rename_columns function with camelCase conversion"""
        test_data = [("1", "John", "2023-01-01", "user123")]
        df = spark.createDataFrame(test_data, ["id", "name", "createdAt", "userId"])

        result_df = rename_columns(df, {"name": "full_name"})
        columns = result_df.columns

        # Custom mapping
        assert "full_name" in columns
        assert "name" not in columns
        # Automatic camelCase to snake_case conversion
        assert "created_at" in columns
        assert "createdAt" not in columns
        assert "patient_id" in columns
        assert "userId" not in columns

    def test_rename_columns_empty_custom_mapping(self, spark: SparkSession) -> None:
        """Test rename_columns function with only automatic camelCase conversion"""
        test_data = [("1", "John", "2023-01-01", "user123")]
        df = spark.createDataFrame(test_data, ["id", "name", "createdAt", "userId"])

        result_df = rename_columns(df)
        columns = result_df.columns

        # Only automatic camelCase to snake_case conversion
        assert "name" in columns  # name stays as is (not in camelCase mapping)
        assert "created_at" in columns
        assert "createdAt" not in columns
        assert "patient_id" in columns
        assert "user_id" not in columns
        assert "userId" not in columns


class TestMockModel:
    """Test cases for the mock_model function from dataframes module"""

    def test_mock_model_with_all_provided_data(self) -> None:
        """Test mock_model when all fields are provided in data"""

        class SimpleModel(ValidatedStruct):
            id: str
            name: str
            age: int
            active: bool

        data = {"id": "test123", "name": "John Doe", "age": 30, "active": True}

        result = mock_model(SimpleModel, data)

        assert result["id"] == "test123"
        assert result["name"] == "John Doe"
        assert result["age"] == 30
        assert result["active"] is True

    def test_mock_model_with_partial_data(self) -> None:
        """Test mock_model when only some fields are provided in data"""

        class TestModel(ValidatedStruct):
            id: str
            name: str
            age: int
            score: float
            active: bool

        data = {"id": "test123", "name": "John Doe"}

        result = mock_model(TestModel, data)

        # Provided fields should match
        assert result["id"] == "test123"
        assert result["name"] == "John Doe"

        # Missing fields should have default values
        assert result["age"] == 42  # Default int value
        assert result["score"] == 3.14  # Default float value
        assert result["active"] is True  # Default bool value

    def test_mock_model_with_no_data(self) -> None:
        """Test mock_model when no data is provided"""

        class TestModel(ValidatedStruct):
            id: str
            count: int
            rate: float
            enabled: bool

        result = mock_model(TestModel)

        # All fields should have default values
        assert result["id"] == "test_string"
        assert result["count"] == 42
        assert result["rate"] == 3.14
        assert result["enabled"] is True

    def test_mock_model_with_datetime_types(self) -> None:
        """Test mock_model with datetime and date types"""

        class DateTimeModel(ValidatedStruct):
            id: str
            created_at: datetime
            birth_date: date

        data = {"id": "test123"}

        result = mock_model(DateTimeModel, data)

        assert result["id"] == "test123"
        assert isinstance(result["created_at"], datetime)
        assert isinstance(result["birth_date"], date)

    def test_mock_model_with_enums(self) -> None:
        """Test mock_model with enum types"""

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"
            PENDING = "pending"

        class Priority(Enum):
            HIGH = 1
            MEDIUM = 2
            LOW = 3

        class ModelWithEnums(ValidatedStruct):
            status: Status
            priority: Priority

        result = mock_model(ModelWithEnums)

        # Should use first enum value (as string for Spark compatibility)
        assert result["status"] == Status.ACTIVE.value
        assert result["priority"] == Priority.HIGH.value

    def test_mock_model_with_optional_fields(self) -> None:
        """Test mock_model with optional fields"""

        class ModelWithOptional(ValidatedStruct):
            required_field: str
            optional_field: str | None = None
            optional_with_default: int | None = 42

        result = mock_model(ModelWithOptional)

        assert result["required_field"] == "test_string"
        assert result["optional_field"] is None
        assert result["optional_with_default"] == 42

    def test_mock_model_with_automatic_optional_handling(self) -> None:
        """Test mock_model with Optional fields without explicit defaults"""

        class ModelWithAutoOptional(ValidatedStruct):
            required_field: str
            optional_field: str | None  # No explicit = None
            optional_int: int | None  # No explicit = None

        result = mock_model(ModelWithAutoOptional)

        # Should automatically provide None for Optional fields
        assert result["required_field"] == "test_string"
        assert result["optional_field"] is None
        assert result["optional_int"] is None

        # Test schema generation - Optional fields should be nullable
        schema = ModelWithAutoOptional.to_struct()
        field_nullable = {field.name: field.nullable for field in schema.fields}

        assert field_nullable["required_field"] is False  # Not nullable
        assert field_nullable["optional_field"] is True  # Nullable
        assert field_nullable["optional_int"] is True  # Nullable

    def test_mock_model_with_lists(self) -> None:
        """Test mock_model with list types"""

        class ModelWithLists(ValidatedStruct):
            tags: list[str]
            scores: list[int]
            optional_items: list[str] | None = None

        result = mock_model(ModelWithLists)

        assert result["tags"] == []  # Empty list for now
        assert result["scores"] == []
        assert result["optional_items"] is None

    def test_mock_model_with_nested_models(self) -> None:
        """Test mock_model with nested model types"""

        class Address(ValidatedStruct):
            street: str
            city: str

        class Person(ValidatedStruct):
            name: str
            address: Address

        result = mock_model(Person)

        assert result["name"] == "test_string"
        assert isinstance(result["address"], dict)
        assert result["address"]["street"] == "test_string"
        assert result["address"]["city"] == "test_string"


class TestCreateDataFrameWithSchema:
    """Test cases for the create_dataframe_with_schema function (list of dicts input)"""

    @pytest.mark.spark
    def test_create_dataframe_with_schema_different_order(
        self, spark: SparkSession
    ) -> None:
        """Test creating DataFrame with schema when columns are in different order"""
        test_data = [
            {"id": "1", "name": "John", "age": 25, "created_at": "2023-01-01"},
        ]
        target_schema = StructType(
            [
                StructField("name", StringType(), False),
                StructField("id", StringType(), False),
                StructField("created_at", StringType(), False),
                StructField("age", IntegerType(), False),
            ]
        )
        result_df = create_dataframe(test_data, target_schema)
        assert result_df.columns == ["name", "id", "created_at", "age"]

    @pytest.mark.spark
    def test_create_dataframe_with_schema_correct_order(
        self, spark: SparkSession
    ) -> None:
        """Test creating DataFrame with schema when columns are in different order"""
        # Create test data as list of dictionaries
        test_data = [
            {"id": "1", "name": "John", "age": 25, "created_at": "2023-01-01"},
            {"id": "2", "name": "Jane", "age": 30, "created_at": "2023-01-02"},
        ]

        # Define target schema with different column order
        target_schema = StructType(
            [
                StructField("name", StringType(), False),
                StructField("id", StringType(), False),
                StructField("created_at", StringType(), False),
                StructField("age", IntegerType(), False),
            ]
        )

        # Create DataFrame with target schema
        result_df = create_dataframe(test_data, target_schema)

        # Check that columns are in target schema order
        assert result_df.columns == ["name", "id", "created_at", "age"]

        # Check data integrity
        rows = result_df.collect()
        assert len(rows) == 2
        assert rows[0]["name"] == "John"
        assert rows[0]["id"] == "1"
        assert rows[0]["age"] == 25
        assert rows[0]["created_at"] == "2023-01-01"

    @pytest.mark.spark
    def test_create_dataframe_with_schema_missing_columns(
        self, spark: SparkSession
    ) -> None:
        """Test error when source data is missing required columns"""
        test_data = [{"id": "1", "name": "John"}]

        target_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("age", IntegerType(), False),  # Missing from source
            ]
        )

        with pytest.raises(KeyError):
            create_dataframe(test_data, target_schema)

    @pytest.mark.spark
    def test_create_dataframe_with_schema_extra_columns(
        self, spark: SparkSession
    ) -> None:
        """Test that extra columns in source are ignored"""
        test_data = [
            {"id": "1", "name": "John", "age": 25, "extra_column": "extra_value"}
        ]

        target_schema = StructType(
            [
                StructField("name", StringType(), False),
                StructField("id", StringType(), False),
            ]
        )

        result_df = create_dataframe(test_data, target_schema)

        # Only target schema columns should be present
        assert result_df.columns == ["name", "id"]
        assert "extra_column" not in result_df.columns

    @pytest.mark.spark
    def test_create_dataframe_with_schema_empty_data(self, spark: SparkSession) -> None:
        """Test creating DataFrame with empty data"""
        test_data: list[dict[str, Any]] = []

        target_schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
            ]
        )

        result_df = create_dataframe(test_data, target_schema)

        # Should create empty DataFrame with correct schema
        assert result_df.columns == ["id", "name"]
        assert result_df.count() == 0
