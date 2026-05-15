from pyspark.sql import SparkSession

from utils.strings import (
    _camel_to_snake_case,
    _get_all_camel_to_snake_mappings,
    default_transformations,
    get_field_mappings,
    short_name_udf,
)


class TestShortName:
    """Test cases for the short_name UDF function"""

    def test_short_name(self, spark: SparkSession) -> None:
        """Test short_name UDF function"""
        test_data = [("John Doe",), ("Jane Smith",), ("Bob",), (None,)]
        df = spark.createDataFrame(test_data, ["name"])

        result_df = df.withColumn("short_name", short_name_udf(df.name))
        rows = result_df.collect()

        assert len(rows) == 4
        assert rows[0]["short_name"] == "John D."
        assert rows[1]["short_name"] == "Jane S."
        assert rows[2]["short_name"] == "Bob"
        assert rows[3]["short_name"] is None


class TestFieldMappings:
    """Test cases for field mapping functionality"""

    def test_camel_to_snake_case_conversion(self) -> None:
        """Test basic camelCase to snake_case conversion"""
        assert _camel_to_snake_case("firstName") == "first_name"
        assert _camel_to_snake_case("lastName") == "last_name"
        assert _camel_to_snake_case("userId") == "user_id"
        assert _camel_to_snake_case("createdAt") == "created_at"
        assert _camel_to_snake_case("questionId") == "question_id"
        assert _camel_to_snake_case("answerType") == "answer_type"

    def test_camel_to_snake_case_edge_cases(self) -> None:
        """Test edge cases for camelCase conversion"""
        # Single word should remain unchanged
        assert _camel_to_snake_case("name") == "name"
        assert _camel_to_snake_case("id") == "id"

        # Already snake_case should remain unchanged
        assert _camel_to_snake_case("first_name") == "first_name"
        assert _camel_to_snake_case("user_id") == "user_id"

        # PascalCase should be converted
        assert _camel_to_snake_case("FirstName") == "first_name"
        assert _camel_to_snake_case("UserId") == "user_id"

        # Multiple consecutive capitals
        assert _camel_to_snake_case("XMLHttpRequest") == "xml_http_request"
        assert _camel_to_snake_case("HTTPSConnection") == "https_connection"

        # Numbers in field names
        assert _camel_to_snake_case("field1Name") == "field1_name"
        assert _camel_to_snake_case("version2Data") == "version2_data"

    def test_default_transformations(self) -> None:
        """Test domain-specific field mappings"""
        mappings = default_transformations()

        # Time field standardization
        assert mappings["timestamp"] == "created_at"
        assert mappings["instant"] == "created_at"

        # Domain convention - userId maps to patient_id
        assert mappings["userId"] == "patient_id"

    def test_get_all_camel_to_snake_mappings(self) -> None:
        """Test comprehensive field mappings combining domain-specific and automatic"""
        field_names = [
            "firstName",  # Should convert to first_name
            "lastName",  # Should convert to last_name
            "userId",  # Should map to patient_id (domain-specific)
            "timestamp",  # Should map to created_at (domain-specific)
            "createdAt",  # Should convert to created_at
            "snake_case",  # Should remain unchanged
            "id",  # Should remain unchanged
            "questionId",  # Should convert to question_id
            "answerType",  # Should convert to answer_type
        ]

        mappings = _get_all_camel_to_snake_mappings(field_names)

        # Domain-specific mappings should take precedence
        assert mappings["userId"] == "patient_id"  # Not user_id
        assert mappings["timestamp"] == "created_at"

        # Automatic conversions
        assert mappings["firstName"] == "first_name"
        assert mappings["lastName"] == "last_name"
        assert mappings["createdAt"] == "created_at"
        assert mappings["questionId"] == "question_id"
        assert mappings["answerType"] == "answer_type"

        # Fields that don't need conversion should not be in mappings
        assert "snake_case" not in mappings
        assert "id" not in mappings

    def test_get_field_mappings_public_interface(self) -> None:
        """Test the public interface function for getting field mappings"""
        field_names = ["firstName", "userId", "createdAt", "snake_case"]

        mappings = get_field_mappings(field_names)

        # Should have same behavior as private function
        assert mappings["firstName"] == "first_name"
        assert mappings["userId"] == "patient_id"  # Domain-specific
        assert mappings["createdAt"] == "created_at"
        assert "snake_case" not in mappings  # No conversion needed

    def test_field_mappings_empty_list(self) -> None:
        """Test field mappings with empty field list"""
        mappings = get_field_mappings([])

        # Should only contain domain-specific mappings
        expected_domain_mappings = default_transformations()
        assert mappings == expected_domain_mappings

    def test_field_mappings_duplicate_fields(self) -> None:
        """Test field mappings with duplicate field names"""
        field_names = ["firstName", "firstName", "userId", "userId"]

        mappings = get_field_mappings(field_names)

        # Should handle duplicates gracefully
        assert mappings["firstName"] == "first_name"
        assert mappings["userId"] == "patient_id"
        assert len([k for k in mappings.keys() if k == "firstName"]) == 1

    def test_field_mappings_complex_nested_scenario(self) -> None:
        """Test field mappings for complex nested data scenarios"""
        # Simulate field names from a complex MongoDB document
        field_names = [
            # Top-level fields
            "patientId",
            "formId",
            "createdAt",
            "updatedAt",
            # Nested array fields (from formAnswers)
            "questionId",
            "questionLabel",
            "answerType",
            "answerValue",
            # Other nested fields
            "userId",
            "timestamp",
            "instant",
            # Already snake_case fields
            "form_type",
            "patient_name",
            "answer_id",
        ]

        mappings = get_field_mappings(field_names)

        # Verify comprehensive mappings
        assert mappings["patientId"] == "patient_id"
        assert mappings["formId"] == "form_id"
        assert mappings["createdAt"] == "created_at"
        assert mappings["updatedAt"] == "updated_at"
        assert mappings["questionId"] == "question_id"
        assert mappings["questionLabel"] == "question_label"
        assert mappings["answerType"] == "answer_type"
        assert mappings["answerValue"] == "answer_value"

        # Domain-specific mappings
        assert mappings["userId"] == "patient_id"
        assert mappings["timestamp"] == "created_at"
        assert mappings["instant"] == "created_at"

        # snake_case fields should not be in mappings
        assert "form_type" not in mappings
        assert "patient_name" not in mappings
        assert "answer_id" not in mappings

    def test_field_mappings_precedence_order(self) -> None:
        """Test that domain-specific mappings take precedence over automatic conversion"""
        # Test case where domain mapping conflicts with automatic conversion
        field_names = ["userId", "timestamp", "instant"]

        mappings = get_field_mappings(field_names)

        # Domain-specific should win over automatic
        assert mappings["userId"] == "patient_id"  # Not "user_id"
        assert mappings["timestamp"] == "created_at"  # Domain-specific
        assert mappings["instant"] == "created_at"  # Domain-specific

        # If we only had automatic conversion, userId would be user_id
        # But domain rules override this
