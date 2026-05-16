from pyspark.sql import SparkSession

from utils.strings import camel_to_snake_case, short_name_udf


class TestShortName:
    def test_short_name(self, spark: SparkSession) -> None:
        test_data = [("John Doe",), ("Jane Smith",), ("Bob",), (None,)]
        df = spark.createDataFrame(test_data, ["name"])

        result_df = df.withColumn("short_name", short_name_udf(df.name))
        rows = result_df.collect()

        assert len(rows) == 4
        assert rows[0]["short_name"] == "John D."
        assert rows[1]["short_name"] == "Jane S."
        assert rows[2]["short_name"] == "Bob"
        assert rows[3]["short_name"] is None


class TestCamelToSnakeCase:
    def test_basic_conversion(self) -> None:
        assert camel_to_snake_case("firstName") == "first_name"
        assert camel_to_snake_case("lastName") == "last_name"
        assert camel_to_snake_case("userId") == "user_id"
        assert camel_to_snake_case("createdAt") == "created_at"
        assert camel_to_snake_case("questionId") == "question_id"
        assert camel_to_snake_case("answerType") == "answer_type"

    def test_edge_cases(self) -> None:
        assert camel_to_snake_case("name") == "name"
        assert camel_to_snake_case("id") == "id"

        assert camel_to_snake_case("first_name") == "first_name"
        assert camel_to_snake_case("user_id") == "user_id"

        assert camel_to_snake_case("FirstName") == "first_name"
        assert camel_to_snake_case("UserId") == "user_id"

        assert camel_to_snake_case("XMLHttpRequest") == "xml_http_request"
        assert camel_to_snake_case("HTTPSConnection") == "https_connection"

        assert camel_to_snake_case("field1Name") == "field1_name"
        assert camel_to_snake_case("version2Data") == "version2_data"
