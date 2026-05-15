"""
Test to verify Spark setup is working correctly.
This test is designed to be simple and fail fast if Spark is not properly configured.
"""

import os

import pytest
from pyspark.sql import SparkSession


@pytest.mark.spark
def test_spark_session_creation(spark: SparkSession) -> None:
    """Test that Spark session can be created successfully"""
    assert spark is not None
    assert spark.version is not None
    print(f"Spark version: {spark.version}")


@pytest.mark.spark
def test_basic_spark_operations(spark: SparkSession) -> None:
    """Test basic Spark DataFrame operations"""
    # Create a simple DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)

    # Test basic operations
    assert df.count() == 3
    assert len(df.columns) == 2
    assert "name" in df.columns
    assert "age" in df.columns

    # Test collect
    rows = df.collect()
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[0]["age"] == 25


@pytest.mark.spark
def test_spark_sql(spark: SparkSession) -> None:
    """Test Spark SQL functionality"""
    # Create a DataFrame and register as temp view
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.createOrReplaceTempView("people")

    # Test SQL query
    result = spark.sql("SELECT name, age FROM people WHERE age > 25")
    rows = result.collect()

    assert len(rows) == 2
    names = [row["name"] for row in rows]
    assert "Bob" in names
    assert "Charlie" in names
    assert "Alice" not in names


def test_environment_variables() -> None:
    """Test that required environment variables are set"""
    # These should be set by our CI configuration
    if os.getenv("CI") or os.getenv("CIRCLECI"):
        assert os.getenv("JAVA_HOME") is not None, "JAVA_HOME should be set in CI"
        assert os.getenv("SPARK_HOME") is not None, "SPARK_HOME should be set in CI"
        assert os.getenv("PYSPARK_PYTHON") is not None, (
            "PYSPARK_PYTHON should be set in CI"
        )


if __name__ == "__main__":
    # Allow running this test directly for debugging
    pytest.main([__file__, "-v"])
