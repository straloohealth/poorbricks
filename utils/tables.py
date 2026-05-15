from pyspark.sql import DataFrame, SparkSession


def read_table(table_name: str) -> DataFrame:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("No active Spark session found")
    return spark.read.table(table_name)
