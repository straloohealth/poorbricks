import pyspark.sql.functions as f
from pyspark.sql import Column


def create_deterministic_id(*args: Column) -> Column:
    string_cols = [arg.cast("string") for arg in args]
    return f.substring(f.sha2(f.concat_ws("-", *string_cols), 256), 1, 24)
