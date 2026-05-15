import os
import time

import pytest
from pyspark.sql import SparkSession

# Set UTC before importing Spark.  The JVM reads TZ at startup, so this must
# run first to ensure deterministic timestamp behaviour across all environments
# (local Brazil UTC-3 vs CircleCI UTC).
os.environ["TZ"] = "UTC"
try:
    time.tzset()
except AttributeError:
    pass  # tzset is not available on Windows


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a local Spark session for testing."""
    from utils.spark_local import build_local_spark

    return build_local_spark("poorbricks-test")
