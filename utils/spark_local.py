"""Local SparkSession builder shared by the test suite and the verify CLI.

Extracted from the old block inside ``source/conftest.py`` so both contexts
get an identical session configuration: deterministic timezone, single-thread
master, small memory footprint, no Spark UI. Tests still call this through
the existing ``spark`` pytest fixture; the verify CLI calls it directly.
"""

from __future__ import annotations

import os
import sys
import tempfile

from pyspark.sql import SparkSession


def _ensure_pyspark_env() -> None:
    """Set PYSPARK_PYTHON / SPARK_HOME if not already pointing somewhere valid.

    The CI Docker image pre-sets these to paths that are incompatible with
    the open-source PySpark installed in our poetry venv. Without this, JVM
    startup fails with ``'JavaPackage' object is not callable``.
    ``conftest.py`` does the same fixup in ``pytest_sessionstart``; we
    duplicate it here so the verify CLI works standalone (no pytest in the loop)."""
    python_executable = sys.executable or "python"
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    try:
        import pyspark as _pyspark

        os.environ["SPARK_HOME"] = os.path.dirname(_pyspark.__file__)
    except ImportError:
        pass


def _discover_extra_jars() -> list[str]:
    """Find driver jars to add to ``spark.jars`` (e.g., postgres JDBC).

    Looks under ``/opt/spark/jars/`` (where our worker image places them) so
    Spark sessions running in the container can use ``spark.read.format('jdbc')``
    against postgres. Returns an empty list locally (CI/dev without the image)
    so existing test setups keep working.
    """
    candidates = ["/opt/spark/jars"]
    found: list[str] = []
    for d in candidates:
        if not os.path.isdir(d):
            continue
        for fname in os.listdir(d):
            if fname.startswith("postgresql-") and fname.endswith(".jar"):
                found.append(os.path.join(d, fname))
    return found


def build_local_spark(app_name: str = "poorbricks-local") -> SparkSession:
    """Build (or return the active) local SparkSession with poorbricks's
    standard configuration."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    _ensure_pyspark_env()

    extra_jars = _discover_extra_jars()
    config = (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    )
    if extra_jars:
        config = config.config("spark.jars", ",".join(extra_jars))
    config = (
        config.config(
            "spark.sql.warehouse.dir",
            tempfile.mkdtemp(prefix="poorbricks-spark-warehouse-"),
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.port", "0")
        .config("spark.blockManager.port", "0")
        .config("spark.port.maxRetries", "100")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.rpc.numRetries", "10")
        .config("spark.rpc.retry.wait", "5s")
        .config("spark.network.timeout", "180s")
    )

    if os.getenv("CI") or os.getenv("CIRCLECI"):
        config = (
            config.config("spark.driver.maxResultSize", "512m")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8MB")
        )

    spark = config.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


__all__ = ["build_local_spark"]
