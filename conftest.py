import os
import sys
import time
from collections.abc import Iterator

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

# The suite runs under pytest-xdist (-n 4): each worker builds its own Spark
# session. Bound cores and heap so the workers do not collectively
# oversubscribe the machine (production uses local[*] with a larger heap).
os.environ.setdefault("SPARK_MASTER", "local[2]")
os.environ.setdefault("SPARK_DRIVER_MEMORY", "768m")


@pytest.fixture(autouse=True)
def _isolate_global_pipeline_state(request: pytest.FixtureRequest) -> Iterator[None]:
    """Roll back process-global import/registry state after every test.

    ``tables`` is a regular package in both this repo and ``test_table_repo``,
    so it caches to one root per process; the pipeline registry, that import
    cache, and the ``sys.path`` entry ``discover_all_pipelines`` inserts are all
    global. Under pytest-xdist a test that imports/discovers ``tables.*`` can
    leave the wrong root cached for the next test on the same worker — the
    source of the cross-test flakiness.

    This is a pure SAVE/RESTORE safety net: it snapshots state on entry and
    restores it on exit (it does NOT clear at setup), so module-level
    registrations made at collection survive while any per-test mutation is
    rolled back. Integration tests manage their own (module-scoped) registry
    and run serially, so they opt out.
    """
    if request.node.get_closest_marker("integration"):
        yield
        return

    from poorbricks import registry as _registry
    from poorbricks.lineage_runtime import clear_captured

    saved_path = list(sys.path)
    saved_tables_modules = {
        name: mod
        for name, mod in sys.modules.items()
        if name == "tables" or name.startswith("tables.")
    }
    saved_pipelines = dict(_registry._pipelines)
    saved_scenarios = {k: dict(v) for k, v in _registry._scenarios.items()}
    try:
        yield
    finally:
        _registry._pipelines.clear()
        _registry._pipelines.update(saved_pipelines)
        _registry._scenarios.clear()
        _registry._scenarios.update(saved_scenarios)
        for name in [
            n for n in sys.modules if n == "tables" or n.startswith("tables.")
        ]:
            del sys.modules[name]
        sys.modules.update(saved_tables_modules)
        sys.path[:] = saved_path
        clear_captured()


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a local Spark session for testing."""
    from utils.spark_local import build_local_spark

    return build_local_spark("poorbricks-test")
