"""API runtime settings loaded from environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings

from poorbricks.airflow.watch import DEFAULT_AIRFLOW_URL
from poorbricks.constants import (
    DEFAULT_CONTRACTS_API_URL,
    DEFAULT_NAMESPACE,
    DEFAULT_POSTGRES_DB,
    DEFAULT_POSTGRES_HOST,
    DEFAULT_POSTGRES_PORT,
    DEFAULT_RUNTIME_SECRET_NAME,
    DEFAULT_WORKER_IMAGE,
    DEV_SCHEMA_SUFFIX,
)


class ApiSettings(BaseSettings):
    """Configuration for the poorbricks API server.

    Loaded from environment variables (``POORBRICKS_API_*`` or unprefixed).
    """

    dag_store: str = "local"
    dags_dir: str = "/opt/airflow/dags"
    dags_local_root: str = "/tmp/poorbricks-dags"

    worker_image: str = DEFAULT_WORKER_IMAGE
    worker_namespace: str = DEFAULT_NAMESPACE
    runtime_secret_name: str = DEFAULT_RUNTIME_SECRET_NAME

    code_pvc_claim: str = "airflow-dags"
    code_pvc_root: str = "__code__"

    upload_timeout_seconds: int = 600

    # Worker Postgres target baked into generated prod DAGs.
    postgres_host: str = DEFAULT_POSTGRES_HOST
    postgres_port: str = DEFAULT_POSTGRES_PORT
    postgres_db: str = DEFAULT_POSTGRES_DB
    contracts_api_url: str = DEFAULT_CONTRACTS_API_URL

    # Dev environment target. A dev upload writes to ``dev_postgres_*`` (falling
    # back to the prod host/db) under the ``dev_schema_suffix`` schema, so dev
    # runs share the cluster Airflow yet never touch prod tables.
    dev_postgres_host: str = ""
    dev_postgres_db: str = ""
    dev_schema_suffix: str = DEV_SCHEMA_SUFFIX

    # Spot-eviction retries baked into generated DAGs (dev DAGs fast-fail).
    worker_retries: int = 2
    worker_retry_delay_minutes: int = 2

    # Airflow REST base URL used by the trigger endpoint + run-watch proxies.
    airflow_url: str = DEFAULT_AIRFLOW_URL

    model_config = {"env_prefix": "POORBRICKS_API_", "extra": "ignore"}


settings = ApiSettings()

__all__ = ["ApiSettings", "settings"]
