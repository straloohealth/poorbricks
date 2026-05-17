"""API runtime settings loaded from environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings

from poorbricks.constants import (
    DEFAULT_NAMESPACE,
    DEFAULT_RUNTIME_SECRET_NAME,
    DEFAULT_WORKER_IMAGE,
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

    table_repo_url_template: str = "https://github.com/{prefix}.git"
    repo_clone_secret_template: str = "repo-clone-{prefix}"
    use_repo_clone_secret: bool = False

    upload_timeout_seconds: int = 600

    model_config = {"env_prefix": "POORBRICKS_API_", "extra": "ignore"}


settings = ApiSettings()

__all__ = ["ApiSettings", "settings"]
