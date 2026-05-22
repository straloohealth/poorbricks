"""Production-deployment constants shared between the CLI, API, and DAG generator.

These values flow into generated Airflow DAGs (image, namespace) and are
used as defaults by the FastAPI upload service.
"""

from __future__ import annotations

DEFAULT_WORKER_IMAGE_REPO = (
    "us-central1-docker.pkg.dev/inner-autonomy-371516/poorbricks/api"
)
DEFAULT_WORKER_IMAGE_TAG = "latest"
DEFAULT_WORKER_IMAGE = f"{DEFAULT_WORKER_IMAGE_REPO}:{DEFAULT_WORKER_IMAGE_TAG}"

DEFAULT_NAMESPACE = "airflow"
DEFAULT_RUNTIME_SECRET_NAME = "poorbricks-runtime"
DEFAULT_POSTGRES_CREDS_SECRET_NAME = "poorbricks-server-postgresql-creds"

DEFAULT_POSTGRES_HOST = "postgresql-rw.storage.svc.cluster.local"
DEFAULT_POSTGRES_PORT = "5432"
DEFAULT_POSTGRES_DB = "poorbricks"

# Base URL of the in-cluster api-server. Worker pod init containers fetch their
# table code from ``{DEFAULT_CODE_API_URL}/v1/code/{prefix}``.
DEFAULT_CODE_API_URL = "http://poorbricks-server.airflow.svc.cluster.local:8080"

__all__ = [
    "DEFAULT_CODE_API_URL",
    "DEFAULT_NAMESPACE",
    "DEFAULT_POSTGRES_CREDS_SECRET_NAME",
    "DEFAULT_POSTGRES_DB",
    "DEFAULT_POSTGRES_HOST",
    "DEFAULT_POSTGRES_PORT",
    "DEFAULT_RUNTIME_SECRET_NAME",
    "DEFAULT_WORKER_IMAGE",
    "DEFAULT_WORKER_IMAGE_REPO",
    "DEFAULT_WORKER_IMAGE_TAG",
]
