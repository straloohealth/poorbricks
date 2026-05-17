"""Production-deployment constants shared between the CLI, API, and DAG generator.

These values flow into generated Airflow DAGs (image, namespace) and are
used as defaults by the FastAPI upload service.
"""

from __future__ import annotations

DEFAULT_WORKER_IMAGE_REPO = "docker.io/danielspeixoto/databricks"
DEFAULT_WORKER_IMAGE_TAG = "latest"
DEFAULT_WORKER_IMAGE = f"{DEFAULT_WORKER_IMAGE_REPO}:{DEFAULT_WORKER_IMAGE_TAG}"

DEFAULT_NAMESPACE = "poorbricks-workers"
DEFAULT_DAGS_BUCKET = "poorbricks-airflow-dags"
DEFAULT_RUNTIME_SECRET_NAME = "poorbricks-runtime"

__all__ = [
    "DEFAULT_DAGS_BUCKET",
    "DEFAULT_NAMESPACE",
    "DEFAULT_RUNTIME_SECRET_NAME",
    "DEFAULT_WORKER_IMAGE",
    "DEFAULT_WORKER_IMAGE_REPO",
    "DEFAULT_WORKER_IMAGE_TAG",
]
