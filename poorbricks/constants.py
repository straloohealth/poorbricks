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

# Analytics writers go through the PgBouncer transaction-mode pooler
# (infra-storage postgresql-pooler.yaml), NOT the cluster's -rw endpoint
# directly: a DAG wave of 17-19 worker pods each open short-lived COPY
# connections per Spark partition, which can exhaust the shared Postgres and
# starve the Airflow metadata DB. The write path is per-statement autocommit
# over persistent staging tables (no cross-statement session state), so it is
# transaction-pooling-safe. Airflow's own metadata DB stays DIRECT on
# postgresql-rw (transaction pooling breaks Airflow) — that connection comes
# from the airflow-postgresql Secret, not this constant.
DEFAULT_POSTGRES_HOST = "postgresql-pooler-rw.storage.svc.cluster.local"
DEFAULT_POSTGRES_PORT = "5432"
DEFAULT_POSTGRES_DB = "poorbricks"

# Base URL of the in-cluster api-server. Worker pod init containers fetch their
# table code from ``{DEFAULT_CODE_API_URL}/v1/code/{prefix}``.
DEFAULT_CODE_API_URL = "http://poorbricks-server.airflow.svc.cluster.local:8080"

# Worker pods have no Tailscale, so ContractSource must resolve contracts over
# the in-cluster Service rather than the default *.ts.net endpoint. Same host
# as the code api-server (one poorbricks server), surfaced separately so the
# DAG generator can parameterize it per environment.
DEFAULT_CONTRACTS_API_URL = "http://poorbricks-server.airflow.svc.cluster.local:8080"

# Dev/test environment defaults (see env-namespaced dev uploads).
DEV_PREFIX_TEMPLATE = "dev-{repo}"
DEV_SCHEMA_SUFFIX = "__dev"

__all__ = [
    "DEFAULT_CODE_API_URL",
    "DEFAULT_CONTRACTS_API_URL",
    "DEFAULT_NAMESPACE",
    "DEFAULT_POSTGRES_CREDS_SECRET_NAME",
    "DEFAULT_POSTGRES_DB",
    "DEFAULT_POSTGRES_HOST",
    "DEFAULT_POSTGRES_PORT",
    "DEFAULT_RUNTIME_SECRET_NAME",
    "DEFAULT_WORKER_IMAGE",
    "DEFAULT_WORKER_IMAGE_REPO",
    "DEFAULT_WORKER_IMAGE_TAG",
    "DEV_PREFIX_TEMPLATE",
    "DEV_SCHEMA_SUFFIX",
]
