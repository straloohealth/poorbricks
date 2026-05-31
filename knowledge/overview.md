# poorbricks — framework overview

poorbricks is a **local-first Spark pipeline framework** using a medallion
architecture (Bronze → Silver → Gold). Deployment is local Spark + local MongoDB
+ local PostgreSQL.

**⚠ Not Databricks.** The repo contains a `databricks.Dockerfile` and the worker
image is named `danielspeixoto/databricks`, but the framework does **not** use
Databricks or Delta Live Tables. `storage="delta"` means Spark memory (test/fixture
mode only), not Delta Lake. Do not suggest DLT or cloud Spark.

## Package name vs import

The published package is `poorbricks-framework` (from the GCP Artifact Registry
`https://us-central1-python.pkg.dev/inner-autonomy-371516/python/simple/`). In
pipeline code it imports as **`framework`**:

```python
from framework import ContractSource, Inputs, pipeline
```

Scripts use `python -m poorbricks.runner` — the public pipeline-code import
surface is `framework`.

## Pipeline layout

Every pipeline lives under `tables/<level>/<source>/<name>/` with **exactly six files**:

| File | Purpose |
|---|---|
| `config.py` | `ValidatedStruct` output schema + `Expectations` thresholds |
| `pipeline.py` | `Inputs` subclass + `@pipeline(...)` function |
| `transform.py` | `def compute(inputs) -> DataFrame` — pure business logic |
| `fixtures.py` | `@scenario("name")` — named test data |
| `test_pipeline.py` | pytest tests on `compute()` output |
| `__init__.py` | empty package marker |

## Storage targets

- `@pipeline(..., storage="delta")` — Spark memory, test/fixture mode **only**
- `@pipeline(..., storage="postgres")` — writes to PostgreSQL via `run_and_persist()`

## Registry key format

Pipelines are keyed as `"<storage>:<table_name>"` (e.g. `"delta:smith_users"`,
`"postgres:dim_patient"`).

## Runner modes

| Mode | Data source | Requires Docker? |
|---|---|---|
| `fixtures` | `@scenario` functions in `fixtures.py` | No |
| `scenario` | Named scenario | No |
| `fault` | Fixtures + injected fault | No |
| `production` | Live MongoDB (recursively runs upstreams) | Yes (MongoDB) |

## Airflow runtime

Generated DAGs run on the shared Airflow. Each task runs:
```bash
poetry run python -m poorbricks.runner --pipeline <key> --mode production
```
Production uses `KubernetesPodOperator`; local dev uses `DockerOperator`.

- DAGs retry by default 2 times; dev DAGs fast-fail.
- Postgres writes are **idempotent** (staging + atomic swap) — a retried or
  spot-evicted task never double-writes.

## Observability (internal, separate from org OTLP)

poorbricks records its own run health in Postgres `poorbricks_meta.run_history`
(via `GET /v1/runs`) and staleness (`GET /v1/staleness`). Anomaly + regression
alerts go through a pluggable sink (`SLACK_WEBHOOK_URL` for Slack). This is
**separate** from the org-wide OpenTelemetry → collector → Tempo/Loki stack.

The FastAPI server runs at `http://poorbricks-server.airflow.svc.cluster.local:8080`
in-cluster. Key endpoints:

| Endpoint | Purpose |
|---|---|
| `POST /v1/upload` | Register DAGs + prune orphaned contracts |
| `GET /v1/runs` | Run history |
| `GET /v1/staleness` | Stale dataset report |

See [uploading-tables.md](uploading-tables.md), [testing-tables-locally.md](testing-tables-locally.md),
and [observability-ui.md](observability-ui.md).
