# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
poetry install

# Run all tests (parallelized, 4 workers)
poetry run pytest

# Run a single test file
poetry run pytest tables/silver/dim_patient/test_pipeline.py -v

# Run a single test
poetry run pytest tables/silver/dim_patient/test_pipeline.py::TestDimPatient::test_nominal -v

# Run tests by marker
poetry run pytest -m "not integration"   # skip integration tests
poetry run pytest -m spark               # only Spark tests

# Type checking
poetry run mypy

# Linting
poetry run ruff check .
poetry run ruff format .

# Pre-commit hooks (run automatically on git commit)
# First-time setup:
poetry run pre-commit install

# Manual run (before committing):
poetry run pre-commit run --all-files

# Start local services (MongoDB, PostgreSQL)
docker-compose up -d

# Run a pipeline locally against fixture data
poetry run python -m poorbricks.runner --pipeline delta:smith_users --mode fixtures

# Compute all pipelines, write to PostgreSQL, and push contracts (fixtures mode)
poetry run pytest tests/test_distributed_pipeline.py -m integration -n 0 -v

# Discover registered pipelines
poetry run python -c "from poorbricks import discover_all_pipelines, list_pipelines; discover_all_pipelines(); print(list_pipelines())"

# Validate a pipeline's architecture and upstream contracts without computing
poetry run python -c "
from poorbricks import run
result = run('postgres:dim_patient', mode='validate')
print(result.errors or ['OK'])
"

# Browse contracts + run tests in the Streamlit UI
poetry run streamlit run streamlit_app/app.py

# Cross-table contract check (lineage-driven): fail if an upstream contract
# change dropped/retyped a column a downstream consumes. Also runs
# automatically at the end of `pytest` via the bundled pytest plugin.
poetry run poorbricks verify --mode contract

# Alert on pipelines that stopped running (reads run history + DAG cadences)
poetry run poorbricks monitor-staleness

# Upload to a DEV environment: namespaces the DAG as `dev-<prefix>` and writes
# to a `*__dev` Postgres schema — runs on the shared Airflow without touching
# prod tables or contracts.
poetry run poorbricks upload --env dev --prefix <repo> --sha <sha> --watch

# Local web-debug loop (server + Streamlit against compose Mongo/Postgres)
docker-compose --profile dev up
```

## Resilience & observability

Every `run_and_persist` is instrumented (see `poorbricks/persist.py`):

- **Run history** — each run is recorded in `poorbricks_meta.run_history`
  (Postgres) via `poorbricks/run_history.py`; a denormalized `last_run` is also
  written into the Mongo contract. Exposed at `GET /v1/runs`.
- **Column lineage** — captured at runtime from the Spark analyzed plan
  (`poorbricks/lineage_runtime.py`) and stored in the contract `lineage` field;
  consumed by `verify --mode contract`.
- **Row-count anomaly** (`poorbricks/anomaly.py`) + **regression-vs-prior**
  (`poorbricks/regression/prior.py`, snapshots to `poorbricks_meta.<t>__prev`) +
  **stale-data** (`poorbricks/staleness.py`, `GET /v1/staleness`) — alerts go
  through a pluggable sink (`poorbricks/alerting.py`; Slack via
  `SLACK_WEBHOOK_URL`, no-op under tests). Tune per pipeline via the
  `ROW_COUNT_ANOMALY_*` / `REGRESSION_*` attributes on `Expectations`.
- **Arch checks** — `verify --mode arch` now also fails on stub columns; literal
  columns are flagged `is_literal` on the contract (info badge in the UI).
- **Pipeline removal** — `POST /v1/upload` prunes both DAGs and orphaned
  contracts for the repo's prefix (a contract still consumed elsewhere is kept
  with a warning).
- **Spot resilience** — generated DAGs retry (default 2; dev DAGs fast-fail);
  the Postgres write is idempotent (staging + atomic swap), so a retried/evicted
  task never double-writes.

## Architecture

This is a **local-first Spark pipeline framework** using a medallion architecture:
- **Bronze** — Source from MongoDB, minimal transformation
- **Silver** — Business logic, read bronze via MongoDB contracts store, write to PostgreSQL
- **Gold** — Analytics tables (not yet implemented)

Deployment: Local Spark + local MongoDB + local PostgreSQL. No Databricks, no DLT.

### Module Map

```
poorbricks/      Core pipeline system (decorator, registry, runner, persist, arch)
validation/      Schema validation (ValidatedStruct, Expectations, rules)
tables/          Pipeline implementations (bronze/smith/, silver/)
utils/           MongoDB reader, PostgreSQL writer, Spark helpers, utilities
tests/           Integration tests (multi-repo, distributed pipeline, wheel install)
docker-compose.yml  Local services: MongoDB 7, PostgreSQL 16
```

### How a Pipeline Is Declared

Every pipeline lives in its own directory under `tables/<level>/<source>/<name>/` with exactly six files:

| File | Purpose |
|---|---|
| `config.py` | `ValidatedStruct` subclass (output schema) + `Expectations` subclass (health thresholds) |
| `pipeline.py` | `Inputs` subclass declaring upstreams + `@pipeline(...)` function |
| `transform.py` | `def compute(inputs) -> DataFrame` — pure business logic |
| `fixtures.py` | `@scenario("name") def ...() -> Inputs` — named test scenarios |
| `test_pipeline.py` | pytest tests asserting on `compute(inputs)` output |
| `__init__.py` | Empty package marker |

```python
# config.py — defines the output contract
class DimPatient(ValidatedStruct):
    patient_id: str
    name: str | None
    is_active: bool

class DimPatientExpectations(Expectations):
    MIN_ROWS = 100
    UNIQUE_KEYS = [["patient_id"]]

# pipeline.py — wires inputs and registers with framework
from framework import ContractSource, Inputs, pipeline

class DimPatientInputs(Inputs):
    smith_users: Annotated[DataFrame, ContractSource("smith.users")]

@pipeline(
    name="dim_patient",
    model=DimPatient,
    level="silver",
    storage="postgres",  # write to PostgreSQL
    comment="..."
)
def dim_patient(inputs: DimPatientInputs) -> DataFrame:
    return compute(inputs)

# transform.py — testable in isolation
def compute(inputs: DimPatientInputs) -> DataFrame:
    return inputs.smith_users.select(...)

# fixtures.py — self-contained test data
@scenario("nominal")
def nominal() -> DimPatientInputs:
    rows = [{"patient_id": "p1", "name": "Alice"}]
    return DimPatientInputs.from_rows({"smith_users": rows})
```

### Data Sources

**Bronze pipelines** read from **MongoDB** via `MongoSource`:
```python
class SmithUsersInputs(Inputs):
    upstream: Annotated[
        DataFrame,
        MongoSource(db="smith", collection="users", schema=SmithUserBronze.to_struct())
    ]
```

**Silver pipelines** read from **MongoDB contracts store** via `ContractSource`:
```python
class DimPatientInputs(Inputs):
    smith_users: Annotated[DataFrame, ContractSource("smith.users")]
```

The `ContractSource` dynamically fetches schema + example rows from `poorbricks_contracts.data_contracts` at runtime, enabling cross-pipeline consumption without model imports.

### Storage Targets

- `@pipeline(..., storage="delta")` — writes to Spark memory (test/fixture mode only)
- `@pipeline(..., storage="postgres")` — writes to PostgreSQL via `run_and_persist()`

### Runner Modes

`framework.runner.run(pipeline_key, mode, scenario_name, fault_name)` supports:

| Mode | Data source | Requires Docker? |
|---|---|---|
| `fixtures` | `@scenario` functions in `fixtures.py` | No |
| `scenario` | Named scenario | No |
| `fault` | Fixtures + injected fault (`null_required_columns`, `duplicate_keys`, `empty_inputs`) | No |
| `production` | All inputs from live MongoDB; recursively runs upstream pipelines | Yes (MongoDB) |

### ValidatedStruct (from `validation/schema.py`)

Pydantic `BaseModel` subclass that additionally:
- Generates a `StructType` via `.to_struct()` from Python type annotations
- Runs validation rules via `.verify(df)` (NotNullRule, StringLengthRule, etc.)
- Type mapping: `str→StringType`, `int→LongType`, `float→DoubleType`, `bool→BooleanType`, `datetime→TimestampType`, `date→DateType`, Optional/None→nullable

### Local PostgreSQL

Start PostgreSQL and MongoDB:
```bash
docker-compose up -d
```

Compute and persist all pipelines to PostgreSQL + MongoDB (fixtures mode):
```bash
poetry run pytest tests/test_distributed_pipeline.py -m integration -n 0 -v
```

Query PostgreSQL directly:
```bash
docker exec -it <container-name> psql -U analytics -d analytics
SELECT * FROM silver.dim_patient LIMIT 10;
```

### Registry and Discovery

`framework/registry.py` holds a module-level dict of `PipelineMeta` objects keyed as `"<storage>:<table_name>"` (e.g. `"delta:smith_users"`, `"postgres:dim_patient"`).

`framework/discovery.py::discover_all_pipelines()` walks `tables/**/pipeline.py`, imports each module, and populates the registry. Call this before accessing `list_pipelines()` or `all_pipelines()` in scripts.

### Configuration

Settings loaded from `.env` (or environment variables) via `pydantic-settings`:
```env
MONGO_URI=mongodb://localhost:27017
CONTRACTS_DB=poorbricks_contracts
CONTRACTS_COLLECTION=data_contracts
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=analytics
POSTGRES_USER=analytics
POSTGRES_PASSWORD=analytics
```
