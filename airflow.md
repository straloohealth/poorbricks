# Plan: table-repo with contract validation + Airflow DAG generation

## Context

Two-repo model for decentralized table authoring:
- **`framework-repo`** — `poorbricks` library + Airflow infrastructure
- **`table-repo`** (many) — pipeline code + workflow YAMLs, uses framework as a lib

Goals:
1. A developer on `table-repo` can detect contract errors from their local machine.
2. CI can run the full pipeline, verify expectations, and export profiler results.
3. CI generates Airflow DAG files and writes them to a DAG store.

Test case: migrate the gold `patients` table from `framework-repo` to `table-repo`.

Default worker image: `docker.io/danielspeixoto/databricks` (defined in `constants.py`).

---

## New CLI commands (both added to `poorbricks`)

| Command | When | What it does |
|---|---|---|
| `poorbricks verify --mode local` | Developer on local machine | Schema drift check against contracts store — fast, no Spark needed |
| `poorbricks verify --mode ci` | CI (data available) | Full pipeline execution, expectations check, profiler export — no write |
| `poorbricks upload-workflows` | CI after verify passes | Generate Airflow DAG files and write to DAG store |

---

## Files to create / modify

### framework-repo

| File | Action | Notes |
|---|---|---|
| `pyproject.toml` | `package-mode = true`, expose packages | Enables `table-repo` to install as a lib |
| `constants.py` | Add `DEFAULT_WORKER_IMAGE` | Reuse existing file |
| `poorbricks/discovery.py` | Configurable tables root | Default = `CWD/tables/` |
| `poorbricks/verify.py` | **NEW** — `verify_local()`, `verify_ci()` | Core verification logic |
| `poorbricks/airflow/__init__.py` | New module marker | |
| `poorbricks/airflow/workflow.py` | `WorkflowConfig`, `TaskConfig` | YAML parsing |
| `poorbricks/airflow/dag_generator.py` | `generate_dag_file()` | Docker vs K8s operator |
| `poorbricks/airflow/dag_store.py` | `LocalDagStore` (+ `GcsDagStore` stub) | Write DAG files |
| `poorbricks/airflow/cli.py` | `upload-workflows` CLI | Verify then upload |
| `poorbricks/cli.py` | **NEW** — `verify` CLI entry point | |
| `pyproject.toml` (scripts) | Add `poorbricks-verify` and `poorbricks-upload-workflows` entry points | |
| `docker-compose.yml` | Add Airflow services | Local dev Airflow |

### table-repo (test/reference)

| File | Purpose |
|---|---|
| `tables/patients/pipeline.py` | Gold `patients` table (migrated from framework-repo) |
| `tables/patients/config.py` | `PatientGold` + `PatientGoldExpectations` |
| `tables/patients/transform.py` | `compute()` |
| `tables/patients/fixtures.py` | `@scenario("nominal")` |
| `tables/patients/test_pipeline.py` | Fixture-based tests |
| `workflows/gold_patients.yaml` | Workflow YAML triggering the gold pipeline |
| `pyproject.toml` | `poorbricks-framework` as a path/git dep |

---

## Step 1 — Make framework-repo installable

**`pyproject.toml`**:
```toml
package-mode = true
packages = [
  { include = "poorbricks" },
  { include = "validation" },
  { include = "utils" },
]
```

---

## Step 2 — Configurable discovery

**`poorbricks/discovery.py`** — replace hardcoded `REPO_ROOT` / `PIPELINES_ROOT`:

```python
def _resolve_roots(override: Path | None) -> tuple[Path, Path]:
    if override is not None:
        return override.parent, override
    env = os.environ.get("TABLES_ROOT")
    if env:
        p = Path(env).resolve()
        return p.parent, p
    cwd = Path.cwd()
    return cwd, cwd / "tables"

def discover_all_pipelines(tables_root: Path | None = None) -> None:
    repo_root, pipelines_root = _resolve_roots(tables_root)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    for pipeline_path in sorted(pipelines_root.rglob("pipeline.py")):
        ...  # rest unchanged
```

---

## Step 3 — `poorbricks verify`

### Local mode (`--mode local`)

Fast. Requires MongoDB connection (contracts store only, no data). For each pipeline:
1. Inspect `Inputs` class annotations for `ContractSource` fields
2. `fetch_contract(table_name)` from MongoDB
3. Compare published `schema_json` against the local pipeline's expected schema (the `ValidatedStruct.to_struct()` of the source model declared via `ContractSource`)
4. Report schema additions, removals, and type changes (reuses `drift.py` schema comparison logic)

```
$ poetry run poorbricks-verify --mode local

✓ patients: dim_patient contract OK (schema matches)
✗ patients: expected field 'email' (StringType) not found in published dim_patient contract
  Contract updated: 2026-05-14 — run: poorbricks push-contract --pipeline dim_patient
```

No Spark needed. Exits non-zero if any contract error is found.

### CI mode (`--mode ci`)

Full execution. Runs each pipeline in `production` mode, checks expectations, exports profiler:

1. `discover_all_pipelines()`
2. For each pipeline: `run(pipeline_key, mode="production")` — returns `DataFrame`, does NOT write
3. `meta.model.verify(df)` — NotNullRule, StringLengthRule, etc.
4. Check `Expectations` (MIN_ROWS, UNIQUE_KEYS, NON_NULL_COLUMNS, NULL_RATE_MAX, ENUM_VALUES)
5. `profile_dataframe(df)` → write to `--export-dir/<table_name>.json`
6. `check_drift(table_name, df)` → report data drift vs published baseline (uses `poorbricks/drift.py`)

```
$ poetry run poorbricks-verify --mode ci --export-dir artifacts/profiles

✓ patients: 1500 rows | expectations OK | profile exported
✗ patients: MIN_ROWS=100 but got 45 rows
✗ patients: drift detected — 'email' null rate 0% → 34%
```

Exits non-zero if any expectation fails or drift is detected above threshold.

**Key**: the runner already returns a `DataFrame` without writing in `production` mode — no new runner changes needed.

### `poorbricks/verify.py` (new file)

Public interface:
```python
def verify_local() -> list[ContractError]: ...
def verify_ci(export_dir: Path | None = None) -> list[VerificationError]: ...
```

Reuses:
- `poorbricks/drift.py::check_drift()` — schema + data drift detection
- `utils/contracts.py::fetch_contract()`, `profile_dataframe()` — contract fetch + profiling
- `poorbricks/runner.py::run()` — pipeline execution (CI mode)
- `validation/schema.py::ValidatedStruct.verify()` — rule enforcement

---

## Step 4 — Workflow YAML + DAG generation

### Workflow YAML format

```yaml
# workflows/gold_patients.yaml
name: gold_patients
schedule: "0 2 * * *"
# image: optional — defaults to docker.io/danielspeixoto/databricks

tasks:
  - id: patients
    pipeline: postgres:patients
```

`image` field optional. If omitted, uses `constants.DEFAULT_WORKER_IMAGE`.
Multi-task example with dependencies:
```yaml
tasks:
  - id: dim_user_activity
    pipeline: postgres:dim_user_activity

  - id: gold_summary
    pipeline: postgres:gold_summary
    depends_on: [dim_user_activity]
```

### DAG generator

`poorbricks/airflow/dag_generator.py::generate_dag_file(workflow: WorkflowConfig) -> str`

Generates Python source for an Airflow DAG. For local dev: `DockerOperator`. For production: `KubernetesPodOperator` (selected via `AIRFLOW_OPERATOR=kubernetes` env var).

Each task runs:
```
poetry run python -m poorbricks.runner --pipeline <key> --mode production
```

### DAG store abstraction

```python
# poorbricks/airflow/dag_store.py
class LocalDagStore:
    def write(self, dag_name: str, dag_content: str) -> None:
        (self.dags_dir / f"{dag_name}.py").write_text(dag_content)

# GcsDagStore — deferred (production)
```

### `poorbricks-upload-workflows` CLI

```
# Local dev — writes to ./dags/ (mounted by Docker Compose Airflow)
poetry run poorbricks-upload-workflows \
  --workflows-dir workflows/ \
  --dags-dir ./dags \
  [--image-tag dev] \
  [--verify] \       # run poorbricks verify --mode local first
  [--dry-run]        # print generated DAG, don't write

# CI — same but with full verify
poetry run poorbricks-upload-workflows \
  --workflows-dir workflows/ \
  --dags-dir ./dags \
  --image-tag $SHA \
  --verify           # triggers verify --mode ci + export profiles
```

`--verify` gate: upload only proceeds if all referenced pipelines exist in the registry and all verifications pass.

---

## Step 5 — Local Airflow (Docker Compose)

Extend `framework-repo/docker-compose.yml` with Airflow services (webserver + scheduler + metadata Postgres):

```yaml
  airflow-db:
    image: postgres:16
    environment: {POSTGRES_DB: airflow, POSTGRES_USER: airflow, POSTGRES_PASSWORD: airflow}

  airflow-webserver:
    image: apache/airflow:2.9.0
    ports: ["8080:8080"]
    volumes:
      - ./dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock   # DockerOperator
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-db/airflow

  airflow-scheduler:
    image: apache/airflow:2.9.0
    volumes:
      - ./dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock
```

`./dags/` is the shared volume. `poorbricks-upload-workflows --dags-dir ./dags` writes there.

---

## Step 6 — Gold table migration (test case)

Move `framework-repo/tables/gold/patients/` → `table-repo/tables/patients/` (no nesting required).

Update `pipeline.py` in `table-repo` to import from `poorbricks` (already the case).
Update `pyproject.toml` in `table-repo`:
```toml
[tool.poetry.dependencies]
poorbricks-framework = {path = "../framework-repo", develop = true}
```

Then verify the end-to-end flow works:
1. `poetry run poorbricks-verify --mode local` — should confirm `dim_patient` contract is present
2. `poetry run pytest tables/ -v` — gold table tests pass with fixtures
3. `poetry run poorbricks-upload-workflows --workflows-dir workflows/ --dags-dir ../framework-repo/dags --dry-run`

---

## Deferred

- `GcsDagStore` + Airflow Helm chart on GKE
- `KubernetesPodOperator` generator path
- `--bucket` flag in `upload-workflows`

---

## Verification

1. `poetry run pytest tables/ -v` in `table-repo` — all pass, no MongoDB
2. `poetry run poorbricks-verify --mode local` — contract check for gold patients
3. `poetry run poorbricks-upload-workflows --dry-run` — prints valid Python DAG
4. `docker-compose up -d && poetry run poorbricks-upload-workflows --dags-dir ./dags` → DAG in Airflow at `localhost:8080`
5. `poetry run pre-commit run --all-files` — passes

---

## Measurement

- `verify --mode local` exits 0 when all upstream contracts match ✓
- `verify --mode ci` exports a `<table>.json` profiler file per pipeline ✓
- Upload is blocked if `verify` fails ✓
- `list_pipelines()` in `table-repo` shows migrated gold pipeline ✓
- `framework-repo` existing tests pass after discovery change ✓
