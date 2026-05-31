# poorbricks — testing tables locally

## Quick start

```bash
# Run the full test suite (parallelized, 4 workers)
poetry run pytest

# Skip integration tests (no Docker needed)
poetry run pytest -m "not integration"

# Run a single pipeline against fixture data
poetry run python -m poorbricks.runner --pipeline delta:smith_users --mode fixtures

# Cross-table contract check (lineage-driven)
poetry run poorbricks verify --mode contract

# Architecture check (naming, required files, stub columns, literal flags)
poetry run poorbricks verify --mode arch
```

## Runner modes — which need Docker

| Mode | Needs local services? | What it runs |
|---|---|---|
| `fixtures` | No | `@scenario` functions in `fixtures.py` |
| `scenario` | No | A named scenario |
| `fault` | No | Fixtures + injected fault (null_required_columns, duplicate_keys, empty_inputs) |
| `production` | Yes (MongoDB) | All inputs from live MongoDB; runs upstreams recursively |

## Starting local services

```bash
docker-compose up -d   # MongoDB 7 + PostgreSQL 16
```

Full integration test (computes all pipelines, writes to Postgres + MongoDB,
fixtures mode):

```bash
poetry run pytest tests/test_distributed_pipeline.py -m integration -n 0 -v
```

## `poorbricks verify` modes

### `--mode arch`
Checks structure, naming, required files (the six-file layout), stub columns
(columns that exist in the schema but have no lineage), and `is_literal` columns
(flagged info in the UI). Runs in CI on every branch.

### `--mode ci --ci-mode fixtures`
Runs every pipeline's full compute against local fixture data and exports profiler
output. This is what the `tools/test-poorbricks-tables` CI job runs.

### `--mode local`
Requires published contracts — skipped in CI; run locally against a live server.

### `--mode contract`
Lineage-driven cross-table contract check: fails if an upstream contract change
drops or retypes a column a downstream pipeline consumes. Also runs automatically
at the end of `pytest` via the bundled pytest plugin.

### `--mode db`
Runs every `MongoSource` pipeline against a DB-derived synthetic contract.
Reads `CONTRACTS_API_URL` (or `--contract-url`) for the db-contract endpoint.

## CI job reference

The `tools/test-poorbricks-tables` orb job (and the Jenkins `testPoorbricksTables()`
step) runs `--mode arch`, then `--mode ci --ci-mode fixtures` (with `--ci-mode
fixtures`), then skips `--mode local`. The `contract` and `db` modes are local-only.
