# poorbricks-framework

Local-first Spark pipeline framework for PySpark pipeline development (medallion architecture: Bronze → Silver → Gold).

## Installing the package

The package is published to GCP Artifact Registry. You need to authenticate before pip or Poetry can resolve it.

### Credentials required

#### Local development

Authenticate once with your Google account:

```bash
gcloud auth application-default login
```

This writes credentials to `~/.config/gcloud/application_default_credentials.json`. They are picked up automatically by pip's GCP keyring helper.

You also need the `keyrings.google-artifactregistry-auth` helper installed in whichever Python environment runs pip/Poetry:

```bash
pip install keyrings.google-artifactregistry-auth
```

#### CircleCI

Add the **`gke` context** and use the `tools/install-python-deps` orb command. It handles GCP authentication, credential injection, and caching automatically — no manual setup needed.

First, add the private source to your `pyproject.toml`:

```toml
[[tool.poetry.source]]
name = "gcp"
url = "https://us-central1-python.pkg.dev/inner-autonomy-371516/python/simple/"
priority = "supplemental"
```

Then in your CircleCI config:

```yaml
orbs:
  tools: straloohealth/tools@3.0.22

jobs:
  my-job:
    docker:
      - image: docker.io/danielspeixoto/python
    steps:
      - checkout
      - tools/install-python-deps   # GCP auth + cache + poetry install
      - run: poetry run pytest
    # Must use the gke context so GOOGLE_SERVICE_ACCOUNT is available
    context:
      - gke
```

`tools/install-python-deps` accepts optional parameters:

| Parameter | Default | Description |
|---|---|---|
| `install-command` | `poetry install --no-interaction` | Override for custom flags |
| `cache-version` | `v1` | Bump to bust the cache |
| `cache-path` | `/root/.cache/pypoetry/virtualenvs` | Venv directory to cache |
| `source-name` | `gcp` | Must match your `[[tool.poetry.source]]` name |

### pip

```bash
pip install keyrings.google-artifactregistry-auth
pip install poorbricks-framework \
  --index-url https://us-central1-python.pkg.dev/inner-autonomy-371516/python/simple/
```

### Poetry

Add the private source to your `pyproject.toml`:

```toml
[[tool.poetry.source]]
name = "gcp"
url = "https://us-central1-python.pkg.dev/inner-autonomy-371516/python/simple/"
priority = "supplemental"
```

Then add the dependency and install:

```bash
poetry add poorbricks-framework --source gcp
poetry install
```

Poetry uses the same GCP keyring helper for auth — make sure `keyrings.google-artifactregistry-auth` is installed in the active environment before running `poetry install`.

## Publishing a new version

Publishing runs automatically on every merge to `main` via the `tools/publish-python-package` orb job in the `deploy` workflow. Each build is versioned as `{base}+{sha7}` (e.g. `0.1.0+abc1234`).

To publish manually:

```bash
poetry build
gcloud auth application-default login   # if not already done
pip install twine
twine upload \
  --repository-url https://us-central1-python.pkg.dev/inner-autonomy-371516/python/ \
  --username oauth2accesstoken \
  --password "$(gcloud auth print-access-token)" \
  dist/*
```

## Development

```bash
# Install all dependencies
poetry install

# Run all tests
poetry run pytest

# Lint + format check
poetry run ruff check .
poetry run ruff format --check .

# Type check
poetry run mypy poorbricks/ utils/ validation/

# Start local services (MongoDB, PostgreSQL)
docker-compose up -d
```

See [CLAUDE.md](CLAUDE.md) for the full architecture reference.
