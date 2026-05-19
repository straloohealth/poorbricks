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

Add the **`gke` context** to your job. It supplies `GOOGLE_SERVICE_ACCOUNT`, which the `deployer` CLI uses to activate the `circleci` service account — the same account that publishes the package and has `roles/artifactregistry.reader` on the registry.

```yaml
jobs:
  my-job:
    steps:
      - run: deployer auth gke   # activates circleci SA → gcloud ADC
      - run: pip install keyrings.google-artifactregistry-auth
      - run: pip install poorbricks-framework --index-url https://us-central1-python.pkg.dev/inner-autonomy-371516/python/simple/
```

Or, if you configure Poetry sources (see below), just `poetry install`.

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

Publishing runs automatically on every merge to `main` via CircleCI (`publish-package` job in the `deploy` workflow). To publish manually:

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

Bump the version in `pyproject.toml` before publishing to avoid a conflict with an existing version.

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
