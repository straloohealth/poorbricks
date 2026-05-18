FROM docker.io/danielspeixoto/databricks:latest

WORKDIR /app

ENV PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=10 \
    POETRY_REQUESTS_TIMEOUT=300

# Postgres JDBC driver for cross-repo ContractSource reads from analytics db.
RUN curl -sSL https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    -o /opt/spark/jars/postgresql-42.7.4.jar

# Deps layer: changes only when pyproject.toml / poetry.lock change.
COPY pyproject.toml poetry.lock /app/
RUN poetry config virtualenvs.in-project true \
 && poetry install --only main --no-interaction --no-root \
 && poetry run pip install fastapi uvicorn httpx google-cloud-storage croniter psycopg2-binary

# Source layer: rebuilds quickly on source changes without re-downloading deps.
COPY poorbricks/ /app/poorbricks/
COPY validation/ /app/validation/
COPY utils/ /app/utils/
COPY api/ /app/api/

RUN poetry run pip install --no-deps .

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH="/app:${PYTHONPATH}" \
    POORBRICKS_API_DAG_STORE=local \
    POORBRICKS_API_DAGS_DIR=/opt/airflow/dags

EXPOSE 8080

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
