FROM docker.io/danielspeixoto/databricks:latest

WORKDIR /app

COPY pyproject.toml poetry.lock /app/
COPY poorbricks/ /app/poorbricks/
COPY validation/ /app/validation/
COPY utils/ /app/utils/
COPY api/ /app/api/

RUN poetry install --only main --no-interaction --no-root \
    && poetry run pip install fastapi uvicorn httpx google-cloud-storage croniter

ENV POORBRICKS_API_DAG_STORE=gcs \
    POORBRICKS_API_DAGS_BUCKET=poorbricks-airflow-dags

EXPOSE 8080

CMD ["poetry", "run", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
