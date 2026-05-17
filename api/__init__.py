"""FastAPI upload service.

Receives a tarball of ``tables/`` + ``workflows/`` from a table-repo,
runs the full verification suite, generates Airflow DAGs, and writes
them to the configured DAG store.
"""
