"""Settings loaded from .env using pydantic."""

from __future__ import annotations

from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuration for MongoDB, contracts store, PostgreSQL, and local paths.

    Load from .env in the repository root.
    """

    mongo_uri: str = "mongodb://localhost:27017"
    contracts_mongo_uri: str | None = None
    contracts_db: str = "poorbricks"
    contracts_collection: str = "data_contracts"
    # Base URL of the poorbricks server. Consumers resolve published
    # contracts over HTTP from here (see utils.contracts.fetch_contract);
    # they never connect to the contracts MongoDB directly.
    contracts_api_url: str = (
        "https://airflow-poorbricks-server-ingress.stingray-ordinal.ts.net"
    )
    # Salt for hashing PII join keys (e.g. cpf) at the bronze boundary so the
    # raw value never reaches Postgres. Override via the PII_HASH_SALT env var
    # in every environment; the default is for local tests only.
    pii_hash_salt: str = "poorbricks-dev-pii-salt"
    delta_output_dir: str = "artifacts/delta"
    tables_root: Path = Path("tables")
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "analytics"
    postgres_user: str = "analytics"
    postgres_password: str = "analytics"

    # Out-of-core execution tuning. The framework processes datasets larger
    # than RAM by partitioning reads, spilling shuffles to disk, and streaming
    # writes — never collecting a whole dataset into the driver.
    spark_master: str = "local[*]"  # all cores; the test suite bounds this
    spark_driver_memory: str = "6g"
    spark_local_dir: str | None = None  # scratch dir for shuffle/sort spill
    read_partitions: int = 64  # partitioned Mongo / JDBC reads — small chunks

    # Environment / dev-namespacing. Workers set POORBRICKS_ENV /
    # POORBRICKS_SCHEMA_SUFFIX; a dev run writes to e.g. ``silver__dev`` so it
    # never touches prod tables, and every run record is stamped with the env.
    environment: str = Field(
        default="prod",
        validation_alias=AliasChoices("POORBRICKS_ENV", "ENVIRONMENT"),
    )
    schema_suffix: str = Field(
        default="",
        validation_alias=AliasChoices("POORBRICKS_SCHEMA_SUFFIX", "SCHEMA_SUFFIX"),
    )

    # Alerting. The webhook is referenced as an env var only — it is injected
    # via the runtime Secret (Vault-managed); no secret value lives in code.
    alert_sink: str = "auto"  # "auto" | "slack" | "noop"
    slack_webhook_url: str | None = None
    alert_min_severity: str = "warn"  # "info" | "warn" | "error"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()

__all__ = ["Settings", "settings"]
