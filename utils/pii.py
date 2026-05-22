"""PII hashing helpers.

Sensitive identifiers that are still needed as join keys (e.g. ``cpf``) are
replaced at the bronze boundary with a salted SHA-256 hash, so the raw value
never lands in Postgres while equality joins keep working.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as f


def hash_pii(value_col: Column) -> Column:
    """Return a salted SHA-256 hash of a PII value, usable as a join key.

    The input is normalised (trim + lower-case) before hashing so values that
    differ only in casing/whitespace hash equally. The salt comes from
    ``settings.pii_hash_salt`` (env ``PII_HASH_SALT``). Returns null when the
    input is null or empty after trimming.

    Args:
        value_col: Column holding the raw PII value (e.g. ``cpf``).

    Returns:
        A ``StringType`` column with the 64-char hex hash, or null.
    """
    from poorbricks.settings import settings

    normalized = f.lower(f.trim(value_col.cast("string")))
    hashed = f.sha2(f.concat_ws("", normalized, f.lit(settings.pii_hash_salt)), 256)
    return f.when(normalized.isNotNull() & (f.length(normalized) > 0), hashed)
