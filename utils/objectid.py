"""Derive timestamps from MongoDB ObjectId hex strings.

A MongoDB ``ObjectId`` encodes its creation time in the first 4 bytes (8 hex
characters) as Unix seconds. Bronze tables that mirror a collection with no
explicit ``createdAt`` field can recover a creation timestamp from the
ObjectId, which the framework stores verbatim as the table's string PK.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as f


def objectid_to_timestamp(objectid_col: Column) -> Column:
    """Derive a UTC creation timestamp from a 24-hex-char MongoDB ObjectId.

    The first 8 hex characters of an ObjectId are the creation time in Unix
    seconds. Returns null when the input is null or is not a 24-character
    hex string (so malformed ids never raise).

    Args:
        objectid_col: Column holding the ObjectId as a 24-char hex string.

    Returns:
        A ``TimestampType`` column with the derived creation time.
    """
    hex_str = f.lower(f.trim(objectid_col.cast("string")))
    is_valid = hex_str.rlike(r"^[0-9a-f]{24}$")
    seconds = f.conv(f.substring(hex_str, 1, 8), 16, 10).cast("long")
    return f.when(is_valid, f.timestamp_seconds(seconds))
