"""Random-sample documents from a MongoDB collection via the ``$sample`` stage.

Used by the poorbricks-server ``/v1/db-contract`` endpoint to draw a
representative slice of a live collection. Complements
``verify._sample_mongo_collection`` (oldest+newest) — that one targets schema
*extremes* for drift detection; this one wants a uniform random sample.
"""

from __future__ import annotations

from typing import Any


class EmptyCollectionError(Exception):
    """Raised when a collection yields no documents to sample."""


def _public_uri(mongo_uri: str) -> str:
    """Strip the Atlas private-peering suffix (-pri) so the public endpoint is used."""
    return mongo_uri.replace("-pri.", ".")


def sample_random_docs(
    mongo_uri: str, db: str, collection: str, sample_size: int = 1000
) -> list[dict[str, Any]]:
    """Return up to ``sample_size`` uniformly-random docs, deduplicated by ``_id``.

    Uses the MongoDB ``$sample`` aggregation stage. Raises
    :class:`EmptyCollectionError` when the collection has no documents.
    """
    import pymongo

    client: pymongo.MongoClient[dict[str, Any]] = pymongo.MongoClient(
        _public_uri(mongo_uri)
    )
    try:
        raw = list(
            client[db][collection].aggregate(
                [{"$sample": {"size": sample_size}}], allowDiskUse=True
            )
        )
    finally:
        client.close()

    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for doc in raw:
        key = str(doc.get("_id", id(doc)))
        if key not in seen:
            seen.add(key)
            out.append(doc)
    if not out:
        raise EmptyCollectionError(f"{db}.{collection} is empty — nothing to sample")
    return out


__all__ = ["EmptyCollectionError", "sample_random_docs"]
