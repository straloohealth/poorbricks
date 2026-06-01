"""GitHub-PR-style line comments on a table's uploaded source.

Comments are stored in MongoDB (collection ``code_comments``, alongside
``data_contracts``), reusing the same Mongo write access the server already has
for contracts. They anchor to a ``(table_name, file, line range)``, are tagged
with the release SHA they were filed in, and persist across releases until a
human deletes them — so a reviewer can flag a bad design for the next developer.

Server-side only (mirrors ``utils.contracts`` — the server owns Mongo access).
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

import pymongo

log = logging.getLogger(__name__)

_COLLECTION = "code_comments"
_index_ready = False


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class CodeComment:
    """One line-anchored source comment (anonymous — no author)."""

    table_name: str
    file: str
    line_start: int
    line_end: int
    body: str
    release_sha: str | None = None
    resolved: bool = False
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: str = field(default_factory=_now_iso)

    def to_public(self) -> dict[str, Any]:
        return asdict(self)


def _coll() -> pymongo.collection.Collection[dict[str, Any]]:
    from poorbricks.settings import settings

    uri = settings.contracts_mongo_uri or settings.mongo_uri
    client: pymongo.MongoClient[dict[str, Any]] = pymongo.MongoClient(uri)
    return client[settings.contracts_db][_COLLECTION]


def _ensure_index(coll: pymongo.collection.Collection[dict[str, Any]]) -> None:
    """Best-effort: create the read index once per process.

    The server's Mongo role can read/write documents but may lack the
    ``createIndex`` (DDL) privilege — that returns ``OperationFailure`` code 13.
    The index is only a query optimization, so a failure must NOT break listing
    or adding comments. Attempt at most once per process and never raise.
    """
    global _index_ready
    if _index_ready:
        return
    _index_ready = True
    try:
        coll.create_index([("table_name", 1), ("file", 1), ("line_start", 1)])
    except pymongo.errors.PyMongoError as exc:
        log.warning("code_comments: skipping index creation (%s)", exc)


def _from_doc(doc: dict[str, Any]) -> CodeComment:
    return CodeComment(
        id=str(doc["_id"]),
        table_name=doc["table_name"],
        file=doc["file"],
        line_start=int(doc["line_start"]),
        line_end=int(doc["line_end"]),
        body=doc["body"],
        release_sha=doc.get("release_sha"),
        resolved=bool(doc.get("resolved", False)),
        created_at=doc.get("created_at") or _now_iso(),
    )


def list_comments(table_name: str, file: str | None = None) -> list[CodeComment]:
    """All comments for a table (optionally one file), in gutter order."""
    coll = _coll()
    _ensure_index(coll)
    query: dict[str, Any] = {"table_name": table_name, "deleted": {"$ne": True}}
    if file is not None:
        query["file"] = file
    cursor = coll.find(query).sort([("file", 1), ("line_start", 1), ("created_at", 1)])
    return [_from_doc(d) for d in cursor]


def add_comment(comment: CodeComment) -> CodeComment:
    """Insert one comment (its ``id`` becomes the Mongo ``_id``)."""
    coll = _coll()
    _ensure_index(coll)
    doc = comment.to_public()
    doc["_id"] = doc.pop("id")
    coll.insert_one(doc)
    return comment


def delete_comment(comment_id: str, table_name: str | None = None) -> bool:
    """Soft-delete one comment by id; returns True if a visible comment matched.

    The server's scoped Mongo role is ``readWriteNoDelete`` — it has no
    destructive REMOVE (org policy keeps deletes out of ephemeral creds) — so
    deletion is an UPDATE that marks the doc ``deleted`` and hides it from
    ``list_comments``. The row stays in Mongo (recoverable), which still
    satisfies the "manually removed" requirement from the user's perspective.
    """
    coll = _coll()
    query: dict[str, Any] = {"_id": comment_id, "deleted": {"$ne": True}}
    if table_name is not None:
        query["table_name"] = table_name
    update = {"$set": {"deleted": True, "deleted_at": _now_iso()}}
    return coll.update_one(query, update).modified_count > 0


__all__ = ["CodeComment", "list_comments", "add_comment", "delete_comment"]
