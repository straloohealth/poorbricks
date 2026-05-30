"""Integration tests for the Mongo-backed code-comment store (local Mongo)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from poorbricks.code_comments import (
    CodeComment,
    add_comment,
    delete_comment,
    list_comments,
)

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("code_comments_dao")]

_TABLE = "_cc_test_table"


def _purge() -> None:
    from poorbricks.code_comments import _coll

    _coll().delete_many({"table_name": _TABLE})


@pytest.fixture
def clean() -> Iterator[None]:
    _purge()
    yield
    _purge()


def _mk(file: str, line: int, body: str, sha: str | None = "abc1234") -> CodeComment:
    return CodeComment(
        table_name=_TABLE,
        file=file,
        line_start=line,
        line_end=line,
        body=body,
        release_sha=sha,
    )


def test_add_list_delete_round_trip(clean: None) -> None:
    a = add_comment(_mk("transform.py", 10, "magic number"))
    add_comment(_mk("transform.py", 3, "O(n^2) join"))
    add_comment(_mk("config.py", 5, "stale threshold"))

    all_for_table = list_comments(_TABLE)
    assert len(all_for_table) == 3
    # Gutter order: by file, then line_start.
    assert [(c.file, c.line_start) for c in all_for_table] == [
        ("config.py", 5),
        ("transform.py", 3),
        ("transform.py", 10),
    ]

    # Filter by file.
    only_transform = list_comments(_TABLE, file="transform.py")
    assert {c.line_start for c in only_transform} == {3, 10}

    # The created comment carries an id, sha, and timestamp.
    assert a.id and a.release_sha == "abc1234" and a.created_at

    # Delete one; a wrong-table delete is a no-op.
    assert delete_comment(a.id, table_name="_other") is False
    assert delete_comment(a.id, table_name=_TABLE) is True
    assert len(list_comments(_TABLE)) == 2
    assert delete_comment("does-not-exist") is False
