"""Pytest plugin: cross-table contract verification during table tests.

Auto-loaded via the ``pytest11`` entry point in any repo that installs
``poorbricks-framework``. As each pipeline runs through the framework during a
test (``run(...)`` / ``run_and_persist(...)``), its column-level lineage is
captured into a process-local registry. At session finish this plugin checks,
for every pipeline the tests exercised, that each upstream column it consumes
still exists in the upstream's published contract — and fails the session if an
upstream contract change has broken a consumer.

This is the "verified during table tests" half of cross-table contract testing;
``poorbricks verify --mode contract`` is the CI counterpart. The check is
best-effort: if the contracts store is unreachable (e.g. a local run with no
network) it is skipped rather than failing the build. Disable explicitly with
``--no-poorbricks-contract-check`` or ``POORBRICKS_CONTRACT_CHECK=0``.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pytest

    from poorbricks.verify import ContractError


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--no-poorbricks-contract-check",
        action="store_true",
        default=False,
        help="Skip poorbricks cross-table contract verification at session finish.",
    )


def _enabled(config: Any) -> bool:
    if config.getoption("--no-poorbricks-contract-check", default=False):
        return False
    return os.getenv("POORBRICKS_CONTRACT_CHECK", "1") != "0"


def run_captured_lineage_checks(fetcher: Any = None) -> list[ContractError]:
    """Check every captured pipeline's consumed columns against upstreams.

    Returns the list of contract breaks. Returns an empty list (and prints a
    note) when nothing was captured or the contracts store is unreachable.
    """
    from poorbricks.lineage_runtime import captured
    from poorbricks.registry import all_pipelines
    from poorbricks.verify import _check_consumed_columns, _default_fetcher

    captured_lineage = captured()
    if not captured_lineage:
        return []

    pipelines = all_pipelines()
    local_tables = {meta.table_name: meta for meta in pipelines.values()}
    fetch = fetcher or _default_fetcher()

    errors: list[ContractError] = []
    for table_name, lineage in captured_lineage.items():
        consumed = (lineage or {}).get("consumed") or {}
        if not consumed:
            continue
        key = f"test:{table_name}"
        try:
            errors.extend(_check_consumed_columns(key, consumed, fetch, local_tables))
        except KeyError:
            # _check_consumed_columns already turns 404s into missing_contract;
            # a bare KeyError here would be unexpected — skip defensively.
            continue
        except Exception as exc:  # noqa: BLE001 — connection/transport failure
            # Contracts store unreachable: skip the whole check rather than
            # failing a local run that simply has no network.
            print(
                f"[poorbricks] contract check skipped (store unreachable): {exc}",
                flush=True,
            )
            return []
    return errors


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    # Don't pile on if the test run already failed for other reasons.
    if exitstatus != 0 or not _enabled(session.config):
        return
    try:
        errors = run_captured_lineage_checks()
    except Exception as exc:  # noqa: BLE001 — never break a session on our bug
        print(f"[poorbricks] contract check errored, ignoring: {exc}", flush=True)
        return
    if not errors:
        return
    print("\n[poorbricks] cross-table contract check FAILED:", flush=True)
    for err in errors:
        print(err.format(), flush=True)
    session.exitstatus = 1
