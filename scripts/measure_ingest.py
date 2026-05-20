"""Phase 0 baseline harness for the out-of-core ingest refactor.

Seeds a synthetic ``watson.notes`` collection of a chosen size into the local
MongoDB, runs the real ``notes`` bronze pipeline end to end (MongoDB -> Spark
-> PostgreSQL + contract), and reports peak RSS of the whole process tree
(Python driver + Spark JVM child).

Run one size per process so each measurement starts from a clean slate::

    poetry run python scripts/measure_ingest.py --rows 20000 --content-bytes 1500

The companion driver ``scripts/measure_curve.sh`` runs several sizes and
prints the memory-vs-size curve. A linear curve is the bug; a flat curve is
the fixed, out-of-core behaviour.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psutil  # type: ignore[import-untyped]

FRAMEWORK_REPO = Path(__file__).resolve().parents[1]
WATSON_REPO = Path(
    os.environ.get("WATSON_REPO", "/home/danielspeixoto/repositories/watson")
)
LOCAL_MONGO_URI = "mongodb://localhost:27017"
SEED_BATCH = 5000


def _seed_notes(mongo_uri: str, rows: int, content_bytes: int) -> int:
    """Replace ``watson.notes`` with ``rows`` synthetic documents.

    Each document mirrors the real collection shape: an ObjectId ``_id``,
    camelCase ``authorId`` / ``createdAt`` / ``userId``, and a free-text
    ``content`` padded to ~``content_bytes`` bytes.
    """
    import pymongo
    from bson import ObjectId

    client: pymongo.MongoClient = pymongo.MongoClient(mongo_uri)
    try:
        collection = client["watson"]["notes"]
        collection.drop()
        filler = "x" * max(content_bytes, 0)
        base = datetime(2024, 1, 1, tzinfo=UTC)
        batch: list[dict] = []
        for i in range(rows):
            batch.append(
                {
                    "_id": ObjectId(),
                    "authorId": f"author-{i % 500}",
                    "content": f"clinical note {i} :: {filler}",
                    "createdAt": base + timedelta(seconds=i),
                    "userId": f"user-{i % 5000}",
                }
            )
            if len(batch) >= SEED_BATCH:
                collection.insert_many(batch)
                batch = []
        if batch:
            collection.insert_many(batch)
        return collection.count_documents({})
    finally:
        client.close()


class PeakRssMonitor(threading.Thread):
    """Background sampler: tracks peak RSS of this process and all children."""

    def __init__(self, interval_s: float = 0.2) -> None:
        super().__init__(daemon=True)
        self._interval_s = interval_s
        self._stop_event = threading.Event()
        self.peak_mb: float = 0.0

    def run(self) -> None:
        proc = psutil.Process()
        while not self._stop_event.is_set():
            total = 0
            try:
                total += proc.memory_info().rss
                for child in proc.children(recursive=True):
                    try:
                        total += child.memory_info().rss
                    except psutil.Error:
                        continue
            except psutil.Error:
                break
            self.peak_mb = max(self.peak_mb, total / 1024 / 1024)
            self._stop_event.wait(self._interval_s)

    def stop(self) -> float:
        self._stop_event.set()
        self.join(timeout=2.0)
        return self.peak_mb


def _run_notes_pipeline() -> int:
    """Discover and run the watson ``notes`` bronze pipeline in production mode."""
    from poorbricks.discovery import discover_all_pipelines
    from poorbricks.persist import run_and_persist

    discover_all_pipelines()
    result = run_and_persist("notes", mode="production")
    if result.errors:
        raise RuntimeError(f"pipeline errors: {result.errors}")
    return result.rows or 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="measure_ingest")
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--content-bytes", type=int, default=1500)
    parser.add_argument(
        "--skip-seed", action="store_true", help="reuse the existing collection"
    )
    args = parser.parse_args(argv)

    # Point every Mongo connection at the local instance *before* poorbricks
    # settings are instantiated, so the harness never touches shared Atlas.
    os.environ["MONGO_URI"] = LOCAL_MONGO_URI
    os.environ["CONTRACTS_MONGO_URI"] = LOCAL_MONGO_URI
    os.environ["TABLES_ROOT"] = str(WATSON_REPO / "tables")
    # poorbricks / utils / validation live in the framework repo; watson owns
    # `tables`. discover_all_pipelines prepends WATSON_REPO, so it wins for
    # `tables` while the framework packages still resolve here.
    sys.path.insert(0, str(FRAMEWORK_REPO))

    seeded = args.rows
    if not args.skip_seed:
        seed_start = time.monotonic()
        seeded = _seed_notes(LOCAL_MONGO_URI, args.rows, args.content_bytes)
        print(
            f"seeded {seeded} docs in {time.monotonic() - seed_start:.1f}s",
            file=sys.stderr,
        )

    monitor = PeakRssMonitor()
    monitor.start()
    run_start = time.monotonic()
    written = _run_notes_pipeline()
    elapsed = time.monotonic() - run_start
    peak_mb = monitor.stop()

    print(
        f"RESULT rows={seeded} content_bytes={args.content_bytes} "
        f"written={written} elapsed_s={elapsed:.1f} peak_rss_mb={peak_mb:.0f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
