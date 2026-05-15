"""Export silver/gold pipelines to PostgreSQL analytics database.

Usage:
    poetry run python scripts/postgres_export.py --mode fixtures
    poetry run python scripts/postgres_export.py --mode production --pipeline dim_patient
    poetry run python scripts/postgres_export.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from poorbricks.discovery import discover_all_pipelines
from poorbricks.registry import all_pipelines
from poorbricks.runner import run
from utils.postgres import PostgresLoader


def _postgres_pipelines() -> dict[str, tuple[str, str, str]]:
    """All registered pipelines with target_storage == 'postgres'.

    Returns dict[registry_key] → (level, table_name, module_path).
    """
    discover_all_pipelines()
    result = {}
    for key, meta in all_pipelines().items():
        if meta.target_storage == "postgres":
            # Extract module path like "silver.dim_patient" from "tables.silver.dim_patient.pipeline"
            module_path = meta.module.removeprefix("tables.").removesuffix(".pipeline")
            result[key] = (meta.level, meta.table_name, module_path)
    return result


def _run_and_write(
    registry_key: str,
    module_path: str,
    level: str,
    table_name: str,
    loader: PostgresLoader,
    mode: str,
) -> int:
    """Run pipeline, write to Postgres, return row count."""
    print(f"  Running {registry_key} (mode={mode})...", end=" ", flush=True)
    result = run(module_path, mode=mode)
    if result.df is None:
        print("FAILED (no DataFrame)")
        return 0

    row_count = result.df.count()
    print(f"{row_count} rows → Postgres", end=" ", flush=True)

    loader.ensure_schema(level)
    written = loader.write(result.df, level, table_name)
    print(f"✓")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export silver/gold pipelines to PostgreSQL"
    )
    parser.add_argument(
        "--mode",
        default="fixtures",
        choices=["fixtures", "production"],
        help="Execution mode (default: fixtures)",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Run single pipeline (e.g. dim_patient; default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover pipelines and print, don't write",
    )
    args = parser.parse_args()

    pipelines = _postgres_pipelines()

    if not pipelines:
        print("ERROR: No pipelines with storage='postgres' found")
        sys.exit(1)

    if args.dry_run:
        print(f"Found {len(pipelines)} Postgres-target pipeline(s):")
        for key, (level, table_name, _) in sorted(pipelines.items()):
            print(f"  {key:30s} → {level}.{table_name}")
        return

    # Filter to single pipeline if requested
    if args.pipeline:
        matches = {k: v for k, v in pipelines.items() if args.pipeline in k}
        if not matches:
            print(f"ERROR: No pipeline matching '{args.pipeline}' found")
            sys.exit(1)
        pipelines = matches

    loader = PostgresLoader()
    summary: dict[str, int] = {}

    print(f"\n{'Pipeline':<30s} {'Rows':<10s} Status")
    print("-" * 60)

    for registry_key in sorted(pipelines.keys()):
        level, table_name, module_path = pipelines[registry_key]
        try:
            row_count = _run_and_write(
                registry_key, module_path, level, table_name, loader, args.mode
            )
            summary[registry_key] = row_count
        except Exception as e:
            print(f"ERROR: {e}")
            summary[registry_key] = -1

    print("\n" + "=" * 60)
    print("SUMMARY:")
    for key, count in sorted(summary.items()):
        status = "✓" if count >= 0 else "✗"
        print(f"  {key:<28s} {count:>6d} rows {status}")
    print("=" * 60)


if __name__ == "__main__":
    main()
