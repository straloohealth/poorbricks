"""Push a pipeline's schema, example rows, and profiling data to the MongoDB contracts store.

Usage:
    poetry run python scripts/push_contract.py --pipeline smith.users
    poetry run python scripts/push_contract.py --pipeline smith.users --mode production

This makes the schema and profiling stats available to downstream pipelines
via ContractSource and drift detection.
"""

from __future__ import annotations

import argparse

from framework.discovery import discover_all_pipelines
from framework.registry import all_pipelines
from framework.runner import run
from utils.contracts import profile_dataframe, push_contract


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push a pipeline's contract to MongoDB"
    )
    parser.add_argument(
        "--pipeline",
        required=True,
        help="Logical table name (e.g., smith.users)",
    )
    parser.add_argument(
        "--mode",
        default="fixtures",
        choices=["fixtures", "production"],
        help="Data source for profiling (fixtures=controlled test data, production=real data)",
    )
    args = parser.parse_args()

    discover_all_pipelines()

    # Find the pipeline by table_name (last component of logical name)
    pipeline_table_name = args.pipeline.split(".")[-1]
    meta = next(
        (m for m in all_pipelines().values() if m.table_name == pipeline_table_name),
        None,
    )
    if meta is None:
        raise ValueError(
            f"No pipeline found with table_name {pipeline_table_name!r}. "
            f"Known pipelines: {[m.table_name for m in all_pipelines().values()]}"
        )

    # Get example rows from fixtures (always use fixtures for reproducibility)
    fixtures_result = run(f"delta:{meta.table_name}", mode="fixtures")
    if fixtures_result.df is None:
        raise ValueError(
            f"Pipeline {pipeline_table_name!r} returned no DataFrame in fixtures mode"
        )
    example_rows = [
        r.asDict(recursive=True) for r in fixtures_result.df.limit(5).collect()
    ]

    # Profile: use specified mode (fixtures or production) for statistics
    profile_result = run(f"delta:{meta.table_name}", mode=args.mode)
    if profile_result.df is None:
        raise ValueError(
            f"Pipeline {pipeline_table_name!r} returned no DataFrame in {args.mode} mode"
        )
    profile = profile_dataframe(profile_result.df)

    push_contract(
        table_name=args.pipeline,
        schema=meta.model.to_struct(),
        example_rows=example_rows,
        pipeline_key=f"delta:{meta.table_name}",
        level=meta.level,
        profile=profile,
    )
    print(
        f"Contract pushed for {args.pipeline!r}: {profile['row_count']} rows, "
        f"{len(example_rows)} example rows, {len(profile['enum_samples'])} enum fields"
    )


if __name__ == "__main__":
    main()
