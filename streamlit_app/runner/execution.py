"""Execute a pipeline transform against edited fixture rows."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from streamlit_app.spark import get_spark


@st.cache_resource
def _ensure_discovery() -> bool:
    """Import every pipeline once so the registry is populated."""
    from poorbricks.discovery import discover_all_pipelines

    discover_all_pipelines()
    return True


def run_pipeline(
    contract: dict[str, Any],
    rows_by_source: dict[str, list[dict[str, Any]]],
) -> bool:
    """Execute the transform with edited rows and report results in the UI.

    Returns True if execution completed and the output could be collected.
    """
    table_name = contract["table_name"]
    storage = contract.get("storage", "delta")

    st.write("Starting SparkSession…")
    spark = get_spark()
    spark.sparkContext.setLocalProperty("spark.sql.session.timeZone", "UTC")
    _ensure_discovery()

    from poorbricks.registry import get_pipeline
    from validation.expectations import find_expectations_class

    try:
        meta = get_pipeline(table_name, target_storage=storage)
    except KeyError as exc:
        st.error(f"Pipeline not found in registry: {exc}")
        return False

    pipeline_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")

    st.write("Executing transform…")
    try:
        sources = meta.inputs_cls.sources()
        cleaned_rows = {
            src: _clean_rows(rows_by_source.get(src, [])) for src in sources
        }
        inputs = meta.inputs_cls.from_rows(cleaned_rows)
        df = meta.original_fn(inputs)
    except Exception as exc:
        st.error(f"Transform crashed: {type(exc).__name__}: {exc}")
        st.exception(exc)
        return False

    st.write("Validating schema and expectations…")
    validation_errors = _validate_schema(meta, df)
    expectations_cls = find_expectations_class(pipeline_key)
    expectation_violations = _check_expectations(expectations_cls, df)

    try:
        pdf = df.limit(500).toPandas()
        row_count = df.count()
    except Exception as exc:
        st.error(f"Could not collect output: {type(exc).__name__}: {exc}")
        return False

    _render_results(
        row_count=row_count,
        pdf=pdf,
        validation_errors=validation_errors,
        expectation_violations=expectation_violations,
        expectations_declared=expectations_cls is not None,
    )
    return True


def _validate_schema(meta: Any, df: Any) -> list[str]:
    try:
        meta.model.verify(df, strict=True)
        return []
    except Exception as exc:
        return [str(exc)]


def _check_expectations(expectations_cls: Any, df: Any) -> list[str]:
    if expectations_cls is None:
        return []
    try:
        return expectations_cls.check(df)
    except Exception as exc:
        return [f"expectations crashed: {type(exc).__name__}: {exc}"]


def _render_results(
    *,
    row_count: int,
    pdf: pd.DataFrame,
    validation_errors: list[str],
    expectation_violations: list[str],
    expectations_declared: bool,
) -> None:
    schema_ok = not validation_errors
    expectations_ok = not expectation_violations

    st.markdown("#### Run summary")
    summary_cols = st.columns(3)
    summary_cols[0].metric("Rows produced", row_count)
    summary_cols[1].metric(
        "Schema",
        "Valid" if schema_ok else f"{len(validation_errors)} error(s)",
    )
    summary_cols[2].metric(
        "Expectations",
        "Passed" if expectations_ok else f"{len(expectation_violations)} violation(s)",
    )

    if schema_ok and expectations_ok:
        st.success(
            "Schema valid. Expectations passed."
            if expectations_declared
            else "Schema valid. (No expectations declared.)"
        )

    output_tab, errors_tab = st.tabs(
        [
            f"Output ({row_count} row(s))",
            f"Issues ({len(validation_errors) + len(expectation_violations)})",
        ]
    )

    with output_tab:
        if row_count > 500:
            st.caption(f"Showing first 500 of {row_count} rows.")
        st.dataframe(pdf, use_container_width=True, hide_index=True)

    with errors_tab:
        if validation_errors:
            st.markdown("**Schema validation errors**")
            for err in validation_errors:
                st.error(err)
        if expectation_violations:
            st.markdown("**Expectations violations**")
            for violation in expectation_violations:
                st.warning(violation)
        if schema_ok and expectations_ok:
            st.caption("No issues found.")


def _clean_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert pandas types and NaN sentinels to Spark-compatible types.

    st.data_editor returns pandas DataFrames; when converted back to dicts,
    datetime columns become pd.Timestamp instead of Python datetime. Convert
    them back so Spark accepts them.
    """
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        cleaned_row: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, float) and pd.isna(value):
                cleaned_row[key] = None
            elif value is pd.NaT:
                cleaned_row[key] = None
            elif isinstance(value, pd.Timestamp):
                cleaned_row[key] = value.to_pydatetime()
            else:
                cleaned_row[key] = value
        cleaned.append(cleaned_row)
    return cleaned
