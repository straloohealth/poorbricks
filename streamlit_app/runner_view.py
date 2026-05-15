"""Editable-fixtures test runner tab.

For each persisted scenario, prefills `st.data_editor` with the fixture
rows captured by `push_contract.py`. The user can edit, add, or delete
rows; clicking *Run* feeds the edited rows through `Inputs.from_rows`
into the pipeline's transform function, then runs schema validation and
expectations checks against the output DataFrame.
"""

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


def render(contract: dict[str, Any]) -> None:
    """Render scenario picker, editable tables per source, and run button."""
    fixtures = contract.get("fixtures") or []
    if not fixtures:
        st.warning(
            "No fixtures were captured for this pipeline. "
            "Add a `@scenario` and re-run `push_contract.py`."
        )
        return

    scenario_names = [f["scenario"] for f in fixtures]
    selected = st.selectbox("Scenario", scenario_names, key="scenario_picker")
    fixture = next(f for f in fixtures if f["scenario"] == selected)

    st.caption(
        "Edit any row, column, or add/remove rows. The transform will run on "
        "the values you see below."
    )

    rows_by_source = fixture.get("rows_by_source") or {}
    edited_by_source: dict[str, list[dict[str, Any]]] = {}
    for src, rows in rows_by_source.items():
        st.markdown(f"**Input: `{src}`**")
        df = pd.DataFrame(rows)
        edited = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            key=f"editor_{contract['table_name']}_{selected}_{src}",
        )
        edited_by_source[src] = edited.to_dict(orient="records")

    if st.button("▶ Run", type="primary"):
        _run_pipeline(contract, edited_by_source)


def _run_pipeline(
    contract: dict[str, Any],
    rows_by_source: dict[str, list[dict[str, Any]]],
) -> None:
    """Execute the transform with edited rows and report results in the UI."""
    table_name = contract["table_name"]
    storage = contract.get("storage", "delta")

    get_spark()  # ensure the SparkSession is alive
    _ensure_discovery()

    from poorbricks.registry import get_pipeline
    from validation.expectations import find_expectations_class

    try:
        meta = get_pipeline(table_name, target_storage=storage)
    except KeyError as exc:
        st.error(f"Pipeline not found in registry: {exc}")
        return

    pipeline_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")

    try:
        sources = meta.inputs_cls.sources()
        cleaned_rows = {
            src: _clean_rows(rows_by_source.get(src, []))
            for src in sources
        }
        inputs = meta.inputs_cls.from_rows(cleaned_rows)
        df = meta.original_fn(inputs)
    except Exception as exc:
        st.error(f"Transform crashed: {type(exc).__name__}: {exc}")
        st.exception(exc)
        return

    validation_errors: list[str] = []
    try:
        meta.model.verify(df, strict=True)
    except Exception as exc:
        validation_errors.append(str(exc))

    expectations_cls = find_expectations_class(pipeline_key)
    expectation_violations: list[str] = []
    if expectations_cls is not None:
        try:
            expectation_violations = expectations_cls.check(df)
        except Exception as exc:
            expectation_violations.append(
                f"expectations crashed: {type(exc).__name__}: {exc}"
            )

    st.subheader("Output")
    try:
        pdf = df.limit(500).toPandas()
        st.metric("rows produced", df.count())
        st.dataframe(pdf, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.error(f"Could not collect output: {type(exc).__name__}: {exc}")
        return

    if validation_errors:
        st.subheader("Schema validation errors")
        for err in validation_errors:
            st.error(err)
    if expectation_violations:
        st.subheader("Expectations violations")
        for violation in expectation_violations:
            st.warning(violation)

    if not validation_errors and not expectation_violations:
        st.success(
            "Schema valid. Expectations passed."
            if expectations_cls is not None
            else "Schema valid. (No expectations declared.)"
        )


def _clean_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert pandas types and NaN sentinels to Spark-compatible types.

    st.data_editor returns pandas DataFrames; when converted back to dicts,
    datetime columns become pd.Timestamp instead of Python datetime. Convert
    them back so Spark accepts them.
    """
    from datetime import datetime

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
