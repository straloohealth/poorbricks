"""Read-only renderer for a contract document loaded from MongoDB."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


def render(contract: dict[str, Any]) -> None:
    """Render every persisted contract field for one pipeline."""
    table_name = contract["table_name"]
    level = contract.get("level", "?")
    storage = contract.get("storage", "?")

    st.title(table_name)

    badges = " ".join(
        [
            f":blue-badge[level: {level}]",
            f":violet-badge[storage: {storage}]",
            f":gray-badge[{contract.get('module', '')}]",
        ]
    )
    st.markdown(badges)

    if contract.get("comment"):
        st.caption(contract["comment"])

    _storage_section(contract)
    _fields_section(contract)
    _validation_rules_section(contract)
    _expectations_section(contract)
    _inputs_section(contract)
    _profile_section(contract)
    _examples_section(contract)
    _fixtures_section(contract)

    pushed_at = contract.get("pushed_at")
    if pushed_at:
        st.caption(f"Pushed at: {pushed_at}")


def _storage_section(contract: dict[str, Any]) -> None:
    storage = contract.get("storage", "delta")
    level = contract.get("level", "?")
    table_name = contract["table_name"]
    st.subheader("Storage")
    if storage == "postgres":
        st.markdown(
            f"Materialized into PostgreSQL as `analytics.{level}.{table_name}`."
        )
    else:
        st.markdown(
            f"`delta` target — kept in Spark memory only (test / fixture mode)."
        )


def _fields_section(contract: dict[str, Any]) -> None:
    st.subheader("Fields")
    fields = contract.get("fields") or []
    if not fields:
        st.info("No field metadata stored.")
        return
    st.dataframe(
        pd.DataFrame(fields),
        use_container_width=True,
        hide_index=True,
    )


def _validation_rules_section(contract: dict[str, Any]) -> None:
    st.subheader("Validation rules (per-row)")
    rules = contract.get("validation_rules") or []
    if not rules:
        st.caption("None declared.")
        return
    st.dataframe(
        pd.DataFrame(rules),
        use_container_width=True,
        hide_index=True,
    )


def _expectations_section(contract: dict[str, Any]) -> None:
    st.subheader("Expectations (production health)")
    expectations = contract.get("expectations") or {}
    if not expectations or not any(
        v not in (None, [], {}, "")
        for k, v in expectations.items()
        if k != "class_name"
    ):
        st.caption("None declared.")
        return

    cols = st.columns(2)
    cols[0].metric("MIN_ROWS", expectations.get("min_rows") or "—")
    unique_keys = expectations.get("unique_keys") or []
    cols[1].metric("UNIQUE_KEYS", len(unique_keys))

    if unique_keys:
        st.write("Unique keys:", unique_keys)
    if expectations.get("non_null_columns"):
        st.write("Non-null columns:", expectations["non_null_columns"])
    if expectations.get("null_rate_max"):
        st.write("Max null rate:", expectations["null_rate_max"])
    if expectations.get("enum_values"):
        st.write("Allowed enum values:", expectations["enum_values"])
    if expectations.get("fresh_column"):
        st.write(
            f"Freshness: max(`{expectations['fresh_column']}`) within "
            f"{expectations.get('fresh_max_age_days')} days."
        )


def _inputs_section(contract: dict[str, Any]) -> None:
    st.subheader("Inputs")
    inputs = contract.get("inputs") or []
    if not inputs:
        st.caption("None declared.")
        return
    rows = []
    for entry in inputs:
        description = ""
        kind = entry.get("kind", "")
        if kind == "ContractSource":
            description = f"contracts store → {entry.get('table_name')}"
        elif kind == "MongoSource":
            description = f"mongo {entry.get('db')}.{entry.get('collection')}"
        elif kind == "TableSource":
            description = f"delta {entry.get('table_name')} ({entry.get('model')})"
        elif kind == "PostgresTableSource":
            description = (
                f"postgres {entry.get('schema_name')}.{entry.get('table')}"
            )
        rows.append({"name": entry["name"], "kind": kind, "details": description})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _profile_section(contract: dict[str, Any]) -> None:
    profile = contract.get("profile") or {}
    if not profile:
        return
    st.subheader("Profile")
    st.metric("row count", profile.get("row_count", 0))

    null_rates = profile.get("null_rates") or {}
    if null_rates:
        st.caption("Null rates")
        ranked = sorted(null_rates.items(), key=lambda kv: kv[1], reverse=True)
        df = pd.DataFrame(ranked, columns=["column", "null_rate"]).set_index(
            "column"
        )
        st.bar_chart(df)

    enum_samples = profile.get("enum_samples") or {}
    if enum_samples:
        st.caption("Enum samples (low-cardinality fields)")
        st.json(enum_samples, expanded=False)


def _examples_section(contract: dict[str, Any]) -> None:
    st.subheader("Example rows")
    example_rows = contract.get("example_rows") or []
    if not example_rows:
        st.caption("None.")
        return
    st.dataframe(
        pd.DataFrame(example_rows),
        use_container_width=True,
        hide_index=True,
    )


def _fixtures_section(contract: dict[str, Any]) -> None:
    st.subheader("Fixtures available")
    fixtures = contract.get("fixtures") or []
    if not fixtures:
        st.caption("None.")
        return
    summary = []
    for fixture in fixtures:
        counts = {
            src: len(rows)
            for src, rows in (fixture.get("rows_by_source") or {}).items()
        }
        summary.append(
            {
                "scenario": fixture["scenario"],
                "rows by source": counts,
            }
        )
    st.dataframe(pd.DataFrame(summary), use_container_width=True, hide_index=True)
    st.caption("Open the **Run tests** tab to edit fixtures and execute.")
