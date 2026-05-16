"""Declared upstream inputs (mongo / contracts store / registered table / postgres)."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


def render(contract: dict[str, Any]) -> None:
    entries = contract.get("inputs") or []
    if not entries:
        return
    st.markdown("### Inputs")
    rows = [_describe(entry) for entry in entries]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _describe(entry: dict[str, Any]) -> dict[str, Any]:
    kind = entry.get("kind", "")
    details = ""
    if kind == "ContractSource":
        details = f"contracts store → {entry.get('table_name')}"
    elif kind == "MongoSource":
        details = f"mongo {entry.get('db')}.{entry.get('collection')}"
    elif kind == "TableSource":
        details = f"table {entry.get('table_name')} ({entry.get('model')})"
    elif kind == "PostgresTableSource":
        details = f"postgres {entry.get('schema_name')}.{entry.get('table')}"
    return {"name": entry["name"], "kind": kind, "details": details}
