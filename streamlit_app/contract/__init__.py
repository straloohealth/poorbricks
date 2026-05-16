"""Read-only contract renderer composed of section modules."""

from __future__ import annotations

from typing import Any

from streamlit_app.contract import (
    expectations,
    fields,
    inputs,
    overview,
    profile,
)


def render(contract: dict[str, Any]) -> None:
    """Render every persisted contract field for one pipeline."""
    overview.metrics(contract)
    overview.storage(contract)
    fields.render(contract)
    fields.validation_rules(contract)
    expectations.render(contract)
    inputs.render(contract)
    profile.render(contract)
    profile.examples(contract)
    profile.fixtures(contract)
