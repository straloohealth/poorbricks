"""Unit tests for poorbricks.verify HTTP contract fetching."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
import requests

from poorbricks.verify import _http_fetcher


def test_http_fetcher_raises_key_error_on_404() -> None:
    """A 404 from the contracts server is reported as a missing contract."""

    class _Resp:
        status_code = 404

        def raise_for_status(self) -> None:  # pragma: no cover - not reached
            raise AssertionError("raise_for_status must not run for a 404")

        def json(self) -> dict[str, Any]:  # pragma: no cover - not reached
            raise AssertionError("json must not run for a 404")

    with patch("requests.get", return_value=_Resp()):
        fetch = _http_fetcher("https://example.invalid")
        with pytest.raises(KeyError):
            fetch("tiny_upstream")


def test_http_fetcher_raises_key_error_when_server_unreachable() -> None:
    """An unreachable contracts server degrades to a missing contract.

    ``verify --mode local`` must not crash with a ConnectionError traceback
    when it cannot resolve or reach the contracts server (e.g. running in
    CI without the Tailscale-only ingress).
    """
    with patch(
        "requests.get",
        side_effect=requests.ConnectionError("name resolution failed"),
    ):
        fetch = _http_fetcher("https://airflow-poorbricks-server.invalid")
        with pytest.raises(KeyError):
            fetch("tiny_upstream")
