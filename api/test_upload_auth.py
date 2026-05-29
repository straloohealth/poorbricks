"""Tests for the prod-upload CI-only deploy-token gate."""

from __future__ import annotations

from api.main import _prod_upload_authorized


def test_dev_uploads_are_always_open() -> None:
    # Dev is developer self-service regardless of the token state.
    assert _prod_upload_authorized("dev", "", None) is True
    assert _prod_upload_authorized("dev", "secret", None) is True
    assert _prod_upload_authorized("dev", "secret", "anything") is True


def test_prod_fail_open_when_token_unset() -> None:
    # Gate disabled (token not yet provisioned) — existing flows keep working.
    assert _prod_upload_authorized("prod", "", None) is True
    assert _prod_upload_authorized("prod", "", "whatever") is True


def test_prod_requires_matching_token_when_enabled() -> None:
    assert _prod_upload_authorized("prod", "secret", "secret") is True
    assert _prod_upload_authorized("prod", "secret", None) is False
    assert _prod_upload_authorized("prod", "secret", "") is False
    assert _prod_upload_authorized("prod", "secret", "wrong") is False
