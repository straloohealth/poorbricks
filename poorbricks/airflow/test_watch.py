"""Tests for poorbricks.airflow.watch."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from poorbricks.airflow import watch


# ---- extract_log_errors ----------------------------------------------------


def test_extract_log_errors_picks_traceback_with_context() -> None:
    lines = [
        "INFO starting task",
        "Traceback (most recent call last):",
        '  File "/opt/x.py", line 12, in run',
        "    raise ValueError('boom')",
        "ValueError: boom",
        "INFO task ended",
    ]
    keep = watch.extract_log_errors(lines, max_errors=10)
    assert any("Traceback" in line for line in keep)
    assert any("ValueError: boom" in line for line in keep)
    # 5 lines of trailing context after the Traceback match keeps the
    # ValueError line; the leading INFO before it is not selected.
    assert keep[0].startswith("Traceback")


def test_extract_log_errors_falls_back_to_tail() -> None:
    lines = [f"INFO step {i}" for i in range(50)]
    keep = watch.extract_log_errors(lines, max_errors=5)
    assert keep == lines[-5:]


def test_extract_log_errors_empty() -> None:
    assert watch.extract_log_errors([]) == []


# ---- fetch_task_logs detection of the secret_key infra block ----------------


def test_fetch_task_logs_detects_secret_key_block() -> None:
    secret_key_body = {
        "content": [
            {
                "event": "::group::Log message source details",
                "sources": [
                    "!!!! Please make sure that all your Airflow components "
                    "(e.g. schedulers, api-servers, dag-processors, workers "
                    "and triggerer) have the same 'secret_key' configured in "
                    "'[api]' section and time is synchronized on all your "
                    "machines"
                ],
            },
            {"event": "::endgroup::"},
        ],
        "continuation_token": None,
    }
    with patch.object(
        watch,
        "_request",
        return_value=(200, __import__("json").dumps(secret_key_body)),
    ):
        lines, reason = watch.fetch_task_logs(
            "http://airflow",
            "gold-biz__gold-biz",
            "manual__abc",
            "acquisition_daily",
            try_number=1,
        )
    assert lines == []
    assert reason == "secret_key_mismatch"


def test_fetch_task_logs_returns_real_lines() -> None:
    body = {
        "content": [
            {
                "sources": [
                    "INFO started\nERROR boom on row 7\nTraceback (most recent call last):"
                ]
            },
        ],
    }
    with patch.object(
        watch,
        "_request",
        return_value=(200, __import__("json").dumps(body)),
    ):
        lines, reason = watch.fetch_task_logs(
            "http://airflow",
            "dag",
            "run",
            "task",
            try_number=1,
        )
    assert reason is None
    assert any("ERROR boom on row 7" in line for line in lines)
    assert any("Traceback" in line for line in lines)


# ---- trigger_dag_run reads run_id ------------------------------------------


def test_trigger_dag_run_returns_run_id() -> None:
    body = {"dag_run_id": "manual__2026-05-23T10:32:05+00:00"}
    with patch.object(
        watch,
        "_request",
        return_value=(200, __import__("json").dumps(body)),
    ) as request_mock:
        run_id = watch.trigger_dag_run("http://airflow", "gold-biz__gold-biz")
    assert run_id == "manual__2026-05-23T10:32:05+00:00"
    method, url = request_mock.call_args.args[:2]
    assert method == "POST"
    assert "/api/v2/dags/gold-biz__gold-biz/dagRuns" in url
    body_kw = request_mock.call_args.kwargs["body"]
    assert "logical_date" in body_kw


# ---- poll_run terminates on terminal state ---------------------------------


def test_poll_run_emits_outcome_on_terminal_state() -> None:
    run_payload = {
        "state": "failed",
        "start_date": "2026-05-23T10:00:00Z",
        "end_date": "2026-05-23T10:05:00Z",
        "duration": 300.0,
    }
    ti_payload = {
        "task_instances": [
            {
                "task_id": "acquisition_daily",
                "state": "failed",
                "try_number": 3,
                "start_date": "2026-05-23T10:04:00Z",
                "end_date": "2026-05-23T10:05:00Z",
                "duration": 60.0,
                "operator_name": "KubernetesPodOperator",
            },
            {
                "task_id": "task_360",
                "state": "success",
                "try_number": 1,
                "start_date": None,
                "end_date": None,
                "duration": 10.0,
                "operator_name": "KubernetesPodOperator",
            },
        ]
    }
    responses: list[tuple[int, str]] = [
        (200, __import__("json").dumps(run_payload)),
        (200, __import__("json").dumps(ti_payload)),
    ]

    def fake_request(method: str, url: str, **kwargs: Any) -> tuple[int, str]:
        return responses.pop(0)

    with patch.object(watch, "_request", side_effect=fake_request):
        outcome = watch.poll_run(
            "http://airflow",
            "gold-biz__gold-biz",
            "manual__abc",
            poll_interval=0.0,
            timeout=5.0,
        )

    assert outcome.state == "failed"
    assert outcome.duration_s == 300.0
    assert len(outcome.tasks) == 2
    assert len(outcome.failed_tasks) == 1
    assert outcome.failed_tasks[0].task_id == "acquisition_daily"


def test_poll_run_times_out_when_still_running(monkeypatch: pytest.MonkeyPatch) -> None:
    running = (200, __import__("json").dumps({"state": "running"}))
    monkeypatch.setattr(watch, "_request", lambda *a, **k: running)
    # Force time.monotonic to advance past the timeout immediately on the
    # second tick so the loop exits without sleeping.
    fake_now = iter([0.0, 10.0])
    monkeypatch.setattr(watch.time, "monotonic", lambda: next(fake_now))
    monkeypatch.setattr(watch.time, "sleep", lambda _s: None)

    outcome = watch.poll_run(
        "http://airflow",
        "dag",
        "run",
        poll_interval=0.0,
        timeout=5.0,
    )
    assert outcome.state == "timeout"
    assert "timed out" in (outcome.error or "")


# ---- render_run ------------------------------------------------------------


def test_render_run_success_summary() -> None:
    outcome = watch.RunOutcome(
        dag_id="gold-biz__gold-biz",
        run_id="manual__abc",
        state="success",
        started_at="2026-05-23T10:00:00Z",
        ended_at="2026-05-23T10:05:00Z",
        duration_s=300.0,
        tasks=[
            watch.TaskOutcome(
                task_id="acquisition_daily",
                state="success",
                try_number=1,
                start_date=None,
                end_date=None,
                duration_s=12.0,
                operator="KubernetesPodOperator",
            )
        ],
    )
    text = watch.render_run(outcome)
    assert "OK" in text
    assert "1/1 success" in text


def test_render_run_failed_includes_log_excerpt() -> None:
    outcome = watch.RunOutcome(
        dag_id="gold-biz__gold-biz",
        run_id="manual__abc",
        state="failed",
        started_at="2026-05-23T10:00:00Z",
        ended_at="2026-05-23T10:05:00Z",
        duration_s=300.0,
        tasks=[
            watch.TaskOutcome(
                task_id="acquisition_daily",
                state="failed",
                try_number=3,
                start_date=None,
                end_date=None,
                duration_s=12.0,
                operator="KubernetesPodOperator",
                log_lines=[
                    "INFO started",
                    "ERROR boom on row 7",
                    "Traceback (most recent call last):",
                    "  File ...",
                    "RuntimeError: boom",
                ],
            )
        ],
    )
    text = watch.render_run(outcome)
    assert "FAIL" in text
    assert "acquisition_daily" in text
    assert "ERROR boom on row 7" in text
    assert "RuntimeError: boom" in text


def test_sanitize_for_pod_name_collapses_runs() -> None:
    assert watch._sanitize_for_pod_name("gold-biz__gold-biz") == "gold-biz-gold-biz"
    assert watch._sanitize_for_pod_name("account_roi_monthly") == "account-roi-monthly"
    assert watch._sanitize_for_pod_name("__lead__trail__") == "lead-trail"


def test_fetch_logs_from_loki_returns_tail() -> None:
    body = {
        "data": {
            "result": [
                {
                    "stream": {
                        "namespace": "airflow",
                        "pod": "gold-biz-gold-biz-acquisition-daily-abc123",
                    },
                    "values": [
                        ["1700000000000000000", "INFO starting"],
                        ["1700000010000000000", "ERROR boom"],
                        ["1700000020000000000", "Traceback (most recent call last):"],
                    ],
                }
            ]
        }
    }
    with patch.object(
        watch,
        "_request",
        return_value=(200, __import__("json").dumps(body)),
    ):
        lines, reason = watch.fetch_logs_from_loki(
            "http://loki",
            dag_id="gold-biz__gold-biz",
            task_id="acquisition_daily",
            start_ts="2026-05-23T10:00:00Z",
            end_ts="2026-05-23T10:05:00Z",
        )
    assert reason is None
    assert any("Traceback" in line for line in lines)
    assert any("ERROR boom" in line for line in lines)


def test_fetch_logs_from_loki_no_streams() -> None:
    body = {"data": {"result": []}}
    with patch.object(
        watch,
        "_request",
        return_value=(200, __import__("json").dumps(body)),
    ):
        lines, reason = watch.fetch_logs_from_loki(
            "http://loki",
            dag_id="d",
            task_id="t",
            start_ts=None,
            end_ts=None,
        )
    assert lines == []
    assert reason == "loki_no_streams"


def test_attach_failed_task_logs_falls_back_to_loki() -> None:
    outcome = watch.RunOutcome(
        dag_id="gold-biz__gold-biz",
        run_id="manual__abc",
        state="failed",
        started_at="2026-05-23T10:00:00Z",
        ended_at="2026-05-23T10:05:00Z",
        duration_s=300.0,
        tasks=[
            watch.TaskOutcome(
                task_id="acquisition_daily",
                state="failed",
                try_number=3,
                start_date="2026-05-23T10:00:00Z",
                end_date="2026-05-23T10:01:00Z",
                duration_s=60.0,
                operator="KubernetesPodOperator",
            )
        ],
    )
    with patch.object(
        watch,
        "fetch_task_logs",
        return_value=([], "secret_key_mismatch"),
    ), patch.object(
        watch,
        "fetch_logs_from_loki",
        return_value=(["ERROR boom", "Traceback (most recent call last):"], None),
    ):
        watch.attach_failed_task_logs("http://airflow", outcome, loki_url="http://loki")

    task = outcome.tasks[0]
    assert "ERROR boom" in task.log_lines
    assert task.log_source == "loki"
    # log_block_reason is cleared on successful fallback so render_run
    # prints the log excerpts instead of treating it as blocked.
    assert task.log_block_reason is None


def test_render_run_failed_with_secret_key_block_surfaces_hint() -> None:
    outcome = watch.RunOutcome(
        dag_id="gold-biz__gold-biz",
        run_id="manual__abc",
        state="failed",
        started_at=None,
        ended_at=None,
        duration_s=None,
        tasks=[
            watch.TaskOutcome(
                task_id="acquisition_daily",
                state="failed",
                try_number=3,
                start_date=None,
                end_date=None,
                duration_s=None,
                operator="KubernetesPodOperator",
                log_block_reason="secret_key_mismatch",
            )
        ],
    )
    text = watch.render_run(outcome)
    assert "secret_key_mismatch" in text
    assert "Helm chart" in text
