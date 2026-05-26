"""Watch Airflow DAG runs triggered by ``poorbricks upload --watch``.

Talks to the Airflow v2 REST API:
* POST ``/api/v2/dags/{dag_id}/dagRuns`` to trigger a manual run
* GET  ``/api/v2/dags/{dag_id}/dagRuns/{run_id}`` to poll the run state
* GET  ``/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances`` to list task
       instances + their final state once the run is terminal
* GET  ``/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/
       {try_number}?full_content=true`` to pull the log payload for each
       failed task

The log endpoint can return a benign-looking 200 with the cluster's
``secret_key`` sync warning instead of the actual task log — when that
happens (detected heuristically: the content lacks any task-execution lines
and contains the secret_key advice), :func:`extract_log_errors` reports it
as an infra-level block rather than a per-task failure, so the user knows
why no detail is available.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from json import JSONDecodeError
from json import dumps as _json_dumps
from json import loads as _json_loads
from typing import Any

TERMINAL_STATES = frozenset({"success", "failed", "skipped", "upstream_failed"})
"""DAG/task states that mean polling can stop."""

DEFAULT_AIRFLOW_URL = (
    "https://airflow-airflow-webserver-ingress.stingray-ordinal.ts.net"
)
DEFAULT_LOKI_URL = "http://loki-gateway.monitoring.svc.cluster.local"
"""Cluster-internal Loki gateway. Only reachable from inside the GKE cluster
(e.g. from coder workspaces, the streamlit pod, the api-server itself).
Override with ``--loki-url`` when invoking from outside."""

DEFAULT_POLL_INTERVAL_S = 15.0
DEFAULT_TIMEOUT_S = 60 * 60  # 1h
LOG_PAGE_SIZE = 200  # how many log lines we'll surface per failed task

_SECRET_KEY_HINT_FRAGMENT = "have the same 'secret_key' configured"


@dataclass
class TaskOutcome:
    task_id: str
    state: str
    try_number: int | None
    start_date: str | None
    end_date: str | None
    duration_s: float | None
    operator: str | None
    log_lines: list[str] = field(default_factory=list)
    log_block_reason: str | None = None  # "secret_key_mismatch" | "http_<code>" | None
    log_source: str | None = None  # "airflow_api" (default) | "loki"


@dataclass
class RunOutcome:
    dag_id: str
    run_id: str
    state: str  # final state; "timeout" when polling gave up
    started_at: str | None
    ended_at: str | None
    duration_s: float | None
    tasks: list[TaskOutcome]
    error: str | None = None  # set when we couldn't get the run

    @property
    def failed_tasks(self) -> list[TaskOutcome]:
        return [t for t in self.tasks if t.state in {"failed", "upstream_failed"}]


# ---- HTTP helpers -----------------------------------------------------------


def _request(
    method: str,
    url: str,
    *,
    body: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> tuple[int, str]:
    """Issue an HTTP request and return ``(status_code, response_text)``.

    Network errors raise; non-2xx HTTP responses do not — they're returned
    along with the body so the caller can react.
    """
    data = _json_dumps(body).encode() if body is not None else None
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


def _json_or_raise(status: int, text: str, where: str) -> Any:
    if status >= 400:
        raise RuntimeError(f"{where}: HTTP {status} {text[:300]}")
    try:
        return _json_loads(text)
    except JSONDecodeError as exc:
        raise RuntimeError(f"{where}: bad JSON ({exc}): {text[:300]}") from exc


# ---- Trigger ---------------------------------------------------------------


def trigger_dag_run(
    airflow_url: str,
    dag_id: str,
    *,
    logical_date: datetime | None = None,
    timeout: float = 30.0,
) -> str:
    """Trigger a manual DAG run and return its ``dag_run_id``."""
    if logical_date is None:
        logical_date = datetime.now(UTC)
    iso = logical_date.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    url = f"{airflow_url.rstrip('/')}/api/v2/dags/{urllib.parse.quote(dag_id, safe='')}/dagRuns"
    status, text = _request("POST", url, body={"logical_date": iso}, timeout=timeout)
    body = _json_or_raise(status, text, f"trigger {dag_id}")
    return body["dag_run_id"]


# ---- Polling ---------------------------------------------------------------


def _get_run(airflow_url: str, dag_id: str, run_id: str) -> dict[str, Any]:
    url = (
        f"{airflow_url.rstrip('/')}/api/v2/dags/"
        f"{urllib.parse.quote(dag_id, safe='')}/dagRuns/"
        f"{urllib.parse.quote(run_id, safe='')}"
    )
    status, text = _request("GET", url)
    return _json_or_raise(status, text, f"get-run {dag_id}/{run_id}")


def _list_task_instances(
    airflow_url: str, dag_id: str, run_id: str
) -> list[dict[str, Any]]:
    url = (
        f"{airflow_url.rstrip('/')}/api/v2/dags/"
        f"{urllib.parse.quote(dag_id, safe='')}/dagRuns/"
        f"{urllib.parse.quote(run_id, safe='')}/taskInstances"
    )
    status, text = _request("GET", url)
    body = _json_or_raise(status, text, f"list-tis {dag_id}/{run_id}")
    return body.get("task_instances", [])


def poll_run(
    airflow_url: str,
    dag_id: str,
    run_id: str,
    *,
    poll_interval: float = DEFAULT_POLL_INTERVAL_S,
    timeout: float = DEFAULT_TIMEOUT_S,
    on_tick: Any | None = None,
) -> RunOutcome:
    """Poll the DAG run until it reaches a terminal state or ``timeout`` elapses.

    Returns a :class:`RunOutcome` describing the final state. When polling
    times out, ``state`` is set to ``"timeout"``.

    ``on_tick(elapsed_s, state)`` is called once per poll for callers that
    want progress UI.
    """
    started = time.monotonic()
    last_state = "unknown"
    while True:
        try:
            run = _get_run(airflow_url, dag_id, run_id)
            last_state = run.get("state") or "unknown"
        except RuntimeError as exc:
            return RunOutcome(
                dag_id=dag_id,
                run_id=run_id,
                state="error",
                started_at=None,
                ended_at=None,
                duration_s=None,
                tasks=[],
                error=str(exc),
            )
        elapsed = time.monotonic() - started
        if on_tick is not None:
            on_tick(elapsed, last_state)
        if last_state in TERMINAL_STATES:
            tasks_raw = _list_task_instances(airflow_url, dag_id, run_id)
            tasks = [
                TaskOutcome(
                    task_id=ti.get("task_id", "?"),
                    state=ti.get("state") or "unknown",
                    try_number=ti.get("try_number"),
                    start_date=ti.get("start_date"),
                    end_date=ti.get("end_date"),
                    duration_s=ti.get("duration"),
                    operator=ti.get("operator_name") or ti.get("operator"),
                )
                for ti in tasks_raw
            ]
            return RunOutcome(
                dag_id=dag_id,
                run_id=run_id,
                state=last_state,
                started_at=run.get("start_date"),
                ended_at=run.get("end_date"),
                duration_s=run.get("duration"),
                tasks=tasks,
            )
        if elapsed >= timeout:
            return RunOutcome(
                dag_id=dag_id,
                run_id=run_id,
                state="timeout",
                started_at=run.get("start_date"),
                ended_at=None,
                duration_s=None,
                tasks=[],
                error=f"timed out after {elapsed:.0f}s in state {last_state!r}",
            )
        time.sleep(poll_interval)


# ---- Log retrieval + analysis ----------------------------------------------


def fetch_task_logs(
    airflow_url: str,
    dag_id: str,
    run_id: str,
    task_id: str,
    *,
    try_number: int,
    max_lines: int = LOG_PAGE_SIZE,
) -> tuple[list[str], str | None]:
    """Fetch logs for one task try.

    Returns ``(lines, block_reason)``. ``block_reason`` is non-None when the
    API returned a non-failure response that nonetheless contained no usable
    log lines — most commonly the cluster's ``secret_key`` sync warning,
    which yields a 200 with only an advisory event.
    """
    url = (
        f"{airflow_url.rstrip('/')}/api/v2/dags/"
        f"{urllib.parse.quote(dag_id, safe='')}/dagRuns/"
        f"{urllib.parse.quote(run_id, safe='')}/taskInstances/"
        f"{urllib.parse.quote(task_id, safe='')}/logs/{int(try_number)}"
        f"?full_content=true"
    )
    status, text = _request("GET", url, timeout=30.0)
    if status >= 400:
        return [], f"http_{status}"
    try:
        body = _json_loads(text)
    except JSONDecodeError:
        return [text[:4000]], None

    lines: list[str] = []
    secret_key_advisory = False
    for chunk in body.get("content") or []:
        sources = chunk.get("sources") or []
        for src in sources:
            if not isinstance(src, str):
                continue
            if _SECRET_KEY_HINT_FRAGMENT in src:
                secret_key_advisory = True
                continue
            for line in src.splitlines():
                line = line.rstrip()
                if line:
                    lines.append(line)
        # Newer Airflow logs may also use {"text": "..."} or {"message": "..."}
        for key in ("text", "message", "event"):
            val = chunk.get(key)
            if isinstance(val, str) and val and not val.startswith("::"):
                for line in val.splitlines():
                    line = line.rstrip()
                    if line:
                        lines.append(line)
    if not lines and secret_key_advisory:
        return [], "secret_key_mismatch"
    return lines[-max_lines:], None


def extract_log_errors(lines: list[str], *, max_errors: int = 30) -> list[str]:
    """Pull the most likely-relevant error lines from a fetched log payload.

    Heuristic: keep the lines containing Python tracebacks, ``Error``, ``ERROR``,
    ``Exception``, ``FAILED``, ``Killed``, ``OOM``, plus 5 lines of trailing
    context after each match. Falls back to the tail of the log when no
    matches are found.
    """
    if not lines:
        return []
    needles = (
        "Traceback",
        "Error",
        "ERROR",
        "Exception",
        "FAILED",
        "Killed",
        "OOMKilled",
        "fatal",
    )
    keep: list[str] = []
    skip_until = -1
    for idx, line in enumerate(lines):
        if any(n in line for n in needles):
            for i in range(idx, min(idx + 5, len(lines))):
                if i > skip_until:
                    keep.append(lines[i])
                    skip_until = i
        if len(keep) >= max_errors:
            break
    if not keep:
        keep = lines[-max_errors:]
    return keep[:max_errors]


_POD_NAME_INVALID = __import__("re").compile(r"[^a-z0-9]+")


def _sanitize_for_pod_name(name: str) -> str:
    """Mirror Airflow's pod-name sanitization for the KubernetesPodOperator.

    The operator names a pod ``{dag_id}-{task_id}-{random}``. Names must
    pass RFC 1123 (DNS label), so non-alphanumeric runs are collapsed to
    a single dash and the whole name is lowercased. Empirically, the
    DAG ``gold-biz__gold-biz`` becomes ``gold-biz-gold-biz`` — the double
    underscore collapses to a single dash, not two.

    Reversing the transformation gives us a Loki regex that matches the
    pod even when the original DAG / task id used underscores or other
    separators.
    """
    return _POD_NAME_INVALID.sub("-", name.lower()).strip("-")


def fetch_logs_from_loki(
    loki_url: str,
    *,
    dag_id: str,
    task_id: str,
    start_ts: str | None,
    end_ts: str | None,
    max_lines: int = LOG_PAGE_SIZE,
) -> tuple[list[str], str | None]:
    """Pull task pod logs from Loki when the Airflow API is blocked.

    Falls back to a 1-hour window around ``end_ts`` when timestamps are
    missing. Returns the same ``(lines, block_reason)`` shape as
    :func:`fetch_task_logs` so :func:`attach_failed_task_logs` can swap
    sources transparently.
    """
    from datetime import datetime, timedelta

    def _ns(dt: datetime) -> str:
        return str(int(dt.timestamp()) * 1_000_000_000)

    def _parse(value: str | None, default: datetime) -> datetime:
        if not value:
            return default
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return default

    now = datetime.now(UTC)
    end_dt = _parse(end_ts, now)
    # Loki ingestion is async; give it a 60s tail past `end_dt`.
    end_dt = end_dt + timedelta(seconds=60)
    start_dt = _parse(start_ts, end_dt - timedelta(hours=1)) - timedelta(seconds=60)

    pod_prefix = f"{_sanitize_for_pod_name(dag_id)}-{_sanitize_for_pod_name(task_id)}"
    # Match any pod whose name begins with `{dag}-{task}` — the trailing
    # random hash is appended by Airflow.
    logql = f'{{namespace="airflow", pod=~"{pod_prefix}.+"}}'

    url = (
        f"{loki_url.rstrip('/')}/loki/api/v1/query_range"
        f"?query={urllib.parse.quote(logql)}"
        f"&start={_ns(start_dt)}&end={_ns(end_dt)}"
        f"&limit={int(max_lines)}&direction=BACKWARD"
    )
    try:
        status, text = _request("GET", url, timeout=20.0)
    except Exception as exc:  # noqa: BLE001 - loki is best-effort
        return [], f"loki_unreachable: {exc}"
    if status >= 400:
        return [], f"loki_http_{status}"
    try:
        body = _json_loads(text)
    except JSONDecodeError:
        return [text[:4000]], None

    streams = (body.get("data") or {}).get("result") or []
    if not streams:
        return [], "loki_no_streams"

    # Each stream is one container-stdout/stderr. Flatten by timestamp ASC
    # and pick the most recent ``max_lines`` so the tail of the run is what
    # the caller sees.
    entries: list[tuple[str, str]] = []
    for stream in streams:
        for ts, line in stream.get("values") or []:
            entries.append((str(ts), str(line).rstrip()))
    entries.sort(key=lambda kv: kv[0])
    lines = [line for _, line in entries if line]
    return lines[-max_lines:], None


def attach_failed_task_logs(
    airflow_url: str,
    outcome: RunOutcome,
    *,
    max_lines: int = LOG_PAGE_SIZE,
    loki_url: str | None = DEFAULT_LOKI_URL,
) -> None:
    """Populate ``log_lines`` / ``log_block_reason`` for every failed task
    in ``outcome``.

    Tries the Airflow v2 logs API first. When that returns the
    ``secret_key_mismatch`` advisory (or any other empty payload), retries
    via Loki using the worker-pod naming pattern, so we still surface logs
    even when the cluster's secret_key sync is broken. Pass
    ``loki_url=None`` to disable the fallback.
    """
    for task in outcome.failed_tasks:
        if task.try_number is None:
            continue
        lines, reason = fetch_task_logs(
            airflow_url,
            outcome.dag_id,
            outcome.run_id,
            task.task_id,
            try_number=task.try_number,
            max_lines=max_lines,
        )
        if not lines and loki_url:
            loki_lines, loki_reason = fetch_logs_from_loki(
                loki_url,
                dag_id=outcome.dag_id,
                task_id=task.task_id,
                start_ts=task.start_date,
                end_ts=task.end_date,
                max_lines=max_lines,
            )
            if loki_lines:
                # Loki succeeded — these are the real log lines. Clear the
                # block_reason so render_run prints the excerpts. The
                # `log_source` attribute records the fallback for callers
                # that want to surface "via Loki" in their UI.
                task.log_lines = loki_lines
                task.log_block_reason = None
                task.log_source = "loki"
                continue
            # Surface both reasons so the user can diagnose
            task.log_lines = []
            task.log_block_reason = (
                f"{reason or 'empty'} (loki: {loki_reason or 'empty'})"
            )
            continue
        task.log_lines = lines
        task.log_block_reason = reason


# ---- Pretty-printing -------------------------------------------------------


_BLOCK_REASON_HELP = {
    "secret_key_mismatch": (
        "Airflow components (api-server / scheduler / workers / triggerer) have "
        "different `[api] secret_key` values, so the API can't decode the path "
        "to the worker's log file. Fix in the Airflow Helm chart: sync the same "
        "secret_key across every component. Until then, this CLI can only "
        "report run + task state, not log content."
    ),
}


def render_run(outcome: RunOutcome) -> str:
    """Format a run outcome as human-readable text."""
    lines: list[str] = []
    head = f"{outcome.dag_id}/{outcome.run_id}"
    state = outcome.state
    badge = {
        "success": "OK",
        "failed": "FAIL",
        "upstream_failed": "FAIL",
        "timeout": "TIMEOUT",
        "error": "ERR",
    }.get(state, state.upper())
    lines.append(f"[{badge}] {head}  duration={outcome.duration_s}s")
    if outcome.error:
        lines.append(f"  ! {outcome.error}")
    if outcome.state == "success":
        ok = sum(1 for t in outcome.tasks if t.state == "success")
        lines.append(f"  tasks: {ok}/{len(outcome.tasks)} success")
        return "\n".join(lines)
    if outcome.tasks:
        ok = sum(1 for t in outcome.tasks if t.state == "success")
        lines.append(
            f"  tasks: {ok}/{len(outcome.tasks)} success, "
            f"{len(outcome.failed_tasks)} failed"
        )
    blocked_reasons: set[str] = set()
    for task in outcome.failed_tasks:
        lines.append(
            f"    - {task.task_id} ({task.state}, try={task.try_number}, "
            f"{task.duration_s}s, {task.operator})"
        )
        if task.log_block_reason and not task.log_lines:
            blocked_reasons.add(task.log_block_reason)
            lines.append(f"        logs blocked: {task.log_block_reason}")
            continue
        excerpts = extract_log_errors(task.log_lines)
        if not excerpts:
            lines.append("        logs empty (no usable lines returned)")
            continue
        if task.log_source == "loki":
            lines.append("        (logs via Loki — Airflow API blocked by secret_key)")
        for line in excerpts:
            lines.append(f"        | {line}")
    for reason in blocked_reasons:
        hint = _BLOCK_REASON_HELP.get(reason)
        if hint:
            lines.append(f"  ! {reason}: {hint}")
    return "\n".join(lines)


__all__ = [
    "DEFAULT_AIRFLOW_URL",
    "DEFAULT_POLL_INTERVAL_S",
    "DEFAULT_TIMEOUT_S",
    "RunOutcome",
    "TaskOutcome",
    "TERMINAL_STATES",
    "attach_failed_task_logs",
    "extract_log_errors",
    "fetch_task_logs",
    "poll_run",
    "render_run",
    "trigger_dag_run",
]
