"""Stale-data monitor: detect pipelines that stopped running.

Each scheduled DAG declares a cron; from it we derive an expected interval per
pipeline. Comparing that to the last run recorded in ``poorbricks_meta.run_history``
tells us whether a pipeline is fresh, overdue (hasn't run within ~1.5× its
interval), or missing (scheduled but never recorded a run). Manual-trigger DAGs
(no schedule) are excluded.

``evaluate`` is pure and unit-tested; ``cadences_from_dags`` and ``run_monitor``
do the I/O (read stored DAGs + run history, emit alerts).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from poorbricks.alerting import AlertSink

# A pipeline is overdue once its last run is older than this multiple of the
# expected interval — half an interval of grace for scheduler jitter / retries.
_OVERDUE_FACTOR = 1.5


@dataclass(frozen=True)
class PipelineCadence:
    pipeline_key: str
    cron: str
    interval_s: float


@dataclass(frozen=True)
class StalenessVerdict:
    pipeline_key: str
    state: str  # "ok" | "overdue" | "missing"
    last_run: datetime | None
    interval_s: float
    age_s: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_key": self.pipeline_key,
            "state": self.state,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "interval_s": self.interval_s,
            "age_s": round(self.age_s, 1) if self.age_s is not None else None,
        }


def _interval_seconds(cron: str, base: datetime) -> float | None:
    """Expected seconds between fires for a cron, or None if not derivable."""
    try:
        from croniter import croniter

        itr = croniter(cron, base)
        t1 = itr.get_next(datetime)
        t2 = itr.get_next(datetime)
        return float((t2 - t1).total_seconds())
    except Exception:
        return None


def evaluate(
    cadences: dict[str, PipelineCadence],
    last_run_finished: dict[str, datetime],
    now: datetime,
) -> list[StalenessVerdict]:
    """Classify each scheduled pipeline as ok / overdue / missing.

    ``last_run_finished`` maps pipeline_key → most recent run finish time.
    """
    verdicts: list[StalenessVerdict] = []
    for key, cadence in cadences.items():
        last = last_run_finished.get(key)
        if last is None:
            verdicts.append(
                StalenessVerdict(key, "missing", None, cadence.interval_s, None)
            )
            continue
        age = (now - last).total_seconds()
        state = "overdue" if age > cadence.interval_s * _OVERDUE_FACTOR else "ok"
        verdicts.append(StalenessVerdict(key, state, last, cadence.interval_s, age))
    return verdicts


def cadences_from_dags(dag_store: Any, now: datetime) -> dict[str, PipelineCadence]:
    """Derive per-pipeline cadence from every stored scheduled DAG."""
    from poorbricks.airflow.dag_parser import parse_generated_dag

    cadences: dict[str, PipelineCadence] = {}
    for prefix in dag_store.list_prefixes():
        # Dev DAGs (``dev-`` prefix) are not subject to prod freshness SLAs and
        # map to the same registry keys as prod — skip them to avoid collisions.
        if prefix.startswith("dev-"):
            continue
        for name in dag_store.list_dags(prefix):
            try:
                parsed = parse_generated_dag(dag_store.get(prefix, name))
            except Exception:
                continue
            if not parsed.schedule:  # manual DAG — no cadence to enforce
                continue
            interval = _interval_seconds(parsed.schedule, now)
            if interval is None or interval <= 0:
                continue
            for task in parsed.tasks:
                cadences[task.pipeline] = PipelineCadence(
                    pipeline_key=task.pipeline,
                    cron=parsed.schedule,
                    interval_s=interval,
                )
    return cadences


def run_monitor(
    sink: AlertSink | None = None,
    dag_store: Any = None,
    store: Any = None,
    now: datetime | None = None,
) -> list[StalenessVerdict]:
    """Evaluate staleness over stored DAGs + run history and alert on issues."""
    from datetime import UTC

    from poorbricks.alerting import Alert, emit
    from poorbricks.run_history import RunHistoryStore

    now = now or datetime.now(UTC)
    if dag_store is None:
        from api.config import settings as api_settings
        from poorbricks.airflow.dag_store import LocalDagStore

        dag_store = LocalDagStore(
            root=__import__("pathlib").Path(api_settings.dags_dir)
        )
    store = store or RunHistoryStore()

    cadences = cadences_from_dags(dag_store, now)
    # Freshness SLAs are a prod concern; scope to prod so dev runs of the same
    # pipeline don't mask staleness.
    last_runs = store.last_run_per_pipeline(environment="prod")
    last_finished = {
        key: rec.finished_at for key, rec in last_runs.items() if rec.finished_at
    }
    verdicts = evaluate(cadences, last_finished, now)

    bad = [v for v in verdicts if v.state != "ok"]
    if bad:
        alerts = [
            Alert(
                kind="staleness",
                pipeline_key=v.pipeline_key,
                severity="error" if v.state == "missing" else "warn",
                summary=(
                    "no run ever recorded for a scheduled pipeline"
                    if v.state == "missing"
                    else f"overdue: last run {round((v.age_s or 0) / 3600, 1)}h ago "
                    f"(expected every {round(v.interval_s / 3600, 1)}h)"
                ),
                context=v.to_dict(),
            )
            for v in bad
        ]
        emit(alerts, sink)
    return verdicts


def main(argv: list[str] | None = None) -> int:
    """CLI: ``poorbricks monitor-staleness`` — used by the monitor DAG."""
    verdicts = run_monitor()
    bad = [v for v in verdicts if v.state != "ok"]
    for v in verdicts:
        print(f"[staleness] {v.pipeline_key}: {v.state}")
    print(f"[staleness] {len(bad)}/{len(verdicts)} pipeline(s) need attention")
    return 0


__all__ = [
    "PipelineCadence",
    "StalenessVerdict",
    "cadences_from_dags",
    "evaluate",
    "main",
    "run_monitor",
]
