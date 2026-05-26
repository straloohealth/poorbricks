"""Tests for poorbricks.airflow.dag_generator."""

from __future__ import annotations

import ast

import pytest

from poorbricks.airflow.dag_generator import generate_dag_file
from poorbricks.airflow.workflow import TaskConfig, WorkflowConfig


def _wf(tasks: tuple[TaskConfig, ...]) -> WorkflowConfig:
    return WorkflowConfig(
        name="gold_patients",
        schedule="0 2 * * *",
        tasks=tasks,
    )


def test_generated_dag_is_valid_python() -> None:
    wf = _wf(
        (
            TaskConfig(id="patients", pipeline="postgres:patients"),
            TaskConfig(
                id="gold_summary",
                pipeline="postgres:gold_summary",
                depends_on=("patients",),
            ),
        )
    )
    source = generate_dag_file(
        wf,
        prefix="deadpool",
        image="docker.io/danielspeixoto/databricks:abc123",
    )
    ast.parse(source)


def test_generated_dag_references_keys() -> None:
    wf = _wf(
        (
            TaskConfig(id="patients", pipeline="postgres:patients"),
            TaskConfig(
                id="gold_summary",
                pipeline="postgres:gold_summary",
                depends_on=("patients",),
            ),
        )
    )
    source = generate_dag_file(
        wf,
        prefix="deadpool",
        image="img:abc",
    )
    assert "deadpool__gold_patients" in source
    assert "'postgres:patients'" in source
    assert "'postgres:gold_summary'" in source
    assert "task_patients >> task_gold_summary" in source
    assert "KubernetesPodOperator" in source
    assert "poorbricks" in source
    assert "production" in source
    assert "_WORKER_RESOURCES" in source
    assert "container_resources=_WORKER_RESOURCES" in source
    assert '"memory": "2Gi"' in source  # worker memory request
    assert '"memory": "12Gi"' in source  # worker memory limit
    assert "startup_timeout_seconds=900" in source


def test_worker_env_uses_internal_contracts_url() -> None:
    """Worker pods have no Tailscale, so ContractSource must hit the
    in-cluster Service rather than the default *.ts.net endpoint."""
    wf = _wf((TaskConfig(id="patients", pipeline="postgres:patients"),))
    source = generate_dag_file(wf, prefix="deadpool", image="img:abc")
    assert "CONTRACTS_API_URL" in source
    assert "http://poorbricks-server.airflow.svc.cluster.local:8080" in source


def test_check_command_renders_check_arguments() -> None:
    wf = _wf(
        (
            TaskConfig(id="patients", pipeline="gold.patients"),
            TaskConfig(
                id="verify",
                pipeline="gold.patients",
                command="check",
                depends_on=("patients",),
            ),
        )
    )
    source = generate_dag_file(
        wf,
        prefix="deadpool",
        image="img:abc",
    )
    ast.parse(source)
    assert "'check'" in source
    assert "'run'" in source
    assert '"run": ["run", "--mode", "production"]' in source
    assert '"check": ["check"]' in source
    assert "task_patients >> task_verify" in source


def test_code_fetched_via_init_container() -> None:
    """Workers fetch table code through a fetch-code init container into an
    emptyDir — no shared PVC mount, so the pod can run on any node."""
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="my-prefix",
        image="img",
    )
    ast.parse(source)
    # The init container downloads the code tarball before the worker starts.
    assert "_INIT_CONTAINERS" in source
    assert "init_containers=_INIT_CONTAINERS" in source
    assert "fetch-code" in source
    assert "poorbricks.airflow.fetch_code" in source
    assert "CODE_TARBALL_URL" in source
    assert "/v1/code/my-prefix" in source
    # Code lands in an emptyDir, never a PVC — so no node pinning is needed.
    assert "V1EmptyDirVolumeSource" in source
    assert "V1PersistentVolumeClaimVolumeSource" not in source
    assert "/workspace" in source
    assert "TABLES_ROOT" in source
    # The old git init container must be gone.
    assert "git clone" not in source
    assert "alpine/git" not in source


def test_workers_prefer_spot() -> None:
    """Worker pods carry soft affinity for Spot nodes and tolerate the Spot
    taint, so they prefer preemptible VMs but fall back to on-demand."""
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="r",
        image="img",
    )
    ast.parse(source)
    # Soft (preferred, not required) node affinity on the GKE Spot label.
    assert "affinity=_AFFINITY" in source
    assert "preferred_during_scheduling_ignored_during_execution" in source
    assert "cloud.google.com/gke-spot" in source
    # Toleration for the spot taint so the pod is admitted onto the pool.
    assert "tolerations=_TOLERATIONS" in source
    assert "V1Toleration" in source


def test_no_node_selector() -> None:
    """Workers no longer pin to the poorbricks.io/dags node — that pinning
    only existed so the RWO code PVC could attach."""
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="r",
        image="img",
    )
    assert "node_selector" not in source
    assert "NODE_SELECTOR" not in source
    assert "poorbricks.io/dags" not in source


def test_invalid_prefix_rejected() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    with pytest.raises(ValueError, match="prefix"):
        generate_dag_file(
            wf,
            prefix="bad prefix!",
            image="img",
        )


def test_dependencies_omitted_when_none() -> None:
    wf = _wf((TaskConfig(id="t", pipeline="postgres:t"),))
    source = generate_dag_file(
        wf,
        prefix="r",
        image="img",
    )
    ast.parse(source)
    assert "no inter-task dependencies" in source
