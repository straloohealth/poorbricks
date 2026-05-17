"""K8s infrastructure health checks (requires running cluster).

Run with: pytest tests/test_k8s_infra.py -m k8s_e2e -n 0 -v

These tests verify that the Kubernetes cluster has the required resources and
that all components are healthy after deployment.
"""

from __future__ import annotations

import json
import subprocess

import pytest


def kubectl_json(cmd: list[str]) -> dict[str, object]:
    """Run kubectl with -o json and return parsed JSON."""
    result = subprocess.run(cmd + ["-o", "json"], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"kubectl failed: {result.stderr}")
    return json.loads(result.stdout)


def kubectl_get_pods(namespace: str) -> dict[str, object]:
    """Get pods in a namespace."""
    return kubectl_json(["kubectl", "get", "pods", "-n", namespace])


def kubectl_get_pvc(namespace: str, name: str) -> dict[str, object]:
    """Get a specific PVC."""
    return kubectl_json(["kubectl", "get", "pvc", "-n", namespace, name])


@pytest.mark.k8s_e2e
class TestK8sNamespaces:
    """Verify required Kubernetes namespaces exist."""

    def test_airflow_namespace_exists(self) -> None:
        """The 'airflow' namespace must exist."""
        result = subprocess.run(
            ["kubectl", "get", "namespace", "airflow"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "airflow namespace does not exist"


@pytest.mark.k8s_e2e
class TestAirflowComponents:
    """Verify Airflow scheduler, webserver, and dag-processor are running."""

    def test_scheduler_pod_running(self) -> None:
        """Airflow scheduler pod must be in Running state."""
        pods = kubectl_get_pods("airflow")
        scheduler_pods = [
            p
            for p in pods.get("items", [])
            if "scheduler" in p["metadata"]["name"].lower()
        ]
        assert len(scheduler_pods) > 0, "No scheduler pod found in airflow namespace"
        assert scheduler_pods[0]["status"]["phase"] == "Running", (
            "Scheduler pod is not Running"
        )

    def test_api_server_pod_running(self) -> None:
        """Airflow API server pod must be in Running state."""
        pods = kubectl_get_pods("airflow")
        api_pods = [
            p
            for p in pods.get("items", [])
            if "api-server" in p["metadata"]["name"].lower()
        ]
        assert len(api_pods) > 0, "No api-server pod found in airflow namespace"
        assert api_pods[0]["status"]["phase"] == "Running", (
            "API server pod is not Running"
        )

    def test_dag_processor_pod_running(self) -> None:
        """Airflow dag-processor pod must be in Running state."""
        pods = kubectl_get_pods("airflow")
        dag_proc_pods = [
            p
            for p in pods.get("items", [])
            if "dag-processor" in p["metadata"]["name"].lower()
        ]
        assert len(dag_proc_pods) > 0, "No dag-processor pod found in airflow namespace"
        assert dag_proc_pods[0]["status"]["phase"] == "Running", (
            "Dag-processor pod is not Running"
        )


@pytest.mark.k8s_e2e
class TestAirflowDagsPVC:
    """Verify PVC for DAGs is properly bound."""

    def test_airflow_dags_pvc_exists(self) -> None:
        """PVC 'airflow-dags' must exist in airflow namespace."""
        pvc = kubectl_get_pvc("airflow", "airflow-dags")
        assert pvc is not None, "airflow-dags PVC not found"

    def test_airflow_dags_pvc_bound(self) -> None:
        """PVC 'airflow-dags' must be in Bound status."""
        pvc = kubectl_get_pvc("airflow", "airflow-dags")
        status = pvc.get("status", {}).get("phase")
        assert status == "Bound", f"airflow-dags PVC is {status}, not Bound"


@pytest.mark.k8s_e2e
class TestPoorbricksAPI:
    """Verify poorbricks API server is running."""

    def test_api_deployment_running(self) -> None:
        """poorbricks-server deployment must be ready."""
        result = subprocess.run(
            [
                "kubectl",
                "rollout",
                "status",
                "deployment/poorbricks-server",
                "-n",
                "airflow",
                "--timeout=30s",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "poorbricks-server deployment is not ready"

    def test_api_pod_running(self) -> None:
        """poorbricks-server pod must be in Running state."""
        pods = kubectl_get_pods("airflow")
        api_pods = [
            p
            for p in pods.get("items", [])
            if "poorbricks-server" in p["metadata"]["name"]
        ]
        assert len(api_pods) > 0, "No poorbricks-server pod found in airflow namespace"
        assert api_pods[0]["status"]["phase"] == "Running", (
            "poorbricks-server pod is not Running"
        )


@pytest.mark.k8s_e2e
class TestWorkerConfiguration:
    """Verify worker configuration and secrets are configured."""

    def test_runtime_secret_exists(self) -> None:
        """Runtime secret must exist in airflow namespace."""
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "secret",
                "poorbricks-runtime",
                "-n",
                "airflow",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "poorbricks-runtime secret not found"


@pytest.mark.k8s_e2e
class TestMongoDatabase:
    """Verify MongoDB is running for test environment."""

    def test_mongodb_pod_running(self) -> None:
        """MongoDB pod must be in Running state in airflow namespace."""
        pods = kubectl_get_pods("airflow")
        mongo_pods = [
            p for p in pods.get("items", []) if "mongo" in p["metadata"]["name"].lower()
        ]
        assert len(mongo_pods) > 0, "No MongoDB pod found in airflow namespace"
        assert mongo_pods[0]["status"]["phase"] == "Running", (
            "MongoDB pod is not Running"
        )


__all__: list[str] = []
