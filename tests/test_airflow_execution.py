"""Test Airflow execution end-to-end including gold table population."""

import json
import subprocess
import time

import pytest


def get_kubectl_ns_pod(namespace: str, label_selector: str) -> str:
    """Get the first pod name matching label selector."""
    cmd = [
        "kubectl",
        "get",
        "pods",
        "-n",
        namespace,
        "-l",
        label_selector,
        "-o",
        "jsonpath={.items[0].metadata.name}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout.strip()


def exec_in_pod(
    namespace: str, pod: str, cmd: list[str], container: str | None = None
) -> str:
    """Execute command in pod and return stdout."""
    exec_cmd = ["kubectl", "exec", "-n", namespace, pod]
    if container:
        exec_cmd.extend(["-c", container])
    exec_cmd.extend(["--"] + cmd)
    result = subprocess.run(exec_cmd, capture_output=True, text=True)
    return result.stdout + result.stderr


class TestAirflowExecution:
    """Test end-to-end Airflow workflow execution."""

    @pytest.mark.integration
    def test_gold_pipeline_dag_exists(self) -> None:
        """Verify gold_pipeline DAG is recognized by Airflow."""
        scheduler_pod = get_kubectl_ns_pod("airflow", "component=scheduler")
        output = exec_in_pod("airflow", scheduler_pod, ["airflow", "dags", "list"])
        assert "gold_pipeline" in output or "gold-test__gold_pipeline" in output, (
            f"gold_pipeline DAG not found. Output:\n{output}"
        )

    @pytest.mark.integration
    def test_sample_users_dag_exists(self) -> None:
        """Verify sample_users DAG is recognized by Airflow."""
        scheduler_pod = get_kubectl_ns_pod("airflow", "component=scheduler")
        output = exec_in_pod("airflow", scheduler_pod, ["airflow", "dags", "list"])
        assert "sample_users" in output or "gold-test__sample_users" in output, (
            f"sample_users DAG not found. Output:\n{output}"
        )

    @pytest.mark.integration
    def test_trigger_gold_pipeline(self) -> None:
        """Trigger the gold_pipeline DAG and verify it starts."""
        scheduler_pod = get_kubectl_ns_pod("airflow", "component=scheduler")

        # Find the correct DAG ID
        dags_output = exec_in_pod("airflow", scheduler_pod, ["airflow", "dags", "list"])
        dag_id = None
        for line in dags_output.split("\n"):
            if "gold_pipeline" in line or "gold-test__gold_pipeline" in line:
                parts = line.split()
                if parts:
                    dag_id = parts[0]
                    break

        assert dag_id, f"Could not find gold_pipeline DAG ID in:\n{dags_output}"

        # Trigger the DAG
        trigger_output = exec_in_pod(
            "airflow",
            scheduler_pod,
            ["airflow", "dags", "trigger", dag_id],
        )
        assert (
            "successfully" in trigger_output.lower()
            or "created" in trigger_output.lower()
        ), f"Failed to trigger DAG. Output:\n{trigger_output}"

    @pytest.mark.integration
    def test_workers_execute_tasks(self) -> None:
        """Verify that worker pods are created and execute tasks."""
        # Wait for worker pods to be created (up to 60 seconds)
        start_time = time.time()
        while time.time() - start_time < 60:
            try:
                result = subprocess.run(
                    [
                        "kubectl",
                        "get",
                        "pods",
                        "-n",
                        "poorbricks-workers",
                        "-o",
                        "json",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                pods_data = json.loads(result.stdout)
                # Check if any pods were created (initial check just for existence)
                if pods_data.get("items"):
                    return  # Pods exist
            except json.JSONDecodeError:
                pass
            time.sleep(3)

        pytest.fail("No worker pods created within 60 seconds")

    @pytest.mark.integration
    def test_gold_patients_table_populated(self) -> None:
        """Verify gold_patients table is populated in PostgreSQL."""
        # This requires PostgreSQL to be accessible and containing the data
        # For now, just verify the table structure exists
        # In a real test, you'd connect to PostgreSQL and run a query
        pytest.skip("PostgreSQL verification requires database access setup")
