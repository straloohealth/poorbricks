#!/usr/bin/env python3
"""Read-only K8s infrastructure health checker.

Verifies all cluster prerequisites before running E2E tests. Never modifies state.
Each check prints PASS or FAIL with diagnosis command if it fails.

Exit codes:
  0 = all checks pass
  1 = at least one check failed
"""

import subprocess
import sys
from collections.abc import Callable


def run_kubectl(args: list[str]) -> tuple[bool, str]:
    """Run kubectl command. Returns (success, output)."""
    try:
        result = subprocess.run(
            ["kubectl"] + args,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0, result.stdout.strip() or result.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except FileNotFoundError:
        return False, "kubectl not found in PATH"


def check_cluster_connectivity() -> bool:
    """Check kubectl cluster connectivity."""
    success, output = run_kubectl(["cluster-info"])
    if success:
        print("✅ [PASS] kubectl cluster connectivity")
        return True
    else:
        print("❌ [FAIL] kubectl cluster connectivity")
        print(f"   Diagnosis: {output}")
        print("   Fix: Ensure kubeconfig is configured and cluster is reachable")
        return False


def check_namespace_exists(namespace: str) -> bool:
    """Check if namespace exists."""
    success, _ = run_kubectl(["get", "namespace", namespace])
    if success:
        print(f"✅ [PASS] Namespace: {namespace}")
        return True
    else:
        print(f"❌ [FAIL] Namespace: {namespace}")
        print(f"   Diagnosis: kubectl get namespace {namespace}")
        print(f"   Fix: Create namespace with 'kubectl create namespace {namespace}'")
        return False


def check_pod_running(namespace: str, label_selector: str, pod_name: str) -> bool:
    """Check if a pod is running."""
    success, output = run_kubectl(
        [
            "get",
            "pod",
            "-n",
            namespace,
            "-l",
            label_selector,
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ]
    )
    if success and output:
        success_ready, status_output = run_kubectl(
            ["get", "pod", "-n", namespace, output, "-o", "jsonpath={.status.phase}"]
        )
        if success_ready and status_output == "Running":
            print(f"✅ [PASS] Pod running: {pod_name} (in {namespace})")
            return True

    print(f"❌ [FAIL] Pod running: {pod_name} (in {namespace})")
    print(f"   Diagnosis: kubectl get pods -n {namespace} -l {label_selector}")
    print(
        f"   Fix: Ensure the deployment is running (kubectl rollout status -n {namespace} deployment/<name>)"
    )
    return False


def check_secret_exists(namespace: str, secret_name: str) -> bool:
    """Check if secret exists."""
    success, _ = run_kubectl(["get", "secret", "-n", namespace, secret_name])
    if success:
        print(f"✅ [PASS] Secret: {secret_name} in {namespace}")
        return True
    else:
        print(f"❌ [FAIL] Secret: {secret_name} in {namespace}")
        print(f"   Diagnosis: kubectl get secret -n {namespace} {secret_name}")
        print(f"   Fix: Create secret or verify it exists in the namespace")
        return False


def check_rbac_role_exists(namespace: str, role_name: str) -> bool:
    """Check if RBAC role exists."""
    success, _ = run_kubectl(["get", "role", "-n", namespace, role_name])
    if success:
        print(f"✅ [PASS] RBAC: Role {role_name} in {namespace}")
        return True
    else:
        print(f"❌ [FAIL] RBAC: Role {role_name} in {namespace}")
        print(f"   Diagnosis: kubectl get role -n {namespace} {role_name}")
        print(f"   Fix: Ensure RBAC role is created")
        return False


def check_postgres_reachable(namespace: str = "postgres") -> bool:
    """Check if PostgreSQL is reachable via kubectl exec."""
    # Get postgres pod name
    success, pod_name = run_kubectl(
        [
            "get",
            "pod",
            "-n",
            namespace,
            "-l",
            "app=postgres",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ]
    )
    if not success or not pod_name:
        print(f"❌ [FAIL] PostgreSQL reachable (pod not found in {namespace})")
        print(f"   Diagnosis: kubectl get pods -n {namespace} -l app=postgres")
        return False

    # Check connectivity
    success, _ = run_kubectl(
        [
            "exec",
            "-n",
            namespace,
            pod_name,
            "--",
            "psql",
            "-U",
            "analytics",
            "-d",
            "analytics",
            "-c",
            "SELECT 1",
        ]
    )
    if success:
        print("✅ [PASS] PostgreSQL reachable")
        return True
    else:
        print("❌ [FAIL] PostgreSQL reachable")
        print(
            f"   Diagnosis: kubectl exec -n {namespace} {pod_name} -- psql -U analytics -d analytics -c 'SELECT 1'"
        )
        print(f"   Fix: Ensure PostgreSQL pod is running and credentials are correct")
        return False


def check_mongodb_reachable(namespace: str = "poorbricks") -> bool:
    """Check if MongoDB is reachable via kubectl exec."""
    # Get mongo pod name
    success, pod_name = run_kubectl(
        [
            "get",
            "pod",
            "-n",
            namespace,
            "-l",
            "app=mongo",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ]
    )
    if not success or not pod_name:
        print(f"❌ [FAIL] MongoDB reachable (pod not found in {namespace})")
        print(f"   Diagnosis: kubectl get pods -n {namespace} -l app=mongo")
        return False

    # Check connectivity
    success, _ = run_kubectl(
        [
            "exec",
            "-n",
            namespace,
            pod_name,
            "--",
            "mongosh",
            "--eval",
            "db.adminCommand('ping')",
            "--quiet",
        ]
    )
    if success:
        print("✅ [PASS] MongoDB reachable")
        return True
    else:
        print("❌ [FAIL] MongoDB reachable")
        print(
            f"   Diagnosis: kubectl exec -n {namespace} {pod_name} -- mongosh --eval \"db.adminCommand('ping')\""
        )
        print(f"   Fix: Ensure MongoDB pod is running")
        return False


def check_airflow_dags_discovered(namespace: str = "airflow") -> bool:
    """Check if Airflow has discovered at least 1 DAG."""
    # Get scheduler pod name
    success, pod_name = run_kubectl(
        [
            "get",
            "pod",
            "-n",
            namespace,
            "-l",
            "component=scheduler",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ]
    )
    if not success or not pod_name:
        print(
            f"❌ [FAIL] Airflow DAGs discovered (scheduler pod not found in {namespace})"
        )
        return False

    # Check DAG count
    success, output = run_kubectl(
        [
            "exec",
            "-n",
            namespace,
            pod_name,
            "--",
            "airflow",
            "dags",
            "list",
            "--output",
            "json",
        ]
    )
    if success and output:
        import json

        try:
            dags = json.loads(output)
            if len(dags) >= 1:
                print(f"✅ [PASS] Airflow DAGs discovered ({len(dags)} DAGs found)")
                return True
        except json.JSONDecodeError:
            pass

    print("❌ [FAIL] Airflow DAGs discovered")
    print(
        f"   Diagnosis: kubectl exec -n {namespace} {pod_name} -- airflow dags list --output json"
    )
    print(f"   Fix: Ensure DAG files are mounted in scheduler pod at /opt/airflow/dags")
    return False


def check_specific_dag_exists(dag_id: str, namespace: str = "airflow") -> bool:
    """Check if a specific DAG is present."""
    # Get scheduler pod name
    success, pod_name = run_kubectl(
        [
            "get",
            "pod",
            "-n",
            namespace,
            "-l",
            "component=scheduler",
            "-o",
            "jsonpath={.items[0].metadata.name}",
        ]
    )
    if not success or not pod_name:
        return False

    # Check for specific DAG
    success, output = run_kubectl(
        [
            "exec",
            "-n",
            namespace,
            pod_name,
            "--",
            "airflow",
            "dags",
            "list",
            "--output",
            "json",
        ]
    )
    if success and output:
        import json

        try:
            dags = json.loads(output)
            for dag in dags:
                if dag.get("dag_id") == dag_id:
                    print(f"✅ [PASS] Expected DAG present: {dag_id}")
                    return True
        except json.JSONDecodeError:
            pass

    print(f"❌ [FAIL] Expected DAG present: {dag_id}")
    print(
        f"   Diagnosis: kubectl exec -n {namespace} {pod_name} -- airflow dags list --output json | grep {dag_id}"
    )
    print(f"   Fix: Ensure DAG file is placed in /opt/airflow/dags/ in scheduler pod")
    return False


def main() -> int:
    """Run all infrastructure checks."""
    print("=" * 70)
    print("Kubernetes Infrastructure Health Check")
    print("=" * 70)
    print()

    checks: list[tuple[str, Callable[[], bool]]] = [
        ("Cluster connectivity", check_cluster_connectivity),
        ("Namespace: airflow", lambda: check_namespace_exists("airflow")),
        ("Namespace: postgres", lambda: check_namespace_exists("postgres")),
        ("Namespace: poorbricks", lambda: check_namespace_exists("poorbricks")),
        (
            "Namespace: poorbricks-workers",
            lambda: check_namespace_exists("poorbricks-workers"),
        ),
        (
            "Pod running: airflow-scheduler",
            lambda: check_pod_running(
                "airflow", "component=scheduler", "airflow-scheduler"
            ),
        ),
        (
            "Pod running: airflow-dag-processor",
            lambda: check_pod_running(
                "airflow", "component=dag-processor", "airflow-dag-processor"
            ),
        ),
        (
            "Pod running: postgres",
            lambda: check_pod_running("postgres", "app=postgres", "postgres"),
        ),
        (
            "Pod running: mongo",
            lambda: check_pod_running("poorbricks", "app=mongo", "mongo"),
        ),
        (
            "Secret: poorbricks-runtime",
            lambda: check_secret_exists("poorbricks-workers", "poorbricks-runtime"),
        ),
        (
            "Secret: airflow-gcs-key",
            lambda: check_secret_exists("airflow", "airflow-gcs-key"),
        ),
        (
            "RBAC: Role poorbricks-worker-orchestrator",
            lambda: check_rbac_role_exists(
                "poorbricks-workers", "poorbricks-worker-orchestrator"
            ),
        ),
        ("PostgreSQL reachable", check_postgres_reachable),
        ("MongoDB reachable", check_mongodb_reachable),
        ("Airflow DAGs discovered", check_airflow_dags_discovered),
        (
            "Expected DAG: gold-test__gold_pipeline",
            lambda: check_specific_dag_exists("gold-test__gold_pipeline"),
        ),
        (
            "Expected DAG: gold-test__sample_users",
            lambda: check_specific_dag_exists("gold-test__sample_users"),
        ),
    ]

    results: list[bool] = []
    for check_name, check_func in checks:
        try:
            result = check_func()
            results.append(result)
        except Exception as e:
            print(f"❌ [ERROR] {check_name}: {str(e)}")
            results.append(False)
        print()

    # Summary
    print("=" * 70)
    passed = sum(results)
    total = len(results)
    print(f"Summary: {passed}/{total} checks passed")
    print("=" * 70)

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
