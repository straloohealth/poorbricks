"""Pure-Python workflow and DAG compilation tests.

No external services required — all tests are in-process. Tests verify:
1. Workflow YAML parsing and validation
2. Task dependency resolution
3. DAG file generation (valid Python with correct structure)

Run with: pytest tests/test_infrastructure_e2e.py -v
"""

from pathlib import Path

import pytest

_GOLD_PIPELINE_YAML = """\
name: gold_pipeline
schedule: "0 * * * *"
tasks:
  - id: sample_users
    pipeline: postgres:sample_users
  - id: gold_patients
    pipeline: postgres:gold_patients
"""

_SIMPLE_WORKFLOW_YAML = """\
name: simple_workflow
schedule: "*/5 * * * *"
tasks:
  - id: bronze_task
    pipeline: bronze.smith.users
"""


class TestWorkflowParsing:
    """Workflow YAML parsing and validation."""

    def test_valid_workflow_parses_correctly(self, tmp_path: Path) -> None:
        """Verify valid workflow YAML parses without errors."""
        from poorbricks.airflow.workflow import load_workflow

        wf_path = tmp_path / "gold_pipeline.yaml"
        wf_path.write_text(_GOLD_PIPELINE_YAML)

        wf = load_workflow(wf_path)
        assert wf.name == "gold_pipeline"
        assert len(wf.tasks) > 0

    def test_all_tasks_parsed(self, tmp_path: Path) -> None:
        """Verify every task in the YAML is parsed. Ordering is derived later."""
        from poorbricks.airflow.workflow import load_workflow

        wf_path = tmp_path / "gold_pipeline.yaml"
        wf_path.write_text(_GOLD_PIPELINE_YAML)

        wf = load_workflow(wf_path)
        assert {t.id for t in wf.tasks} == {"sample_users", "gold_patients"}
        # depends_on is derived from pipeline inputs, never parsed from YAML.
        assert all(t.depends_on == () for t in wf.tasks)

    def test_invalid_cron_raises_error(self, tmp_path: Path) -> None:
        """Verify invalid cron expression raises WorkflowParseError."""
        from poorbricks.airflow.workflow import WorkflowParseError, load_workflow

        invalid_yaml = """
name: test_workflow
schedule_cron: "invalid-cron"
tasks:
  - id: task1
    image: test:latest
"""
        wf_path = tmp_path / "invalid.yaml"
        wf_path.write_text(invalid_yaml)
        with pytest.raises(WorkflowParseError):
            load_workflow(wf_path)

    def test_depends_on_in_yaml_rejected(self, tmp_path: Path) -> None:
        """Verify a depends_on key in the YAML raises WorkflowParseError."""
        from poorbricks.airflow.workflow import WorkflowParseError, load_workflow

        invalid_yaml = """
name: test_workflow
schedule: "0 * * * *"
tasks:
  - id: task1
    pipeline: postgres:task1
  - id: task2
    pipeline: postgres:task2
    depends_on:
      - task1
"""
        wf_path = tmp_path / "invalid.yaml"
        wf_path.write_text(invalid_yaml)
        with pytest.raises(WorkflowParseError, match="depends_on is not accepted"):
            load_workflow(wf_path)

    def test_duplicate_task_id_raises_error(self, tmp_path: Path) -> None:
        """Verify duplicate task IDs raise WorkflowParseError."""
        from poorbricks.airflow.workflow import WorkflowParseError, load_workflow

        invalid_yaml = """
name: test_workflow
tasks:
  - id: task1
    image: test:latest
  - id: task1
    image: test:latest
"""
        wf_path = tmp_path / "invalid.yaml"
        wf_path.write_text(invalid_yaml)
        with pytest.raises(WorkflowParseError):
            load_workflow(wf_path)

    def test_load_workflows_from_directory(self, tmp_path: Path) -> None:
        """Verify workflows can be discovered and loaded from a directory."""
        from poorbricks.airflow.workflow import load_workflows

        workflows_dir = tmp_path / "workflows"
        workflows_dir.mkdir()
        (workflows_dir / "gold_pipeline.yaml").write_text(_GOLD_PIPELINE_YAML)
        (workflows_dir / "simple_workflow.yaml").write_text(_SIMPLE_WORKFLOW_YAML)

        workflows = load_workflows(workflows_dir)
        assert len(workflows) > 0, "No workflows found in directory"
        assert any(
            wf.name == "gold_pipeline" for wf in workflows
        ), "gold_pipeline not found"


class TestDagGeneration:
    """DAG file generation and validation."""

    def _load_gold_pipeline(self, tmp_path: Path) -> object:
        from poorbricks.airflow.workflow import load_workflow

        wf_path = tmp_path / "gold_pipeline.yaml"
        wf_path.write_text(_GOLD_PIPELINE_YAML)
        return load_workflow(wf_path)

    def test_dag_file_is_valid_python(self, tmp_path: Path) -> None:
        """Verify generated DAG file compiles as valid Python."""
        from poorbricks.airflow.dag_generator import generate_dag_file

        wf = self._load_gold_pipeline(tmp_path)
        dag_source = generate_dag_file(
            wf,  # type: ignore[arg-type]
            prefix="test",
            image="test/image:latest",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        compile(dag_source, "<string>", "exec")

    def test_dag_contains_kubernetes_pod_operator(self, tmp_path: Path) -> None:
        """Verify DAG contains KubernetesPodOperator definitions."""
        from poorbricks.airflow.dag_generator import generate_dag_file

        wf = self._load_gold_pipeline(tmp_path)
        dag_source = generate_dag_file(
            wf,  # type: ignore[arg-type]
            prefix="test",
            image="test/image:latest",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        assert "KubernetesPodOperator" in dag_source

    def test_dag_id_includes_prefix(self, tmp_path: Path) -> None:
        """Verify DAG ID follows {prefix}__{workflow.name} format."""
        from poorbricks.airflow.dag_generator import generate_dag_file

        wf = self._load_gold_pipeline(tmp_path)
        prefix = "myprefix"
        dag_source = generate_dag_file(
            wf,  # type: ignore[arg-type]
            prefix=prefix,
            image="test/image:latest",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        expected_dag_id = f"{prefix}__{wf.name}"  # type: ignore[attr-defined]
        assert (
            f"DAG_ID = '{expected_dag_id}'" in dag_source
            or f'DAG_ID = "{expected_dag_id}"' in dag_source
        )

    def test_dag_uses_pvc_code_mount(self, tmp_path: Path) -> None:
        """Verify DAG uses PVC subpath mount for code, not an init container."""
        from poorbricks.airflow.dag_generator import generate_dag_file

        wf = self._load_gold_pipeline(tmp_path)
        dag_source = generate_dag_file(
            wf,  # type: ignore[arg-type]
            prefix="myrepo",
            image="test/image:latest",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        assert "CODE_PVC_CLAIM" in dag_source, "DAG must reference CODE_PVC_CLAIM"
        assert "CODE_SUBPATH" in dag_source, "DAG must reference CODE_SUBPATH"
        assert "__code__/myrepo" in dag_source, "subpath must include prefix"
        assert (
            "init_containers" not in dag_source
        ), "DAG must not use init containers for code access (PVC approach)"

    def test_dag_includes_postgres_creds_secret(self, tmp_path: Path) -> None:
        """Verify DAG env_from includes the postgres credentials secret."""
        from poorbricks.airflow.dag_generator import generate_dag_file

        wf = self._load_gold_pipeline(tmp_path)
        dag_source = generate_dag_file(
            wf,  # type: ignore[arg-type]
            prefix="test",
            image="test/image:latest",
            namespace="test-ns",
            runtime_secret="my-runtime-secret",
            postgres_creds_secret="my-pg-creds",
        )

        assert "POSTGRES_CREDS_SECRET" in dag_source
        assert "my-pg-creds" in dag_source
        assert "POSTGRES_USER" in dag_source
        assert "POSTGRES_PASSWORD" in dag_source


class TestWorkerPodDagAccess:
    """Verify executor worker pods can access DAGs via PVC.

    These tests check that the required Airflow configuration files use the
    official Externally Populated PVC pattern (no GCS, no sidecars).
    """

    def test_pod_template_file_exists(self) -> None:
        """deploy/k8s/airflow/pod_template.yaml must exist."""
        pod_tmpl = Path("deploy/k8s/airflow/pod_template.yaml")
        assert (
            pod_tmpl.exists()
        ), "pod_template.yaml missing — executor pods will not receive DAGs"

    def test_pod_template_uses_pvc_not_init_container(self) -> None:
        """pod_template.yaml must mount PVC, not use fetch-dags init container."""
        import yaml

        pod_tmpl_path = Path("deploy/k8s/airflow/pod_template.yaml")
        assert pod_tmpl_path.exists()

        tmpl = yaml.safe_load(pod_tmpl_path.read_text())
        assert tmpl["apiVersion"] == "v1"
        assert tmpl["kind"] == "Pod"
        assert tmpl["spec"]["serviceAccountName"] == "airflow"

        init_containers = tmpl["spec"].get("initContainers", [])
        assert (
            len(init_containers) == 0
        ), "pod_template must not have initContainers (no GCS fetch-dags)"

        volumes = {v["name"]: v for v in tmpl["spec"]["volumes"]}
        assert "dags" in volumes, "Missing 'dags' volume"
        assert (
            "persistentVolumeClaim" in volumes["dags"]
        ), "'dags' volume must be persistentVolumeClaim, not emptyDir"
        assert (
            volumes["dags"]["persistentVolumeClaim"]["claimName"] == "airflow-dags"
        ), "PVC claim must be named 'airflow-dags'"

        containers = tmpl["spec"]["containers"]
        base = next((c for c in containers if c["name"] == "base"), None)
        assert base is not None, "No container named 'base'"

        dag_mount = next(
            (
                m
                for m in base.get("volumeMounts", [])
                if m["mountPath"] == "/opt/airflow/dags"
            ),
            None,
        )
        assert dag_mount is not None, "base container must mount /opt/airflow/dags"
        assert (
            dag_mount.get("readOnly") is True
        ), "DAG mount must be read-only in executor pods"

    def test_pod_template_has_no_gcs_references(self) -> None:
        """pod_template.yaml must not reference GCS, gsutil, or GCP credentials."""

        pod_tmpl_path = Path("deploy/k8s/airflow/pod_template.yaml")
        assert pod_tmpl_path.exists()

        content = pod_tmpl_path.read_text()

        forbidden = [
            "gsutil",
            "google/cloud-sdk",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "gcs-key",
            "gs://poorbricks-airflow-dags",
        ]
        for term in forbidden:
            assert (
                term not in content
            ), f"pod_template.yaml contains '{term}' — remove all GCS references"

    def test_no_cross_namespace_rbac_needed(self) -> None:
        """Single-namespace architecture eliminates need for cross-namespace RBAC."""
        rbac_path = Path("deploy/k8s/workers/rbac.yaml")
        assert not rbac_path.exists(), "deploy/k8s/workers/rbac.yaml should be deleted in single-namespace architecture"


class TestDeploymentManifests:
    """Verify K8s deployment manifests use PVC and local DAG store."""

    def test_pvc_yaml_exists_and_is_valid(self) -> None:
        """deploy/k8s/airflow-custom/00-pvc.yaml must exist and declare airflow-dags PVC."""
        import yaml

        pvc_path = Path("deploy/k8s/airflow-custom/00-pvc.yaml")
        assert pvc_path.exists(), "deploy/k8s/airflow-custom/00-pvc.yaml not found"

        pvc = yaml.safe_load(pvc_path.read_text())
        assert (
            pvc["kind"] == "PersistentVolumeClaim"
        ), "pvc.yaml must define kind: PersistentVolumeClaim"
        assert (
            pvc["metadata"]["name"] == "airflow-dags"
        ), "PVC must be named 'airflow-dags'"
        assert (
            pvc["metadata"]["namespace"] == "airflow"
        ), "PVC must be in 'airflow' namespace"

        spec = pvc["spec"]
        assert "ReadWriteOnce" in spec.get(
            "accessModes", []
        ), "PVC must allow ReadWriteOnce access"
        storage = spec.get("resources", {}).get("requests", {}).get("storage")
        assert storage is not None and storage.endswith(
            ("Gi", "Mi")
        ), "PVC must declare storage request (e.g., 10Gi)"

    def test_api_deployment_uses_local_dag_store(self) -> None:
        """deploy/k8s/api/deployment.yaml must use local DAG store + PVC mount."""
        import yaml

        api_path = Path("deploy/k8s/api/deployment.yaml")
        assert api_path.exists(), "deploy/k8s/api/deployment.yaml not found"

        api = yaml.safe_load(api_path.read_text())
        assert api["kind"] == "Deployment"

        containers = api["spec"]["template"]["spec"]["containers"]
        server = next((c for c in containers if c["name"] == "api"), None)
        assert server is not None, "No api container found"

        env_dict = {e["name"]: e.get("value") for e in server.get("env", [])}
        assert (
            env_dict.get("POORBRICKS_API_DAG_STORE") == "local"
        ), "POORBRICKS_API_DAG_STORE must be 'local'"
        assert (
            "POORBRICKS_API_DAGS_BUCKET" not in env_dict
        ), "POORBRICKS_API_DAGS_BUCKET must not be set (GCS removed)"
        assert (
            env_dict.get("POORBRICKS_API_DAGS_DIR") == "/opt/airflow/dags"
        ), "POORBRICKS_API_DAGS_DIR must point to /opt/airflow/dags (PVC mount)"

        vol_mounts = {m["name"]: m for m in server.get("volumeMounts", [])}
        assert (
            "dags" in vol_mounts
        ), "poorbricks-server container must mount 'dags' volume"
        assert vol_mounts["dags"]["mountPath"] == "/opt/airflow/dags"

        volumes = {v["name"]: v for v in api["spec"]["template"]["spec"]["volumes"]}
        assert "dags" in volumes, "Pod spec must define 'dags' volume"
        assert (
            "persistentVolumeClaim" in volumes["dags"]
        ), "'dags' volume must be persistentVolumeClaim"
        assert volumes["dags"]["persistentVolumeClaim"]["claimName"] == "airflow-dags"

    def test_api_ingress_uses_tailscale(self) -> None:
        """deploy/k8s/api/ingress.yaml must define Tailscale Ingress."""
        import yaml

        ingress_path = Path("deploy/k8s/api/ingress.yaml")
        assert ingress_path.exists(), "deploy/k8s/api/ingress.yaml not found"

        ingress = yaml.safe_load(ingress_path.read_text())
        assert ingress["kind"] == "Ingress"
        assert ingress["metadata"]["name"] == "poorbricks-server"
        assert ingress["metadata"]["namespace"] == "airflow"

        spec = ingress["spec"]
        assert (
            spec.get("ingressClassName") == "tailscale"
        ), "Ingress must use ingressClassName: tailscale (VPN exposure)"

        rules = spec.get("rules", [])
        assert len(rules) > 0, "Ingress must have rules"

        for rule in rules:
            paths = rule.get("http", {}).get("paths", [])
            for path in paths:
                backend = path.get("backend", {}).get("service", {})
                assert (
                    backend.get("name") == "poorbricks-server"
                ), "Backend service must be named 'poorbricks-server'"
                assert (
                    backend.get("port", {}).get("number") == 8080
                ), "Backend service port must be 8080"

    def test_deploy_script_exists(self) -> None:
        """scripts/deploy_k8s.sh must exist and be executable."""
        import stat

        deploy_script = Path("scripts/deploy_k8s.sh")
        assert deploy_script.exists(), "scripts/deploy_k8s.sh not found"

        mode = deploy_script.stat().st_mode
        assert mode & stat.S_IXUSR, "scripts/deploy_k8s.sh must be executable"
