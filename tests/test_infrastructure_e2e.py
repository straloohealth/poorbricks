"""Pure-Python workflow and DAG compilation tests.

No external services required — all tests are in-process. Tests verify:
1. Workflow YAML parsing and validation
2. Task dependency resolution
3. DAG file generation (valid Python with correct structure)

Run with: pytest tests/test_infrastructure_e2e.py -v
"""

from pathlib import Path

import pytest


class TestWorkflowParsing:
    """Workflow YAML parsing and validation."""

    def test_valid_workflow_parses_correctly(self) -> None:
        """Verify valid workflow YAML parses without errors."""
        from poorbricks.airflow.workflow import load_workflow

        wf_path = Path("../table-repo/workflows/gold_pipeline.yaml")
        assert wf_path.exists(), "gold_pipeline.yaml not found in table-repo"

        wf = load_workflow(wf_path)
        assert wf.name == "gold_pipeline"
        assert len(wf.tasks) > 0

    def test_task_dependencies_resolved(self) -> None:
        """Verify task dependencies are correctly parsed."""
        from poorbricks.airflow.workflow import load_workflow

        wf_path = Path("../table-repo/workflows/gold_pipeline.yaml")
        assert wf_path.exists()

        wf = load_workflow(wf_path)
        assert len(wf.tasks) == 2

        # Verify task IDs and dependencies
        sample_users_task = next((t for t in wf.tasks if t.id == "sample_users"), None)
        gold_patients_task = next(
            (t for t in wf.tasks if t.id == "gold_patients"), None
        )

        assert sample_users_task is not None, "sample_users task not found"
        assert gold_patients_task is not None, "gold_patients task not found"
        assert "sample_users" in gold_patients_task.depends_on, (
            "gold_patients should depend on sample_users"
        )

    def test_invalid_cron_raises_error(self) -> None:
        """Verify invalid cron expression raises WorkflowParseError."""
        import tempfile
        from pathlib import Path

        from poorbricks.airflow.workflow import WorkflowParseError, load_workflow

        # Invalid cron: "invalid-cron"
        invalid_yaml = """
name: test_workflow
schedule_cron: "invalid-cron"
tasks:
  - id: task1
    image: test:latest
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(invalid_yaml)
            f.flush()
            try:
                with pytest.raises(WorkflowParseError):
                    load_workflow(Path(f.name))
            finally:
                Path(f.name).unlink()

    def test_unknown_depends_on_raises_error(self) -> None:
        """Verify referencing unknown task in depends_on raises WorkflowParseError."""
        import tempfile
        from pathlib import Path

        from poorbricks.airflow.workflow import WorkflowParseError, load_workflow

        invalid_yaml = """
name: test_workflow
tasks:
  - id: task1
    image: test:latest
  - id: task2
    image: test:latest
    depends_on:
      - unknown_task
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(invalid_yaml)
            f.flush()
            try:
                with pytest.raises(WorkflowParseError):
                    load_workflow(Path(f.name))
            finally:
                Path(f.name).unlink()

    def test_duplicate_task_id_raises_error(self) -> None:
        """Verify duplicate task IDs raise WorkflowParseError."""
        import tempfile
        from pathlib import Path

        from poorbricks.airflow.workflow import WorkflowParseError, load_workflow

        invalid_yaml = """
name: test_workflow
tasks:
  - id: task1
    image: test:latest
  - id: task1
    image: test:latest
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(invalid_yaml)
            f.flush()
            try:
                with pytest.raises(WorkflowParseError):
                    load_workflow(Path(f.name))
            finally:
                Path(f.name).unlink()

    def test_load_workflows_from_directory(self) -> None:
        """Verify workflows can be discovered and loaded from a directory."""
        from poorbricks.airflow.workflow import load_workflows

        workflows_dir = Path("../table-repo/workflows")
        assert workflows_dir.exists(), "workflows directory not found"

        workflows = load_workflows(workflows_dir)
        assert len(workflows) > 0, "No workflows found in directory"
        assert any(wf.name == "gold_pipeline" for wf in workflows), (
            "gold_pipeline not found"
        )


class TestDagGeneration:
    """DAG file generation and validation."""

    def test_dag_file_is_valid_python(self) -> None:
        """Verify generated DAG file compiles as valid Python."""
        from poorbricks.airflow.dag_generator import generate_dag_file
        from poorbricks.airflow.workflow import load_workflow

        wf_path = Path("../table-repo/workflows/gold_pipeline.yaml")
        assert wf_path.exists()

        wf = load_workflow(wf_path)
        dag_source = generate_dag_file(
            wf,
            prefix="test",
            image="test/image:latest",
            table_repo_url="https://github.com/test.git",
            table_repo_sha="abc123",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        # Verify it's valid Python
        compile(dag_source, "<string>", "exec")

    def test_dag_contains_kubernetes_pod_operator(self) -> None:
        """Verify DAG contains KubernetesPodOperator definitions."""
        from poorbricks.airflow.dag_generator import generate_dag_file
        from poorbricks.airflow.workflow import load_workflow

        wf_path = Path("../table-repo/workflows/gold_pipeline.yaml")
        assert wf_path.exists()

        wf = load_workflow(wf_path)
        dag_source = generate_dag_file(
            wf,
            prefix="test",
            image="test/image:latest",
            table_repo_url="https://github.com/test.git",
            table_repo_sha="abc123",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        assert "KubernetesPodOperator" in dag_source

    def test_dag_id_includes_prefix(self) -> None:
        """Verify DAG ID follows {prefix}__{workflow.name} format."""
        from poorbricks.airflow.dag_generator import generate_dag_file
        from poorbricks.airflow.workflow import load_workflow

        wf_path = Path("../table-repo/workflows/gold_pipeline.yaml")
        assert wf_path.exists()

        wf = load_workflow(wf_path)
        prefix = "myprefix"
        dag_source = generate_dag_file(
            wf,
            prefix=prefix,
            image="test/image:latest",
            table_repo_url="https://github.com/test.git",
            table_repo_sha="abc123",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        expected_dag_id = f"{prefix}__{wf.name}"
        # Check for DAG_ID variable assignment
        assert (
            f"DAG_ID = '{expected_dag_id}'" in dag_source
            or f'DAG_ID = "{expected_dag_id}"' in dag_source
        )

    def test_task_dependency_wired_correctly(self) -> None:
        """Verify task dependencies are wired in DAG Python code (>> operator)."""
        from poorbricks.airflow.dag_generator import generate_dag_file
        from poorbricks.airflow.workflow import load_workflow

        wf_path = Path("../table-repo/workflows/gold_pipeline.yaml")
        assert wf_path.exists()

        wf = load_workflow(wf_path)
        dag_source = generate_dag_file(
            wf,
            prefix="test",
            image="test/image:latest",
            table_repo_url="https://github.com/test.git",
            table_repo_sha="abc123",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        # Verify >> operator for task dependencies
        assert ">>" in dag_source, "No task dependencies (>>) found in DAG"

    def test_init_container_git_clone_present(self) -> None:
        """Verify init container for git clone is included in KubernetesPodOperator."""
        from poorbricks.airflow.dag_generator import generate_dag_file
        from poorbricks.airflow.workflow import load_workflow

        wf_path = Path("../table-repo/workflows/gold_pipeline.yaml")
        assert wf_path.exists()

        wf = load_workflow(wf_path)
        dag_source = generate_dag_file(
            wf,
            prefix="test",
            image="test/image:latest",
            table_repo_url="https://github.com/test.git",
            table_repo_sha="abc123",
            namespace="test-ns",
            runtime_secret="test-secret",
        )

        # Verify init container references git clone
        assert "init_containers" in dag_source or "initContainers" in dag_source
        assert "git clone" in dag_source or "git" in dag_source

    def test_ssh_secret_volume_added_when_set(self) -> None:
        """Verify SSH secret volume is added when repo_clone_secret is provided."""
        from poorbricks.airflow.dag_generator import generate_dag_file
        from poorbricks.airflow.workflow import load_workflow

        wf_path = Path("../table-repo/workflows/gold_pipeline.yaml")
        assert wf_path.exists()

        wf = load_workflow(wf_path)
        secret_name = "my-ssh-secret"
        dag_source = generate_dag_file(
            wf,
            prefix="test",
            image="test/image:latest",
            table_repo_url="https://github.com/test.git",
            table_repo_sha="abc123",
            namespace="test-ns",
            runtime_secret="test-secret",
            repo_clone_secret=secret_name,
        )

        # Verify secret is referenced in the DAG
        assert secret_name in dag_source, f"SSH secret '{secret_name}' not found in DAG"


class TestWorkerPodDagAccess:
    """Verify executor worker pods can access DAGs via PVC.

    These tests check that the required Airflow configuration files use the
    official Externally Populated PVC pattern (no GCS, no sidecars).
    """

    def test_values_yaml_has_pod_template_file_config(self) -> None:
        """values.yaml must declare config.kubernetes.pod_template_file."""
        import yaml

        values_path = Path("deploy/k8s/airflow/values.yaml")
        assert values_path.exists(), "deploy/k8s/airflow/values.yaml not found"

        values = yaml.safe_load(values_path.read_text())
        assert "kubernetes" in values.get("config", {}), (
            "values.yaml missing config.kubernetes — executor pods cannot find pod template"
        )
        k8s_cfg = values["config"]["kubernetes"]
        assert "pod_template_file" in k8s_cfg, (
            "values.yaml missing config.kubernetes.pod_template_file"
        )
        assert (
            k8s_cfg["pod_template_file"]
            == "/opt/airflow/pod_templates/pod_template_file.yaml"
        )

    def test_pod_template_file_exists(self) -> None:
        """deploy/k8s/airflow/pod_template.yaml must exist."""
        pod_tmpl = Path("deploy/k8s/airflow/pod_template.yaml")
        assert pod_tmpl.exists(), (
            "pod_template.yaml missing — executor pods will not receive DAGs"
        )

    def test_values_yaml_uses_pvc_dag_persistence(self) -> None:
        """values.yaml must enable PVC-based DAG persistence.

        Asserts: dags.persistence.enabled=true, existingClaim="airflow-dags",
        dags.gitSync.enabled=false (we generate DAGs via API, not git).
        """
        import yaml

        values_path = Path("deploy/k8s/airflow/values.yaml")
        assert values_path.exists()

        values = yaml.safe_load(values_path.read_text())
        dags = values.get("dags", {})
        persistence = dags.get("persistence", {})

        assert persistence.get("enabled") is True, (
            "dags.persistence.enabled must be true"
        )
        assert persistence.get("existingClaim") == "airflow-dags", (
            "dags.persistence.existingClaim must be 'airflow-dags'"
        )
        git_sync = dags.get("gitSync", {})
        assert git_sync.get("enabled") is False, (
            "dags.gitSync.enabled must be false (DAGs are generated, not git-synced)"
        )

    def test_pod_template_uses_pvc_not_init_container(self) -> None:
        """pod_template.yaml must mount PVC, not use fetch-dags init container."""
        import yaml

        pod_tmpl_path = Path("deploy/k8s/airflow/pod_template.yaml")
        assert pod_tmpl_path.exists()

        tmpl = yaml.safe_load(pod_tmpl_path.read_text())
        assert tmpl["apiVersion"] == "v1"
        assert tmpl["kind"] == "Pod"
        assert tmpl["spec"]["serviceAccountName"] == "airflow"

        # Must NOT have init containers (no gsutil fetch-dags)
        init_containers = tmpl["spec"].get("initContainers", [])
        assert len(init_containers) == 0, (
            "pod_template must not have initContainers (no GCS fetch-dags)"
        )

        # Must have a PVC volume named 'dags'
        volumes = {v["name"]: v for v in tmpl["spec"]["volumes"]}
        assert "dags" in volumes, "Missing 'dags' volume"
        assert "persistentVolumeClaim" in volumes["dags"], (
            "'dags' volume must be persistentVolumeClaim, not emptyDir"
        )
        assert (
            volumes["dags"]["persistentVolumeClaim"]["claimName"] == "airflow-dags"
        ), "PVC claim must be named 'airflow-dags'"

        # Base container must mount the PVC at /opt/airflow/dags (read-only)
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
        assert dag_mount.get("readOnly") is True, (
            "DAG mount must be read-only in executor pods"
        )

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
            assert term not in content, (
                f"pod_template.yaml contains '{term}' — remove all GCS references"
            )

    def test_rbac_subjects_match_helm_sa_name(self) -> None:
        """deploy/k8s/workers/rbac.yaml subjects must match Helm-created SA name.

        Helm creates a single ServiceAccount named 'airflow' in the airflow
        namespace. The RoleBinding must grant it permission to spawn pods in
        poorbricks-workers namespace (KubernetesExecutor creates ephemeral pods).
        """
        import yaml

        rbac_path = Path("deploy/k8s/workers/rbac.yaml")
        assert rbac_path.exists(), "deploy/k8s/workers/rbac.yaml not found"

        # File has multiple documents; find the RoleBinding
        documents = list(yaml.safe_load_all(rbac_path.read_text()))
        rbac = next((d for d in documents if d and d.get("kind") == "RoleBinding"), None)
        assert rbac is not None, "No RoleBinding found in rbac.yaml"

        subjects = rbac.get("subjects", [])
        assert len(subjects) > 0, "RoleBinding must have subjects"

        # All subjects must be the 'airflow' SA in 'airflow' namespace
        for subject in subjects:
            assert subject.get("kind") == "ServiceAccount"
            assert subject.get("name") == "airflow", (
                f"RoleBinding subject name must be 'airflow', got '{subject.get('name')}'"
            )
            assert subject.get("namespace") == "airflow", (
                "RoleBinding subject namespace must be 'airflow'"
            )


class TestDeploymentManifests:
    """Verify K8s deployment manifests use PVC and local DAG store."""

    def test_pvc_yaml_exists_and_is_valid(self) -> None:
        """deploy/k8s/airflow/pvc.yaml must exist and declare airflow-dags PVC."""
        import yaml

        pvc_path = Path("deploy/k8s/airflow/pvc.yaml")
        assert pvc_path.exists(), "deploy/k8s/airflow/pvc.yaml not found"

        pvc = yaml.safe_load(pvc_path.read_text())
        assert pvc["kind"] == "PersistentVolumeClaim", (
            "pvc.yaml must define kind: PersistentVolumeClaim"
        )
        assert pvc["metadata"]["name"] == "airflow-dags", (
            "PVC must be named 'airflow-dags'"
        )
        assert pvc["metadata"]["namespace"] == "airflow", (
            "PVC must be in 'airflow' namespace"
        )

        spec = pvc["spec"]
        assert "ReadWriteOnce" in spec.get("accessModes", []), (
            "PVC must allow ReadWriteOnce access"
        )
        storage = spec.get("resources", {}).get("requests", {}).get("storage")
        assert storage is not None and storage.endswith(("Gi", "Mi")), (
            "PVC must declare storage request (e.g., 10Gi)"
        )

    def test_api_deployment_uses_local_dag_store(self) -> None:
        """deploy/k8s/api/deployment.yaml must use local DAG store + PVC mount."""
        import yaml

        api_path = Path("deploy/k8s/api/deployment.yaml")
        assert api_path.exists(), "deploy/k8s/api/deployment.yaml not found"

        api = yaml.safe_load(api_path.read_text())
        assert api["kind"] == "Deployment"

        # Find the api container
        containers = api["spec"]["template"]["spec"]["containers"]
        server = next((c for c in containers if c["name"] == "api"), None)
        assert server is not None, "No api container found"

        # Check env vars
        env_dict = {e["name"]: e.get("value") for e in server.get("env", [])}
        assert env_dict.get("POORBRICKS_API_DAG_STORE") == "local", (
            "POORBRICKS_API_DAG_STORE must be 'local'"
        )
        assert "POORBRICKS_API_DAGS_BUCKET" not in env_dict, (
            "POORBRICKS_API_DAGS_BUCKET must not be set (GCS removed)"
        )
        assert env_dict.get("POORBRICKS_API_DAGS_DIR") == "/opt/airflow/dags", (
            "POORBRICKS_API_DAGS_DIR must point to /opt/airflow/dags (PVC mount)"
        )

        # Check volume mount
        vol_mounts = {m["name"]: m for m in server.get("volumeMounts", [])}
        assert "dags" in vol_mounts, (
            "poorbricks-server container must mount 'dags' volume"
        )
        assert vol_mounts["dags"]["mountPath"] == "/opt/airflow/dags"

        # Check volume definition in pod spec
        volumes = {v["name"]: v for v in api["spec"]["template"]["spec"]["volumes"]}
        assert "dags" in volumes, "Pod spec must define 'dags' volume"
        assert "persistentVolumeClaim" in volumes["dags"], (
            "'dags' volume must be persistentVolumeClaim"
        )
        assert volumes["dags"]["persistentVolumeClaim"]["claimName"] == "airflow-dags"

    def test_api_ingress_uses_tailscale(self) -> None:
        """deploy/k8s/api/ingress.yaml must define Tailscale Ingress.

        Exposes poorbricks-server API on Tailscale VPN at /health and /v1/upload.
        """
        import yaml

        ingress_path = Path("deploy/k8s/api/ingress.yaml")
        assert ingress_path.exists(), "deploy/k8s/api/ingress.yaml not found"

        ingress = yaml.safe_load(ingress_path.read_text())
        assert ingress["kind"] == "Ingress"
        assert ingress["metadata"]["name"] == "poorbricks-server"
        assert ingress["metadata"]["namespace"] == "poorbricks"

        spec = ingress["spec"]
        assert spec.get("ingressClassName") == "tailscale", (
            "Ingress must use ingressClassName: tailscale (VPN exposure)"
        )

        # Verify backend service
        rules = spec.get("rules", [])
        assert len(rules) > 0, "Ingress must have rules"

        for rule in rules:
            paths = rule.get("http", {}).get("paths", [])
            for path in paths:
                backend = path.get("backend", {}).get("service", {})
                assert backend.get("name") == "poorbricks-server", (
                    "Backend service must be named 'poorbricks-server'"
                )
                assert backend.get("port", {}).get("number") == 8080, (
                    "Backend service port must be 8080"
                )

    def test_deploy_script_exists(self) -> None:
        """scripts/deploy_k8s.sh must exist and be executable."""
        import stat

        deploy_script = Path("scripts/deploy_k8s.sh")
        assert deploy_script.exists(), "scripts/deploy_k8s.sh not found"

        # Check executable permission
        mode = deploy_script.stat().st_mode
        assert mode & stat.S_IXUSR, "scripts/deploy_k8s.sh must be executable"
