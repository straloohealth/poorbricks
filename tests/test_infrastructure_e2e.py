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
    """Verify executor worker pods can access DAGs at startup.

    These tests check that the required Airflow configuration files exist and
    are correctly structured. They fail on the current codebase (worker pods
    have no DAG access) and pass once the fix is applied.
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

    def test_pod_template_has_dag_fetch_structure(self) -> None:
        """pod_template.yaml must have fetch-dags init container and dags emptyDir."""
        import yaml

        pod_tmpl_path = Path("deploy/k8s/airflow/pod_template.yaml")
        assert pod_tmpl_path.exists()

        tmpl = yaml.safe_load(pod_tmpl_path.read_text())

        assert tmpl["apiVersion"] == "v1", "pod_template must have apiVersion: v1"
        assert tmpl["kind"] == "Pod", "pod_template must have kind: Pod"
        assert tmpl["spec"]["serviceAccountName"] == "airflow"

        init_containers = tmpl["spec"]["initContainers"]
        fetch_dags = next(
            (c for c in init_containers if c["name"] == "fetch-dags"), None
        )
        assert fetch_dags is not None, "No init container named 'fetch-dags'"
        assert fetch_dags["image"] == "google/cloud-sdk:alpine", (
            "fetch-dags image must be google/cloud-sdk:alpine"
        )

        all_args = " ".join(fetch_dags.get("command", []) + fetch_dags.get("args", []))
        assert "gsutil" in all_args, "fetch-dags must run gsutil"
        assert "gs://poorbricks-airflow-dags" in all_args, (
            "fetch-dags must reference the GCS bucket gs://poorbricks-airflow-dags"
        )

        containers = tmpl["spec"]["containers"]
        base = next((c for c in containers if c["name"] == "base"), None)
        assert base is not None, "No container named 'base' in pod template"

        dag_mount = next(
            (
                m
                for m in base.get("volumeMounts", [])
                if m["mountPath"] == "/opt/airflow/dags"
            ),
            None,
        )
        assert dag_mount is not None, (
            "base container missing /opt/airflow/dags volume mount"
        )

        volumes = {v["name"]: v for v in tmpl["spec"]["volumes"]}
        assert "dags" in volumes, "Missing 'dags' volume"
        assert "emptyDir" in volumes["dags"], "'dags' volume must be emptyDir"
        assert "gcs-key" in volumes, "Missing 'gcs-key' volume"
        assert (
            volumes["gcs-key"].get("secret", {}).get("secretName") == "airflow-gcs-key"
        ), "gcs-key volume must reference secret 'airflow-gcs-key'"
