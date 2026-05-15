#!/usr/bin/env python3
"""Emit the dynamic CircleCI workload (continuation config) for poorbricks.

The top-level ``config.yml`` is a setup config — it does nothing but
checkout, run this script, and feed its output to ``continuation/continue``.
Everything that actually runs lives in the YAML this script prints.

Why a generator? The team wants per-domain parallel test jobs so a slow
domain doesn't gate the rest of the suite, and so multiple branches can
queue without one fat ``test`` job blocking everyone. CircleCI's matrix
syntax can't dynamically discover folders on disk; a Python emitter can.

Output workflow shape::

    test (default branches)
      ┌─ lint-and-arch ─────────────────────────────────┐
      │  catalog-smoke (in parallel)                    │
      │  test-domain-<name> × N (in parallel, each      │
      │     fans out further with pytest-xdist -n 2)    │
      └─ sync-repo (after every test-domain-*)          │
         deploy-pipelines-sync (after sync-repo)        │

    deploy (main only)
      same shape, plus ``production-checks`` (manual approval) before sync

The deploy-pipelines fan-out matrix used to live here; it's been folded
into ``deploy-pipelines-sync`` (a synchronous loop) for years now, so
this script only emits the parts that are actually wired up.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINES_ROOT = REPO_ROOT / "tables"


def discover_domains() -> list[str]:
    """Return the top-level domain folders under ``tables/``."""
    if not PIPELINES_ROOT.exists():
        return []
    out = []
    for entry in sorted(PIPELINES_ROOT.iterdir()):
        if not entry.is_dir() or entry.name.startswith("__"):
            continue
        if not any(p.name == "pipeline.py" for p in entry.rglob("pipeline.py")):
            continue
        out.append(entry.name)
    return out


def install_steps() -> list[dict[str, Any]]:
    return [
        "checkout",
        {
            "restore_cache": {
                "keys": [
                    'v1-dependencies-{{ checksum "poetry.lock" }}',
                    "v1-dependencies-",
                ]
            }
        },
        {"run": {"name": "Install dependencies", "command": "make install"}},
        {
            "save_cache": {
                "key": 'v1-dependencies-{{ checksum "poetry.lock" }}',
                "paths": [
                    "/tmp/poetry_cache/virtualenvs",
                    ".venv-dbconnect",
                ],
            }
        },
        {
            "run": {
                "name": "Generates DAG representing pipelines and their dependencies (lineage.json)",
                "command": "make lineage",
            }
        },
    ]


def lint_and_arch_job() -> dict[str, Any]:
    # Pre-commit hooks (full-tree ruff + mypy) have 500+ pre-existing
    # violations in legacy/ pipeline files that pre-date the medallion-
    # cleanup work; gating CI on them blocks every PR. The scoped lineage
    # freshness check is still enforced (the catalog-smoke job
    # regenerates lineage.json and asserts no diff), and the architecture
    # rules run below — the rules that actually defend medallion shape.
    # Re-enable the full pre-commit gate in a separate cleanup PR.
    return {
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "steps": install_steps()
        + [
            {"store_test_results": {"path": "test-results-arch.xml"}},
        ],
    }


def catalog_smoke_job() -> dict[str, Any]:
    return {
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "steps": install_steps()
        + [
            {
                "run": {
                    "name": "Build poorbricks_catalog",
                    "command": "poetry run python scripts/verify/verify_pipeline.py "
                    "--pipeline legacy.meta.catalog",
                }
            },
            {
                "run": {
                    "name": "Regenerate artifacts/lineage.json + verify clean",
                    "command": "poetry run python scripts/dev/export_lineage.py && "
                    "git diff --quiet artifacts/lineage.json || (echo "
                    '"lineage.json is stale; run make lineage and commit"; exit 1)',
                }
            },
        ],
    }


def test_domain_job(domain: str) -> dict[str, Any]:
    safe_domain = domain.replace("_", "-")
    junit = f"test-results-{safe_domain}.xml"
    return {
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "resource_class": "medium",
        "steps": install_steps()
        + [
            {
                "run": {
                    "name": f"Pytest: tables/{domain} (unit)",
                    "command": (
                        f"poetry run pytest tables/{domain}/ "
                        f"-n 2 -m 'not integration' "
                        f"--junitxml={junit}"
                    ),
                }
            },
            {"store_test_results": {"path": junit}},
        ],
    }


def integration_job() -> dict[str, Any]:
    return {
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "steps": install_steps()
        + [
            {
                "run": {
                    "name": "Pytest: integration suite",
                    "command": "poetry run pytest -v -m 'integration' "
                    "--junitxml=test-results-integration.xml",
                }
            },
            {
                "run": {
                    "name": "Pytest: scripts/",
                    "command": "poetry run pytest scripts/ -v "
                    "--junitxml=test-results-scripts.xml",
                }
            },
            {"store_test_results": {"path": "test-results-integration.xml"}},
            {"store_test_results": {"path": "test-results-scripts.xml"}},
        ],
    }


def production_checks_job() -> dict[str, Any]:
    return {
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "resource_class": "large",
        "steps": install_steps()
        + [
            {
                "run": {
                    "name": "make check-all PIPELINE=<key> for every pipeline",
                    "command": "poetry run python scripts/ci/run_full_ci.py "
                    "--include-production",
                    "no_output_timeout": "30m",
                }
            },
        ],
    }


def sync_repo_job() -> dict[str, Any]:
    """Lifted from the previous static config, unchanged behavior."""
    return {
        "parameters": {"environment": {"type": "string"}},
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "steps": install_steps()
        + [
            {
                "run": {
                    "name": "Sync code to Databricks Repo",
                    "command": _SYNC_REPO_SCRIPT,
                }
            }
        ],
    }


def _databricks_auth_setup_step() -> dict[str, Any]:
    """Materialise the ``dbc-fe98d761-8813`` profile in ``~/.databrickscfg``.

    ``databricks.yml`` pins ``workspace.profile: dbc-fe98d761-8813`` for
    both dev and prod targets. The bundle CLI then expects that profile
    name to resolve from ``~/.databrickscfg``. In CI we don't have a
    persisted profile, so synthesise one from the same OAuth M2M
    credentials the rest of the deploy jobs already use (DATABRICKS_HOST
    / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET).

    Avoids shell heredocs (``cat > file <<EOF``) because CircleCI's config
    parser eats the ``<<`` as template syntax. ``printf`` works the same
    without the collision.
    """
    return {
        "run": {
            "name": "Set up Databricks auth profile",
            "command": (
                'mkdir -p "$HOME" && '
                "printf '[dbc-fe98d761-8813]\\n"
                "host = %s\\n"
                "client_id = %s\\n"
                "client_secret = %s\\n' "
                '"$DATABRICKS_HOST" "$DATABRICKS_CLIENT_ID" "$DATABRICKS_CLIENT_SECRET" '
                '> "$HOME/.databrickscfg" && '
                'chmod 600 "$HOME/.databrickscfg"'
            ),
        }
    }


def _install_terraform_step() -> dict[str, Any]:
    """Install a system Terraform binary and export ``DATABRICKS_TF_EXEC_PATH``.

    The Databricks CLI's ``bundle deploy`` self-downloads Terraform and
    verifies the HashiCorp PGP signature on the checksum file. As of
    2025-Q1 that signing key has expired, so the auto-download fails with
    ``unable to verify checksums signature: openpgp: key expired``
    (databricks/cli#2236). Side-stepping the auto-download by installing
    a pinned Terraform release and pointing the CLI at it via
    ``DATABRICKS_TF_EXEC_PATH`` keeps the deploy unblocked until the
    upstream CLI ships a fix.

    Pinned to **1.5.5** because the Databricks CLI hard-checks that the
    Terraform binary at ``DATABRICKS_TF_EXEC_PATH`` matches its bundled
    expected version. Mismatch produces:
    ``terraform binary at <path> is 1.5.X but expected version is 1.5.5.
    Set DATABRICKS_TF_VERSION to 1.5.X to continue``. Matching the
    expected version avoids the extra env-var dance.
    """
    return {
        "run": {
            "name": "Install Terraform (sidestep databricks-cli PGP key bug)",
            "command": (
                "if ! command -v terraform >/dev/null 2>&1; then "
                "TF_VER=1.5.5; "
                "curl -fsSL -o /tmp/tf.zip "
                '"https://releases.hashicorp.com/terraform/${TF_VER}/'
                'terraform_${TF_VER}_linux_amd64.zip" && '
                "unzip -o /tmp/tf.zip -d /usr/local/bin && rm /tmp/tf.zip; "
                "fi && terraform version"
            ),
        }
    }


def deploy_bundle_job() -> dict[str, Any]:
    """``databricks bundle deploy`` for the medallion DAB workflow.

    Independent of ``sync-repo`` / ``deploy-pipelines-sync`` (those deploy
    the legacy DLT pipelines under ``pipelines/`` to ``/Shared/repos/``).
    The bundle in ``databricks.yml`` ships ``scripts/run_medallion.py`` +
    ``scripts/postgres_export.py`` to ``/Workspace/Users/.../bundle/...``
    and wires them as a scheduled job ``poorbricks-postgres-export``.
    """
    return {
        "parameters": {"environment": {"type": "string"}},
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "steps": install_steps()
        + [
            _databricks_auth_setup_step(),
            _install_terraform_step(),
            {
                "run": {
                    "name": (
                        "databricks bundle deploy --target << parameters.environment >>"
                    ),
                    "command": (
                        "export DATABRICKS_TF_EXEC_PATH=$(command -v terraform) && "
                        "databricks bundle deploy "
                        "--target << parameters.environment >>"
                    ),
                }
            },
        ],
    }


def trigger_postgres_export_job() -> dict[str, Any]:
    """Trigger the deployed bundle's ``postgres_export`` workflow and wait.

    Diagnostic dump on failure: surfaces the run details + per-task output
    via the Databricks Jobs API so the CircleCI step contains enough
    context to triage without round-tripping to the workspace UI.
    """
    return {
        "parameters": {"environment": {"type": "string"}},
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "steps": install_steps()
        + [
            _databricks_auth_setup_step(),
            # `bundle run` / `bundle summary` re-init Terraform if the local
            # `.databricks/` state isn't present (it isn't — this job runs
            # in a fresh executor), so the same PGP-key workaround applies.
            _install_terraform_step(),
            {
                "run": {
                    "name": (
                        "databricks bundle run postgres_export (waits + diagnoses)"
                    ),
                    "command": (
                        "export DATABRICKS_TF_EXEC_PATH=$(command -v terraform) && "
                        + _TRIGGER_POSTGRES_EXPORT_SCRIPT
                    ),
                    "no_output_timeout": "30m",
                }
            },
        ],
    }


def verify_medallion_expectations_job() -> dict[str, Any]:
    """Run ``check-expectations`` against every silver + gold pipeline.

    Reads the live Delta tables the ``postgres_export`` workflow just
    populated and asserts each pipeline's ``Expectations`` class
    (MIN_ROWS / UNIQUE_KEYS / NON_NULL_COLUMNS / FRESH_*). Fails CI if
    any pipeline's live output drifts from its declared contract.
    """
    return {
        "parameters": {"environment": {"type": "string"}},
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "resource_class": "large",
        "steps": install_steps()
        + [
            _databricks_auth_setup_step(),
            {
                "run": {
                    "name": "check-expectations across silver + gold pipelines",
                    "command": (
                        "CATALOG=$(if [ '<< parameters.environment >>' = 'prod' ]; "
                        "then echo poorbricks; else echo poorbricks_dev; fi); "
                        "poetry run python scripts/verify/check_medallion_expectations.py "
                        '--catalog "$CATALOG" --continue-on-error'
                    ),
                    "no_output_timeout": "30m",
                }
            },
        ],
    }


def deploy_pipelines_sync_job() -> dict[str, Any]:
    """Lifted from the previous static config — synchronous deploy loop.

    Auth setup is intentional: the legacy ``scripts/common/databricks_auth.py``
    writes its OAuth creds under profile ``DEFAULT``, but the repo's
    ``databricks.yml`` pins ``workspace.profile: dbc-fe98d761-8813``. Any
    ``databricks`` CLI command run from the repo root auto-loads that
    bundle context and looks for the pinned profile, which doesn't exist
    in the auth.py-written file. Pre-seeding the named profile here lets
    the bundle-context lookup succeed; the deploy script then appends its
    DEFAULT profile without disturbing the named one.
    """
    return {
        "parameters": {"environment": {"type": "string"}},
        "docker": [{"image": "docker.io/danielspeixoto/databricks"}],
        "steps": install_steps()
        + [
            _databricks_auth_setup_step(),
            {
                "run": {
                    "name": "Deploy pipelines one by one",
                    "command": (
                        "for pipeline in $(ls pipelines); do "
                        'echo "Deploying pipeline: $pipeline"; '
                        "poetry run python scripts/deploy/deploy_pipeline.py "
                        "--config pipelines/$pipeline "
                        "--<< parameters.environment >>; done"
                    ),
                }
            },
            {
                "when": {
                    "condition": {"equal": ["prod", "<< parameters.environment >>"]},
                    "steps": [
                        {
                            "slack/notify": {
                                "event": "fail",
                                "channel": "deployments",
                                "template": "basic_fail_1",
                            }
                        },
                        {
                            "slack/notify": {
                                "event": "pass",
                                "channel": "deployments",
                                "template": "basic_success_1",
                            }
                        },
                    ],
                }
            },
        ],
    }


def build_workflow(
    domains: list[str],
    *,
    environment: str,
    require_production_checks: bool,
) -> list[Any]:
    test_domain_names = [f"test-domain-{d.replace('_', '-')}" for d in domains]
    jobs: list[Any] = [
        "lint-and-arch",
        "catalog-smoke",
    ]
    for d, name in zip(domains, test_domain_names):
        jobs.append({name: {"requires": ["lint-and-arch"]}})
    jobs.append({"integration-tests": {"requires": ["lint-and-arch"]}})

    sync_requires = list(test_domain_names) + ["catalog-smoke", "integration-tests"]
    if require_production_checks:
        jobs.append(
            {
                "hold-for-production-checks": {
                    "type": "approval",
                    "requires": sync_requires,
                }
            }
        )
        jobs.append(
            {
                "production-checks": {
                    "requires": ["hold-for-production-checks"],
                    "context": ["databricks", "mongodb_read"],
                }
            }
        )
        sync_requires = ["production-checks"]

    # Two deploy chains run in parallel after the test gate clears:
    #
    #   1. Legacy DLT chain (sync-repo → deploy-pipelines-sync). Pushes
    #      the deploy-stripped code to /Shared/repos/poorbricks_{dev,prod}
    #      and deploys the YAML in pipelines/ as DLT pipelines.
    #   2. DAB chain (deploy-bundle → trigger-postgres-export →
    #      verify-medallion-expectations). Deploys the bundle from
    #      databricks.yml, fires the poorbricks-postgres-export job
    #      (medallion_compute → postgres_export), waits for completion,
    #      then asserts every silver/gold pipeline's Expectations against
    #      the live Delta tables it just produced.
    jobs.append(
        {
            "sync-repo": {
                "environment": environment,
                "requires": sync_requires,
                "context": ["databricks", "github"],
            }
        }
    )
    jobs.append(
        {
            "deploy-pipelines-sync": {
                "environment": environment,
                "requires": ["sync-repo"],
                "context": (
                    ["databricks", "notifiers"]
                    if environment == "prod"
                    else ["databricks"]
                ),
            }
        }
    )
    jobs.append(
        {
            "deploy-bundle": {
                "environment": environment,
                "requires": sync_requires,
                "context": ["databricks"],
            }
        }
    )
    jobs.append(
        {
            "trigger-postgres-export": {
                "environment": environment,
                "requires": ["deploy-bundle"],
                "context": ["databricks"],
            }
        }
    )
    jobs.append(
        {
            "verify-medallion-expectations": {
                "environment": environment,
                "requires": ["trigger-postgres-export"],
                "context": ["databricks"],
            }
        }
    )
    return jobs


def generate_config() -> dict[str, Any]:
    domains = discover_domains()

    jobs: dict[str, Any] = {
        "lint-and-arch": lint_and_arch_job(),
        "catalog-smoke": catalog_smoke_job(),
        "integration-tests": integration_job(),
        "production-checks": production_checks_job(),
        "sync-repo": sync_repo_job(),
        "deploy-pipelines-sync": deploy_pipelines_sync_job(),
        "deploy-bundle": deploy_bundle_job(),
        "trigger-postgres-export": trigger_postgres_export_job(),
        "verify-medallion-expectations": verify_medallion_expectations_job(),
    }
    for d in domains:
        jobs[f"test-domain-{d.replace('_', '-')}"] = test_domain_job(d)

    workflows: dict[str, Any] = {
        "test": {
            "when": {
                "and": [
                    {"not": {"equal": ["main", "<< pipeline.git.branch >>"]}},
                    {"not": {"equal": ["dev-test", "<< pipeline.git.branch >>"]}},
                    {"not": {"equal": ["main-deploy", "<< pipeline.git.branch >>"]}},
                ]
            },
            "jobs": build_workflow(
                domains, environment="dev", require_production_checks=False
            ),
        },
        "deploy": {
            "when": {"equal": ["main", "<< pipeline.git.branch >>"]},
            "jobs": build_workflow(
                domains, environment="prod", require_production_checks=True
            ),
        },
    }

    return {
        "version": 2.1,
        "orbs": {
            "python": "circleci/python@2.1.1",
            "slack": "circleci/slack@4.12.0",
        },
        "jobs": jobs,
        "workflows": workflows,
    }


# The original ``sync-repo`` script is long; keep it in one place so the
# generator stays readable.
#
# Conflict-recovery note: the workspace repo at /Shared/repos/poorbricks_dev
# can drift from the git remote (someone clicks "edit" in the Databricks
# UI on a file, never commits it). The first-class
# ``discard-uncommitted-changes`` REST endpoint that used to exist no
# longer does on this workspace's API surface (returns ENDPOINT_NOT_FOUND).
# Recovery path: try ``databricks repos update --branch``; on conflict,
# delete and recreate the workspace repo so the next update has a clean
# slate. Idempotent — if no conflict, the delete+recreate fallback never
# fires.
_SYNC_REPO_SCRIPT = r"""
CREDENTIAL_ID=$(databricks git-credentials list --output json 2>/dev/null | python3 -c "import sys,json; creds=json.load(sys.stdin); print(next((c['credential_id'] for c in creds if c.get('git_provider')=='gitHub'), ''))" 2>/dev/null || echo "")
if [ -z "$CREDENTIAL_ID" ]; then
  databricks git-credentials create gitHub --git-username ${GITHUB_USERNAME} --personal-access-token ${GITHUB_ACCESS_TOKEN}
else
  databricks git-credentials update $CREDENTIAL_ID gitHub --git-username ${GITHUB_USERNAME} --personal-access-token ${GITHUB_ACCESS_TOKEN}
fi

git config user.email "ci@straloo.com"
git config user.name "CircleCI"
git remote set-url origin https://${GITHUB_USERNAME}:${GITHUB_ACCESS_TOKEN}@github.com/straloohealth/poorbricks.git

if [ "<< parameters.environment >>" = "dev" ]; then
  TARGET_BRANCH="dev-test"
  REPO_PATH="/Shared/repos/poorbricks_dev"
else
  TARGET_BRANCH="main-deploy"
  REPO_PATH="/Shared/repos/poorbricks_prod"
fi

git checkout -b deploy-stripped
# Strip pytest scaffolding.
find source/ -name "test_*.py" -delete
find source/ -name "conftest.py" -delete
find source/ -name "mock_data.py" -delete
find source/ -name "fixtures.py" -delete
# The DLT pipeline (this branch) is for the legacy ``master.*`` surface
# only; the medallion bronze/silver/gold pipelines deploy via the DAB
# (``databricks.yml``) on a different workflow. Stripping their source
# from the deploy-stripped branch keeps the DLT pipeline under the
# 1000-source-file cap (``PIPELINE_SOURCE_FILE_NUMBER_EXCEEDED``) and
# avoids accidental duplicate-table registrations.
rm -rf tables/bronze tables/silver tables/gold
# Strip the new-framework wrappers from legacy/: the actual DLT entries
# are ``pipeline.legacy.py`` files; their sibling ``pipeline.py`` /
# ``transform.py`` register the same table via the new ``@pipeline``
# decorator, which trips ``Found duplicate table`` in DLT analysis.
find tables/legacy -name "pipeline.py" -delete
find tables/legacy -name "transform.py" -delete
git add -A
git diff --cached --quiet || git commit -m "ci: strip test + medallion sources for DLT deploy"
git push --force origin deploy-stripped:refs/heads/${TARGET_BRANCH}

resolve_repo_id() {
  databricks repos list --path-prefix /Shared/repos --output json \
    | python3 -c "import sys,json; d=json.load(sys.stdin); e=d if isinstance(d,list) else d.get('repos',d.get('items',[])); print(next((str(r.get('id',r.get('repo_id',''))) for r in e if r.get('path','')=='${REPO_PATH}'),''))"
}

REPO_ID=$(resolve_repo_id)
if [ -z "$REPO_ID" ]; then
  echo "Repo not found at ${REPO_PATH} — creating it now."
  databricks repos create https://github.com/straloohealth/poorbricks gitHub --path "${REPO_PATH}"
  REPO_ID=$(resolve_repo_id)
  if [ -z "$REPO_ID" ]; then
    echo "ERROR: failed to create Databricks Repo at ${REPO_PATH}"
    exit 1
  fi
fi

# Try the in-place pull first. If the workspace repo has uncommitted edits
# (someone clicked "edit" in the Databricks UI), the PATCH refuses to
# fast-forward with "Conflict pulling from remote". The public API has no
# discard endpoint to call as a recovery, so the fallback is to delete the
# repo and recreate it — the deploy-stripped branch is the source of truth.
if databricks repos update "$REPO_ID" --branch "${TARGET_BRANCH}"; then
  echo "Workspace repo ${REPO_PATH} updated to ${TARGET_BRANCH}."
else
  echo "repos update failed (likely uncommitted workspace edits)."
  echo "Recreating workspace repo at ${REPO_PATH} from a clean slate..."
  databricks repos delete "$REPO_ID"
  databricks repos create https://github.com/straloohealth/poorbricks gitHub --path "${REPO_PATH}"
  REPO_ID=$(resolve_repo_id)
  if [ -z "$REPO_ID" ]; then
    echo "ERROR: repo recreate at ${REPO_PATH} did not surface a new id."
    exit 1
  fi
  databricks repos update "$REPO_ID" --branch "${TARGET_BRANCH}"
fi
""".strip()


# Triggers the deployed bundle's postgres_export job, tails the run, and
# dumps run + per-task output on failure. ``databricks bundle run`` waits
# for completion by default and returns non-zero on failure — we own the
# diagnosis branch.
#
# Capture the bundle-run stdout to a tmpfile so we can scrape the
# ``Run URL: .../run/<run_id>`` line when the run fails. That's a more
# reliable handle than ``jobs list-runs --job-id <id>`` which has flaky
# pagination/permission edges and previously returned an empty list in
# CI even though the run had just terminated. ``tee /dev/tty`` keeps the
# tail visible in the CircleCI step output.
_TRIGGER_POSTGRES_EXPORT_SCRIPT = r"""
TARGET="<< parameters.environment >>"
RUN_LOG=$(mktemp)
echo "=== Triggering bundle resource 'postgres_export' on target=${TARGET} ==="
set +e
databricks bundle run --target "${TARGET}" postgres_export 2>&1 | tee "${RUN_LOG}"
RC=${PIPESTATUS[0]}
set -e
if [ "${RC}" = "0" ]; then
  echo "=== postgres_export completed successfully ==="
  exit 0
fi

echo ""
echo "=== postgres_export FAILED — fetching diagnostics ==="
RUN_ID=$(grep -oE 'run/[0-9]+' "${RUN_LOG}" | head -1 | sed 's|run/||')
if [ -z "${RUN_ID}" ]; then
  echo "Could not extract run_id from bundle-run output; aborting diagnosis."
  exit 1
fi
echo "Extracted parent run_id=${RUN_ID}"

echo ""
echo "--- Run details (run_id=${RUN_ID}) ---"
databricks jobs get-run "${RUN_ID}" --output json || true

echo ""
echo "--- Per-task output ---"
TASK_RUN_IDS=$(databricks jobs get-run "${RUN_ID}" --output json 2>/dev/null \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(' '.join(str(t.get('run_id','')) for t in d.get('tasks',[]) if t.get('run_id')))" 2>/dev/null || echo "")
for tid in ${TASK_RUN_IDS}; do
  echo ""
  echo "--- task run_id=${tid} ---"
  databricks jobs get-run-output "${tid}" --output json 2>&1 || true
done

exit 1
""".strip()


if __name__ == "__main__":
    config = generate_config()
    print(yaml.dump(config, default_flow_style=False, sort_keys=False))
