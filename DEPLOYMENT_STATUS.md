# Kubernetes Deployment Status & Next Steps

**Date**: 2026-05-17
**Status**: ✓ Clean Single-Manifest Deployment Ready
**Commit**: Single custom Kubernetes manifest deployment

## What Was Done

Consolidated Kubernetes deployment from mixed Helm + custom approach to **single custom manifest deployment** in the `airflow` namespace.

### Cleanup
- **Deleted** 3 redundant files: `values.yaml`, `pvc.yaml` (duplicate), `rbac.yaml` (cross-namespace RBAC)
- **Created** `deploy/k8s/airflow-custom/` with 11 complete custom manifests (namespace, PVC, secrets, configs, RBAC, PostgreSQL, migrations, all Airflow components)
- **Migrated** MongoDB and Poorbricks API to `airflow` namespace with correct DNS and environment variables
- **Updated** deployment script to 10 clear ordered steps using custom manifests only

### Verification
```bash
✓ pytest tests/test_infrastructure_e2e.py: 20/20 passed
✓ poetry run pre-commit run --all-files: all green
✓ No dead code references
✓ No orphaned manifests
```

## Architecture

```
Single airflow namespace (all workloads):
├── airflow-dags-rwx PVC (ReadWriteMany, Filestore standard-rwx)
├── MongoDB (test data source)
├── Scheduler ×2, Triggerer ×2, WebServer ×2, DAG Processor ×1 (HA, anti-affinity)
├── Poorbricks API server ×2 (HA, anti-affinity)
└── Control plane spread across the on-demand pool (PodDisruptionBudgets)

storage namespace:
└── PostgreSQL (CNPG, 2 instances HA) — metadata DB + analytics (silver/gold)
```

**Why single namespace**: Eliminates cross-namespace RBAC complexity. The DAG
volume is shared by all DAG consumers. Multi-node HA is enabled by a
ReadWriteMany Filestore volume (replacing the old single-node RWO PVC, which
forced every DAG-mounting pod onto one `poorbricks.io/dags`-labelled node).

## Next Steps

### 1. Deploy to Cluster
```bash
bash scripts/deploy_k8s.sh
```
Expected: All pods Running, airflow-dags PVC Bound, no restarts.

### 2. Verify Infrastructure (CI)
```bash
poetry run pytest tests/test_k8s_infra.py -m k8s_e2e -n 0 -v
```
Should pass: 8+ infrastructure tests confirming all services online.

### 3. Upload Workflows to API
```bash
# From table-repo root
tar czf /tmp/table-repo.tar.gz tables/ workflows/
curl -X POST https://<ingress-host>/v1/upload \
  -F "prefix=gold-test" \
  -F "sha=$(git rev-parse HEAD)" \
  -F "code=@/tmp/table-repo.tar.gz"
```
Expected response: `{"ok": true, "dag_names": ["gold_pipeline", "sample_users"]}`

### 4. Run E2E Tests
```bash
poetry run pytest tests/test_airflow_e2e.py -m k8s_e2e -n 0 -v
```
Validates DAG discovery, triggering, and data persistence across 4 phases:
- Phase 1: Infrastructure pre-check
- Phase 2: DAG discovery
- Phase 3: DAG triggering
- Phase 4: Data verification

### 5. Verify Gold Table
After DAG runs:
```bash
# Option A: kubectl exec
kubectl exec -n postgres $(kubectl get pod -n postgres -l app=postgres -o jsonpath='{.items[0].metadata.name}') \
  -- psql -U analytics -d analytics \
  -c "SELECT COUNT(*) as total, COUNT(patient_id) as non_null FROM gold.patients;"

# Option B: Local port-forward
kubectl port-forward -n postgres svc/postgres 15432:5432 &
psql -h localhost -p 15432 -U analytics -d analytics \
  -c "SELECT * FROM gold.patients LIMIT 5;"
```
Success criterion: `COUNT(*) > 0` with no NULL patient_ids.

## File Structure

**Custom Manifests** (deployment source of truth):
```
deploy/k8s/airflow-custom/
├── 00-namespace.yaml          # airflow namespace
├── 00-pvc.yaml                # 10Gi airflow-dags PVC (RWO)
├── 01-secrets.yaml            # Fernet key, DB credentials
├── 02-configmap.yaml          # Airflow configuration
├── 03-postgresql.yaml         # Metadata database
├── 04-serviceaccount.yaml     # RBAC (airflow-user, worker-launcher)
├── 05-migrations.yaml         # DB init job
├── 05-scheduler.yaml          # Scheduler (nodeSelector)
├── 06-triggerer.yaml          # Triggerer (nodeSelector)
├── 07-dag-processor.yaml      # DAG Processor (nodeSelector)
└── 08-webserver.yaml          # WebServer API (nodeSelector)
```

**Supporting Files**:
```
deploy/k8s/airflow/pod_template.yaml    # Worker pod template (still used)
deploy/k8s/mongo/mongo.yaml             # MongoDB (migrated to airflow NS)
deploy/k8s/api/                         # API server (migrated to airflow NS)
  ├── deployment.yaml
  ├── service.yaml
  ├── serviceaccount.yaml
  └── ingress.yaml
scripts/deploy_k8s.sh                   # Deployment orchestration (10 steps)
```

## Known Constraints

1. ~~**RWO PVC Single-Attach**~~ **(resolved)**: the DAG volume is now a
   ReadWriteMany Filestore PVC (`airflow-dags-rwx`, `standard-rwx`), so the
   control plane + API spread across the on-demand node pool and run as multiple
   replicas. The old single-node `poorbricks.io/dags` pinning is gone. The spot
   pool's taint keeps control-plane pods on on-demand nodes without a selector.
   Migrate an existing cluster with `migrations/copy-dags-to-rwx.yaml`.

2. **Multi-scheduler logs**: with 2 schedulers (LocalExecutor), each task's logs
   live on the scheduler pod that ran it and are fetched via that pod's log
   server (port 8793, stable headless-StatefulSet DNS). Robust, but consider
   GCS remote logging now that RWX exists.

3. **Manual Node Labeling**: no longer required for the DAG volume (RWX mounts on
   any node). The `poorbricks.io/dags` label is only consulted by the one-shot
   DAG-copy migration Job on an existing cluster.

## Key Commands

| Command | Purpose |
|---------|---------|
| `bash scripts/deploy_k8s.sh` | Deploy all components in correct order |
| `kubectl get pods -n airflow -o wide` | Check pod status and node placement |
| `kubectl get pvc -n airflow` | Verify DAG PVC is Bound |
| `kubectl logs -n airflow -l component=dag-processor -f` | Monitor DAG discovery |
| `kubectl logs -n airflow -l component=triggerer -f` | Monitor event triggers |

## Measurement

**Deployment Success**:
- [ ] All pods Running (no Init/CrashLoop)
- [ ] airflow-dags PVC Bound
- [ ] Infrastructure tests pass (8+/8)

**E2E Success**:
- [ ] DAG discovery within 60s
- [ ] sample_users DAG runs → silver.sample_users populated
- [ ] gold_pipeline DAG runs → gold.patients populated
- [ ] Data verification query returns COUNT > 0

## Previous Session Context

This session completed the consolidation started in prior work on single-namespace architecture. Earlier achievements:
- [[fix_mongo_id_mapping]] — Fixed ObjectId→string conversion in MongoDB preprocessing
- Single-namespace design choice (vs multi-namespace Helm)
- RWO PVC constraint mitigation via node affinity

This session removed the last Helm remnants and verified zero dead code.
