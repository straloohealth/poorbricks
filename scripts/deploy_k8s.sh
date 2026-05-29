#!/usr/bin/env bash

set -euo pipefail

echo "Deploying single-namespace Airflow + Poorbricks to K8s..."

# 1. Create namespace and core Airflow infrastructure (PVC, secrets, config, RBAC)
echo "1. Creating airflow namespace and core infrastructure..."
kubectl apply -f deploy/k8s/airflow-custom/00-namespace.yaml
# ReadWriteMany DAG volume on Filestore — shared across all on-demand nodes so
# the control plane + API can run multi-replica/multi-node. The legacy
# ReadWriteOnce PVC (00-pvc.yaml) is retained in the repo only for migrating an
# existing cluster (see migrations/copy-dags-to-rwx.yaml) and is NOT applied on
# a fresh install.
kubectl apply -f deploy/k8s/airflow-custom/00-pvc-rwx.yaml
kubectl apply -f deploy/k8s/airflow-custom/01-secrets.yaml
kubectl apply -f deploy/k8s/airflow-custom/02-configmap.yaml
kubectl apply -f deploy/k8s/airflow-custom/04-serviceaccount.yaml

# 2. (no-op) Local airflow-postgresql removed; using CNPG cluster in storage namespace.
#    Run the pg-migration job before deleting the old Deployment if migrating an
#    existing cluster:
#      kubectl apply -f deploy/k8s/airflow-custom/migrations/pg-migration.yaml
#      kubectl wait --for=condition=complete job/pg-migration -n airflow --timeout=30m
kubectl apply -f deploy/k8s/airflow-custom/03-postgresql.yaml

# 3. Deploy MongoDB
echo "3. Deploying MongoDB..."
kubectl apply -f deploy/k8s/mongo/mongo.yaml

# 4. (removed) Node-labeling for DAG pod co-location is no longer needed: the
#    DAG volume is now ReadWriteMany (Filestore), so the control plane spreads
#    across the on-demand pool instead of being pinned to one node. The spot
#    pool's taint keeps these pods on on-demand nodes without a selector.
#    NOTE: when migrating an *existing* cluster, the poorbricks.io/dags label
#    (still on the old node) is what lets migrations/copy-dags-to-rwx.yaml mount
#    the old RWO PD to copy DAGs into the RWX volume before cutover.
echo "4. (skipped) RWX DAG volume — no single-node pinning required."

# 5. Run database migrations
echo "5. Running Airflow database migrations..."
kubectl apply -f deploy/k8s/airflow-custom/05-migrations.yaml
kubectl wait --for=condition=complete job/airflow-migrations -n airflow --timeout=5m

# 6. Create pod template ConfigMap for KubernetesExecutor worker pods
echo "6. Creating pod template ConfigMap..."
kubectl create configmap airflow-pod-template \
  --from-file=pod_template_file.yaml=deploy/k8s/airflow/pod_template.yaml \
  -n airflow --dry-run=client -o yaml | kubectl apply -f -

# 7. Deploy Airflow components (scheduler, dag-processor, triggerer, webserver)
echo "7. Deploying Airflow components..."
kubectl apply -f deploy/k8s/airflow-custom/05-scheduler.yaml
kubectl apply -f deploy/k8s/airflow-custom/06-triggerer.yaml
kubectl apply -f deploy/k8s/airflow-custom/07-dag-processor.yaml
kubectl apply -f deploy/k8s/airflow-custom/08-webserver.yaml
kubectl apply -f deploy/k8s/airflow-custom/09-ingress.yaml
# PodDisruptionBudgets for the 2-replica components (keep ≥1 up during drains).
kubectl apply -f deploy/k8s/airflow-custom/10-pdb.yaml

# 8. Create runtime secret for worker pods (sources production creds from .env)
echo "8. Creating poorbricks-runtime secret..."
if [[ ! -f .env ]]; then
  echo "   ERROR: .env not found in $(pwd). MONGO_URI + POSTGRES_* must be set there." >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source .env
set +a
: "${MONGO_URI:?MONGO_URI must be set in .env}"
# POSTGRES_* credentials are now VSO-managed via poorbricks-server-postgresql-creds.
# Only MONGO_URI and API configuration belong in this secret.
kubectl create secret generic poorbricks-runtime \
  -n airflow \
  --from-literal=POORBRICKS_API_DAG_STORE=local \
  --from-literal=POORBRICKS_API_DAGS_DIR=/opt/airflow/dags \
  --from-literal=POORBRICKS_API_TABLE_REPO_URL_TEMPLATE="https://github.com/{prefix}.git" \
  --from-literal=MONGO_URI="${MONGO_URI}" \
  --from-literal=CONTRACTS_MONGO_URI="${CONTRACTS_MONGO_URI:-${MONGO_URI}}" \
  --dry-run=client -o yaml | kubectl apply -f -

# 9. Deploy Poorbricks API server
echo "9. Deploying Poorbricks API server..."
kubectl apply -f deploy/k8s/api/serviceaccount.yaml
kubectl apply -f deploy/k8s/api/service.yaml
kubectl apply -f deploy/k8s/api/ingress.yaml
kubectl apply -f deploy/k8s/api/deployment.yaml
kubectl apply -f deploy/k8s/api/pdb.yaml

# 10. Wait for critical services to be ready
echo "10. Waiting for services to be ready..."
kubectl wait --for=condition=Ready pod -l component=dag-processor -n airflow --timeout=5m || true
kubectl wait --for=condition=Ready pod -l component=triggerer -n airflow --timeout=5m || true
kubectl wait --for=condition=Ready pod -l app=mongo -n airflow --timeout=5m || true
kubectl wait --for=condition=Ready pod -l app=poorbricks-server -n airflow --timeout=5m || true

echo "✓ Deployment complete!"
echo ""
echo "Cluster status:"
kubectl get pods -n airflow -o wide
echo ""
kubectl get pvc -n airflow
echo ""
echo "To upload DAGs to the Poorbricks API:"
echo "  curl -X POST https://<ingress-host>/v1/upload -F 'prefix=gold-test' -F 'code=@/tmp/table-repo.tar.gz'"
echo ""
echo "Airflow UI:"
echo "  Hostname: kubectl get ingress airflow-webserver -n airflow -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'"
echo "  User: admin"
echo "  Password (auto-generated by SimpleAuthManager, printed on first boot):"
echo "    kubectl logs -n airflow -l component=webserver --tail=200 | grep -i password"
