#!/usr/bin/env bash
set -euo pipefail

# Deploy Poorbricks infrastructure to Kubernetes in the correct order.
# Run from repo root: bash scripts/deploy_k8s.sh

echo "=== Deploying Poorbricks to Kubernetes ==="

# 1. Namespaces
echo "1. Creating namespaces..."
kubectl apply -f deploy/k8s/namespace.yaml

# 2. Airflow DAGs PVC (must exist before Helm references it)
echo "2. Creating Airflow DAGs PVC..."
kubectl apply -f deploy/k8s/airflow/pvc.yaml

# 3. MongoDB (ephemeral, test-only)
echo "3. Deploying MongoDB (ephemeral)..."
kubectl apply -f deploy/k8s/mongo/mongo.yaml

# 4. RBAC — grants airflow SA permission to spawn pods in poorbricks-workers
echo "4. Setting up RBAC for worker pods..."
kubectl apply -f deploy/k8s/workers/rbac.yaml

# 5. Runtime secret for worker pods
echo "5. Creating poorbricks-runtime secret..."
kubectl create secret generic poorbricks-runtime \
  -n poorbricks-workers \
  --from-literal=MONGO_URI=mongodb://mongo.poorbricks.svc.cluster.local:27017 \
  --from-literal=POSTGRES_HOST=postgres.postgres.svc.cluster.local \
  --from-literal=POSTGRES_PORT=5432 \
  --from-literal=POSTGRES_DB=analytics \
  --from-literal=POSTGRES_USER=analytics \
  --from-literal=POSTGRES_PASSWORD=analytics \
  --dry-run=client -o yaml | kubectl apply -f -

# 6. Airflow (Helm)
echo "6. Installing Airflow via Helm..."
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
  -n airflow --create-namespace \
  -f deploy/k8s/airflow/values.yaml

# 7. Pod template ConfigMap for KubernetesExecutor worker pods
echo "7. Creating pod template ConfigMap..."
kubectl create configmap airflow-pod-templates \
  --from-file=pod_template_file.yaml=deploy/k8s/airflow/pod_template.yaml \
  -n airflow --dry-run=client -o yaml | kubectl apply -f -

# 8. Wait for scheduler
echo "8. Waiting for Airflow scheduler..."
kubectl rollout status deployment/airflow-scheduler -n airflow --timeout=5m

# 9. API server
echo "9. Deploying poorbricks API server..."
kubectl apply -f deploy/k8s/api/serviceaccount.yaml
kubectl apply -f deploy/k8s/api/service.yaml
kubectl apply -f deploy/k8s/api/ingress.yaml
kubectl apply -f deploy/k8s/api/deployment.yaml
kubectl rollout status deployment/poorbricks-server -n poorbricks --timeout=5m

echo "=== Deployment complete ==="
echo "API reachable via Tailscale VPN:"
kubectl get ingress -n poorbricks poorbricks-server -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "(Tailscale hostname not yet assigned)"
