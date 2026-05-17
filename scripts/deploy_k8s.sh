#!/usr/bin/env bash

set -euo pipefail

echo "Deploying single-namespace Airflow + Poorbricks to K8s..."

# 1. Create namespace and core Airflow infrastructure (PVC, secrets, config, RBAC)
echo "1. Creating airflow namespace and core infrastructure..."
kubectl apply -f deploy/k8s/airflow-custom/00-namespace.yaml
kubectl apply -f deploy/k8s/airflow-custom/00-pvc.yaml
kubectl apply -f deploy/k8s/airflow-custom/01-secrets.yaml
kubectl apply -f deploy/k8s/airflow-custom/02-configmap.yaml
kubectl apply -f deploy/k8s/airflow-custom/04-serviceaccount.yaml

# 2. Deploy PostgreSQL for Airflow metadata database
echo "2. Deploying PostgreSQL..."
kubectl apply -f deploy/k8s/airflow-custom/03-postgresql.yaml

# 3. Deploy MongoDB
echo "3. Deploying MongoDB..."
kubectl apply -f deploy/k8s/mongo/mongo.yaml

# 4. Label one node for DAG pod co-location (fixes RWO PVC multi-attach)
echo "4. Labeling node for DAG pod affinity..."
NODE=$(kubectl get nodes --no-headers | awk 'NR==1{print $1}')
echo "   Using node: ${NODE}"
kubectl label node "${NODE}" poorbricks.io/dags=true --overwrite

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

# 8. Create runtime secret for worker pods
echo "8. Creating poorbricks-runtime secret..."
kubectl create secret generic poorbricks-runtime \
  -n airflow \
  --from-literal=POORBRICKS_API_DAG_STORE=local \
  --from-literal=POORBRICKS_API_DAGS_DIR=/opt/airflow/dags \
  --from-literal=POORBRICKS_API_TABLE_REPO_URL_TEMPLATE="https://github.com/{prefix}.git" \
  --dry-run=client -o yaml | kubectl apply -f -

# 9. Deploy Poorbricks API server
echo "9. Deploying Poorbricks API server..."
kubectl apply -f deploy/k8s/api/serviceaccount.yaml
kubectl apply -f deploy/k8s/api/service.yaml
kubectl apply -f deploy/k8s/api/ingress.yaml
kubectl apply -f deploy/k8s/api/deployment.yaml

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
