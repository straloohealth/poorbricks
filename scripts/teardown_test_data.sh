#!/usr/bin/env bash
set -euo pipefail

# Teardown test data from K8s-hosted services.
# Resets PostgreSQL schemas (silver/gold) and MongoDB contracts collection.
# Does NOT modify infrastructure — data resets, services stay running.

echo "==================================================================="
echo "Tearing down test data from Kubernetes services..."
echo "==================================================================="
echo

# PostgreSQL teardown
echo "[1/2] PostgreSQL: Dropping and recreating schemas..."
PG_POD=$(kubectl get pod -n postgres -l app=postgres -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -z "$PG_POD" ]; then
    echo "❌ PostgreSQL pod not found in postgres namespace"
    echo "   Run: kubectl get pods -n postgres -l app=postgres"
    exit 1
fi

kubectl exec -n postgres "$PG_POD" -- psql -U analytics -d analytics <<EOF
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
EOF

echo "✅ PostgreSQL schemas reset (silver, gold)"
echo

# MongoDB teardown
echo "[2/2] MongoDB: Clearing contracts collection..."
MONGO_POD=$(kubectl get pod -n poorbricks -l app=mongo -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -z "$MONGO_POD" ]; then
    echo "❌ MongoDB pod not found in poorbricks namespace"
    echo "   Run: kubectl get pods -n poorbricks -l app=mongo"
    exit 1
fi

kubectl exec -n poorbricks "$MONGO_POD" -- mongosh poorbricks_contracts \
    --eval 'db.data_contracts.deleteMany({})' --quiet

echo "✅ MongoDB contracts collection cleared"
echo

echo "==================================================================="
echo "Test data teardown complete."
echo "==================================================================="
