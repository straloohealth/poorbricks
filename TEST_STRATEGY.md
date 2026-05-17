# Infrastructure End-to-End Test Strategy

## Overview

Comprehensive testing ensures the poorbricks infrastructure works end-to-end from upload → DAG generation → workflow execution → data persistence.

## Test Layers

### Layer 1: Upload Verification
**File:** `tests/test_infrastructure_e2e.py::TestUploadVerification`

Tests the API upload endpoint:
- ✅ API health check
- ✅ Valid tarball acceptance
- ✅ Workflow metadata in response
- ✅ Contract validation
- ✅ DAG file generation

**Run:** `pytest tests/test_infrastructure_e2e.py::TestUploadVerification -m infrastructure -v`

### Layer 2: Data Source Connections
**File:** `tests/test_infrastructure_e2e.py::TestDataSourceConnections`

Verifies connectivity to external systems:
- ✅ MongoDB contracts store accessible
- ✅ PostgreSQL writable
- ✅ Table-repo structure discoverable

**Run:** `pytest tests/test_infrastructure_e2e.py::TestDataSourceConnections -m infrastructure -v`

### Layer 3: Workflow Communication
**File:** `tests/test_infrastructure_e2e.py::TestWorkflowCommunication`

Validates DAG structure and dependencies:
- ✅ Workflow YAML parsing valid
- ✅ Task dependencies correct (silver → gold)
- ✅ KubernetesPodOperator generation valid

**Run:** `pytest tests/test_infrastructure_e2e.py::TestWorkflowCommunication -m infrastructure -v`

### Layer 4: Silver Table Persistence
**File:** `tests/test_infrastructure_e2e.py::TestDataPersistence`

Verifies silver tables write to PostgreSQL:
- ✅ sample_users table created
- ✅ Data matches profiles from upload response

**Requires:** Pipeline execution in Airflow

### Layer 5: Gold Table Population
**File:** `tests/test_infrastructure_e2e.py::TestGoldTablePopulation`

Verifies gold tables are computed and persisted:
- ✅ gold_patients table computed from silver
- ✅ Schema matches pipeline definition
- ✅ Data integrity constraints satisfied

**Requires:** Pipeline execution completing silver → gold chain

## Quick Test Scenarios

### Scenario 1: Upload Only (Offline)
```bash
# Tests upload without requiring Airflow/database
pytest tests/test_infrastructure_e2e.py::TestUploadVerification \
       tests/test_infrastructure_e2e.py::TestWorkflowCommunication \
       -m infrastructure -v
```

### Scenario 2: With Data Source Checks
```bash
# Adds data source connectivity (requires running MongoDB/PostgreSQL)
pytest tests/test_infrastructure_e2e.py::TestUploadVerification \
       tests/test_infrastructure_e2e.py::TestDataSourceConnections \
       tests/test_infrastructure_e2e.py::TestWorkflowCommunication \
       -m infrastructure -v
```

### Scenario 3: Full End-to-End (Requires Airflow)
```bash
# Complete test including Airflow execution and data verification
pytest tests/test_infrastructure_e2e.py -m infrastructure -v
```

## CI Integration

### Circle CI Configuration
Place in `.circleci/config.yml`:

```yaml
workflows:
  test-infrastructure:
    jobs:
      - test-upload-verification:
          name: Upload Verification Tests
          command: pytest tests/test_infrastructure_e2e.py::TestUploadVerification -m infrastructure -v

      - test-data-sources:
          name: Data Source Connection Tests
          requires:
            - test-upload-verification
          command: |
            docker-compose up -d mongodb postgres
            sleep 10
            pytest tests/test_infrastructure_e2e.py::TestDataSourceConnections -m infrastructure -v

      - test-workflows:
          name: Workflow Structure Tests
          requires:
            - test-upload-verification
          command: pytest tests/test_infrastructure_e2e.py::TestWorkflowCommunication -m infrastructure -v

      - test-end-to-end:
          name: End-to-End Pipeline Tests
          requires:
            - test-data-sources
            - test-workflows
          command: |
            docker-compose up -d mongodb postgres airflow
            sleep 30
            # Manually trigger gold_pipeline DAG
            kubectl exec ... -- airflow dags trigger gold_pipeline
            sleep 120
            pytest tests/test_infrastructure_e2e.py::TestDataPersistence \
                   tests/test_infrastructure_e2e.py::TestGoldTablePopulation \
                   -m infrastructure -v
```

## What Each Test Validates

| Test | Validates | Requires | Priority |
|------|-----------|----------|----------|
| Upload Verification | API accepts/processes uploads | API running | Critical |
| Workflow Parsing | DAG YAML is valid | None (local) | Critical |
| DAG Generation | DAGs are syntactically valid | None (local) | Critical |
| Contract Connection | MongoDB accessible | MongoDB running | High |
| Database Connection | PostgreSQL writable | PostgreSQL running | High |
| Pipeline Execution | Worker pods created/executed | Airflow + K8s | High |
| Silver Persistence | sample_users table populated | Full pipeline | High |
| Gold Computation | gold_patients computed from silver | Full pipeline | High |
| Data Integrity | Gold table has valid data | Full pipeline | High |

## Blocking Issues for Full End-to-End

Currently blocking complete E2E tests:

1. **DAG Delivery to Airflow** — Generated DAGs not being picked up by scheduler
   - GCS sync sidecar auth issues (Workload Identity)
   - Workaround: Manual copy of DAG files to Airflow
   - Status: Under investigation

2. **PostgreSQL Connection in Workers** — Workers need access to PostgreSQL
   - Requires Secret/poorbricks-runtime with POSTGRES_* env vars
   - Status: Partially configured (localhost placeholder)

3. **MongoDB Replica Set** — Single-node MongoDB may need special handling
   - Current setup: MongoDB 7 single instance
   - Status: Working for contract validation

## Next Steps

1. ✅ Create test suite (done)
2. ⏳ Fix DAG delivery mechanism (in progress)
3. ⏳ Configure PostgreSQL access in workers (pending)
4. ⏳ Run full E2E tests (pending)
5. ⏳ Add tests to CI pipeline (pending)

## Running Locally

```bash
# Install test dependencies
poetry add pytest requests psycopg pymongo --group test

# Run upload verification only
poetry run pytest tests/test_infrastructure_e2e.py::TestUploadVerification -m infrastructure -v

# Run all tests that don't require databases
poetry run pytest tests/test_infrastructure_e2e.py -m infrastructure -k "not slow" -v

# Run specific test
poetry run pytest tests/test_infrastructure_e2e.py::TestWorkflowCommunication::test_gold_pipeline_workflow_valid -v
```
