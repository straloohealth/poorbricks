# poorbricks — uploading tables to dashboards

How a repo's pipeline tables are published so they appear in the poorbricks
dashboard. Two paths: **developer dev-upload** (quick iteration, doesn't touch
prod) and **CI upload** (automated on merge).

## Developer dev upload

```bash
poetry run poorbricks upload --env dev --prefix <repo> --sha <sha> --watch
```

- `--env dev` namespaces the DAG as `dev-<prefix>` and writes to a `*__dev`
  Postgres schema — runs on the shared Airflow **without touching prod tables or
  contracts**.
- `--prefix` is a short identifier for your repo (e.g. `smith`, `doctor`).
- `--sha` is your commit SHA (used for versioning).
- `--watch` tails the upload and DAG registration progress.

## CI upload

### CircleCI orb (if repo uses orb)

Add to `.circleci/config.yml`:

```yaml
orbs:
  tools: straloohealth/tools@3

workflows:
  deploy:
    jobs:
      - tools/poorbricks-upload:
          context: [tailscale]
          prefix: myservice
```

The job requires the `tailscale` context.

### Jenkins (if repo uses `@Library('straloo-tools')`)

Add a `poorbricksUpload()` stage to the Jenkinsfile:

```groovy
stage('upload-poorbricks') {
  when { branch 'main' }
  agent { kubernetes { yaml podTemplates.python() } }
  steps { checkout scm; poorbricksUpload() }
}
```

Both paths package `tables/` + `workflows/` into a gzipped tar and `POST`
multipart (fields: `prefix`, `sha`, file field `code`) to the server's
`POST /v1/upload` endpoint (`POORBRICKS_API_URL`, defaults to
`http://poorbricks-server.airflow.svc.cluster.local:8080`). Exits 0 only on
HTTP 200.

## What `POST /v1/upload` does

1. Registers DAGs for the repo's prefix on the shared Airflow.
2. **Prunes** DAGs and orphaned contracts for that prefix (pipelines you deleted
   from the repo).
3. A contract still consumed by another pipeline is **kept with a warning** —
   remove the consumer first, then re-upload.

## Env-var split (important gotcha)

| Variable | Used for |
|---|---|
| `POORBRICKS_API_URL` | upload path (`POST /v1/upload`) |
| `CONTRACTS_API_URL` | `verify --mode db` contract resolution |

Both point to `http://poorbricks-server.airflow.svc.cluster.local:8080` in-cluster.
The Jenkins step sets both; if you run `verify --mode db` locally without
`CONTRACTS_API_URL` it falls back to an unresolvable `.ts.net` ingress.
