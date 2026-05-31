# poorbricks — observability UI

The poorbricks observability UI is a **Next.js 14** (App Router, TypeScript) app
that replaces the old Streamlit app. It talks to the poorbricks FastAPI server
(`api/main.py`, default `:8088`) over HTTP — the frontend never touches
MongoDB/PostgreSQL directly.

## Start the UI

```bash
cd web
npm install
npm run dev      # dev server → http://localhost:3000
```

The FastAPI server must be running:

```bash
# From the repo root
poetry run uvicorn api.main:app --port 8088
```

If the API isn't on `:8088`, override:

```bash
export NEXT_PUBLIC_API_URL=http://localhost:8088
```

Optional: link out to Airflow from the Live Now page:

```bash
export NEXT_PUBLIC_AIRFLOW_URL=https://airflow.internal
```

## Pages

### Main (`/`)
- Alerts grouped by severity: runtime findings, verification findings (stubs,
  literal columns, contract breaks).
- Interactive lineage navigator — click a table to highlight its sources and
  destinations.
- Table-detail panel: contract, field descriptions, **field lineage** (how each
  silver column is generated), previous runs, Postgres status.

### Live Now (`/live`)
- Dev/prod toggle.
- Run history per pipeline.
- Recent errors, deduped per pipeline.
- Stale datasets list.
- Freshness dot-plot (bucketed; click a column to list its tables).

## Legacy Streamlit UI

The old Streamlit UI is still available for quick local browsing:

```bash
poetry run streamlit run streamlit_app/app.py
```

## Viewing dev vs prod data

The Live Now page has a dev/prod toggle. Dev pipelines are namespaced
`dev-<prefix>` and write to `*__dev` Postgres schemas (see
[uploading-tables.md](uploading-tables.md)).
