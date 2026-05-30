# Poorbricks observability UI

A Next.js 14 (App Router, TypeScript) replacement for the old Streamlit app. It
talks to the poorbricks FastAPI server (`api/main.py`, default
`http://localhost:8088`) over HTTP — the frontend never touches Mongo/Postgres
directly.

Two pages:

- **Main** (`/`) — alerts grouped by severity (runtime + verification findings:
  stubs, literal columns, contract breaks), an interactive lineage navigator
  (click a table to highlight its sources/destinations), and a table-detail
  panel (contract, field descriptions, **field lineage explaining how each
  silver column is generated**, previous runs, Postgres status).
- **Live Now** (`/live`) — dev/prod toggle, run history, recent errors deduped
  per pipeline, stale datasets, and a freshness dot-plot (bucketed; click a
  column to list its tables).

## Run

```bash
npm install
# point at the API if it isn't on :8088
export NEXT_PUBLIC_API_URL=http://localhost:8088
# optional: link out to Airflow from the Live Now page
export NEXT_PUBLIC_AIRFLOW_URL=https://airflow.internal

npm run dev      # dev server on :3000
npm run build    # production build
npm start        # serve the build
```

## Tests

| Command | What | Browser? |
|---|---|---|
| `npm test` | Vitest component/unit tests (jsdom, headless) | no |
| `npm run cy:ct` | Cypress component tests (LineageGraph, FreshnessChart, AlertsPanel) | yes |
| `npm run cy:e2e` | Cypress e2e against a running app + API | yes |
| `.venv/bin/python scripts/e2e_live.py` | Selenium e2e (sandbox fallback, see below) | yes (Firefox) |

`data-cy` is the shared test-id attribute. Vitest is configured
(`vitest.setup.ts`) to resolve `getByTestId` against it, so component selectors
match across both runners.

### Cypress

`cypress/component/*` mount components in isolation; `cypress/e2e/*` drive the
live app. On a normal machine:

```bash
npm run dev &                       # or: npm run build && npm start -- -p 3100
CY_BROWSER=firefox npm run cy:e2e   # or chrome/electron
CY_BROWSER=firefox npm run cy:ct
```

Cypress needs its bundled **Electron**, which requires an X server. CI images
(`cypress/included`) or any desktop with X work out of the box.

### Selenium fallback (no-X / no-sudo sandboxes)

This dev container has **no X server and no sudo**, so Cypress's Electron can't
boot (it fails on `Missing X server or $DISPLAY`). A conda Firefox *does* run
headless here, so `scripts/e2e_live.py` drives it via geckodriver + Selenium and
exercises the **same flows** as the Cypress e2e specs against the live stack.
It's the executable proof that the UI works end-to-end with real data in this
environment; the Cypress specs remain the canonical e2e for normal machines.

```bash
# data stack on :8088 must be up (FastAPI + Mongo + Postgres)
export LD_LIBRARY_PATH=~/.mm/envs/browser/lib MOZ_HEADLESS=1 MOZ_DISABLE_CONTENT_SANDBOX=1
.venv/bin/python web/scripts/e2e_live.py   # 23 checks, writes /tmp/pb-e2e-*.png
```
