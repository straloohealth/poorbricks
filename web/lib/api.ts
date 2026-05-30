// Typed client for the poorbricks FastAPI server.
// Base URL is overridable so Cypress/e2e can point at the live API.

import { toUtcIso } from "./datetime";

export const API_BASE =
  process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8088";

// Cosmo's knowledge API (Ask / Docs / Glossary / Memory) is a separate service.
export const COSMO_BASE =
  process.env.NEXT_PUBLIC_COSMO_URL ?? "http://localhost:8077";

export type Severity = "error" | "warn" | "info";
export type Grouped<T> = Record<Severity, T[]>;

export interface Alert {
  kind: string;
  pipeline_key: string;
  summary: string;
}

export interface LineageNode {
  id: string;
  label: string;
  kind: string; // bronze | silver | gold | mongo | postgres | unknown
}
export interface LineageEdge {
  source: string;
  target: string;
}
export interface LineageGraph {
  nodes: LineageNode[];
  edges: LineageEdge[];
}

export interface RunRecord {
  id?: number;
  pipeline_key: string;
  table_name: string;
  environment: string;
  sha: string | null;
  mode: string;
  status: string; // ok | failed
  started_at: string | null;
  finished_at: string | null;
  duration_s: number;
  row_count: number | null;
  schema_hash: string | null;
  error: string | null;
  anomaly: Record<string, unknown> | null;
  drift_summary: Record<string, unknown> | null;
  timings: Record<string, number>;
  // Cheap precomputed severity of this (dev) run vs the prod baseline, so the
  // run-history badge needs no extra fetch. Omitted on prod runs / when absent.
  prod_diff_severity?: "none" | "minor" | "major";
}

// Structured dev-run-vs-production diff (GET /v1/runs/{id}/prod-diff).
export interface ProdDiff {
  run_id: number;
  table: string;
  prod_sha: string | null;
  dev_sha: string | null;
  severity: "none" | "minor" | "major";
  row_count: { dev: number | null; prod: number | null; delta_pct: number | null; major: boolean };
  null_dist: { column: string; dev: number; prod: number; delta: number; major: boolean }[];
  fields: {
    added: string[];
    removed: string[];
    retyped: { column: string; from: string; to: string }[];
  };
  alerts: { added: string[]; removed: string[]; existing: string[] };
}

// A GitHub-PR-style line comment on a table's uploaded source file.
export interface SourceComment {
  id: string;
  table_name: string;
  file: string;
  line_start: number;
  line_end: number;
  body: string;
  release_sha: string | null;
  resolved: boolean;
  created_at: string | null;
}

export interface StalenessVerdict {
  pipeline_key: string;
  state: string; // ok | overdue | missing
  last_run: string | null;
  interval_s: number;
  age_s: number | null;
}

export interface TableSnapshot {
  schema: string;
  name: string;
  row_count: number;
  size_bytes: number;
  columns: { name: string; data_type: string; nullable: boolean }[];
  sample_rows: Record<string, unknown>[];
}

// /v1/stats projects tables without columns/sample_rows (sample_size=0), so it
// is NOT a full TableSnapshot — keep this lightweight shape honest.
export interface TableStat {
  schema: string;
  name: string;
  row_count: number;
  size_bytes: number;
}

export interface Contract {
  table_name: string;
  level?: string;
  storage?: string;
  comment?: string;
  fields?: {
    name: string;
    type: string;
    nullable?: boolean;
    description?: string;
    is_literal?: boolean;
  }[];
  expectations?: Record<string, unknown>;
  // Heterogeneous per source kind (see poorbricks/persist.py::_serialize_inputs).
  inputs?: (
    | { name: string; kind: "TableSource"; table_name: string; model?: string; schema?: unknown }
    | { name: string; kind: "ContractSource"; table_name: string }
    | { name: string; kind: "MongoSource"; db: string; collection: string; schema?: unknown }
    | { name: string; kind: "PostgresTableSource"; schema_name: string; table: string }
    | { name: string; kind: string; table_name?: string }
  )[];
  lineage?: {
    columns?: Record<
      string,
      { sources: { input?: string; table?: string; column?: string }[]; exact: boolean }
    >;
    consumed?: Record<string, string[]>;
  };
  last_run?: Record<string, unknown>;
  profile?: {
    row_count?: number;
    null_rates?: Record<string, number>;
    enum_samples?: Record<string, unknown[]>;
  };
}

export interface ErrorRow {
  pipeline_key: string;
  table_name: string;
  environment: string;
  finished_at: string | null;
  headline: string;
  error: string;
}

export interface SourceFiles {
  table_name: string;
  module: string;
  prefix: string;
  files: Record<string, string>;
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} → HTTP ${res.status}`);
  return (await res.json()) as T;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`${path} → HTTP ${res.status}`);
  return (res.status === 204 ? (undefined as T) : ((await res.json()) as T));
}

async function del<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { method: "DELETE", cache: "no-store" });
  if (!res.ok) throw new Error(`${path} → HTTP ${res.status}`);
  return (res.status === 204 ? (undefined as T) : ((await res.json()) as T));
}

export const api = {
  alerts: (env = "prod") =>
    get<Grouped<Alert>>(`/v1/alerts?environment=${encodeURIComponent(env)}`),
  verification: () => get<Grouped<Alert>>("/v1/verification"),
  lineage: () => get<LineageGraph>("/v1/lineage"),
  runs: (limit = 200) => get<RunRecord[]>(`/v1/runs?limit=${limit}`),
  errors: (limit = 200) => get<ErrorRow[]>(`/v1/errors?limit=${limit}`),
  source: (table: string) =>
    get<SourceFiles>(`/v1/source/${encodeURIComponent(table)}`),
  staleness: () => get<StalenessVerdict[]>("/v1/staleness"),
  stats: () =>
    get<{
      server: Record<string, unknown>;
      tables: TableStat[];
      total_rows: number;
      table_count: number;
    }>("/v1/stats"),
  contract: (name: string) =>
    get<Contract>(`/v1/contracts/${encodeURIComponent(name)}`),
  contracts: () => get<{ table_name: string }[]>("/v1/contracts"),
  tablePreview: (schema: string, name: string, limit = 20) =>
    get<TableSnapshot>(
      `/v1/table/${encodeURIComponent(schema)}/${encodeURIComponent(name)}?limit=${limit}`,
    ),
  prodDiff: (runId: number) => get<ProdDiff>(`/v1/runs/${runId}/prod-diff`),
  sourceComments: (table: string) =>
    get<SourceComment[]>(`/v1/source/${encodeURIComponent(table)}/comments`),
  addSourceComment: (
    table: string,
    body: { file: string; line_start: number; line_end: number; body: string; release_sha?: string | null },
  ) => post<SourceComment>(`/v1/source/${encodeURIComponent(table)}/comments`, body),
  deleteSourceComment: (table: string, id: string) =>
    del<{ ok: boolean }>(`/v1/source/${encodeURIComponent(table)}/comments/${encodeURIComponent(id)}`),
};

// --- Cosmo knowledge API ---------------------------------------------------

export interface CosmoDoc {
  id: string;
  title: string;
  doc_type: string; // markdown | pdf
  chunks: number;
  ingested_at: string;
}
export interface GlossaryTerm {
  id: string;
  term: string;
  definition: string;
  aliases: string[];
  updated_at: string;
}
export interface MemoryItem {
  id: string;
  question: string;
  answer: string;
  lesson: string;
  status: string; // recent | corrected
  updated_at: string;
}
export interface AskResponse {
  answer: string;
  intent: string; // data | concept | clarify
}

async function cget<T>(path: string): Promise<T> {
  const res = await fetch(`${COSMO_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} → HTTP ${res.status}`);
  return (await res.json()) as T;
}
async function cjson<T>(path: string, method: string, body?: unknown): Promise<T> {
  const res = await fetch(`${COSMO_BASE}${path}`, {
    method,
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    cache: "no-store",
  });
  if (!res.ok) throw new Error(`${path} → HTTP ${res.status}`);
  return (res.status === 204 ? (undefined as T) : ((await res.json()) as T));
}

export const cosmo = {
  ask: (question: string, threadId?: string) =>
    cjson<AskResponse>("/v1/ask", "POST", { question, thread_id: threadId }),
  docs: () => cget<CosmoDoc[]>("/v1/docs"),
  createDoc: (title: string, text: string) =>
    cjson<CosmoDoc>("/v1/docs", "POST", { title, text }),
  uploadDoc: async (title: string, file: File): Promise<CosmoDoc> => {
    const form = new FormData();
    form.append("title", title);
    form.append("file", file);
    const res = await fetch(`${COSMO_BASE}/v1/docs/upload`, { method: "POST", body: form });
    if (!res.ok) throw new Error(`/v1/docs/upload → HTTP ${res.status}`);
    return (await res.json()) as CosmoDoc;
  },
  deleteDoc: (id: string) => cjson<{ ok: boolean }>(`/v1/docs/${encodeURIComponent(id)}`, "DELETE"),
  glossary: () => cget<GlossaryTerm[]>("/v1/glossary"),
  saveTerm: (term: string, definition: string, aliases: string[]) =>
    cjson<GlossaryTerm>("/v1/glossary", "POST", { term, definition, aliases }),
  deleteTerm: (key: string) =>
    cjson<{ ok: boolean }>(`/v1/glossary/${encodeURIComponent(key)}`, "DELETE"),
  memory: () => cget<MemoryItem[]>("/v1/memory"),
  editMemory: (id: string, lesson: string) =>
    cjson<MemoryItem>(`/v1/memory/${encodeURIComponent(id)}`, "PATCH", { lesson }),
  deleteMemory: (id: string) =>
    cjson<{ ok: boolean }>(`/v1/memory/${encodeURIComponent(id)}`, "DELETE"),
  reindex: () => cjson<{ docs: number; experiences: number; glossary: number }>("/v1/reindex", "POST"),
};

// helpers
export const tableOf = (pipelineKey: string): string =>
  pipelineKey.includes(":") ? pipelineKey.split(":").slice(-1)[0] : pipelineKey;

export const fmtAgeHours = (iso: string | null): number | null => {
  if (!iso) return null;
  // Normalise bare-UTC timestamps so freshness ages aren't skewed by the local
  // tz (see lib/datetime.ts::toUtcIso).
  const ms = Date.now() - new Date(toUtcIso(iso)).getTime();
  return ms / 3_600_000;
};
