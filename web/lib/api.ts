// Typed client for the poorbricks FastAPI server.
// Base URL is overridable so Cypress/e2e can point at the live API.

export const API_BASE =
  process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8088";

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
};

// helpers
export const tableOf = (pipelineKey: string): string =>
  pipelineKey.includes(":") ? pipelineKey.split(":").slice(-1)[0] : pipelineKey;

export const fmtAgeHours = (iso: string | null): number | null => {
  if (!iso) return null;
  const ms = Date.now() - new Date(iso).getTime();
  return ms / 3_600_000;
};
