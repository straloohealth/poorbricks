"use client";

import { useEffect, useState } from "react";
import { AlertsPanel } from "@/components/AlertsPanel";
import { LineageGraph } from "@/components/LineageGraph";
import { TableDetail } from "@/components/TableDetail";
import {
  api,
  type Alert,
  type Grouped,
  type LineageGraph as Graph,
} from "@/lib/api";

const EMPTY: Grouped<Alert> = { error: [], warn: [], info: [] };

export default function MainPage() {
  const [runtime, setRuntime] = useState<Grouped<Alert>>(EMPTY);
  const [verification, setVerification] = useState<Grouped<Alert>>(EMPTY);
  const [graph, setGraph] = useState<Graph>({ nodes: [], edges: [] });
  const [selected, setSelected] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.allSettled([api.alerts("prod"), api.verification(), api.lineage()]).then(
      ([a, v, g]) => {
        if (a.status === "fulfilled") setRuntime(a.value);
        if (v.status === "fulfilled") setVerification(v.value);
        if (g.status === "fulfilled") setGraph(g.value);
        if (a.status === "rejected" || v.status === "rejected" || g.status === "rejected")
          setError("Could not reach the API server — data may be incomplete.");
        setLoaded(true);
      },
    );
  }, []);

  return (
    <div data-cy="main-page">
      {error && (
        <div className="alert error" data-cy="fetch-error">
          {error}
        </div>
      )}
      <AlertsPanel runtime={runtime} verification={verification} loading={!loaded} />

      <section className="panel">
        <h2>Lineage navigator</h2>
        <p className="muted">
          Click a table to highlight its{" "}
          <span style={{ color: "var(--info)" }}>sources</span> and{" "}
          <span style={{ color: "var(--ok)" }}>destinations</span>, and load its detail.
        </p>
        {!loaded ? (
          <div className="empty">Loading lineage…</div>
        ) : graph.nodes.length === 0 ? (
          <div className="empty">{error ? "Lineage unavailable." : "No contracts found."}</div>
        ) : (
          <LineageGraph graph={graph} selected={selected} onSelect={setSelected} />
        )}
        <div className="rowflex" style={{ marginTop: "0.5rem" }}>
          <label className="muted">Inspect:</label>
          <select
            data-cy="table-picker"
            value={selected ?? ""}
            onChange={(e) => setSelected(e.target.value || null)}
          >
            <option value="">—</option>
            {graph.nodes.map((n) => (
              <option key={n.id} value={n.id}>
                {n.label}
              </option>
            ))}
          </select>
        </div>
      </section>

      <TableDetail table={selected} environment="prod" />
    </div>
  );
}
