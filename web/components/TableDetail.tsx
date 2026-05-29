"use client";

import { useEffect, useState } from "react";
import {
  api,
  tableOf,
  type Contract,
  type RunRecord,
  type TableSnapshot,
} from "@/lib/api";

export function TableDetail({
  table,
  environment = "prod",
}: {
  table: string | null;
  environment?: string;
}) {
  const [contract, setContract] = useState<Contract | null>(null);
  const [contractLoaded, setContractLoaded] = useState(false);
  const [runs, setRuns] = useState<RunRecord[]>([]);
  const [snap, setSnap] = useState<TableSnapshot | null>(null);
  // "na" = table has no Postgres materialisation (e.g. a Mongo upstream node).
  const [preview, setPreview] = useState<"loading" | "ok" | "na" | "missing">("loading");

  // Fetch the contract + run history for the selected table. The `ignore` flag
  // discards results from a superseded selection so a slow response for table A
  // can't land on table B (and every table-scoped bit of state is reset up front
  // so the previous table's data never lingers).
  useEffect(() => {
    if (!table) return;
    let ignore = false;
    setContract(null);
    setContractLoaded(false);
    setRuns([]);
    setSnap(null);
    setPreview("loading");
    api
      .contract(table)
      .then((c) => !ignore && (setContract(c), setContractLoaded(true)))
      .catch(() => !ignore && (setContract(null), setContractLoaded(true)));
    api
      .runs(300)
      .then((all) => !ignore && setRuns(all.filter((r) => tableOf(r.pipeline_key) === table)))
      .catch(() => !ignore && setRuns([]));
    return () => {
      ignore = true;
    };
  }, [table]);

  // Once the contract is known, preview Postgres — but only if the table is
  // actually materialised there (has a level). Otherwise mark it "na" rather
  // than spinning on "loading…" forever.
  useEffect(() => {
    if (!table || !contractLoaded) return;
    let ignore = false;
    const level = contract?.level;
    if (!level) {
      setSnap(null);
      setPreview("na");
      return;
    }
    const schema = environment === "dev" ? `${level}__dev` : level;
    setPreview("loading");
    api
      .tablePreview(schema, table, 15)
      .then((s) => !ignore && (setSnap(s), setPreview("ok")))
      .catch(() => !ignore && (setSnap(null), setPreview("missing")));
    return () => {
      ignore = true;
    };
  }, [table, contractLoaded, contract?.level, environment]);

  if (!table)
    return (
      <section className="panel" data-cy="table-detail">
        <div className="empty">Select a table in the lineage graph to see its details.</div>
      </section>
    );

  const cols = contract?.lineage?.columns ?? {};
  const consumed = contract?.lineage?.consumed ?? {};
  const lastRun = runs[0];

  return (
    <section className="panel" data-cy="table-detail">
      <h2 data-cy="detail-title">◆ {table}</h2>
      {lastRun && (
        <div className="rowflex" data-cy="last-run">
          <span className={`badge ${lastRun.status}`}>{lastRun.status}</span>
          <span className="muted">env {lastRun.environment}</span>
          <span className="muted">{lastRun.row_count ?? "?"} rows</span>
          <span className="muted">{lastRun.finished_at ?? ""}</span>
          {lastRun.sha && <span className="muted">sha {lastRun.sha.slice(0, 7)}</span>}
        </div>
      )}

      {!contract && <div className="empty">No published contract for this table.</div>}

      {contract && (
        <>
          {contract.comment && <p className="muted">{contract.comment}</p>}

          <h3>Fields</h3>
          <table className="grid" data-cy="fields-table">
            <thead>
              <tr>
                <th>column</th>
                <th>type</th>
                <th>nullable</th>
                <th>description</th>
                <th>literal</th>
              </tr>
            </thead>
            <tbody>
              {(contract.fields ?? []).map((f) => (
                <tr key={f.name}>
                  <td>{f.name}</td>
                  <td className="muted">{f.type}</td>
                  <td className="muted">{f.nullable ? "nullable" : "required"}</td>
                  <td className="muted">{f.description ?? ""}</td>
                  <td>{f.is_literal ? "⚠ literal" : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <h3>Field lineage — how each field is generated</h3>
          <table className="grid" data-cy="lineage-table">
            <thead>
              <tr>
                <th>output column</th>
                <th>source(s)</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(cols).map(([col, info]) => (
                <tr key={col}>
                  <td>{col}</td>
                  <td className={info.sources.length ? "" : "muted"}>
                    {info.sources.length
                      ? info.sources
                          .map((s) => `${s.table ?? "?"}.${s.column ?? "?"}`)
                          .join(", ")
                      : "(literal / stub — no upstream source)"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {Object.keys(consumed).length > 0 && (
            <p className="muted">
              Consumes:{" "}
              {Object.entries(consumed)
                .map(([t, c]) => `${t} [${c.join(", ")}]`)
                .join(" · ")}
            </p>
          )}
        </>
      )}

      <h3>Previous runs</h3>
      {runs.length === 0 ? (
        <div className="empty">no recorded runs</div>
      ) : (
        <table className="grid" data-cy="runs-table">
          <thead>
            <tr>
              <th>status</th>
              <th>env</th>
              <th>rows</th>
              <th>dur (s)</th>
              <th>anomaly</th>
              <th>finished</th>
            </tr>
          </thead>
          <tbody>
            {runs.slice(0, 10).map((r, i) => (
              <tr key={i}>
                <td>
                  <span className={`badge ${r.status}`}>{r.status}</span>
                </td>
                <td className="muted">{r.environment}</td>
                <td>{r.row_count ?? ""}</td>
                <td className="muted">{r.duration_s}</td>
                <td className="muted">
                  {r.anomaly && (r.anomaly as { is_anomaly?: boolean }).is_anomaly
                    ? String((r.anomaly as { reason?: string }).reason ?? "anomaly")
                    : ""}
                </td>
                <td className="muted">{r.finished_at ?? ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <h3>Postgres status</h3>
      {preview === "ok" && snap ? (
        <div data-cy="pg-status">
          <div className="rowflex">
            <span className="muted">{snap.schema}.{snap.name}</span>
            <span>{snap.row_count} rows</span>
          </div>
          {snap.sample_rows.length > 0 && (
            <table className="grid">
              <thead>
                <tr>
                  {Object.keys(snap.sample_rows[0]).map((k) => (
                    <th key={k}>{k}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {snap.sample_rows.slice(0, 8).map((row, i) => (
                  <tr key={i}>
                    {Object.values(row).map((v, j) => (
                      <td key={j} className="muted">{String(v)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      ) : (
        <div className="empty" data-cy="pg-empty">
          {preview === "loading"
            ? "loading…"
            : preview === "na"
              ? "no Postgres table for this dataset"
              : "not materialised"}
        </div>
      )}
    </section>
  );
}
