"use client";

import { Fragment, useEffect, useMemo, useState } from "react";
import {
  api,
  tableOf,
  type Contract,
  type ProdDiff,
  type RunRecord,
  type SourceFiles,
  type TableSnapshot,
} from "@/lib/api";
import { fmtDateTime } from "@/lib/datetime";
import { usePagination } from "@/lib/usePagination";
import { PaginationControls } from "@/components/PaginationControls";
import { TruncatedCell } from "@/components/TruncatedCell";
import { CodeViewer } from "@/components/CodeViewer";
import { ProdDiffBadge } from "@/components/ProdDiffBadge";
import { ProdDiffDetail } from "@/components/ProdDiffDetail";

type DiffState = ProdDiff | "loading" | "error";

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
  const [source, setSource] = useState<SourceFiles | null>(null);
  const [openFile, setOpenFile] = useState<string | null>(null);
  // "na" = table has no Postgres materialisation (e.g. a Mongo upstream node).
  const [preview, setPreview] = useState<"loading" | "ok" | "na" | "missing">("loading");
  // Dev-run-vs-prod diff: which run row is expanded + a per-run-id cache.
  const [expanded, setExpanded] = useState<number | null>(null);
  const [diffs, setDiffs] = useState<Record<number, DiffState>>({});

  const isDev = environment === "dev";

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
    setSource(null);
    setOpenFile(null);
    setPreview("loading");
    setExpanded(null);
    setDiffs({});
    api
      .contract(table)
      .then((c) => !ignore && (setContract(c), setContractLoaded(true)))
      .catch(() => !ignore && (setContract(null), setContractLoaded(true)));
    // Original repo source (transform/pipeline/config) for debugging.
    api
      .source(table)
      .then((s) => !ignore && setSource(s))
      .catch(() => !ignore && setSource(null));
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

  // Derived arrays memoised so pagination keeps a stable identity across renders.
  const cols = contract?.lineage?.columns ?? {};
  const consumed = contract?.lineage?.consumed ?? {};
  const fieldsArr = useMemo(() => contract?.fields ?? [], [contract]);
  const lineageEntries = useMemo(() => Object.entries(cols), [contract]); // eslint-disable-line react-hooks/exhaustive-deps
  const sampleRows = useMemo(() => snap?.sample_rows ?? [], [snap]);

  // Pagination hooks — declared before the early return (rules of hooks).
  const fieldsPg = usePagination(fieldsArr, 25);
  const lineagePg = usePagination(lineageEntries, 25);
  const runsPg = usePagination(runs, 10);
  const samplePg = usePagination(sampleRows, 8);

  const toggleExpand = (run: RunRecord): void => {
    if (run.id == null) return;
    const id = run.id;
    if (expanded === id) {
      setExpanded(null);
      return;
    }
    setExpanded(id);
    if (diffs[id] === undefined) {
      setDiffs((d) => ({ ...d, [id]: "loading" }));
      api
        .prodDiff(id)
        .then((diff) => setDiffs((d) => ({ ...d, [id]: diff })))
        .catch(() => setDiffs((d) => ({ ...d, [id]: "error" })));
    }
  };

  if (!table)
    return (
      <section className="panel" data-cy="table-detail">
        <div className="empty">Select a table in the lineage graph to see its details.</div>
      </section>
    );

  const lastRun = runs[0];
  const runCols = isDev ? 7 : 6;

  return (
    <section className="panel" data-cy="table-detail">
      <h2 data-cy="detail-title">◆ {table}</h2>
      {lastRun && (
        <div className="rowflex" data-cy="last-run">
          <span className={`badge ${lastRun.status}`}>{lastRun.status}</span>
          <span className="muted">env {lastRun.environment}</span>
          <span className="muted">{lastRun.row_count ?? "?"} rows</span>
          <span className="muted">{fmtDateTime(lastRun.finished_at)}</span>
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
              {fieldsPg.pageItems.map((f) => (
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
          <PaginationControls p={fieldsPg} cyPrefix="fields" />

          <h3>Field lineage — how each field is generated</h3>
          <table className="grid" data-cy="lineage-table">
            <thead>
              <tr>
                <th>output column</th>
                <th>source(s)</th>
              </tr>
            </thead>
            <tbody>
              {lineagePg.pageItems.map(([col, info]) => (
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
          <PaginationControls p={lineagePg} cyPrefix="lineage" />
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

      {source && Object.keys(source.files).length > 0 && (
        <div data-cy="source">
          <h3>Source (as written in the repo)</h3>
          <div className="rowflex">
            {Object.keys(source.files).map((f) => (
              <button
                key={f}
                className={`btn ${openFile === f ? "active" : ""}`}
                data-cy="source-file-btn"
                onClick={() => setOpenFile(openFile === f ? null : f)}
              >
                {f}
              </button>
            ))}
          </div>
          {openFile && source.files[openFile] && (
            <CodeViewer
              table={table}
              file={openFile}
              code={source.files[openFile]}
              sha={runs[0]?.sha ?? null}
            />
          )}
        </div>
      )}

      <h3>Previous runs</h3>
      {runs.length === 0 ? (
        <div className="empty">no recorded runs</div>
      ) : (
        <>
          <table className="grid" data-cy="runs-table">
            <thead>
              <tr>
                <th>status</th>
                <th>env</th>
                <th>rows</th>
                <th>dur (s)</th>
                <th>anomaly</th>
                <th>finished</th>
                {isDev && <th>vs prod</th>}
              </tr>
            </thead>
            <tbody>
              {runsPg.pageItems.map((r, i) => {
                const anom = r.anomaly as { is_anomaly?: boolean; reason?: string } | null;
                const canExpand = isDev && r.id != null;
                const isOpen = r.id != null && expanded === r.id;
                return (
                  <Fragment key={r.id ?? i}>
                    <tr
                      data-cy="run-history-row"
                      onClick={canExpand ? () => toggleExpand(r) : undefined}
                      style={{ cursor: canExpand ? "pointer" : undefined }}
                    >
                      <td>
                        <span className={`badge ${r.status}`}>{r.status}</span>
                      </td>
                      <td className="muted">{r.environment}</td>
                      <td>{r.row_count ?? ""}</td>
                      <td className="muted">{r.duration_s}</td>
                      <td className="muted">
                        {anom?.is_anomaly ? (
                          <TruncatedCell
                            value={String(anom.reason ?? "anomaly")}
                            max={28}
                            modalTitle="anomaly"
                          />
                        ) : (
                          ""
                        )}
                      </td>
                      <td className="muted">{fmtDateTime(r.finished_at)}</td>
                      {isDev && (
                        <td data-cy="run-expand-toggle">
                          {r.prod_diff_severity ? (
                            <ProdDiffBadge severity={r.prod_diff_severity} />
                          ) : canExpand ? (
                            <span className="muted">{isOpen ? "▾" : "▸"}</span>
                          ) : (
                            ""
                          )}
                        </td>
                      )}
                    </tr>
                    {isOpen && r.id != null && (
                      <tr>
                        <td colSpan={runCols}>
                          {diffs[r.id] === "loading" && <div className="empty">loading diff…</div>}
                          {diffs[r.id] === "error" && (
                            <div className="empty">
                              no diff — re-run this dev DAG to populate its profile snapshot.
                            </div>
                          )}
                          {diffs[r.id] && diffs[r.id] !== "loading" && diffs[r.id] !== "error" && (
                            <ProdDiffDetail diff={diffs[r.id] as ProdDiff} />
                          )}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
          <PaginationControls p={runsPg} cyPrefix="runs" />
        </>
      )}

      <h3>Postgres status</h3>
      {preview === "ok" && snap ? (
        <div data-cy="pg-status">
          <div className="rowflex">
            <span className="muted">
              {snap.schema}.{snap.name}
            </span>
            <span>{snap.row_count} rows</span>
          </div>
          {sampleRows.length > 0 && (
            <>
              <table className="grid">
                <thead>
                  <tr>
                    {Object.keys(sampleRows[0]).map((k) => (
                      <th key={k}>{k}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {samplePg.pageItems.map((row, i) => (
                    <tr key={i}>
                      {Object.entries(row).map(([k, v], j) => (
                        <td key={j} className="muted">
                          <TruncatedCell value={v} max={48} modalTitle={k} />
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              <PaginationControls p={samplePg} cyPrefix="samples" />
            </>
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
