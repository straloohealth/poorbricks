"use client";

import type { ProdDiff } from "@/lib/api";
import { usePagination } from "@/lib/usePagination";
import { PaginationControls } from "@/components/PaginationControls";

function pct(v: number | null): string {
  if (v == null) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(1)}%`;
}

function rate(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

/** Expanded detail panel for a dev-run vs production diff. */
export function ProdDiffDetail({ diff }: { diff: ProdDiff }) {
  const nulls = usePagination(diff.null_dist, 10);
  const { row_count: rc, fields, alerts } = diff;
  const dir = rc.delta_pct == null ? "" : rc.delta_pct >= 0 ? "delta-up" : "delta-down";
  return (
    <div className="proddiff" data-cy="proddiff-detail">
      <h4>Row count</h4>
      <div className="rowflex" data-cy="proddiff-rowcount">
        <span>
          dev <b>{rc.dev ?? "?"}</b> vs prod <b>{rc.prod ?? "?"}</b>
        </span>
        <span className={dir}>{pct(rc.delta_pct)}</span>
        {rc.major && <span className="badge major">major</span>}
      </div>

      <h4>Null distribution</h4>
      {diff.null_dist.length === 0 ? (
        <div className="empty">no null-rate changes</div>
      ) : (
        <div data-cy="proddiff-nulls">
          <table className="grid">
            <thead>
              <tr>
                <th>column</th>
                <th>dev</th>
                <th>prod</th>
                <th>Δ</th>
              </tr>
            </thead>
            <tbody>
              {nulls.pageItems.map((n) => (
                <tr key={n.column}>
                  <td>{n.column}</td>
                  <td className="muted">{rate(n.dev)}</td>
                  <td className="muted">{rate(n.prod)}</td>
                  <td className={n.major ? "delta-down" : "muted"}>
                    {n.delta >= 0 ? "+" : ""}
                    {(n.delta * 100).toFixed(1)}pp
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <PaginationControls p={nulls} cyPrefix="proddiff-nulls" />
        </div>
      )}

      <h4>Fields</h4>
      <div data-cy="proddiff-fields" className="rowflex">
        <span className="delta-up">+ {fields.added.join(", ") || "none"}</span>
        <span className="delta-down">− {fields.removed.join(", ") || "none"}</span>
        {fields.retyped.length > 0 && (
          <span className="muted">
            retyped: {fields.retyped.map((r) => `${r.column} ${r.from}→${r.to}`).join(", ")}
          </span>
        )}
      </div>

      <h4>Alerts</h4>
      <div data-cy="proddiff-alerts" className="rowflex">
        <span className="delta-down">new: {alerts.added.join(", ") || "none"}</span>
        <span className="delta-up">cleared: {alerts.removed.join(", ") || "none"}</span>
        <span className="muted">existing: {alerts.existing.join(", ") || "none"}</span>
      </div>
    </div>
  );
}
