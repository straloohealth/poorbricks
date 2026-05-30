"use client";

import { useMemo } from "react";
import { tableOf, type StalenessVerdict } from "@/lib/api";
import { fmtDateTime } from "@/lib/datetime";
import { rowActivate } from "@/lib/interactive";
import { usePagination } from "@/lib/usePagination";
import { PaginationControls } from "@/components/PaginationControls";

function ageLabel(s: number | null): string {
  if (s == null) return "never";
  const h = s / 3600;
  return h >= 1 ? `${h.toFixed(1)}h` : `${Math.round(s / 60)}m`;
}

export function StaleList({
  verdicts,
  onSelect,
  loading,
}: {
  verdicts: StalenessVerdict[];
  onSelect?: (table: string) => void;
  loading?: boolean;
}) {
  // Memoise so the derived array keeps a stable identity across renders —
  // otherwise usePagination would reset to page 0 on every render.
  const stale = useMemo(() => verdicts.filter((v) => v.state !== "ok"), [verdicts]);
  const pg = usePagination(stale, 15);
  return (
    <section className="panel" data-cy="stale-list">
      <h2>Stale datasets</h2>
      {stale.length === 0 ? (
        loading ? (
          <div className="empty" data-cy="stale-loading">Loading…</div>
        ) : (
          <div className="empty" data-cy="no-stale">Everything is fresh.</div>
        )
      ) : (
        <table className="grid">
          <thead>
            <tr>
              <th>table</th>
              <th>state</th>
              <th>last run</th>
              <th>age</th>
              <th>cadence</th>
            </tr>
          </thead>
          <tbody>
            {pg.pageItems.map((v) => (
              <tr
                key={v.pipeline_key}
                data-cy="stale-row"
                {...rowActivate(onSelect ? () => onSelect(tableOf(v.pipeline_key)) : undefined)}
                style={{ cursor: onSelect ? "pointer" : undefined }}
              >
                <td>{tableOf(v.pipeline_key)}</td>
                <td>
                  <span className={`badge ${v.state}`}>{v.state}</span>
                </td>
                <td className="muted">{fmtDateTime(v.last_run)}</td>
                <td className="muted">{ageLabel(v.age_s)}</td>
                <td className="muted">every {ageLabel(v.interval_s)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {stale.length > 0 && <PaginationControls p={pg} cyPrefix="stale" />}
    </section>
  );
}
