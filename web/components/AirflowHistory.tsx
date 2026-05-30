"use client";

import { tableOf, type RunRecord } from "@/lib/api";
import { fmtDateTime } from "@/lib/datetime";
import { rowActivate } from "@/lib/interactive";
import { usePagination } from "@/lib/usePagination";
import { PaginationControls } from "@/components/PaginationControls";

export function AirflowHistory({
  runs,
  airflowUrl,
  selected,
  onSelect,
}: {
  runs: RunRecord[];
  airflowUrl?: string;
  selected: string | null;
  onSelect: (table: string) => void;
}) {
  const pg = usePagination(runs, 25);
  return (
    <section className="panel" data-cy="airflow-history">
      <div className="rowflex">
        <h2 style={{ margin: 0 }}>Run history</h2>
        {airflowUrl && (
          <a href={airflowUrl} target="_blank" rel="noreferrer" data-cy="airflow-link">
            open Airflow ↗
          </a>
        )}
      </div>
      {runs.length === 0 ? (
        <div className="empty">No runs recorded for this environment.</div>
      ) : (
        <table className="grid">
          <thead>
            <tr>
              <th>table</th>
              <th>status</th>
              <th>rows</th>
              <th>dur (s)</th>
              <th>finished</th>
            </tr>
          </thead>
          <tbody>
            {pg.pageItems.map((r, i) => {
              const t = tableOf(r.pipeline_key);
              return (
                <tr
                  key={i}
                  data-cy="run-row"
                  aria-pressed={selected === t}
                  {...rowActivate(() => onSelect(t))}
                  style={{
                    cursor: "pointer",
                    background: selected === t ? "var(--panel-2)" : undefined,
                  }}
                >
                  <td>{t}</td>
                  <td>
                    <span className={`badge ${r.status}`}>{r.status}</span>
                  </td>
                  <td>{r.row_count ?? ""}</td>
                  <td className="muted">{r.duration_s}</td>
                  <td className="muted">{fmtDateTime(r.finished_at)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      <PaginationControls p={pg} cyPrefix="airflow" />
    </section>
  );
}
