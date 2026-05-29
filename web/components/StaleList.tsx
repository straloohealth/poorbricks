"use client";

import { tableOf, type StalenessVerdict } from "@/lib/api";
import { rowActivate } from "@/lib/interactive";

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
  const stale = verdicts.filter((v) => v.state !== "ok");
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
            {stale.map((v) => (
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
                <td className="muted">{v.last_run ?? "—"}</td>
                <td className="muted">{ageLabel(v.age_s)}</td>
                <td className="muted">every {ageLabel(v.interval_s)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
