"use client";

import { tableOf, type RunRecord } from "@/lib/api";
import { fmtDateTime } from "@/lib/datetime";
import { rowActivate } from "@/lib/interactive";
import { errorHeadline } from "@/lib/errorFormat";

// "5 recent errors per dag (not dag run, no repeats)": dedupe failed runs by
// pipeline key, keeping the most recent failure for each, then take the top 5.
export function dedupeErrors(runs: RunRecord[], limit = 5): RunRecord[] {
  const seen = new Set<string>();
  const out: RunRecord[] = [];
  for (const r of runs) {
    if (r.status !== "failed") continue;
    if (seen.has(r.pipeline_key)) continue;
    seen.add(r.pipeline_key);
    out.push(r);
    if (out.length >= limit) break;
  }
  return out;
}

export function RecentErrors({
  runs,
  onSelect,
  loading,
}: {
  runs: RunRecord[];
  onSelect?: (table: string) => void;
  loading?: boolean;
}) {
  const errors = dedupeErrors(runs);
  return (
    <section className="panel" data-cy="recent-errors">
      <h2>Recent errors (deduped per pipeline)</h2>
      {errors.length === 0 ? (
        loading ? (
          <div className="empty" data-cy="errors-loading">Loading…</div>
        ) : (
          <div className="empty" data-cy="no-errors">No failing pipelines.</div>
        )
      ) : (
        errors.map((r) => (
          <div
            className="alert error"
            data-cy="recent-error"
            key={r.pipeline_key}
            {...rowActivate(onSelect ? () => onSelect(tableOf(r.pipeline_key)) : undefined)}
            style={{ cursor: onSelect ? "pointer" : undefined }}
          >
            <span className="k">{tableOf(r.pipeline_key)}</span>{" "}
            <span className="pk">{fmtDateTime(r.finished_at)}</span>
            <div className="muted" data-cy="error-headline">{errorHeadline(r.error)}</div>
          </div>
        ))
      )}
    </section>
  );
}
