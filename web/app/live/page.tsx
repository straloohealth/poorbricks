"use client";

import { useEffect, useMemo, useState } from "react";
import { AirflowHistory } from "@/components/AirflowHistory";
import { EnvToggle } from "@/components/EnvToggle";
import { FreshnessChart, type FreshPoint } from "@/components/FreshnessChart";
import { RecentErrors } from "@/components/RecentErrors";
import { StaleList } from "@/components/StaleList";
import { TableDetail } from "@/components/TableDetail";
import {
  api,
  fmtAgeHours,
  tableOf,
  type RunRecord,
  type StalenessVerdict,
} from "@/lib/api";

// Newest run per pipeline → one freshness dot each.
function freshness(runs: RunRecord[]): FreshPoint[] {
  const seen = new Set<string>();
  const points: FreshPoint[] = [];
  for (const r of runs) {
    if (seen.has(r.pipeline_key)) continue;
    const age = fmtAgeHours(r.finished_at);
    if (age == null) continue;
    seen.add(r.pipeline_key);
    points.push({ table: tableOf(r.pipeline_key), ageHours: age, status: r.status });
  }
  return points;
}

export default function LivePage() {
  const [env, setEnv] = useState("prod");
  const [runs, setRuns] = useState<RunRecord[]>([]);
  const [stale, setStale] = useState<StalenessVerdict[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoaded(false);
    setError(null);
    // allSettled so one failure doesn't hide the other panel, but a rejection
    // surfaces an error banner instead of being mistaken for a healthy/empty
    // dashboard (critical for an observability tool).
    Promise.allSettled([api.runs(300), api.staleness()]).then(([r, s]) => {
      if (r.status === "fulfilled") setRuns(r.value.filter((x) => x.environment === env));
      else setRuns([]);
      if (s.status === "fulfilled") setStale(s.value);
      else setStale([]);
      if (r.status === "rejected" || s.status === "rejected")
        setError("Could not reach the API server — this dashboard may be stale.");
      setLoaded(true);
    });
  }, [env]);

  const points = useMemo(() => freshness(runs), [runs]);
  const airflowUrl = process.env.NEXT_PUBLIC_AIRFLOW_URL || undefined;

  return (
    <div data-cy="live-page">
      <section className="panel">
        <div className="rowflex">
          <h2 style={{ margin: 0 }}>Live Now</h2>
          <EnvToggle env={env} onChange={setEnv} />
          <span className="muted">environment: {env}</span>
        </div>
        {error && (
          <div className="alert error" data-cy="fetch-error" style={{ marginTop: "0.6rem" }}>
            {error}
          </div>
        )}
      </section>

      <AirflowHistory
        runs={runs}
        airflowUrl={airflowUrl}
        selected={selected}
        onSelect={setSelected}
      />

      <RecentErrors runs={runs} onSelect={setSelected} loading={!loaded} />

      <StaleList verdicts={stale} onSelect={setSelected} loading={!loaded} />

      <section className="panel">
        <h2>Freshness distribution</h2>
        <p className="muted">
          Each dot is a table; hover for names, click a column to list its tables.
        </p>
        <FreshnessChart points={points} />
      </section>

      <TableDetail table={selected} environment={env} />
    </div>
  );
}
