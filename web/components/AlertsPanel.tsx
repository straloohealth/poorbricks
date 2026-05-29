import type { Alert, Grouped, Severity } from "@/lib/api";

function count(g: Grouped<Alert>) {
  return { error: g.error.length, warn: g.warn.length, info: g.info.length };
}

function Line({ a, sev }: { a: Alert; sev: Severity }) {
  return (
    <div className={`alert ${sev}`} data-cy="alert">
      <span className="k">{a.kind}</span> <span className="pk">{a.pipeline_key}</span>
      {a.summary ? <>: {a.summary}</> : null}
    </div>
  );
}

function Group({
  title,
  group,
  loading,
}: {
  title: string;
  group: Grouped<Alert>;
  loading?: boolean;
}) {
  const empty = !group.error.length && !group.warn.length && !group.info.length;
  return (
    <div data-cy={`group-${title.toLowerCase()}`}>
      <h3>{title}</h3>
      {empty && loading && (
        <div className="empty" data-cy="alerts-loading">
          Loading…
        </div>
      )}
      {empty && !loading && (
        <div className="empty" data-cy="all-clear">
          All clear — no findings.
        </div>
      )}
      {group.error.map((a, i) => (
        <Line a={a} sev="error" key={`e${i}`} />
      ))}
      {group.warn.map((a, i) => (
        <Line a={a} sev="warn" key={`w${i}`} />
      ))}
      {group.info.map((a, i) => (
        <Line a={a} sev="info" key={`i${i}`} />
      ))}
    </div>
  );
}

export function AlertsPanel({
  runtime,
  verification,
  loading,
}: {
  runtime: Grouped<Alert>;
  verification: Grouped<Alert>;
  loading?: boolean;
}) {
  const rt = count(runtime);
  const vf = count(verification);
  return (
    <section className="panel" data-cy="alerts-panel">
      <h2>Alerts</h2>
      <div className="metrics">
        <div className="metric">
          <div className="val" data-cy="count-errors">{rt.error + vf.error}</div>
          <div className="label">Errors</div>
        </div>
        <div className="metric">
          <div className="val" data-cy="count-warnings">{rt.warn + vf.warn}</div>
          <div className="label">Warnings</div>
        </div>
        <div className="metric">
          <div className="val" data-cy="count-info">{rt.info + vf.info}</div>
          <div className="label">Info</div>
        </div>
      </div>
      <Group title="Runtime" group={runtime} loading={loading} />
      <Group title="Verification" group={verification} loading={loading} />
    </section>
  );
}
