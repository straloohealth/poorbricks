"use client";

import { Fragment, useEffect, useState } from "react";
import { api, tableOf, type ErrorRow } from "@/lib/api";

export default function ErrorsPage() {
  const [rows, setRows] = useState<ErrorRow[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [open, setOpen] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    api
      .errors(300)
      .then(setRows)
      .catch(() => setError("Could not load errors from the API."))
      .finally(() => setLoaded(true));
  }, []);

  return (
    <div data-cy="errors-page">
      <section className="panel">
        <h2>Pipeline errors</h2>
        <p className="muted">
          The latest failure per pipeline. Alerts show only the headline; click a row to
          see the full stacktrace here.
        </p>
        {error && (
          <div className="alert error" data-cy="fetch-error">
            {error}
          </div>
        )}
        {!loaded ? (
          <div className="empty">Loading…</div>
        ) : rows.length === 0 ? (
          <div className="empty" data-cy="no-errors">No failing pipelines. 🎉</div>
        ) : (
          <table className="grid" data-cy="errors-table">
            <thead>
              <tr>
                <th>table</th>
                <th>env</th>
                <th>headline</th>
                <th>finished</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const isOpen = open === r.pipeline_key;
                return (
                  <Fragment key={r.pipeline_key}>
                    <tr
                      data-cy="error-row"
                      onClick={() => setOpen(isOpen ? null : r.pipeline_key)}
                      style={{ cursor: "pointer" }}
                    >
                      <td>{tableOf(r.pipeline_key)}</td>
                      <td className="muted">{r.environment}</td>
                      <td>{r.headline}</td>
                      <td className="muted">{r.finished_at ?? ""}</td>
                      <td className="muted">{isOpen ? "▾" : "▸"}</td>
                    </tr>
                    {isOpen && (
                      <tr>
                        <td colSpan={5}>
                          <pre className="stacktrace" data-cy="stacktrace">
                            {r.error || "(no stacktrace recorded)"}
                          </pre>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}
