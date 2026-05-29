import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { RecentErrors, dedupeErrors } from "./RecentErrors";
import type { RunRecord } from "@/lib/api";

function run(p: Partial<RunRecord>): RunRecord {
  return {
    pipeline_key: "postgres:x",
    table_name: "x",
    environment: "prod",
    sha: null,
    mode: "production",
    status: "ok",
    started_at: null,
    finished_at: null,
    duration_s: 1,
    row_count: 1,
    schema_hash: null,
    error: null,
    anomaly: null,
    drift_summary: null,
    ...p,
  };
}

describe("dedupeErrors", () => {
  it("keeps only the newest failure per pipeline, max 5", () => {
    const runs = [
      run({ pipeline_key: "postgres:a", status: "failed", error: "newest a", finished_at: "t3" }),
      run({ pipeline_key: "postgres:a", status: "failed", error: "older a", finished_at: "t1" }),
      run({ pipeline_key: "postgres:b", status: "failed", error: "b" }),
      run({ pipeline_key: "postgres:c", status: "ok" }),
    ];
    const out = dedupeErrors(runs);
    expect(out).toHaveLength(2);
    expect(out[0].error).toBe("newest a");
    expect(out.map((r) => r.pipeline_key)).toEqual(["postgres:a", "postgres:b"]);
  });

  it("ignores non-failed runs", () => {
    expect(dedupeErrors([run({ status: "ok" })])).toHaveLength(0);
  });
});

describe("RecentErrors", () => {
  it("renders empty state with no failures", () => {
    render(<RecentErrors runs={[run({ status: "ok" })]} />);
    expect(screen.getByTestId("no-errors")).toBeInTheDocument();
  });

  it("fires onSelect with the table name when an error is clicked", () => {
    const onSelect = vi.fn();
    render(
      <RecentErrors
        runs={[run({ pipeline_key: "postgres:dim_patient", status: "failed", error: "nulls" })]}
        onSelect={onSelect}
      />,
    );
    fireEvent.click(screen.getByTestId("recent-error"));
    expect(onSelect).toHaveBeenCalledWith("dim_patient");
  });

  it("shows 'Loading…' instead of 'no failing pipelines' while loading", () => {
    render(<RecentErrors runs={[]} loading />);
    expect(screen.getByTestId("errors-loading")).toBeInTheDocument();
    expect(screen.queryByTestId("no-errors")).toBeNull();
  });
});
