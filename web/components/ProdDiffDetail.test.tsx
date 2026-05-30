import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import type { ProdDiff } from "@/lib/api";
import { ProdDiffDetail } from "./ProdDiffDetail";

const DIFF: ProdDiff = {
  run_id: 7,
  table: "dim_patient",
  prod_sha: "abc1234",
  dev_sha: "def5678",
  severity: "major",
  row_count: { dev: 8000, prod: 10000, delta_pct: -0.2, major: false },
  null_dist: [{ column: "name", dev: 0.3, prod: 0.1, delta: 0.2, major: true }],
  fields: { added: ["new_col"], removed: [], retyped: [{ column: "age", from: "int", to: "string" }] },
  alerts: { added: ["row_count_anomaly"], removed: [], existing: ["null_rate_max"] },
};

describe("ProdDiffDetail", () => {
  it("renders the row-count delta, nulls, fields and alerts", () => {
    render(<ProdDiffDetail diff={DIFF} />);
    expect(screen.getByTestId("proddiff-rowcount").textContent).toContain("-20.0%");
    expect(screen.getByTestId("proddiff-nulls").textContent).toContain("name");
    expect(screen.getByTestId("proddiff-fields").textContent).toContain("new_col");
    expect(screen.getByTestId("proddiff-fields").textContent).toContain("age int→string");
    expect(screen.getByTestId("proddiff-alerts").textContent).toContain("row_count_anomaly");
  });

  it("shows an empty state when there are no null-rate changes", () => {
    render(<ProdDiffDetail diff={{ ...DIFF, null_dist: [] }} />);
    expect(screen.getByText("no null-rate changes")).toBeInTheDocument();
  });
});
