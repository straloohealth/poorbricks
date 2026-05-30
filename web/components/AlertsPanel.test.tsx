import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { AlertsPanel } from "./AlertsPanel";
import type { Alert, Grouped } from "@/lib/api";

const empty: Grouped<Alert> = { error: [], warn: [], info: [] };

describe("AlertsPanel", () => {
  it("shows zero counts and 'all clear' for both groups when empty", () => {
    render(<AlertsPanel runtime={empty} verification={empty} />);
    expect(screen.getByTestId("count-errors")).toHaveTextContent("0");
    expect(screen.getByTestId("count-warnings")).toHaveTextContent("0");
    expect(screen.getByTestId("count-info")).toHaveTextContent("0");
    expect(screen.getAllByTestId("all-clear")).toHaveLength(2);
  });

  it("shows 'Loading…' instead of 'all clear' while loading", () => {
    render(<AlertsPanel runtime={empty} verification={empty} loading />);
    expect(screen.getAllByTestId("alerts-loading")).toHaveLength(2);
    expect(screen.queryByTestId("all-clear")).toBeNull();
  });

  it("sums counts across runtime and verification groups", () => {
    const runtime: Grouped<Alert> = {
      error: [{ kind: "failure", pipeline_key: "postgres:a", summary: "boom" }],
      warn: [{ kind: "row_count_anomaly", pipeline_key: "postgres:b", summary: "" }],
      info: [],
    };
    const verification: Grouped<Alert> = {
      error: [{ kind: "contract_break", pipeline_key: "postgres:c", summary: "" }],
      warn: [],
      info: [{ kind: "literal", pipeline_key: "postgres:d", summary: "" }],
    };
    render(<AlertsPanel runtime={runtime} verification={verification} />);
    expect(screen.getByTestId("count-errors")).toHaveTextContent("2");
    expect(screen.getByTestId("count-warnings")).toHaveTextContent("1");
    expect(screen.getByTestId("count-info")).toHaveTextContent("1");
    expect(screen.getAllByTestId("alert")).toHaveLength(4);
    expect(screen.getByText("boom", { exact: false })).toBeInTheDocument();
  });
});
