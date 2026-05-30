import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { StaleList } from "./StaleList";
import type { StalenessVerdict } from "@/lib/api";

function v(p: Partial<StalenessVerdict>): StalenessVerdict {
  return {
    pipeline_key: "postgres:x",
    state: "ok",
    last_run: null,
    interval_s: 3600,
    age_s: 0,
    ...p,
  };
}

describe("StaleList", () => {
  it("hides healthy datasets and shows the empty state", () => {
    render(<StaleList verdicts={[v({ state: "ok" })]} />);
    expect(screen.getByTestId("no-stale")).toBeInTheDocument();
  });

  it("lists overdue and missing datasets only", () => {
    render(
      <StaleList
        verdicts={[
          v({ pipeline_key: "postgres:a", state: "overdue", age_s: 7200 }),
          v({ pipeline_key: "postgres:b", state: "missing", age_s: null }),
          v({ pipeline_key: "postgres:c", state: "ok" }),
        ]}
      />,
    );
    expect(screen.getAllByTestId("stale-row")).toHaveLength(2);
  });

  it("fires onSelect when a stale row is clicked", () => {
    const onSelect = vi.fn();
    render(
      <StaleList
        verdicts={[v({ pipeline_key: "postgres:fact_sales", state: "overdue" })]}
        onSelect={onSelect}
      />,
    );
    fireEvent.click(screen.getByTestId("stale-row"));
    expect(onSelect).toHaveBeenCalledWith("fact_sales");
  });

  it("shows 'Loading…' instead of 'everything is fresh' while loading", () => {
    render(<StaleList verdicts={[]} loading />);
    expect(screen.getByTestId("stale-loading")).toBeInTheDocument();
    expect(screen.queryByTestId("no-stale")).toBeNull();
  });

  it("stale rows are keyboard-accessible and activate on Enter", () => {
    const onSelect = vi.fn();
    render(
      <StaleList
        verdicts={[v({ pipeline_key: "postgres:fact_sales", state: "overdue" })]}
        onSelect={onSelect}
      />,
    );
    const row = screen.getByTestId("stale-row");
    expect(row).toHaveAttribute("role", "button");
    expect(row).toHaveAttribute("tabindex", "0");
    fireEvent.keyDown(row, { key: "Enter" });
    expect(onSelect).toHaveBeenCalledWith("fact_sales");
  });
});
