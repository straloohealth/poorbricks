import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { TruncatedCell } from "./TruncatedCell";

describe("TruncatedCell", () => {
  it("renders short values plainly with no expand button", () => {
    render(<TruncatedCell value="short" max={80} />);
    expect(screen.getByTestId("trunc-cell").textContent).toBe("short");
    expect(screen.queryByTestId("trunc-expand")).toBeNull();
  });

  it("ellipsizes long values and opens the modal", async () => {
    const long = "x".repeat(120);
    render(<TruncatedCell value={long} max={20} modalTitle="cell" />);
    expect(screen.getByTestId("trunc-expand")).toBeInTheDocument();
    expect(screen.getByTestId("trunc-cell").textContent).toContain("…");
    await userEvent.click(screen.getByTestId("trunc-expand"));
    expect(screen.getByTestId("modal")).toBeInTheDocument();
    expect(screen.getByTestId("modal-title").textContent).toBe("cell");
  });

  it("pretty-prints a long JSON value in the modal", async () => {
    const json = JSON.stringify({ a: 1, b: "two", c: [1, 2, 3], d: true });
    render(<TruncatedCell value={json} max={10} />);
    await userEvent.click(screen.getByTestId("trunc-expand"));
    expect(screen.getByTestId("json-pretty")).toBeInTheDocument();
  });

  it("renders null as empty", () => {
    render(<TruncatedCell value={null} />);
    expect(screen.getByTestId("trunc-cell").textContent).toBe("");
  });
});
