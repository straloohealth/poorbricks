import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { ProdDiffBadge } from "./ProdDiffBadge";

describe("ProdDiffBadge", () => {
  it("maps severity to class + label", () => {
    const { rerender } = render(<ProdDiffBadge severity="major" />);
    let b = screen.getByTestId("proddiff-badge");
    expect(b.className).toContain("major");
    expect(b.textContent).toContain("major change");

    rerender(<ProdDiffBadge severity="minor" />);
    b = screen.getByTestId("proddiff-badge");
    expect(b.className).toContain("minor");

    rerender(<ProdDiffBadge severity="none" />);
    b = screen.getByTestId("proddiff-badge");
    expect(b.className).toContain("none");
    expect(b.textContent).toContain("in sync");
  });
});
