import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { Pagination } from "@/lib/usePagination";
import { PaginationControls } from "./PaginationControls";

function mk(over: Partial<Pagination<unknown>> = {}): Pagination<unknown> {
  return {
    page: 1,
    pageCount: 3,
    pageItems: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    from: 11,
    to: 20,
    total: 23,
    setPage: vi.fn(),
    next: vi.fn(),
    prev: vi.fn(),
    canNext: true,
    canPrev: true,
    ...over,
  };
}

describe("PaginationControls", () => {
  it("renders the X–Y of Z caption and fires next/prev", async () => {
    const p = mk();
    render(<PaginationControls p={p} cyPrefix="runs" />);
    expect(screen.getByTestId("runs-page-info").textContent).toBe("11–20 of 23");
    await userEvent.click(screen.getByTestId("runs-page-next"));
    expect(p.next).toHaveBeenCalledOnce();
    await userEvent.click(screen.getByTestId("runs-page-prev"));
    expect(p.prev).toHaveBeenCalledOnce();
  });

  it("renders nothing when everything fits on one page", () => {
    const { container } = render(
      <PaginationControls p={mk({ page: 0, total: 3, pageItems: [0, 1, 2], canNext: false, canPrev: false, from: 1, to: 3, pageCount: 1 })} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it("disables Prev on the first page", () => {
    render(<PaginationControls p={mk({ page: 0, canPrev: false, from: 1, to: 10 })} />);
    expect(screen.getByTestId("page-prev")).toBeDisabled();
    expect(screen.getByTestId("page-next")).not.toBeDisabled();
  });
});
