import { describe, it, expect } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { usePagination } from "./usePagination";

const items = Array.from({ length: 23 }, (_, i) => i);

describe("usePagination", () => {
  it("slices the first page and reports the range", () => {
    const { result } = renderHook(() => usePagination(items, 10));
    expect(result.current.pageItems).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(result.current.from).toBe(1);
    expect(result.current.to).toBe(10);
    expect(result.current.total).toBe(23);
    expect(result.current.pageCount).toBe(3);
    expect(result.current.canPrev).toBe(false);
    expect(result.current.canNext).toBe(true);
  });

  it("advances and clamps at the last page", () => {
    const { result } = renderHook(() => usePagination(items, 10));
    act(() => result.current.next());
    expect(result.current.page).toBe(1);
    expect(result.current.pageItems).toEqual([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
    act(() => result.current.next());
    expect(result.current.page).toBe(2);
    expect(result.current.pageItems).toEqual([20, 21, 22]);
    expect(result.current.to).toBe(23);
    expect(result.current.canNext).toBe(false);
    act(() => result.current.next()); // no-op past the end
    expect(result.current.page).toBe(2);
  });

  it("handles an empty list", () => {
    const { result } = renderHook(() => usePagination([] as number[], 10));
    expect(result.current.pageItems).toEqual([]);
    expect(result.current.from).toBe(0);
    expect(result.current.to).toBe(0);
    expect(result.current.pageCount).toBe(1);
    expect(result.current.canNext).toBe(false);
  });

  it("resets to page 0 when the backing array changes", () => {
    const { result, rerender } = renderHook(({ data }) => usePagination(data, 10), {
      initialProps: { data: items },
    });
    act(() => result.current.next());
    expect(result.current.page).toBe(1);
    rerender({ data: [1, 2, 3] });
    expect(result.current.page).toBe(0);
    expect(result.current.pageItems).toEqual([1, 2, 3]);
  });
});
