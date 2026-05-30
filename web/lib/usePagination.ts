"use client";

import { useEffect, useMemo, useState } from "react";

export interface Pagination<T> {
  page: number; // 0-based
  pageCount: number;
  pageItems: T[];
  from: number; // 1-based index of the first item on the page (0 when empty)
  to: number; // 1-based index of the last item on the page
  total: number;
  setPage: (p: number) => void;
  next: () => void;
  prev: () => void;
  canNext: boolean;
  canPrev: boolean;
}

/**
 * Client-side pagination over an in-memory array. Keeps each table's own
 * markup — callers map over `pageItems` and render `<PaginationControls>`.
 * Resets to the first page whenever the backing array identity changes (e.g. a
 * new table is selected) so the view never lands on an out-of-range page.
 */
export function usePagination<T>(items: T[], pageSize = 10): Pagination<T> {
  const [page, setPage] = useState(0);
  const pageCount = Math.max(1, Math.ceil(items.length / pageSize));

  // Clamp when the data shrinks or is replaced.
  useEffect(() => {
    setPage(0);
  }, [items]);
  const safePage = Math.min(page, pageCount - 1);

  return useMemo(() => {
    const start = safePage * pageSize;
    const pageItems = items.slice(start, start + pageSize);
    const total = items.length;
    return {
      page: safePage,
      pageCount,
      pageItems,
      from: total === 0 ? 0 : start + 1,
      to: Math.min(start + pageSize, total),
      total,
      setPage: (p: number) => setPage(Math.max(0, Math.min(p, pageCount - 1))),
      next: () => setPage((p) => Math.min(p + 1, pageCount - 1)),
      prev: () => setPage((p) => Math.max(p - 1, 0)),
      canNext: safePage < pageCount - 1,
      canPrev: safePage > 0,
    };
  }, [items, safePage, pageSize, pageCount]);
}
