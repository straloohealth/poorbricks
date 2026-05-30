"use client";

import type { Pagination } from "@/lib/usePagination";

/**
 * Prev/Next footer for a paginated table. Renders nothing when everything fits
 * on one page, so small tables look exactly as before. Pass `cyPrefix` to
 * namespace the `data-cy` hooks when several paginators share a screen.
 */
export function PaginationControls({
  p,
  cyPrefix = "",
}: {
  p: Pagination<unknown>;
  cyPrefix?: string;
}) {
  if (p.total <= p.pageItems.length && p.page === 0 && !p.canNext) return null;
  const cy = (s: string): string => (cyPrefix ? `${cyPrefix}-${s}` : s);
  return (
    <div className="rowflex pagination" data-cy={cy("pagination")}>
      <button
        className="btn"
        data-cy={cy("page-prev")}
        onClick={p.prev}
        disabled={!p.canPrev}
      >
        « Prev
      </button>
      <span className="muted" data-cy={cy("page-info")}>
        {p.from}–{p.to} of {p.total}
      </span>
      <button
        className="btn"
        data-cy={cy("page-next")}
        onClick={p.next}
        disabled={!p.canNext}
      >
        Next »
      </button>
    </div>
  );
}
