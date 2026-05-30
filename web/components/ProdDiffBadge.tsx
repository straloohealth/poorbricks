"use client";

import type { ProdDiff } from "@/lib/api";

const LABEL: Record<ProdDiff["severity"], string> = {
  major: "● major change",
  minor: "● minor change",
  none: "● in sync",
};

/** Collapsed indicator of how a dev run diverges from the prod baseline. */
export function ProdDiffBadge({ severity }: { severity: ProdDiff["severity"] }) {
  return (
    <span className={`badge ${severity}`} data-cy="proddiff-badge">
      {LABEL[severity]}
    </span>
  );
}
