"use client";

import { useState } from "react";
import { Modal } from "@/components/Modal";
import { JsonOrText } from "@/components/JsonOrText";

/**
 * Inline value that ellipsizes when long and offers a fullscreen viewer. JSON
 * values are pretty-printed in the modal. Short values render plainly with no
 * button so existing cells look unchanged.
 */
export function TruncatedCell({
  value,
  max = 80,
  modalTitle,
}: {
  value: unknown;
  max?: number;
  modalTitle?: string;
}) {
  const s = value == null ? "" : String(value);
  const [open, setOpen] = useState(false);
  const long = s.length > max;
  return (
    <span className="trunccell" data-cy="trunc-cell">
      <span className="trunc-inline" title={long ? undefined : s}>
        {long ? `${s.slice(0, max)}…` : s}
      </span>
      {long && (
        <button
          className="btn btn-xs"
          data-cy="trunc-expand"
          title="View full value"
          onClick={() => setOpen(true)}
        >
          ⛶
        </button>
      )}
      {open && (
        <Modal title={modalTitle ?? "Full value"} onClose={() => setOpen(false)}>
          <JsonOrText value={s} />
        </Modal>
      )}
    </span>
  );
}
