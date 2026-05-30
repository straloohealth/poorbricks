"use client";

import { type ReactNode, useEffect } from "react";
import { createPortal } from "react-dom";

/**
 * Minimal fullscreen dialog. Portals to <body> so it isn't clipped by the
 * overflow:auto scroll columns on the Main page. Closes on the ×, an overlay
 * click, or Escape.
 */
export function Modal({
  title,
  onClose,
  children,
}: {
  title?: string;
  onClose: () => void;
  children: ReactNode;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  // SSR guard: no document during server render.
  if (typeof document === "undefined") return null;

  return createPortal(
    <div
      className="modal-overlay"
      data-cy="modal-overlay"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal" data-cy="modal" role="dialog" aria-modal="true">
        <div className="modal-head">
          <h3 data-cy="modal-title">{title ?? "Details"}</h3>
          <button className="btn btn-xs" data-cy="modal-close" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>,
    document.body,
  );
}
