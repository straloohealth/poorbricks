"use client";

import { useState } from "react";

/** Inline new-comment form anchored under a line (or line range). Anonymous —
 * no author field. */
export function NewCommentForm({
  lineStart,
  lineEnd,
  onSubmit,
  onCancel,
  busy,
}: {
  lineStart: number;
  lineEnd: number;
  onSubmit: (body: string) => void;
  onCancel: () => void;
  busy?: boolean;
}) {
  const [body, setBody] = useState("");
  const range = lineEnd > lineStart ? `lines ${lineStart}–${lineEnd}` : `line ${lineStart}`;
  return (
    <div className="newcomment" data-cy="new-comment-form">
      <div className="muted" style={{ marginBottom: "0.3rem" }}>
        Commenting on {range} (shift-click a line number to extend the range)
      </div>
      <textarea
        data-cy="new-comment-body"
        rows={3}
        placeholder="Flag a bad design, note context for the next developer…"
        value={body}
        onChange={(e) => setBody(e.target.value)}
        autoFocus
      />
      <div className="actions">
        <button
          className="btn active"
          data-cy="new-comment-save"
          disabled={!body.trim() || busy}
          onClick={() => onSubmit(body.trim())}
        >
          {busy ? "Saving…" : "Comment"}
        </button>
        <button className="btn" data-cy="new-comment-cancel" onClick={onCancel}>
          Cancel
        </button>
      </div>
    </div>
  );
}
