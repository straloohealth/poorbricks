"use client";

import type { SourceComment } from "@/lib/api";
import { fmtDateTime } from "@/lib/datetime";

/** GitHub-PR-style thread anchored under a source line. Each comment is tagged
 * with the release (SHA) it was filed in; comments from the current release are
 * highlighted. Deletion is manual (no auto-cleanup across releases). */
export function CommentThread({
  comments,
  currentSha,
  onDelete,
}: {
  comments: SourceComment[];
  currentSha: string | null;
  onDelete: (id: string) => void;
}) {
  return (
    <div className="comment-thread" data-cy="comment-thread">
      {comments.map((c) => {
        const isCurrent = !!c.release_sha && c.release_sha === currentSha;
        return (
          <div className="comment" data-cy="comment" key={c.id}>
            <div className="body">{c.body}</div>
            <div className="meta">
              {c.release_sha && (
                <span
                  className={`sha-tag${isCurrent ? " current" : ""}`}
                  data-cy="comment-sha"
                  title={isCurrent ? "filed in the current release" : "filed in an earlier release"}
                >
                  {c.release_sha.slice(0, 7)}
                </span>
              )}
              <span className="muted">{fmtDateTime(c.created_at)}</span>
              <button
                className="btn btn-xs"
                data-cy="comment-delete"
                title="Delete comment"
                onClick={() => onDelete(c.id)}
              >
                ✕
              </button>
            </div>
          </div>
        );
      })}
    </div>
  );
}
