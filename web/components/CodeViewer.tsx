"use client";

import { Fragment, useEffect, useMemo, useState } from "react";
import { api, type SourceComment } from "@/lib/api";
import { CommentThread } from "@/components/CommentThread";
import { NewCommentForm } from "@/components/NewCommentForm";

/**
 * GitHub-PR-style source viewer: line numbers, a per-line "+" to start a
 * comment (shift-click a line number to extend the range), and existing
 * comment threads anchored under their line. Comments are tagged with the
 * release (SHA) they were filed in and persist until manually deleted.
 */
export function CodeViewer({
  table,
  file,
  code,
  sha,
}: {
  table: string;
  file: string;
  code: string;
  sha: string | null;
}) {
  const [comments, setComments] = useState<SourceComment[]>([]);
  const [active, setActive] = useState<{ start: number; end: number } | null>(null);
  const [busy, setBusy] = useState(false);

  const lines = useMemo(() => code.replace(/\n$/, "").split("\n"), [code]);

  // Load comments for this table, filtered to the open file. Ignore-guarded so
  // a slow response for a previous file can't land here.
  useEffect(() => {
    let ignore = false;
    setComments([]);
    setActive(null);
    api
      .sourceComments(table)
      .then((all) => !ignore && setComments(all.filter((c) => c.file === file)))
      .catch(() => !ignore && setComments([]));
    return () => {
      ignore = true;
    };
  }, [table, file]);

  const byLine = useMemo(() => {
    const m = new Map<number, SourceComment[]>();
    for (const c of comments) {
      const arr = m.get(c.line_start) ?? [];
      arr.push(c);
      m.set(c.line_start, arr);
    }
    return m;
  }, [comments]);

  const onLineClick = (line: number, shift: boolean): void => {
    if (shift && active && line >= active.start) {
      setActive({ start: active.start, end: line });
    } else {
      setActive({ start: line, end: line });
    }
  };

  const submit = async (body: string): Promise<void> => {
    if (!active) return;
    setBusy(true);
    try {
      const created = await api.addSourceComment(table, {
        file,
        line_start: active.start,
        line_end: active.end,
        body,
        release_sha: sha,
      });
      setComments((cs) => [...cs, created]);
      setActive(null);
    } catch {
      // Surface failure by keeping the form open; a toast system is out of scope.
    } finally {
      setBusy(false);
    }
  };

  const remove = async (id: string): Promise<void> => {
    const prev = comments;
    setComments((cs) => cs.filter((c) => c.id !== id)); // optimistic
    try {
      await api.deleteSourceComment(table, id);
    } catch {
      setComments(prev); // roll back on failure
    }
  };

  return (
    <div className="codeviewer" data-cy="source-code">
      <div className="code-rows">
        {lines.map((text, i) => {
          const ln = i + 1;
          const lineComments = byLine.get(ln);
          const selected = active && ln >= active.start && ln <= active.end;
          return (
            <Fragment key={ln}>
              <div className={`code-line${selected ? " selected" : ""}`} data-cy="code-line">
                <span
                  className="ln"
                  onClick={(e) => onLineClick(ln, e.shiftKey)}
                  title="Click to comment; shift-click to extend a range"
                >
                  {ln}
                </span>
                <span
                  className="add"
                  data-cy="code-line-add"
                  title="Comment on this line"
                  onClick={() => setActive({ start: ln, end: ln })}
                >
                  +
                </span>
                <span className="src">{text || " "}</span>
              </div>
              {lineComments && (
                <div className="comment-row">
                  <CommentThread comments={lineComments} currentSha={sha} onDelete={remove} />
                </div>
              )}
              {active && active.end === ln && (
                <div className="comment-row">
                  <NewCommentForm
                    lineStart={active.start}
                    lineEnd={active.end}
                    busy={busy}
                    onSubmit={submit}
                    onCancel={() => setActive(null)}
                  />
                </div>
              )}
            </Fragment>
          );
        })}
      </div>
    </div>
  );
}
