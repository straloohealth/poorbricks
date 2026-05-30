"use client";

import { foldTrace, shortPath } from "@/lib/errorFormat";

// Structured, readable view of a failed run's stacktrace: lead with the root
// cause (and its Spark error code), point at the repo frame that failed when we
// can find one, and tuck the full Java/Python trace behind a disclosure so the
// page isn't a wall of `at org.apache.spark…` frames.
export function ErrorDetail({ error }: { error: string | null | undefined }) {
  if (!error) {
    return <pre className="stacktrace" data-cy="stacktrace">(no stacktrace recorded)</pre>;
  }
  const { headline, codeTag, origin, javaHidden, full } = foldTrace(error);
  // The code tag is shown as a badge, so drop it from the inline message.
  const message = codeTag
    ? headline.replace(/^\[[A-Z][A-Z0-9_.]{2,}\]\s*/, "")
    : headline;
  return (
    <div className="errdetail" data-cy="error-detail">
      <div className="rootcause" data-cy="root-cause">
        {codeTag && <span className="codetag">{codeTag}</span>}
        {message}
      </div>
      {origin && (
        <div className="origin" data-cy="error-origin">
          at <b>{shortPath(origin.file)}</b>:<b>{origin.line}</b> in{" "}
          <b>{origin.fn}</b>
        </div>
      )}
      <details className="trace">
        <summary data-cy="trace-toggle">
          Full stacktrace{javaHidden ? ` (${javaHidden} JVM frames)` : ""}
        </summary>
        <pre className="stacktrace" data-cy="stacktrace">
          {full}
        </pre>
      </details>
    </div>
  );
}
