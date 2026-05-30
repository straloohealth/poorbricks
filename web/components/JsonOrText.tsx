"use client";

import { type ReactNode } from "react";

export function tryParseJson(s: string): unknown {
  const t = s.trim();
  if (!t || (t[0] !== "{" && t[0] !== "[")) return undefined;
  try {
    return JSON.parse(t);
  } catch {
    return undefined;
  }
}

// Token-level highlighter over already-indented JSON. Builds React nodes (no
// dangerouslySetInnerHTML) so user content can't inject markup.
const TOKEN = /("(?:\\.|[^"\\])*"\s*:?|\b(?:true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;

export function highlightJson(json: string): ReactNode[] {
  const out: ReactNode[] = [];
  let last = 0;
  let m: RegExpExecArray | null;
  let i = 0;
  TOKEN.lastIndex = 0;
  while ((m = TOKEN.exec(json)) !== null) {
    if (m.index > last) out.push(json.slice(last, m.index));
    const tok = m[0];
    let cls = "tok-num";
    if (tok.startsWith('"')) cls = tok.trimEnd().endsWith(":") ? "tok-key" : "tok-str";
    else if (tok === "true" || tok === "false") cls = "tok-bool";
    else if (tok === "null") cls = "tok-null";
    out.push(
      <span key={i++} className={cls}>
        {tok}
      </span>,
    );
    last = m.index + tok.length;
  }
  if (last < json.length) out.push(json.slice(last));
  return out;
}

/** Render a value as prettified+highlighted JSON when it parses as an object/
 * array, otherwise as plain preformatted text. */
export function JsonOrText({ value }: { value: string }) {
  const parsed = tryParseJson(value);
  if (parsed !== undefined) {
    return (
      <pre className="stacktrace json" data-cy="json-pretty">
        {highlightJson(JSON.stringify(parsed, null, 2))}
      </pre>
    );
  }
  return (
    <pre className="stacktrace" data-cy="raw-text">
      {value}
    </pre>
  );
}
