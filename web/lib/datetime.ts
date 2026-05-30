// Timestamp formatting for the Brazil-based team (America/Sao_Paulo, UTC−3).
//
// Backend timestamps are UTC. Run-history values come from a TIMESTAMPTZ via
// `datetime.isoformat()` so they usually carry an explicit "+00:00" offset, but
// some surfaces emit a bare "2026-05-29T14:33:22" with no designator — which JS
// `Date` would (wrongly) read as *local* time. `toUtcIso` normalises that case
// to UTC before parsing. We render via `Intl.DateTimeFormat` with an explicit
// timeZone so the offset is DST-correct rather than a hardcoded −3.

const TZ = "America/Sao_Paulo";

// Brazil abolished DST in 2019, so the abbreviation is a stable "BRT". We label
// it explicitly (rather than via timeZoneName) to keep the output compact.
const TZ_LABEL = "BRT";

const _abs = new Intl.DateTimeFormat("en-CA", {
  timeZone: TZ,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

// A bare datetime (has a "T" but no trailing Z / ±HH:MM offset) is UTC per the
// backend contract — append "Z" so `new Date` doesn't reinterpret it as local.
const _HAS_TZ = /(?:Z|[+-]\d{2}:?\d{2})$/;

export function toUtcIso(iso: string): string {
  return iso.includes("T") && !_HAS_TZ.test(iso) ? `${iso}Z` : iso;
}

function _parse(iso: string | null | undefined): Date | null {
  if (!iso) return null;
  const d = new Date(toUtcIso(iso));
  return isNaN(d.getTime()) ? null : d;
}

/** Absolute timestamp in Brazil time, e.g. "2026-05-29 11:33 BRT". */
export function fmtDateTime(iso: string | null | undefined): string {
  const d = _parse(iso);
  if (!d) return iso ? String(iso) : "—";
  const parts = _abs.formatToParts(d);
  const get = (t: string): string => parts.find((p) => p.type === t)?.value ?? "";
  return `${get("year")}-${get("month")}-${get("day")} ${get("hour")}:${get("minute")} ${TZ_LABEL}`;
}

/** Coarse relative age, e.g. "3h ago" / "just now" / "in 2m". */
export function fmtRelative(iso: string | null | undefined, now: number = Date.now()): string {
  const d = _parse(iso);
  if (!d) return "";
  const diffMs = now - d.getTime();
  const future = diffMs < 0;
  const s = Math.abs(diffMs) / 1000;
  let label: string;
  if (s < 45) label = "just now";
  else if (s < 3600) label = `${Math.round(s / 60)}m`;
  else if (s < 86400) label = `${Math.round(s / 3600)}h`;
  else label = `${Math.round(s / 86400)}d`;
  if (label === "just now") return label;
  return future ? `in ${label}` : `${label} ago`;
}
