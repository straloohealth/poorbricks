// Client-side formatting for pipeline error stacktraces.
//
// A failed Spark run produces a wall of text: a Python traceback nested inside
// a Java/Scala driver stacktrace, often repeated (driver + "Caused by"). The
// readable signal is the *root cause* line and *where* it happened; the dozens
// of `at org.apache.spark…` frames are noise. These helpers extract the signal
// and fold the noise, so the UI can lead with the cause and tuck the raw trace
// behind a disclosure.
//
// `errorHeadline` mirrors the server's `_error_headline`
// (poorbricks/api/main.py) so the one-liner is identical whether it comes from
// /v1/errors (which carries a server `headline`) or /v1/runs (which carries
// only the raw `error`, used by the dashboard's RecentErrors panel).

const BASE_PREFIX = /^\[\w+\]\s*/; // strip a leading "[base] " worker-log prefix
const CODE_TAG = /\[[A-Z][A-Z0-9_.]{2,}\]/; // Spark error class, e.g. [UNRESOLVED_COLUMN]
// A leading fully-qualified (dotted) exception class, e.g.
// "pyspark.errors.exceptions.base.PySparkValueError: " — pure noise in the
// one-liner. A bare "ValueError: " has no dot and is kept (it IS the signal).
const EXC_PREFIX = /^(?:[A-Za-z_]\w*\.)+[A-Za-z_]\w*:\s+/;

function clean(line: string): string {
  return line.replace(BASE_PREFIX, "").replace(EXC_PREFIX, "").slice(0, 400);
}

// Query-plan tree fragments (`+- Project`, `patient_id#0`, `LogicalRDD`) are
// never a useful headline.
function isPlanNoise(line: string): boolean {
  const s = line.replace(/^\[base\]\s*/, "").trim();
  return (
    /^[+:|=]/.test(s) ||
    /#\d+\b/.test(line) ||
    /\b(LogicalRDD|Relation|Project|Filter|Join)\b/.test(line)
  );
}

/** Collapse a (possibly multi-line) stacktrace to one readable line. */
export function errorHeadline(raw: string | null | undefined): string {
  if (!raw) return "run failed";
  const lines = raw
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  if (!lines.length) return "run failed";

  const usable = lines.filter((l) => !isPlanNoise(l));
  const src = usable.length ? usable : lines;

  // 1. structured Spark error-class line (the real root cause).
  const coded = src.filter((l) => CODE_TAG.test(l));
  if (coded.length) return clean(coded[coded.length - 1]);

  // 2. deepest real exception, skipping generic Spark wrappers.
  const exc = src.filter(
    (l) => /(Error|Exception):/.test(l) && !l.includes("Traceback"),
  );
  const nonwrap = exc.filter(
    (l) => !/Job aborted|stage failure|SparkException/.test(l),
  );
  if (nonwrap.length) return clean(nonwrap[nonwrap.length - 1]);
  if (exc.length) return clean(exc[exc.length - 1]);
  return clean(src[src.length - 1]);
}

/** The Spark error class (e.g. `FIELD_NOT_NULLABLE_WITH_NAME`), if present. */
export function errorCodeTag(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const m = raw.match(/\[([A-Z][A-Z0-9_.]{2,})\]/);
  return m ? m[1] : null;
}

export interface PyFrame {
  file: string;
  line: string;
  fn: string;
  lib: boolean; // true for stdlib / site-packages / pyspark internals
}

export interface FoldedTrace {
  headline: string;
  codeTag: string | null;
  /** Deepest non-library Python frame — the repo code that failed, if any. */
  origin: PyFrame | null;
  /** Count of folded Java/Scala `at …` frames (the noise we hid). */
  javaHidden: number;
  /** Raw, unmodified stacktrace (for the disclosure). */
  full: string;
}

const JAVA_FRAME = /^\s*at\s+[\w$.<>/]+\(/;
const PY_FILE = /^\s*File\s+"(.+?)",\s+line\s+(\d+),\s+in\s+(.+?)\s*$/;
const LIB_PATH = /\/site-packages\/|\/pyspark\/|\/lib\/python|<frozen/;

/** Parse a stacktrace into its readable parts. */
export function foldTrace(raw: string | null | undefined): FoldedTrace {
  const full = raw ?? "";
  const frames: PyFrame[] = [];
  let javaHidden = 0;
  for (const ln of full.split("\n")) {
    if (JAVA_FRAME.test(ln)) {
      javaHidden++;
      continue;
    }
    const m = ln.match(PY_FILE);
    if (m) {
      frames.push({
        file: m[1],
        line: m[2],
        fn: m[3],
        lib: LIB_PATH.test(m[1]),
      });
    }
  }
  const repoFrames = frames.filter((f) => !f.lib);
  const origin = repoFrames.length
    ? repoFrames[repoFrames.length - 1]
    : null;
  return {
    headline: errorHeadline(raw),
    codeTag: errorCodeTag(raw),
    origin,
    javaHidden,
    full,
  };
}

/** Trim absolute prefixes so a frame path reads as repo-relative when possible. */
export function shortPath(file: string): string {
  const m = file.match(/(?:^|\/)(tables\/.+)$/);
  if (m) return m[1];
  const parts = file.split("/");
  return parts.length > 3 ? "…/" + parts.slice(-3).join("/") : file;
}
