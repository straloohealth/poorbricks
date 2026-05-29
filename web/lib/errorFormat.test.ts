import { describe, it, expect } from "vitest";
import {
  errorHeadline,
  errorCodeTag,
  foldTrace,
  shortPath,
} from "./errorFormat";

// A real failed-run stacktrace (smith_navigators, is_active null) — Python
// traceback nested in a Java driver stacktrace, repeated under "Caused by".
const SMITH = `An error occurred while calling o209.count.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 63 in stage 19.0 failed 1 times, most recent failure: Lost task 63.0 in stage 19.0 (TID 136) (executor driver): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/app/.venv/lib/python3.12/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 2044, in main
    process()
  File "/app/.venv/lib/python3.12/site-packages/pyspark/sql/types.py", line 2691, in verify_nullability
    raise PySparkValueError(
pyspark.errors.exceptions.base.PySparkValueError: [FIELD_NOT_NULLABLE_WITH_NAME] field is_active: This field is not nullable, but got None.

\tat org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:581)
\tat org.apache.spark.scheduler.Task.run(Task.scala:147)
\tat java.base/java.lang.Thread.run(Thread.java:1583)

Driver stacktrace:
\tat org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2927)`;

describe("errorHeadline", () => {
  it("prefers the Spark error-class line over the Java wrapper", () => {
    expect(errorHeadline(SMITH)).toBe(
      "[FIELD_NOT_NULLABLE_WITH_NAME] field is_active: This field is not nullable, but got None.",
    );
  });

  it("falls back to the deepest non-wrapper exception when no code tag", () => {
    const t = `: org.apache.spark.SparkException: Job aborted due to stage failure
ValueError: bad input row`;
    expect(errorHeadline(t)).toBe("ValueError: bad input row");
  });

  it("returns a friendly default for empty/missing input", () => {
    expect(errorHeadline(null)).toBe("run failed");
    expect(errorHeadline("")).toBe("run failed");
    expect(errorHeadline("   \n  ")).toBe("run failed");
  });

  it("strips a leading [base] worker-log prefix", () => {
    expect(errorHeadline("[base] RuntimeError: boom")).toBe("RuntimeError: boom");
  });
});

describe("errorCodeTag", () => {
  it("extracts the Spark error class", () => {
    expect(errorCodeTag(SMITH)).toBe("FIELD_NOT_NULLABLE_WITH_NAME");
  });
  it("is null when absent", () => {
    expect(errorCodeTag("ValueError: x")).toBeNull();
    expect(errorCodeTag(null)).toBeNull();
  });
});

describe("foldTrace", () => {
  it("folds the JVM frames and surfaces the headline + code tag", () => {
    const f = foldTrace(SMITH);
    expect(f.headline).toContain("is_active");
    expect(f.codeTag).toBe("FIELD_NOT_NULLABLE_WITH_NAME");
    expect(f.javaHidden).toBe(4); // the 4 `at …` frames (incl. java.base/ module)
    expect(f.full).toBe(SMITH);
  });

  it("reports no repo origin when every Python frame is library code", () => {
    // smith's failure is entirely inside pyspark internals (pass-through table).
    expect(foldTrace(SMITH).origin).toBeNull();
  });

  it("surfaces the deepest repo frame as the origin", () => {
    const t = `Traceback (most recent call last):
  File "/app/.venv/lib/python3.12/site-packages/pyspark/sql/session.py", line 10, in run
    f()
  File "/app/tables/navigators/transform.py", line 22, in compute
    return create_dataframe(df, schema)
ValueError: boom`;
    const origin = foldTrace(t).origin;
    expect(origin).not.toBeNull();
    expect(origin!.file).toContain("tables/navigators/transform.py");
    expect(origin!.line).toBe("22");
    expect(origin!.fn).toBe("compute");
    expect(origin!.lib).toBe(false);
  });
});

describe("shortPath", () => {
  it("trims to repo-relative when a tables/ path is present", () => {
    expect(shortPath("/app/tables/navigators/transform.py")).toBe(
      "tables/navigators/transform.py",
    );
  });
  it("keeps the last 3 segments otherwise", () => {
    expect(shortPath("/app/.venv/lib/python3.12/site-packages/pyspark/worker.py")).toBe(
      "…/site-packages/pyspark/worker.py",
    );
  });
});
