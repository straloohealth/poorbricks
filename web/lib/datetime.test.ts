import { describe, it, expect } from "vitest";
import { fmtDateTime, fmtRelative, toUtcIso } from "./datetime";

describe("toUtcIso", () => {
  it("appends Z to a bare datetime (no tz designator)", () => {
    expect(toUtcIso("2026-05-29T14:33:22")).toBe("2026-05-29T14:33:22Z");
  });
  it("leaves a Z-suffixed datetime untouched", () => {
    expect(toUtcIso("2026-05-29T14:33:22Z")).toBe("2026-05-29T14:33:22Z");
  });
  it("leaves an offset datetime untouched", () => {
    expect(toUtcIso("2026-05-29T14:33:22+00:00")).toBe("2026-05-29T14:33:22+00:00");
  });
  it("does not corrupt a date-only string", () => {
    expect(toUtcIso("2026-05-29")).toBe("2026-05-29");
  });
});

describe("fmtDateTime", () => {
  it("renders UTC 14:33 as 11:33 BRT (UTC−3)", () => {
    expect(fmtDateTime("2026-05-29T14:33:22Z")).toBe("2026-05-29 11:33 BRT");
  });
  it("treats a bare (no-Z) datetime as UTC", () => {
    expect(fmtDateTime("2026-05-29T14:33:22")).toBe("2026-05-29 11:33 BRT");
  });
  it("handles an explicit +00:00 offset the same way", () => {
    expect(fmtDateTime("2026-05-29T14:33:22+00:00")).toBe("2026-05-29 11:33 BRT");
  });
  it("crosses the day boundary correctly (01:00Z → previous day 22:00 BRT)", () => {
    expect(fmtDateTime("2026-05-30T01:00:00Z")).toBe("2026-05-29 22:00 BRT");
  });
  it("returns an em-dash for null/empty", () => {
    expect(fmtDateTime(null)).toBe("—");
    expect(fmtDateTime("")).toBe("—");
  });
  it("returns the raw string for an unparseable value", () => {
    expect(fmtDateTime("not-a-date")).toBe("not-a-date");
  });
});

describe("fmtRelative", () => {
  const now = Date.parse("2026-05-29T14:33:22Z");
  it("'just now' under 45s", () => {
    expect(fmtRelative("2026-05-29T14:33:00Z", now)).toBe("just now");
  });
  it("minutes", () => {
    expect(fmtRelative("2026-05-29T14:03:22Z", now)).toBe("30m ago");
  });
  it("hours", () => {
    expect(fmtRelative("2026-05-29T11:33:22Z", now)).toBe("3h ago");
  });
  it("days", () => {
    expect(fmtRelative("2026-05-27T14:33:22Z", now)).toBe("2d ago");
  });
  it("future", () => {
    expect(fmtRelative("2026-05-29T15:33:22Z", now)).toBe("in 1h");
  });
  it("empty → empty string", () => {
    expect(fmtRelative(null, now)).toBe("");
  });
});
