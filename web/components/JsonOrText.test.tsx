import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { JsonOrText, tryParseJson } from "./JsonOrText";

describe("tryParseJson", () => {
  it("parses objects and arrays", () => {
    expect(tryParseJson('{"a":1}')).toEqual({ a: 1 });
    expect(tryParseJson("[1,2]")).toEqual([1, 2]);
  });
  it("returns undefined for plain text or bare scalars", () => {
    expect(tryParseJson("hello")).toBeUndefined();
    expect(tryParseJson("42")).toBeUndefined(); // not an object/array
    expect(tryParseJson("{bad")).toBeUndefined();
  });
});

describe("JsonOrText", () => {
  it("pretty-prints a JSON object", () => {
    render(<JsonOrText value='{"is_anomaly":true,"reason":"drop"}' />);
    const el = screen.getByTestId("json-pretty");
    expect(el.textContent).toContain('"is_anomaly"');
    expect(el.textContent).toContain("true");
    // indented across multiple lines
    expect(el.textContent?.split("\n").length).toBeGreaterThan(1);
  });

  it("renders plain text as raw", () => {
    render(<JsonOrText value="just a string" />);
    expect(screen.getByTestId("raw-text").textContent).toBe("just a string");
  });
});
