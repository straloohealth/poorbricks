import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { EnvToggle } from "./EnvToggle";

describe("EnvToggle", () => {
  it("marks the active environment", () => {
    render(<EnvToggle env="prod" onChange={() => {}} />);
    expect(screen.getByTestId("env-prod").className).toContain("active");
    expect(screen.getByTestId("env-dev").className).not.toContain("active");
  });

  it("fires onChange when switching", () => {
    const onChange = vi.fn();
    render(<EnvToggle env="prod" onChange={onChange} />);
    fireEvent.click(screen.getByTestId("env-dev"));
    expect(onChange).toHaveBeenCalledWith("dev");
  });
});
