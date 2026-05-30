import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { NewCommentForm } from "./NewCommentForm";

describe("NewCommentForm", () => {
  it("disables save until there is text, then submits the trimmed body", async () => {
    const onSubmit = vi.fn();
    render(<NewCommentForm lineStart={5} lineEnd={5} onSubmit={onSubmit} onCancel={() => {}} />);
    expect(screen.getByTestId("new-comment-save")).toBeDisabled();
    await userEvent.type(screen.getByTestId("new-comment-body"), "  needs an index  ");
    expect(screen.getByTestId("new-comment-save")).not.toBeDisabled();
    await userEvent.click(screen.getByTestId("new-comment-save"));
    expect(onSubmit).toHaveBeenCalledWith("needs an index");
  });

  it("renders a range label and cancels", async () => {
    const onCancel = vi.fn();
    render(<NewCommentForm lineStart={3} lineEnd={6} onSubmit={() => {}} onCancel={onCancel} />);
    expect(screen.getByTestId("new-comment-form").textContent).toContain("lines 3–6");
    await userEvent.click(screen.getByTestId("new-comment-cancel"));
    expect(onCancel).toHaveBeenCalledOnce();
  });
});
