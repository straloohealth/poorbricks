import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { SourceComment } from "@/lib/api";
import { CommentThread } from "./CommentThread";

const base: SourceComment = {
  id: "1",
  table_name: "t",
  file: "transform.py",
  line_start: 3,
  line_end: 3,
  body: "this join is O(n^2)",
  release_sha: "abc1234def",
  resolved: false,
  created_at: "2026-05-29T14:33:22Z",
};

describe("CommentThread", () => {
  it("shows the body, short sha, and highlights current-release comments", () => {
    render(<CommentThread comments={[base]} currentSha="abc1234def" onDelete={() => {}} />);
    expect(screen.getByText("this join is O(n^2)")).toBeInTheDocument();
    const tag = screen.getByTestId("comment-sha");
    expect(tag.textContent).toBe("abc1234");
    expect(tag.className).toContain("current");
  });

  it("does not mark earlier-release comments as current", () => {
    render(<CommentThread comments={[base]} currentSha="zzz9999" onDelete={() => {}} />);
    expect(screen.getByTestId("comment-sha").className).not.toContain("current");
  });

  it("fires onDelete", async () => {
    const onDelete = vi.fn();
    render(<CommentThread comments={[base]} currentSha={null} onDelete={onDelete} />);
    await userEvent.click(screen.getByTestId("comment-delete"));
    expect(onDelete).toHaveBeenCalledWith("1");
  });
});
