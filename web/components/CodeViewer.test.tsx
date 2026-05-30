import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { SourceComment } from "@/lib/api";
import { CodeViewer } from "./CodeViewer";

const existing: SourceComment = {
  id: "c1",
  table_name: "dim_patient",
  file: "transform.py",
  line_start: 2,
  line_end: 2,
  body: "magic number",
  release_sha: "abc1234",
  resolved: false,
  created_at: "2026-05-29T14:33:22Z",
};

const sourceComments = vi.fn();
const addSourceComment = vi.fn();
const deleteSourceComment = vi.fn();

vi.mock("@/lib/api", () => ({
  api: {
    sourceComments: (t: string) => sourceComments(t),
    addSourceComment: (t: string, b: unknown) => addSourceComment(t, b),
    deleteSourceComment: (t: string, id: string) => deleteSourceComment(t, id),
  },
}));

const CODE = "def compute(x):\n    return x + 1\n";

beforeEach(() => {
  vi.clearAllMocks();
  sourceComments.mockResolvedValue([existing]);
});

describe("CodeViewer", () => {
  it("renders numbered lines and the existing comment thread", async () => {
    render(<CodeViewer table="dim_patient" file="transform.py" code={CODE} sha="abc1234" />);
    expect(screen.getAllByTestId("code-line")).toHaveLength(2);
    await waitFor(() => expect(screen.getByTestId("comment-thread")).toBeInTheDocument());
    expect(screen.getByText("magic number")).toBeInTheDocument();
  });

  it("opens the new-comment form via the line + and posts", async () => {
    addSourceComment.mockResolvedValue({ ...existing, id: "c2", line_start: 1, line_end: 1, body: "new note" });
    render(<CodeViewer table="dim_patient" file="transform.py" code={CODE} sha="abc1234" />);
    await waitFor(() => expect(screen.getByTestId("comment-thread")).toBeInTheDocument());
    await userEvent.click(screen.getAllByTestId("code-line-add")[0]);
    expect(screen.getByTestId("new-comment-form")).toBeInTheDocument();
    await userEvent.type(screen.getByTestId("new-comment-body"), "new note");
    await userEvent.click(screen.getByTestId("new-comment-save"));
    await waitFor(() =>
      expect(addSourceComment).toHaveBeenCalledWith("dim_patient", {
        file: "transform.py",
        line_start: 1,
        line_end: 1,
        body: "new note",
        release_sha: "abc1234",
      }),
    );
  });
});
