import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Modal } from "./Modal";

describe("Modal", () => {
  it("renders title + children and closes on the × button", async () => {
    const onClose = vi.fn();
    render(
      <Modal title="anomaly" onClose={onClose}>
        <div>payload</div>
      </Modal>,
    );
    expect(screen.getByTestId("modal-title").textContent).toBe("anomaly");
    expect(screen.getByText("payload")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("modal-close"));
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("closes on Escape", async () => {
    const onClose = vi.fn();
    render(
      <Modal onClose={onClose}>
        <div>x</div>
      </Modal>,
    );
    await userEvent.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("closes on overlay click but not on inner click", async () => {
    const onClose = vi.fn();
    render(
      <Modal onClose={onClose}>
        <div>inner</div>
      </Modal>,
    );
    await userEvent.click(screen.getByText("inner"));
    expect(onClose).not.toHaveBeenCalled();
    await userEvent.click(screen.getByTestId("modal-overlay"));
    expect(onClose).toHaveBeenCalledOnce();
  });
});
