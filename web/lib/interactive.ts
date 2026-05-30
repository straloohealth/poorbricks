import type { KeyboardEvent } from "react";

// Make a non-button element (a clickable <tr>/<div>) behave like a button for
// keyboard + screen-reader users: focusable, announced as interactive, and
// activatable with Enter/Space. Returns nothing when there's no handler so the
// element stays inert (and out of the tab order).
export function rowActivate(onActivate?: () => void) {
  if (!onActivate) return {};
  return {
    role: "button",
    tabIndex: 0,
    onClick: onActivate,
    onKeyDown: (e: KeyboardEvent) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onActivate();
      }
    },
  } as const;
}
