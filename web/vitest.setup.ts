import "@testing-library/jest-dom/vitest";
import { cleanup, configure } from "@testing-library/react";
import { afterEach } from "vitest";

// Components are tagged with data-cy (shared with Cypress); make
// getByTestId() resolve against that attribute too.
configure({ testIdAttribute: "data-cy" });

afterEach(() => cleanup());
