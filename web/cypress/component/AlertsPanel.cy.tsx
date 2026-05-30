import { AlertsPanel } from "@/components/AlertsPanel";
import type { Alert, Grouped } from "@/lib/api";

const empty: Grouped<Alert> = { error: [], warn: [], info: [] };

describe("<AlertsPanel />", () => {
  it("renders 'all clear' for both groups when empty", () => {
    cy.mount(<AlertsPanel runtime={empty} verification={empty} />);
    cy.get('[data-cy="all-clear"]').should("have.length", 2);
    cy.get('[data-cy="count-errors"]').should("have.text", "0");
  });

  it("sums severity counts across both groups", () => {
    const runtime: Grouped<Alert> = {
      error: [{ kind: "failure", pipeline_key: "postgres:a", summary: "boom" }],
      warn: [],
      info: [],
    };
    const verification: Grouped<Alert> = {
      error: [{ kind: "contract_break", pipeline_key: "postgres:b", summary: "" }],
      warn: [{ kind: "stub", pipeline_key: "postgres:c", summary: "" }],
      info: [],
    };
    cy.mount(<AlertsPanel runtime={runtime} verification={verification} />);
    cy.get('[data-cy="count-errors"]').should("have.text", "2");
    cy.get('[data-cy="count-warnings"]').should("have.text", "1");
    cy.get('[data-cy="alert"]').should("have.length", 3);
  });
});
