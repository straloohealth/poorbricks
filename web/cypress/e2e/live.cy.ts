// Exercises the Live Now page against the live FastAPI server + real run history.
describe("Live Now page", () => {
  beforeEach(() => cy.visit("/live"));

  it("renders every panel", () => {
    cy.get('[data-cy="live-page"]').should("be.visible");
    cy.get('[data-cy="airflow-history"]').should("exist");
    cy.get('[data-cy="recent-errors"]').should("exist");
    cy.get('[data-cy="stale-list"]').should("exist");
    // Freshness shows either the chart or its empty state.
    cy.get('[data-cy="freshness-chart"], [data-cy="freshness-empty"]').should("exist");
  });

  it("toggles between prod and dev environments", () => {
    cy.get('[data-cy="env-prod"]').should("have.class", "active");
    cy.get('[data-cy="env-dev"]').click();
    cy.get('[data-cy="env-dev"]').should("have.class", "active");
    cy.contains("environment: dev").should("exist");
    cy.get('[data-cy="env-prod"]').click();
    cy.get('[data-cy="env-prod"]').should("have.class", "active");
  });

  it("navigates from the nav bar between pages", () => {
    cy.get('[data-cy="nav-main"]').click();
    cy.get('[data-cy="main-page"]').should("exist");
    cy.get('[data-cy="nav-live-now"]').click();
    cy.get('[data-cy="live-page"]').should("exist");
  });
});
