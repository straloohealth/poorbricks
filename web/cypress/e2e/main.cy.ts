// Exercises the Main page against the live FastAPI server + real contracts.
describe("Main page", () => {
  beforeEach(() => cy.visit("/"));

  it("shows the alerts panel with severity counts", () => {
    cy.get('[data-cy="alerts-panel"]').should("be.visible");
    cy.get('[data-cy="count-errors"]').invoke("text").should("match", /^\d+$/);
    cy.get('[data-cy="count-warnings"]').invoke("text").should("match", /^\d+$/);
    cy.get('[data-cy="count-info"]').invoke("text").should("match", /^\d+$/);
  });

  it("renders the lineage graph with at least one node", () => {
    cy.get('[data-cy="lineage-graph"]').should("be.visible");
    cy.get(".react-flow__node").its("length").should("be.greaterThan", 0);
  });

  it("loads a table's detail when picked from the lineage", () => {
    // The picker is populated from the live lineage; choose the first real table.
    cy.get('[data-cy="table-picker"] option').eq(1).then(($opt) => {
      const table = $opt.val() as string;
      cy.get('[data-cy="table-picker"]').select(table);
      cy.get('[data-cy="detail-title"]').should("contain.text", table);
      // Detail always renders a "Previous runs" section (table or empty state).
      cy.get('[data-cy="table-detail"]').should("contain.text", "Previous runs");
    });
  });

  it("clicking a lineage node selects it", () => {
    cy.get(".react-flow__node").first().click();
    cy.get('[data-cy="detail-title"]').should("exist");
  });
});
