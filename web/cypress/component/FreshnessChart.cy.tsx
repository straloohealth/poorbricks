import { FreshnessChart, type FreshPoint } from "@/components/FreshnessChart";

const POINTS: FreshPoint[] = [
  { table: "dim_patient", ageHours: 2.1, status: "ok" },
  { table: "fact_visit", ageHours: 2.0, status: "ok" }, // same bucket as dim_patient (~2h)
  { table: "smith_users", ageHours: 9.4, status: "ok" },
];

describe("<FreshnessChart />", () => {
  it("shows the empty state with no points", () => {
    cy.mount(<FreshnessChart points={[]} />);
    cy.get('[data-cy="freshness-empty"]').should("exist");
  });

  it("renders dots and reveals the bucket list on click", () => {
    cy.mount(
      <div style={{ width: 600 }}>
        <FreshnessChart points={POINTS} />
      </div>,
    );
    cy.get('[data-cy="freshness-chart"]').should("exist");
    // three points, two of which share a bucket → 3 dots total
    cy.get(".recharts-scatter-symbol").should("have.length", 3);
    cy.get(".recharts-scatter-symbol").first().click({ force: true });
    cy.get('[data-cy="freshness-bucket"]').should("be.visible");
    cy.get('[data-cy="freshness-bucket-item"]').should("have.length.greaterThan", 0);
  });
});
