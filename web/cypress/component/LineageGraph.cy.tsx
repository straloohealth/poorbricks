import { useState } from "react";
import { LineageGraph } from "@/components/LineageGraph";
import type { LineageGraph as Graph } from "@/lib/api";

// a → b → c , and a → d (so b/d are siblings, c is a leaf)
const GRAPH: Graph = {
  nodes: [
    { id: "a", label: "a", kind: "bronze" },
    { id: "b", label: "b", kind: "silver" },
    { id: "c", label: "c", kind: "gold" },
    { id: "d", label: "d", kind: "silver" },
  ],
  edges: [
    { source: "a", target: "b" },
    { source: "b", target: "c" },
    { source: "a", target: "d" },
  ],
};

function Harness() {
  const [sel, setSel] = useState<string | null>(null);
  return (
    <div style={{ height: 500 }}>
      <div data-cy="selected">{sel ?? "none"}</div>
      <LineageGraph graph={GRAPH} selected={sel} onSelect={setSel} />
    </div>
  );
}

describe("<LineageGraph />", () => {
  it("renders one node per contract", () => {
    cy.mount(<Harness />);
    cy.get('[data-cy="lineage-graph"]').should("exist");
    cy.get(".react-flow__node").should("have.length", 4);
  });

  it("selecting a node reports it and dims unrelated nodes", () => {
    cy.mount(<Harness />);
    // click node "b": ancestors {a}, descendants {c}; "d" is unrelated → dimmed
    cy.get(".react-flow__node").contains("b").click();
    cy.get('[data-cy="selected"]').should("have.text", "b");
    // unrelated node "d" is dimmed (opacity 0.4)
    cy.get(".react-flow__node")
      .contains("d")
      .parents(".react-flow__node")
      .should(($n) => {
        expect(parseFloat($n.css("opacity"))).to.be.lessThan(0.6);
      });
  });
});
