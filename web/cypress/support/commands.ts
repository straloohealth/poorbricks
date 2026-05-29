/// <reference types="cypress" />

// Find an element by its data-cy tag — keeps specs decoupled from styling/text.
Cypress.Commands.add("cy", (sel: string) => cy.get(`[data-cy="${sel}"]`));

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Cypress {
    interface Chainable {
      cy(sel: string): Chainable<JQuery<HTMLElement>>;
    }
  }
}

export {};
