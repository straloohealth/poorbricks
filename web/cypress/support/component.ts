// Loaded before every component spec.
import { mount } from "cypress/react";
import "../../app/globals.css";
import "./commands";

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Cypress {
    interface Chainable {
      mount: typeof mount;
    }
  }
}

Cypress.Commands.add("mount", mount);
