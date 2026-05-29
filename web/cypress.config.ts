import { defineConfig } from "cypress";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";

const alias = { "@": fileURLToPath(new URL("./", import.meta.url)) };

export default defineConfig({
  // Quietens noise; the app is small so default viewport is fine.
  video: false,
  screenshotOnRunFailure: true,
  defaultCommandTimeout: 8000,

  e2e: {
    // Override with CYPRESS_BASE_URL when the dev server runs elsewhere.
    baseUrl: process.env.CYPRESS_BASE_URL ?? "http://localhost:3100",
    specPattern: "cypress/e2e/**/*.cy.{ts,tsx}",
    supportFile: "cypress/support/e2e.ts",
  },

  component: {
    devServer: {
      framework: "react",
      bundler: "vite",
      viteConfig: {
        plugins: [react()],
        resolve: { alias },
      },
    },
    specPattern: "cypress/component/**/*.cy.{ts,tsx}",
    supportFile: "cypress/support/component.ts",
    indexHtmlFile: "cypress/support/component-index.html",
  },
});
