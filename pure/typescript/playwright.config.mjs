import { defineConfig } from "@playwright/test";

export default defineConfig({
  reporter: "list",
  testDir: ".",
  testMatch: "test/browser-interop.playwright.mjs",
  projects: [
    {
      name: "firefox",
      use: {
        browserName: "firefox",
        headless: true,
        ignoreHTTPSErrors: true,
      },
    },
  ],
});
