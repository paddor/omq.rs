import { defineConfig } from "@playwright/test";

export default defineConfig({
  reporter: "list",
  testDir: ".",
  testMatch: [
    "test/browser-interop.playwright.mjs",
    "test/webtransport-browser.playwright.mjs",
  ],
  projects: [
    {
      name: "firefox",
      use: {
        browserName: "firefox",
        headless: true,
        ignoreHTTPSErrors: true,
      },
    },
    {
      name: "chromium",
      testMatch: "test/webtransport-browser.playwright.mjs",
      use: {
        browserName: "chromium",
        headless: true,
      },
    },
  ],
});
