import { defineConfig } from "@playwright/test";

// OMQ_TEST_CHROMIUM and OMQ_TEST_FIREFOX select browser builds outside
// Playwright's pinned ones. Stock Firefox has no Juggler, so Playwright drives
// it over WebDriver BiDi through the moz-firefox channel.
const chromium = process.env.OMQ_TEST_CHROMIUM;
const firefox = process.env.OMQ_TEST_FIREFOX;

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
        ...(firefox
          ? {
            channel: "moz-firefox",
            launchOptions: { executablePath: firefox },
          }
          : {}),
        headless: true,
        ignoreHTTPSErrors: true,
      },
    },
    {
      name: "chromium",
      testMatch: "test/webtransport-browser.playwright.mjs",
      use: {
        browserName: "chromium",
        ...(chromium ? { launchOptions: { executablePath: chromium } } : {}),
        headless: true,
      },
    },
  ],
});
