import { defineConfig, devices } from "@playwright/test";

const frontendBaseUrl = process.env.PLAYWRIGHT_FRONTEND_URL ?? "http://127.0.0.1:5173";
const backendBaseUrl = process.env.PLAYWRIGHT_BACKEND_URL ?? "http://127.0.0.1:8000";

export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  expect: {
    timeout: 10_000
  },
  fullyParallel: false,
  retries: 0,
  reporter: [["list"]],
  use: {
    baseURL: frontendBaseUrl,
    extraHTTPHeaders: {
      "x-playwright-backend-url": backendBaseUrl
    },
    trace: "on-first-retry"
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"]
      }
    }
  ],
  webServer: {
    command: "npm run dev",
    reuseExistingServer: true,
    url: frontendBaseUrl
  }
});
