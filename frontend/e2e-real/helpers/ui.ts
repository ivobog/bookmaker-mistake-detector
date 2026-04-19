import { expect, type Page } from "@playwright/test";

export function collectPageErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on("pageerror", (error) => {
    errors.push(error.message);
  });
  return errors;
}

export async function expectNoFatalUiErrors(page: Page, pageErrors: string[]): Promise<void> {
  await expect(page.locator(".banner-error")).toHaveCount(0);
  expect(pageErrors, `unexpected page errors: ${pageErrors.join(" | ")}`).toEqual([]);
}

export async function openRoute(page: Page, hashRoute: string): Promise<void> {
  await page.goto(`/${hashRoute}`);
}

export async function readHeadingNumber(page: Page, pattern: RegExp): Promise<number> {
  const heading = page.getByRole("heading").filter({ hasText: pattern }).first();
  await expect(heading).toBeVisible();
  const text = (await heading.textContent()) ?? "";
  const match = text.match(/#(\d+)/);
  if (!match) {
    throw new Error(`Could not extract numeric id from heading: ${text}`);
  }
  return Number(match[1]);
}
