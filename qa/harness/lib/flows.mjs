// Reusable UI flows for the dashboard, built from selectors discovered during
// recon. Shared by scenarios that need geography + filters set up before
// exercising a specific feature (map, ranking, profiles).

/** Expand the Administrative Panel if it is collapsed. */
export async function openAdmin(page) {
  const stateVisible = await page.getByRole('button', { name: /Select State/i }).isVisible().catch(() => false);
  if (!stateVisible) {
    await page.getByRole('button', { name: /Administrative Panel/i }).click();
    await page.waitForTimeout(700);
  }
}

/** Select a State by exact name. */
export async function selectState(page, name) {
  await openAdmin(page);
  await page.getByRole('button', { name: /Select State/i }).click();
  await page.waitForTimeout(500);
  await page.locator('li[role="option"]', { hasText: new RegExp(`^${name}$`) }).first().click();
  await page.waitForTimeout(1000);
}

/** Select one District by exact name (District view zone must be active). */
export async function selectDistrict(page, name) {
  const dBtn = page.getByRole('button', { name: /Select District/i });
  await dBtn.click();
  await page.waitForTimeout(600);
  await page.locator('li[role="option"]').filter({ hasText: new RegExp(`^${name}$`) }).first().click();
  await page.waitForTimeout(400);
  await dBtn.click(); // close dropdown
  await page.waitForTimeout(400);
}

/**
 * Apply the core resilience filters that require manual selection: Risk Domain,
 * Metric, Scenario, Period (Statistic + Map Mode auto-default). Uses the
 * "first exact-Select trigger" trick, valid when selecting in DOM order.
 */
export async function applyCoreFilters(page) {
  await page.getByText(/Select Resilience Filters/i).first().click();
  await page.waitForTimeout(800);
  const pick = async () => {
    await page.getByText('Select', { exact: true }).first().click();
    await page.waitForTimeout(500);
    const chosen = await page.locator('li[role="option"]').first().innerText().catch(() => '');
    await page.locator('li[role="option"]').first().click();
    await page.waitForTimeout(700);
    return chosen.trim();
  };
  const riskDomain = await pick();
  const metric = await pick();
  const scenario = await pick();
  await page.waitForFunction(() => !document.body.innerText.includes('Select a scenario first'),
    { timeout: 10000 }).catch(() => {});
  const period = await pick();
  await page.waitForTimeout(1500);
  return { riskDomain, metric, scenario, period };
}
