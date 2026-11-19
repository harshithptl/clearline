import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';

import { expect, test, type Page } from '@playwright/test';

// The stack has no account API, so accounts are created directly in the
// compose Postgres. Fresh ids per run keep reruns independent, and the WEB
// book is used only by this test.
const compose = fileURLToPath(new URL('../../deploy/compose/docker-compose.yml', import.meta.url));

function createAccounts(...ids: string[]) {
  const values = ids.map((id) => `('${id}')`).join(',');
  execFileSync('docker', [
    'compose',
    '-f',
    compose,
    'exec',
    '-T',
    'postgres',
    'psql',
    '-U',
    'clearline',
    '-qc',
    `INSERT INTO accounts (account_id) VALUES ${values}`,
  ]);
}

async function placeOrder(page: Page, account: string, side: 'buy' | 'sell') {
  const form = page.getByRole('form', { name: 'Place order' });
  await form.getByLabel('Account').fill(account);
  await form.getByRole('radio', { name: side }).click();
  await form.getByRole('button', { name: `Place ${side}` }).click();
}

test('an order pair placed in the UI settles and shows up live', async ({ page }) => {
  const run = randomBytes(4).toString('hex');
  const seller = `web-s-${run}`;
  const buyer = `web-b-${run}`;
  createAccounts(seller, buyer);

  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Clearline' })).toBeVisible();
  await expect(page.getByText('Live', { exact: true })).toBeVisible();

  // Shared fields: WEB, 2 @ 50.25.
  const form = page.getByRole('form', { name: 'Place order' });
  await form.getByRole('combobox', { name: 'Symbol' }).click();
  await page.getByRole('option', { name: 'WEB' }).click();
  await form.getByLabel('Quantity').fill('2');
  await form.getByLabel(/^Price/).fill('50.25');

  await placeOrder(page, seller, 'sell');
  await expect(form.getByRole('status')).toContainText('Accepted');

  await placeOrder(page, buyer, 'buy');
  await expect(form.getByRole('status')).toContainText('Accepted');
  await expect(form.getByRole('status')).not.toContainText('replayed');

  // Same order again without editing: same Idempotency-Key, replayed.
  await form.getByRole('button', { name: 'Place buy' }).click();
  await expect(form.getByRole('status')).toContainText('replayed');

  // The trade arrives on the live tape with both sides and the exact price.
  const trade = page.getByRole('list', { name: 'Trade tape' }).getByRole('listitem').filter({
    hasText: buyer,
  });
  await expect(trade).toContainText('2 @ 50.25 USD');
  await expect(trade).toContainText(seller);
  await expect(trade).toContainText(/WEB-\d+-\d+-1/);

  // Balances refresh from the stream: exact amounts, no float noise.
  const row = page.getByRole('table', { name: 'Balances' }).getByRole('row', { name: buyer });
  await expect(row).toContainText('-100.5');
  await expect(row.getByRole('cell').nth(0)).toHaveText(buyer);
  await expect(
    page.getByRole('table', { name: 'Balances' }).getByRole('row', { name: seller }),
  ).toContainText('100.5');

  // The reconciler still agrees with everything.
  await expect(
    page.getByRole('status').filter({ hasText: /Books balanced|Cache catching up/ }),
  ).toBeVisible();
});
