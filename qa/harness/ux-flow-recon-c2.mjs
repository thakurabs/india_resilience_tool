// UX-flow recon, Phase C2: the Coordinate Analysis path, captured properly.
//
// Phase C stalled on the mode-switch confirm ("Switch to Coordinate Analysis?"),
// whose proceed button is labelled "Use Coordinate Analysis" — not the generic
// switch/proceed wording the first pass looked for. Read-only: a coordinate is
// typed and resolved, nothing is uploaded, saved or submitted.
import { withSession } from './lib/session.mjs';
import { createRun, attachCollectors, finalize } from './lib/evidence.mjs';
import { attachApiRecorder, captureState, stage, writeJson, writeApiLog } from './lib/recon.mjs';

await withSession(async (page) => {
  const run = createRun('ux-flow-C2-coords');
  attachCollectors(page, run);
  attachApiRecorder(page, run);
  page.setDefaultTimeout(9000);
  const found = {};

  await page.goto('https://dev.resilience.org.in/', { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 25000 }).catch(() => {});
  await page.waitForTimeout(2500);

  await stage(run, 'X1-switch-to-coordinates', async () => {
    await page.getByRole('button', { name: /^Coordinate Analysis$/ }).first().click();
    await page.waitForTimeout(1500);
    const modal = page.locator('[data-modal-root]').first();
    if (await modal.isVisible().catch(() => false)) {
      found.switchConfirmText = (await modal.innerText().catch(() => '')).replace(/\s+/g, ' ').slice(0, 400);
      await captureState(page, run, 'X1-switch-confirm', { note: 'mode-switch confirm dialog' });
      await page.getByRole('button', { name: /^Use Coordinate Analysis$/ }).click();
      await page.waitForTimeout(2500);
    }
    await captureState(page, run, 'X1-coordinate-panel',
      { shots: 'all', note: 'Coordinate Analysis panel open' });
    found.panel = await page.evaluate(() => {
      const vis = (el) => {
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
      };
      return {
        inputs: [...document.querySelectorAll('input,textarea')].filter(vis).map((i) => ({
          tag: i.tagName.toLowerCase(), type: i.type, placeholder: i.placeholder || null,
          name: i.name || null, aria: i.getAttribute('aria-label'), accept: i.accept || null,
        })),
        buttons: [...document.querySelectorAll('button,a')].filter(vis).map((b) => ({
          text: (b.innerText || '').trim().slice(0, 60),
          aria: b.getAttribute('aria-label'), href: b.getAttribute('href'),
          disabled: b.disabled === true,
        })).filter((b) => b.text || b.aria),
      };
    });
  });

  await stage(run, 'X2-manual-entry', async () => {
    const ins = page.locator('input:visible');
    const n = await ins.count();
    const metas = [];
    for (let i = 0; i < n; i += 1) {
      metas.push({
        i, type: await ins.nth(i).getAttribute('type'),
        ph: await ins.nth(i).getAttribute('placeholder'),
      });
    }
    found.visibleInputs = metas;
    const latIdx = metas.findIndex((m) => /lat/i.test(m.ph || ''));
    const lonIdx = metas.findIndex((m) => /lon|lng/i.test(m.ph || ''));
    if (latIdx >= 0 && lonIdx >= 0) {
      await ins.nth(latIdx).fill('17.9784');
      await ins.nth(lonIdx).fill('79.5941');
      const nameIdx = metas.findIndex((m) => /name|label/i.test(m.ph || ''));
      if (nameIdx >= 0) await ins.nth(nameIdx).fill('Recon Point');
      await page.waitForTimeout(600);
      await captureState(page, run, 'X2-coordinate-filled', { note: 'lat/lon typed, not yet resolved' });
      const show = page.getByRole('button', { name: /show on map/i }).first();
      if (await show.isEnabled().catch(() => false)) {
        await show.click();
        await page.waitForTimeout(3500);
        await captureState(page, run, 'X2-coordinate-resolved',
          { shots: 'all', note: 'Show on Map — resolution result' });
      }
      const addPt = page.getByRole('button', { name: /^Add Coordinate$/i }).first();
      if (await addPt.isVisible().catch(() => false) && await addPt.isEnabled().catch(() => false)) {
        await addPt.click();
        await page.waitForTimeout(2500);
        await captureState(page, run, 'X2-coordinate-staged', { note: 'coordinate staged in the list' });
      }
    } else {
      throw new Error(`no lat/lon inputs among ${JSON.stringify(metas).slice(0, 300)}`);
    }
  });

  await stage(run, 'X3-upload-affordance', async () => {
    // Look at, but never use, the file-upload path.
    found.fileInputs = await page.evaluate(() => [...document.querySelectorAll('input[type=file]')]
      .map((i) => ({ accept: i.accept, name: i.name, id: i.id })));
    const up = page.getByText(/upload|csv|xlsx|shapefile|browse|drag/i).first();
    if (await up.isVisible().catch(() => false)) {
      await up.scrollIntoViewIfNeeded().catch(() => {});
      await captureState(page, run, 'X3-upload-affordance', { note: 'upload controls (nothing uploaded)' });
    }
  });

  await stage(run, 'X4-back-to-admin', async () => {
    await page.getByRole('button', { name: /^Administrative Analysis$/ }).first().click();
    await page.waitForTimeout(1500);
    const modal = page.locator('[data-modal-root]').first();
    if (await modal.isVisible().catch(() => false)) {
      found.backConfirmText = (await modal.innerText().catch(() => '')).replace(/\s+/g, ' ').slice(0, 400);
      await captureState(page, run, 'X4-back-confirm', { note: 'confirm when switching back' });
    }
  });

  writeJson(run, '_found', found);
  const summary = writeApiLog(run);
  finalize(run);
  console.log('\n  Run:', run.dir);
  for (const s of summary) console.log(`    ${s.endpoint}  x${s.calls}  ${JSON.stringify(s.statuses)}`);
  console.log('  Failed:', run.steps.filter((s) => s.ok === false).map((s) => `${s.name}(${s.note})`).join('; ') || 'none');
}, { viewport: { width: 1600, height: 1000 } });
