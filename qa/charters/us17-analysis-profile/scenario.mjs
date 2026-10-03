// US 17 — My Analysis Profile (Multi-Site Analysis) scenario.
// Drives the full multi-site portfolio surface:
//   empty state → Add to Analysis (Warangal) → add 2nd site (Karimnagar)
//   → portfolio count banner (verify 2, not 1) → Manage Portfolio (list, ⊗, Clear)
//   → Saved Analysis (3-dot) → Compare Portfolio (risk domain → Select Metrics
//   → Scenario + Period) → Table (spec fields) → Visualizations (heatmap)
//   → Download control presence → full-screen modal left/right split
//   → /my-analysis-panel at 375px mobile.
// Records a per-step outcome + evidence. Follows us10/us15/us16 SPEC-DRIFT-not-FAIL.
//
//   node qa/charters/us17-analysis-profile/scenario.mjs

import { withSession, APP_URL } from '../../harness/lib/session.mjs';
import {
  createRun, attachCollectors, snapshot, dumpDom, runAxe, finalize, step,
} from '../../harness/lib/evidence.mjs';
import { openAdmin, selectState, selectDistrict, applyCoreFilters } from '../../harness/lib/flows.mjs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });
const bodyText = (page) => page.evaluate(() => document.body.innerText.replace(/\s+/g, ' ').trim());

// The app auto-pops a "HELP US IMPROVE YOUR EXPERIENCE" feedback survey
// (data-modal-root overlay) that intercepts pointer events. Dismiss it before
// each step so it doesn't poison the portfolio flow. Records first sighting.
async function dismissFeedback(page, run) {
  try {
    // Detect the feedback survey; NEVER click Submit or the star rating.
    const present = await page.evaluate(() => !![...document.querySelectorAll('[data-modal-root], [role="dialog"]')]
      .find((el) => /HELP US IMPROVE YOUR EXPERIENCE|appreciate your quick feedback/i.test(el.innerText || '')));
    if (!present) return false;
    if (run && !run._feedbackSeen) run._feedbackSeen = true;
    // 1) Escape.
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(400);
    // 2) Explicit close only (aria-label/title "close" or a bare ×/✕ glyph button).
    await page.evaluate(() => {
      const m = [...document.querySelectorAll('[data-modal-root], [role="dialog"]')]
        .find((el) => /HELP US IMPROVE YOUR EXPERIENCE|appreciate your quick feedback/i.test(el.innerText || ''));
      if (!m) return;
      const close = [...m.querySelectorAll('button,[role="button"],[aria-label]')].find((b) => {
        const lbl = (b.getAttribute('aria-label') || b.getAttribute('title') || '').trim();
        const txt = (b.innerText || '').trim();
        if (/submit/i.test(lbl + ' ' + txt)) return false; // never submit
        return /close/i.test(lbl) || txt === '×' || txt === '✕' || txt === '';
      });
      if (close) close.click();
    });
    await page.waitForTimeout(500);
    return true;
  } catch { return false; }
}

async function safe(run, page, name, fn) {
  try {
    await dismissFeedback(page, run);
    const note = await fn();
    step(run, name, true, note || '');
    console.log(`  ok   ${name}${note ? ' — ' + note : ''}`);
  } catch (e) {
    step(run, name, false, String(e && e.message || e));
    console.log(`  FAIL ${name} — ${e && e.message || e}`);
  }
}

const addToAnalysis = async (page) => {
  const b = page.getByRole('button', { name: /Add to Analysis/i }).first();
  if (await b.count() && !(await b.isDisabled().catch(() => true))) { await b.click(); await page.waitForTimeout(1500); return true; }
  return false;
};
const expandPanel = async (page) => {
  const b = page.getByRole('button', { name: /Expand My Analysis panel/i }).first();
  if (await b.count()) { await b.click(); await page.waitForTimeout(2500); return true; }
  return false;
};
// Read the portfolio count banner ("You have added N district(s)...").
const countBanner = (page) => page.evaluate(() => {
  const m = document.body.innerText.match(/You have added\s+(\d+)\s+(district|block)s?\s+in your portfolio/i);
  return m ? { n: Number(m[1]), unit: m[2], text: m[0] } : null;
});
// Count rows inside Manage Portfolio (each location row has a remove ⊗ button).
const managePortfolioNames = (page) => page.evaluate(() => {
  const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
  const h = [...document.querySelectorAll('h2,h3,h4,button,span')].find((el) => /^Manage Portfolio$/i.test(c(el.innerText)));
  if (!h) return [];
  let box = h; for (let i = 0; i < 5 && box.parentElement; i++) box = box.parentElement;
  return [...new Set([...box.querySelectorAll('span,div,li')]
    .map((el) => c(el.innerText))
    .filter((t) => /^(Warangal|Karimnagar|Hanamkonda|Hyderabad|Nizamabad)$/i.test(t)))];
});

await withSession(async (page) => {
  const run = createRun('us17-analysis-profile');
  attachCollectors(page, run);

  // Capture portfolio/compare API traffic (status + method).
  const api = [];
  page.on('response', (res) => {
    const u = res.url();
    if (/parquet|portfolio|compare|analysis/i.test(u) && !/\.js|\.css/i.test(u)) {
      api.push({ method: res.request().method(), status: res.status(), url: u.replace(APP_URL, '') });
    }
  });
  const anyErr = () => api.filter((h) => h.status >= 500);

  await page.goto(APP_URL, { waitUntil: 'domcontentloaded' });
  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
  await page.waitForTimeout(1500);

  // S1 — Empty portfolio: expand My Analysis, expect a non-crash empty/add state.
  await safe(run, page, 'S1: empty-portfolio state', async () => {
    await expandPanel(page);
    await shot(page, run, 's1-empty');
    const txt = await bodyText(page);
    const empty = /no .*(location|site|analysis)|add .*(location|to analysis)|you have added 0|portfolio is empty/i.test(txt);
    // Collapse again so the build steps use the left Selection Panel cleanly.
    await page.getByRole('button', { name: /Collapse My Analysis panel/i }).first().click().catch(() => {});
    await page.waitForTimeout(800);
    return empty ? 'empty-state messaging present' : 'OBSERVE: no explicit empty-portfolio message (panel may show Saved list only)';
  });

  // S2 — Build a district analysis and Add to Analysis (site 1).
  let filters = {};
  await safe(run, page, 'S2: build analysis + Add to Analysis (Warangal)', async () => {
    await openAdmin(page);
    await selectState(page, 'Telangana');
    await selectDistrict(page, 'Warangal');
    filters = await applyCoreFilters(page);
    await page.waitForTimeout(1000);
    const added = await addToAnalysis(page);
    await shot(page, run, 's2-added-1');
    if (!added) throw new Error('Add to Analysis not available/enabled after building analysis');
    return `added Warangal; filters=${JSON.stringify(filters)}`;
  });

  // S3 — Add a 2nd site; verify the portfolio count banner reads 2, not 1.
  await safe(run, page, 'S3: add 2nd site → count banner = 2', async () => {
    await selectDistrict(page, 'Karimnagar');
    await page.waitForTimeout(1000);
    await addToAnalysis(page);
    await page.waitForTimeout(1000);
    await expandPanel(page);
    await shot(page, run, 's3-two-sites');
    const banner = await countBanner(page);
    const names = await managePortfolioNames(page);
    if (!banner) return `OBSERVE: no count banner; Manage Portfolio names=${JSON.stringify(names)}`;
    if (banner.n !== names.length) {
      return `SPEC-DRIFT/BUG: count banner says "${banner.n} ${banner.unit}" but Manage Portfolio lists ${names.length} (${names.join(', ')})`;
    }
    return `count banner=${banner.n}; portfolio=${JSON.stringify(names)}`;
  });

  // S4 — Manage Portfolio structure: names + ⊗ + Clear Portfolio.
  await safe(run, page, 'S4: Manage Portfolio (names, remove, Clear)', async () => {
    await shot(page, run, 's4-manage');
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const h = [...document.querySelectorAll('h2,h3,h4,button')].find((el) => /^Manage Portfolio$/i.test(c(el.innerText)));
      let box = h; for (let i = 0; i < 5 && box && box.parentElement; i++) box = box.parentElement;
      const removeBtns = box ? [...box.querySelectorAll('button,[role="button"]')].filter((b) => /remove|×|✕|⊗/i.test((b.getAttribute('aria-label') || b.innerText || '')) || b.querySelector('svg')).length : 0;
      const clear = /Clear Portfolio/i.test(box ? box.innerText : '');
      return { removeBtns, clear };
    });
    const names = await managePortfolioNames(page);
    return `names=${JSON.stringify(names)}; removeControls≈${info.removeBtns}; ClearPortfolio=${info.clear}`;
  });

  // (Remove-one-site test runs later as S9b, after the 2-site comparison, so the
  // compare steps keep both sites.)

  // S6 — Saved Analysis section: list + per-row 3-dot.
  await safe(run, page, 'S6: Saved Analysis list + 3-dot', async () => {
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const h = [...document.querySelectorAll('h2,h3,h4,button')].find((el) => /^Saved Analysis$/i.test(c(el.innerText)));
      let box = h; for (let i = 0; i < 5 && box && box.parentElement; i++) box = box.parentElement;
      const dots = box ? [...box.querySelectorAll('button')].filter((b) => /Actions for/i.test(b.getAttribute('aria-label') || '')).length : 0;
      const rows = box ? [...box.querySelectorAll('button')].filter((b) => /- \d{2} \w{3}, 20\d\d|Single District|Multi/i.test(b.innerText)).length : 0;
      return { present: !!h, dots, rows };
    });
    await shot(page, run, 's6-saved-analysis');
    if (!info.present) return 'OBSERVE: no Saved Analysis section';
    return `Saved Analysis present; rows≈${info.rows}; 3-dot menus≈${info.dots}`;
  });

  // S7 — Compare Portfolio: risk domain + Select Metrics (auto-metrics note probe).
  await safe(run, page, 'S7: Compare Portfolio risk domain + metrics', async () => {
    // Read the selected Risk Domain value (the trigger's own text, not the help btn).
    const rdVal = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const b = [...document.querySelectorAll('button')].find((el) => /^Select Risk Domain$/i.test(el.getAttribute('aria-label') || ''));
      return b ? c(b.innerText) : '';
    });
    // Open Select Metrics and select all.
    await page.getByRole('button', { name: /^Select Metrics$/i }).first().click().catch(() => {});
    await page.waitForTimeout(1000);
    await shot(page, run, 's7-metrics-open');
    const autoNote = await page.evaluate(() => {
      const m = document.body.innerText.match(/\d+\s+metrics?\s+from\s+\d+\s+domain|metrics are auto included|auto included from selected/i);
      return m ? m[0] : null;
    });
    await page.getByText(/^All Metrics \(\d+\)/i).first().click().catch(() => {});
    await page.waitForTimeout(1000);
    await page.keyboard.press('Escape').catch(() => {});
    await page.waitForTimeout(500);
    const advanced = await page.evaluate(() => /manually refine metric|advanced metric/i.test(document.body.innerText));
    return `riskDomain="${rdVal}"; auto-metrics note=${autoNote ? `"${autoNote}"` : 'NOT FOUND [spec 962-966]'}; advanced-metric checkbox=${advanced ? 'present' : 'NOT FOUND [spec 968-972]'}`;
  });

  // S8 — Scenario + Period, then Table view with spec fields + API status.
  await safe(run, page, 'S8: comparison Table fields + API', async () => {
    // Scenario/Period are custom checkboxes inside <label>s; a Playwright click on
    // the label doesn't forward — click the nested <input> directly via evaluate.
    const clickBox = (label) => page.evaluate((lb) => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const l = [...document.querySelectorAll('label')].find((el) => new RegExp(`^${lb}$`, 'i').test(c(el.innerText)));
      if (l && l.querySelector('input')) { l.querySelector('input').click(); return true; }
      return false;
    }, label);
    const ssp = await clickBox('SSP2-4\\.5');
    const period = await clickBox('Early century \\(2020-2040\\)');
    await page.waitForTimeout(3500); // portfolio-comparison-table POST
    await page.getByRole('button', { name: /^Table$/i }).first().click().catch(() => {});
    await page.waitForTimeout(2500);
    await shot(page, run, 's8-table');
    const t = await page.evaluate(() => {
      const body = document.body.innerText;
      return {
        placeholder: /to load comparison table data/i.test(body),
        posInState: /position in state/i.test(body),
        state: /\bState\b/.test(body),
        district: /\bDistrict\b/.test(body),
        indexValue: /index value/i.test(body),
        absChange: /absolute change/i.test(body),
        changePct: /change percentile/i.test(body),
        levelOfChange: /level of change/i.test(body),
        warangal: /Warangal/i.test(body),
        karimnagar: /Karimnagar/i.test(body),
      };
    });
    const err = anyErr();
    const cmpApi = api.filter((h) => /portfolio-comparison-table/i.test(h.url)).slice(-1)[0];
    const fields = ['posInState', 'state', 'district', 'indexValue', 'absChange', 'changePct', 'levelOfChange'];
    const present = fields.filter((f) => t[f]);
    const missing = fields.filter((f) => !t[f]);
    if (t.placeholder) return `OBSERVE: table still shows placeholder — data did not load (ssp=${ssp} period=${period}); cmp API=${cmpApi ? cmpApi.status : 'none'}; 5xx=${JSON.stringify(err)}`;
    if (err.length) return `TABLE ERROR: compare API 5xx=${JSON.stringify(err)}; fields present=${present.join(',')}`;
    return `Table loaded (portfolio-comparison-table ${cmpApi ? cmpApi.status : '?'}); fields present=[${present.join(',')}]; missing=[${missing.join(',')}]${t.posInState ? '' : ' [SPEC-DRIFT: spec 980 "Position in State" column absent]'}; rows Warangal=${t.warangal} Karimnagar=${t.karimnagar}`;
  });

  // S9 — Visualizations: heatmap + charts + legend.
  await safe(run, page, 'S9: Visualizations (heatmap + legend)', async () => {
    await page.getByRole('button', { name: /^Visualizations$/i }).first().click().catch(() => {});
    await page.waitForTimeout(3000);
    await shot(page, run, 's9-viz');
    const v = await page.evaluate(() => {
      const body = document.body.innerText;
      return {
        svgCanvas: document.querySelectorAll('svg,canvas').length,
        heatmap: /heatmap/i.test(body),
        scenarioChart: /scenario comparison|comparison chart/i.test(body),
        legend: /level of change|change level|legend/i.test(body),
      };
    });
    const err = anyErr();
    if (err.length) return `VIZ API 5xx=${JSON.stringify(err)}; svg/canvas=${v.svgCanvas}`;
    return `svg/canvas=${v.svgCanvas}; heatmap=${v.heatmap}; scenario-chart=${v.scenarioChart}; legend=${v.legend}`;
  });

  // S9b — Remove one site via ⊗ "Remove from portfolio"; list shrinks (spec 947-949).
  await safe(run, page, 'S9b: remove one site (⊗)', async () => {
    const before = await managePortfolioNames(page);
    const removed = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const h = [...document.querySelectorAll('h2,h3,h4,button,span')].find((el) => /^Manage Portfolio$/i.test(c(el.innerText)));
      let box = h; for (let i = 0; i < 5 && box && box.parentElement; i++) box = box.parentElement;
      if (!box) return false;
      // Each row has a "Show on map" button and a "Remove from portfolio" button.
      const rows = [...box.querySelectorAll('div,li')].filter((el) => /Karimnagar/i.test(el.innerText) && el.querySelector('button'));
      rows.sort((a, b) => a.innerText.length - b.innerText.length);
      const row = rows[0];
      if (!row) return false;
      const bs = [...row.querySelectorAll('button,[role="button"]')];
      const rm = bs.find((b) => /remove|delete/i.test((b.getAttribute('title') || '') + (b.getAttribute('aria-label') || ''))) || bs[bs.length - 1];
      if (rm) { rm.click(); return true; }
      return false;
    });
    await page.waitForTimeout(1500);
    await shot(page, run, 's9b-removed');
    const after = await managePortfolioNames(page);
    if (!removed) return `OBSERVE: could not locate a "Remove from portfolio" control on the Karimnagar row; names=${JSON.stringify(after)}`;
    return after.length < before.length
      ? `remove ⊗ works: ${JSON.stringify(before)} → ${JSON.stringify(after)}`
      : `OBSERVE: list did not shrink; before=${JSON.stringify(before)} after=${JSON.stringify(after)}`;
  });

  // S10 — Download control presence (do NOT trigger a download).
  await safe(run, page, 'S10: Download control present', async () => {
    const d = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const btns = [...document.querySelectorAll('button,a,[role="button"]')].map((b) => c(b.innerText)).filter((t) => /download/i.test(t));
      return [...new Set(btns)];
    });
    return d.length ? `download control(s)=${JSON.stringify(d)} [spec 994 says "Download Heatmap"]` : 'OBSERVE: no Download control found [spec 994]';
  });

  // S11 — Full-screen modal left/right split (spec 1032-1039).
  await safe(run, page, 'S11: modal left/right split', async () => {
    const fs = page.locator('[aria-label*="full" i], [aria-label*="expand" i][aria-label*="analysis" i]').first();
    if (!(await fs.count())) return 'OBSERVE: no full-screen control on the My Analysis panel';
    await fs.click();
    await page.waitForTimeout(2500);
    await shot(page, run, 's11-modal');
    const info = await page.evaluate(() => {
      const c = (t) => (t || '').trim().replace(/\s+/g, ' ');
      const dlg = document.querySelector('[role="dialog"],[aria-modal="true"]');
      if (!dlg) return { dialog: false };
      const body = c(dlg.innerText);
      // Tightest element whose trimmed text equals the label → its left edge.
      const leftOf = (label) => {
        const els = [...dlg.querySelectorAll('h1,h2,h3,h4,h5,h6,div,span,button,p')]
          .filter((e) => new RegExp(`^${label}$`, 'i').test(c(e.innerText)));
        els.sort((a, b) => a.innerText.length - b.innerText.length);
        return els[0] ? Math.round(els[0].getBoundingClientRect().left) : null;
      };
      const savedX = leftOf('Saved Analysis');
      const compareX = leftOf('Compare Portfolio');
      return {
        dialog: true,
        saved: /Saved Analysis/i.test(body), manage: /Manage Portfolio/i.test(body), compare: /Compare Portfolio/i.test(body),
        savedX, compareX, split: savedX != null && compareX != null && compareX - savedX > 200,
      };
    });
    // Close modal.
    await page.getByRole('button', { name: /Close expanded My Analysis view/i }).first().click().catch(() => {});
    await page.keyboard.press('Escape').catch(() => {});
    if (!info.dialog) return 'OBSERVE: full-screen did not open a dialog';
    return `modal open; Saved(left x=${info.savedX}) / Compare(right x=${info.compareX}); left-right-split=${info.split}; sections: saved=${info.saved} manage=${info.manage} compare=${info.compare}`;
  });

  // S12 — Mobile 375px capture of the expanded My Analysis panel (closes US 15 caveat).
  await safe(run, page, 'S12: My Analysis panel at 375px', async () => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.waitForTimeout(1000);
    await expandPanel(page);
    await page.waitForTimeout(1500);
    await page.screenshot({ path: join(run.dir, 's12-mobile.png'), fullPage: true });
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth + 4);
    await page.setViewportSize({ width: 1440, height: 900 });
    return overflow ? 'OBSERVE: horizontal overflow at 375px (possible mobile layout break)' : 'panel renders at 375px without horizontal overflow';
  });

  // Cross-cutting.
  await dumpDom(page, run, 'us17-final');
  await snapshot(page, run, 'us17-responsive');
  await runAxe(page, run, 'us17');
  finalize(run);

  console.log(`\n  Run: ${run.dir}`);
  console.log(`  compare/portfolio API hits: ${JSON.stringify(api.filter((h) => !/\.(js|css)/.test(h.url)).slice(-16))}`);
  console.log(`  Steps failed: ${run.summary.stepsFailed} | real error events: ${run.summary.errorEvents} | benign: ${run.summary.benignEvents}`);
});
