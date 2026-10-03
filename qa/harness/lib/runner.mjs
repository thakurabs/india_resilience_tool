// Shared scenario helpers: a guarded step runner and a screenshot shortcut.
import { join } from 'node:path';
import { step } from './evidence.mjs';

/** Screenshot into the run dir by name. */
export const shot = (page, run, name) => page.screenshot({ path: join(run.dir, `${name}.png`) });

/**
 * Run one named step, recording its outcome. Never throws — a failed step is
 * captured as ok=false so the scenario continues and the reviewer sees the full
 * picture. The step fn may return a note string.
 */
export async function safe(run, name, fn) {
  try {
    const note = await fn();
    step(run, name, true, note || '');
    console.log(`  ok   ${name}${note ? ' — ' + note : ''}`);
  } catch (e) {
    step(run, name, false, String((e && e.message) || e));
    console.log(`  FAIL ${name} — ${(e && e.message) || e}`);
  }
}
