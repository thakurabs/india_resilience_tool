export const QUOTA_PARSER_VERSION = 'quota-v1';

export function parseQuota(rawText) {
  const raw = String(rawText ?? '').trim();
  const matches = [...raw.matchAll(/(?<!\d)(\d{1,9})\s*(?:\/|of)\s*(\d{1,9})(?!\d)/gi)];
  if (matches.length !== 1) return { ok: false, raw, parserVersion: QUOTA_PARSER_VERSION, reason: matches.length ? 'ambiguous' : 'malformed' };
  const current = Number(matches[0][1]);
  const maximum = Number(matches[0][2]);
  if (maximum <= 0 || current < 0 || current > maximum) return { ok: false, raw, parserVersion: QUOTA_PARSER_VERSION, reason: 'out_of_range' };
  return { ok: true, raw, current, maximum, parserVersion: QUOTA_PARSER_VERSION };
}

export function quotaPollDelays() {
  return [...Array(20).fill(250), ...Array(25).fill(1000)];
}

export async function confirmQuotaDecrement(before, readQuota, { sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), now = () => new Date().toISOString(), selector = null } = {}) {
  if (!before?.ok) return { ok: false, reason: 'invalid_baseline', observations: [] };
  const observations = [];
  for (const delay of quotaPollDelays()) {
    await sleep(delay);
    const parsed = parseQuota(await readQuota());
    const observation = { ...parsed, timestamp: now(), sourceSelector: selector };
    observations.push(observation);
    if (!parsed.ok) continue;
    if (parsed.maximum !== before.maximum) return { ok: false, reason: 'maximum_changed', observations };
    const decrement = before.current - parsed.current;
    if (decrement === 1) return { ok: true, before, after: observation, observations };
    if (decrement > 1 || decrement < 0) return { ok: false, reason: decrement > 1 ? 'multi_decrement' : 'quota_increased', observations };
  }
  return { ok: false, reason: observations.some((o) => o.ok) ? 'unchanged' : 'missing_or_malformed', observations };
}
