import { isIP } from 'node:net';

function normalizedOrigin(value) {
  const url = new URL(value);
  return url.origin;
}

export function isLoopbackHostname(hostname) {
  const lower = hostname.toLowerCase();
  return lower === 'localhost' || lower === '::1' || lower === '[::1]' || lower.startsWith('127.');
}

export function validateTargetUrl(value, approvedOrigins, { simulation = false } = {}) {
  const url = new URL(value);
  const loopback = isLoopbackHostname(url.hostname);
  if (url.protocol !== 'https:' && !(simulation && url.protocol === 'http:' && loopback)) {
    throw new Error('Live navigation requires HTTPS; HTTP is limited to loopback simulation');
  }
  const approved = new Set(approvedOrigins.map(normalizedOrigin));
  if (!approved.has(url.origin) && !(simulation && loopback)) throw new Error(`Unapproved origin: ${url.origin}`);
  if (isIP(url.hostname) && !loopback && url.protocol !== 'https:') throw new Error('Non-loopback IP targets require HTTPS and approval');
  return url;
}

export function installRequestPolicy(page, approvedOrigins, { simulation = false, onBlocked = () => {} } = {}) {
  const allowed = new Set(approvedOrigins.map(normalizedOrigin));
  return page.route('**/*', async (route) => {
    let url;
    try { url = new URL(route.request().url()); } catch { onBlocked('invalid-url'); return route.abort('blockedbyclient'); }
    if ((allowed.has(url.origin) && url.protocol === 'https:') || (simulation && isLoopbackHostname(url.hostname) && url.protocol === 'http:')) return route.continue();
    onBlocked(url.origin);
    return route.abort('blockedbyclient');
  });
}

