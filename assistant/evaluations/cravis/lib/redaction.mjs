const SECRET_PATTERNS = [
  /\bBearer\s+[A-Za-z0-9._~+\/-]+=*/gi,
  /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g,
  /\b(?:api[_-]?key|secret|token|password)\s*[:=]\s*[^\s,;]+/gi,
  /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi,
  /\b(?:session|cookie)\s*[:=]\s*[^\s,;]+/gi
];
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const OPAQUE = /^[A-Za-z0-9_-]{24,}$/;

export function redactText(value) {
  let result = String(value ?? '');
  for (const pattern of SECRET_PATTERNS) result = result.replace(pattern, '[REDACTED]');
  return result;
}

export function sanitizePathname(pathname) {
  return pathname.split('/').map((segment) => {
    if (!segment) return segment;
    let decoded = segment;
    try { decoded = decodeURIComponent(segment); } catch {}
    if (UUID.test(decoded) || OPAQUE.test(decoded) || decoded.includes('@')) return ':redacted';
    return decoded.replace(/\b\d{8,}\b/g, ':id');
  }).join('/');
}

export function sanitizeNetworkRecord({ url, method, resourceType, status = null, durationMs = null, failureClass = null }, excludedPathPrefixes = []) {
  const parsed = new URL(url);
  if (excludedPathPrefixes.some((prefix) => parsed.pathname.startsWith(prefix))) return null;
  return {
    origin: parsed.origin,
    pathname: sanitizePathname(parsed.pathname),
    method: String(method).toUpperCase(),
    resourceType: String(resourceType),
    status: status == null ? null : Number(status),
    durationMs: durationMs == null ? null : Number(durationMs),
    failureClass: failureClass == null ? null : redactText(failureClass)
  };
}

