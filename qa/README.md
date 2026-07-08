# QA harness — India Resilience Tool (vendor UAT app)

Agent-assisted QA for the vendor web app at `dev.resilience.org.in` (a full
reimplementation of the IRT, separate from this repo's Streamlit tool).

Scope is driven by **`Resilience Actions User Stories (v1.3)`** (US 01–17) and
covers functional, data-correctness, visual/responsive, and accessibility/perf
checks.

## How it works (three layers)

1. **Harness (deterministic, no LLM)** — Playwright scenarios drive each user
   story and auto-capture evidence: console errors, failed/HTTP-error requests,
   multi-viewport screenshots, an interactive-DOM map, a NaN/blank scan, and
   axe-core violations. See `harness/lib/evidence.mjs`.
2. **Reviewer (cheap model)** — a Haiku subagent reads one charter's expected
   outcome + its evidence and returns pass/fail/blocked + observations. It judges
   over concrete artifacts; it never free-drives the browser.
3. **Triage (Opus)** — dedupe, drop false positives, assign severity, write the
   report under `reports/`.

## Layout

```
charters/   one folder per user story: spec.md (steps+expected) + scenario.mjs
harness/    capture-session.mjs, explore.mjs, run helpers, lib/
runs/       per-run evidence (gitignored)
reports/    triaged bug reports
.auth/      saved login session (gitignored — credentials-equivalent)
```

## First-time setup

```bash
# 1. Install the OS libs Playwright's chromium needs (one-time, needs sudo)
sudo node_modules/.bin/playwright install-deps chromium

# 2. Capture a logged-in session (opens a real window; log in + 2FA by hand)
node qa/harness/capture-session.mjs

# 3. Verify the session reaches the dashboard + do recon
node qa/harness/explore.mjs / dashboard-root
```

When a run reports "bounced to login", the session expired — re-run step 2.

## Environment

- `IRT_QA_URL` — override the target base URL (default `https://dev.resilience.org.in`).

## Auth scope note

The saved-session approach covers the dashboard (US 09–17) and post-login
visitor stories (US 05–08) plus the public landing page (US 01). The auth
*flows* themselves (US 02–04: sign-in/2FA, signup+email-verify, password reset)
need an email inbox and are handled semi-manually, out of the autonomous path.
