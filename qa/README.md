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
- `QA_SOFTWARE_GL` — set to `1` to launch chromium with software GL (SwiftShader:
  `--use-gl=angle --use-angle=swiftshader --enable-unsafe-swiftshader --ignore-gpu-blocklist`).
  Needed for map probes that rely on deck.gl WebGL hit-testing headless (e.g. the
  map-interactivity probe). Default (unset) launches with no extra GL args.

### Data coverage Phase 0 preflight

```bash
node qa/harness/data-coverage-runner.mjs --check-selectors
node qa/harness/data-coverage-runner.mjs --dry-run
node qa/harness/data-coverage-runner.mjs --discover-only --states Telangana --levels district,block
node qa/harness/data-coverage-runner.mjs --estimate-only --run-dir qa/runs/<id>_data-coverage
node qa/harness/data-coverage-runner.mjs --pilot --run-dir qa/runs/<id>_data-coverage --max-units 1
```

`--check-selectors` verifies saved auth reaches the dashboard, dismisses
visitor-guide and feedback overlays, checks canonical `IRT_DATA_DIR` roster
files, and writes `run_metadata.json` plus `selector_preflight.json` under
`qa/runs/`.

`--dry-run` builds the Phase 1 local expected roster CSVs from
`districts_4326.geojson` and `blocks_4326.geojson` without probing the vendor
app.

`--discover-only` runs Phase 2 exposed-option cascade discovery for the selected
states/levels and writes `filter_universe.jsonl`, `filter_universe.csv`, and
`filter_universe_summary.json`.

`--estimate-only` reads an existing discovered `filter_universe.csv` and writes
`coverage_plan_summary.csv` plus `coverage_plan_summary.json`. Add
`--max-units`, `--shard N/M`, or `--confirm-large-run` to record the safety gate
that a later probe run will require.

`--pilot` currently runs Phase 3A scaffolding only: it replays selected cascade
rows from `filter_universe.csv` and writes `coverage_attempts.jsonl`,
`coverage_observations.jsonl`, and `coverage_observations.csv`. Map, ranking,
profile, and network assertions are deferred to Phase 3B.

### Map-interactivity probe (dropdown-gating + portfolio commutativity)

```bash
node --check qa/harness/add-to-analysis-map-interactivity.mjs   # syntax check first
node qa/harness/capture-session.mjs                             # refresh session if >~24h old
QA_SOFTWARE_GL=1 node qa/harness/add-to-analysis-map-interactivity.mjs
```

The probe emits **no Claim verdict unless three hard gates pass** (`stateNormalized`,
`interactionCalibrated`, `mapPopoverScopedAdd`); otherwise it records `BLOCKED (<gate>)`.
Evidence lands in `runs/<id>_us-map-interactivity/` with a machine-generated
`automated-summary.md` (not a vendor report — the triaged report is authored by hand). If
calibration surfaces nothing even with `QA_SOFTWARE_GL=1`, deck.gl likely needs a real GL
context — fall back to a headed / `xvfb-run` invocation.

## Auth scope note

The saved-session approach covers the dashboard (US 09–17) and post-login
visitor stories (US 05–08) plus the public landing page (US 01). The auth
*flows* themselves (US 02–04: sign-in/2FA, signup+email-verify, password reset)
need an email inbox and are handled semi-manually, out of the autonomous path.
