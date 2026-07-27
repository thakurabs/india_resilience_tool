# Exhaustive Vendor Resilience Filter Coverage Plan

## Summary

Build a bounded, resumable QA coverage system for `dev.resilience.org.in` that validates every vendor-exposed resilience-filter cascade combination across all canonical states at `District` and `Block` levels.

Coverage unit:

`state x admin_level x risk_domain x metric x scenario x period x statistic x map_mode`

Primary outputs:
- raw discovery, attempt, and observation matrices
- bounded run plan summary
- deterministic deduplicated defect ledger
- reproducible defect report with affected combinations

Important source-of-truth rule:
- The filter universe is vendor-exposed UI options.
- `OPTION_MISSING` applies only when a required control/stage is absent, expected baseline controls such as state/level selectors are missing, or a separate expected filter catalog is supplied later. A vendor filter option cannot be called missing merely because it was not discovered in an exposed-options-only run.

## Key Implementation Changes

### Runner Structure

Add a coverage harness under `qa/harness/` with reusable pure helpers under `qa/harness/lib/coverage/`:
- roster extraction and field alias resolution
- JSONL/CSV writers
- cascade dropdown discovery
- stable observation keys and resume logic
- per-observation network attribution
- retry/attempt recording
- defect classification and deterministic deduplication
- Markdown/CSV/JSON report generation

Main commands:
- `node qa/harness/data-coverage-runner.mjs --check-selectors`
- `node qa/harness/data-coverage-runner.mjs --dry-run`
- `node qa/harness/data-coverage-runner.mjs --pilot`
- `node qa/harness/data-coverage-runner.mjs --discover-only`
- `node qa/harness/data-coverage-runner.mjs --estimate-only`
- `node qa/harness/data-coverage-runner.mjs --shard 1/10 --max-units 500`
- `node qa/harness/data-coverage-runner.mjs --only-failed --run-dir qa/runs/<id>_data-coverage`
- `node qa/harness/data-coverage-triage.mjs --run-dir qa/runs/<id>_data-coverage`

### Phase 0 - Preflight, Metadata, And Selector Audit

- Verify saved auth reaches dashboard and pause immediately if login bounce is detected.
- Dismiss visitor guide and feedback overlays using existing safe patterns.
- Validate local canonical rosters exist under `IRT_DATA_DIR`.
- `--check-selectors` verifies required controls without running the cascade:
  - state selector
  - District/Block level controls
  - resilience filter panel
  - map/ranking view controls
  - profile panel controls
- Write:
  - `run_metadata.json`
  - `selector_preflight.json`
- `run_metadata.json` captures:
  - target URL
  - timestamp
  - auth state mtime
  - browser version
  - user agent
  - viewport
  - git branch/short SHA
  - dirty status summary
  - app title
  - visible app version/build hash if present

### Phase 1 - Local Expected Roster

- Read:
  - `districts_4326.geojson`
  - `blocks_4326.geojson`
- Resolve identity fields via alias maps, failing preflight clearly if required fields cannot be found.
- Required logical fields:
  - district: state, district, district code if present
  - block: state, district, block, block code/key if present
- District expected count is all canonical districts in the selected state.
- Block expected count is all canonical blocks in the selected state across all districts, not only the representative district.
- Do not rely on WGS84 geometry area for coverage decisions. Use existing `area_km2` if present; if area is needed and absent, compute using an equal-area CRS in a later enhancement.
- Write:
  - `expected_states.csv`
  - `expected_districts.csv`
  - `expected_blocks.csv`
  - `expected_counts_by_state_level.csv`

### Phase 2 - Deterministic Cascade Discovery

- For each state and level, enumerate cascade stages:
  - `risk_domain`
  - `metric`
  - `scenario`
  - `period`
  - `statistic`
  - `map_mode`
- Discovery records missing and disabled required controls/stages separately from absent vendor options.
- Dropdown introspection captures:
  - visible label
  - normalized label
  - disabled state
  - cascade stage
  - selected prior path
  - timeout or error reason
- Write incrementally:
  - `filter_universe.jsonl`
  - `filter_universe.csv`
- Stable key format:
  - `state_slug|level|risk_domain|metric|scenario|period|statistic|map_mode`
- Discovery must be resumable and idempotent.
- CSV files contain compact summaries only; full labels, errors, API details, and evidence paths stay in JSONL/JSON.

### Phase 2.5 - Scope Estimation And Sharding

- Before any exhaustive probe, write:
  - `coverage_plan_summary.csv`
  - `coverage_plan_summary.json`
- Summary groups counts by:
  - `state_name`
  - `admin_level`
  - `risk_domain`
  - `metric`
- Full execution requires one of:
  - `--max-units <N>`
  - `--shard N/M`
  - `--confirm-large-run`
- Support:
  - `--state-file`
  - `--states`
  - `--levels district,block`
  - `--resume`
  - `--run-dir`
  - `--delay-ms`
  - `--max-defects`
  - `--fail-fast-domain`
  - `--only-failed`

### Phase 3 - Pilot And Exhaustive Coverage Probe

- Pilot scope:
  - `Telangana x District`
  - `Telangana x Block`
  - one full exposed `Thematic - Heat Risk` cascade
- Pilot must prove discovery, full selection, map check, ranking check, profile check, network attribution, raw attempt writing, terminal observation writing, and defect generation.

For every selected universe row:
- select full cascade
- reset per-observation event/network bucket
- validate map surface
- validate ranking surface
- validate representative profile surface
- retry transient failures according to policy
- write one terminal observation row after retries

Retry behavior:
- each attempt is written to `coverage_attempts.jsonl`
- only the final retry writes the terminal `coverage_observations.jsonl` row
- attempt details are preserved in `attempts_json` or by attempt IDs linked from the terminal observation

Relevant network calls:
- include vendor app/API calls matching `/api/`, `/parquet/`, `/ranking`, `/map`, `/trend`, `/scenario-comparison`, profile/table/chart endpoints
- exclude analytics, fonts, CDN assets, favicon, telemetry, static JS/CSS, and benign aborted range requests
- attach calls only to the current observation window

Execution controls:
- default concurrency: `3`
- default retry count: `2`
- default delay: `500ms` between observation starts per worker
- exponential backoff for `429`, `503`, network timeouts, or auth/session instability
- auth bounce produces `HARNESS_BLOCKED` with `blocked_reason=AUTH_BOUNCE`, not a data defect
- pause run on auth bounce; mark shard invalid if auth bounce happens mid-shard before clean pause

Write:
- `coverage_attempts.jsonl`
- `coverage_observations.jsonl`
- `coverage_observations.csv`

Each observation includes:
- stable key
- full cascade labels
- expected local count
- map status
- ranking status
- profile status
- API status summary
- visible error summary
- observed count source
- retry count
- terminal status: `pass`, `fail`, `needs_triage`, or `blocked`
- blocked reason if applicable
- linked attempt IDs
- evidence path

### Phase 3.5 - Observation Integrity Audit

After probing, write `coverage_run_audit.json` verifying:
- every selected universe row has exactly one terminal observation
- no duplicate observation keys
- retry policy was honored
- skipped rows have explicit reasons
- auth bounce did not invalidate a shard
- failures are not contaminated by prior-observation network events
- every terminal observation links to its attempts

### Phase 4 - Coverage Semantics

Classify absence and partial data carefully:
- `LEVEL_UNAVAILABLE`: vendor does not expose the state/level control or cannot select that level
- `OPTION_MISSING`: required control/stage or separately expected catalog label is absent
- `OPTION_DISABLED`: option exists but cannot be selected
- `EXPOSED_BUT_EMPTY`: full cascade is selectable but map/ranking/profile has no usable data
- `PARTIAL_COVERAGE`: exposed combination returns fewer units than canonical roster

Ranking counts:
- prefer API payload length
- then visible total count text
- then export/download row count if available
- then accessibility/table model
- DOM visible row count alone is not sufficient because ranking may be virtualized
- if no reliable count source exists, classify count coverage as `needs_triage`

Representative profile unit:
- prefer first alphabetic valid unit from ranking/API rows
- otherwise fall back to canonical roster
- record the selection source as `vendor_observed` or `canonical_fallback`

Map empty/all-null evidence:
- require measurable signal such as API payload zero rows, API values all null, visible no-data state, visible NaN scan, tooltip all null, or map layer response zero features
- do not rely on canvas/WebGL pixels alone

### Phase 5 - Defect Classification And Deduplication

Classify failures into:
- `LEVEL_UNAVAILABLE`
- `OPTION_MISSING`
- `OPTION_DISABLED`
- `SELECTION_FAILED`
- `MAP_API_ERROR`
- `MAP_EMPTY`
- `MAP_ALL_NULL`
- `RANKING_API_ERROR`
- `RANKING_EMPTY`
- `PARTIAL_COVERAGE`
- `PROFILE_API_ERROR`
- `PROFILE_EMPTY`
- `CHART_MISSING`
- `TABLE_MISSING`
- `VISIBLE_ERROR`
- `HARNESS_BLOCKED`

Deduplicate by:
- category
- endpoint/status/error text
- admin level
- risk domain
- metric
- affected state set
- cascade stage

Each defect gets:
- sequential display ID
- deterministic `defect_signature_hash` from the dedupe signature

Write:
- `defects.json`
- `defects.csv`
- `affected_combinations.csv`
- `qa/reports/DATA_COVERAGE_DEFECTS.md`

Each defect includes:
- defect ID
- signature hash
- severity draft
- affected states
- affected levels
- affected filters
- expected vs observed count
- endpoint/status/error text
- exact reproduction steps
- evidence path
- likely root-cause hint

## Test Plan

Add pure-function tests for:
- roster field alias resolution
- missing required roster fields
- cascade matrix expansion
- stable key generation
- JSONL resume/idempotency
- endpoint include/exclude attribution
- retry-to-terminal observation behavior
- auth bounce classification
- observation integrity audit
- defect signature hashing
- defect classification
- defect deduplication
- Markdown/CSV report generation

Manual acceptance:
- selector preflight writes `selector_preflight.json`
- pilot completes for Telangana District and Block
- discovery writes non-empty `filter_universe.csv`
- estimation writes `coverage_plan_summary.csv`
- exhaustive run refuses to start without `--max-units`, `--shard`, or `--confirm-large-run`
- interrupted run resumes without duplicate observations
- every terminal observation links to preserved attempts
- every defect has reproduction steps, affected combinations, and a stable signature hash
- no single-attempt flaky failure is reported as a defect unless retry policy confirms it

## Assumptions And Defaults

- Target app is `https://dev.resilience.org.in`.
- Local canonical boundaries are the expected state/district/block roster.
- "All combinations" means all cascade options exposed by the vendor UI.
- Missing filter options are defects only when required controls/stages are absent or an explicit expected filter catalog is supplied.
- Portfolio comparison is not part of the per-filter exhaustive matrix.
- Profile validation uses one representative unit per state/level/filter combination.
- Default execution is polite parallelism: concurrency `3`, retries `2`, delay `500ms`.
- Human-facing deliverable is the defect list, but raw matrices are retained for audit and reproduction.
