# IRT Data Analysis Assistant — Recommended Architecture

Status: Proposed architecture; no runtime implementation yet.

## 1. Objective

Build a conversational assistant that can answer questions about the data serving
the India Resilience Tool dashboard and execute governed, reproducible analyses
against the `processed_optimised/` runtime bundle.

The assistant should support questions such as:

- Which districts have the highest heat risk in Gujarat under SSP5-8.5 for
  2040-2060?
- How does a district's drought risk change between two periods?
- Which persisted drivers explain a composite risk score?
- How consistent are the climate models for a selected metric and geography?
- How does a portfolio of districts compare with its state or with India?

The assistant must preserve the dashboard's metric definitions, ranking
directionality, scenario-period contracts, geography identifiers, missing-data
behavior, and persisted composite methodology.

## 2. Architectural principles

1. **Use tool calls for data analysis.** The language model interprets intent and
   explains results; deterministic Python functions perform data access and
   calculations.
2. **Treat `processed_optimised/` as read-only.** Analysis outputs belong in a
   separate temporary or export location.
3. **Use existing registries as the semantic authority.** Metric units,
   directionality, supported dimensions, and aliases must not be inferred from
   filenames or invented by the model.
4. **Separate dashboard parity from exploratory analysis.** Persisted scores,
   ranks, bands, drivers, attributes, and distributions are official dashboard
   results. Newly calculated trends, correlations, or summaries must be labelled
   as assistant analyses.
5. **Make every numerical answer reproducible.** Responses should report filters,
   units, comparison scope, source artifacts, bundle version, missingness, and
   calculation method.
6. **Fail safely on ambiguity or insufficient data.** The assistant should ask for
   missing analytical dimensions or return a clear insufficient-data result.

## 3. High-level architecture

```text
User question
    |
    v
Conversation and orchestration layer
    |
    v
Metric, geography, and dimension resolver
    |
    v
Validated analysis request
    |
    v
Read-only analytical tools
    |
    v
processed_optimised Parquet / GeoJSON artifacts
    |
    v
Structured result + provenance
    |
    v
Narrative answer + table/chart/map + export
```

The model must not directly construct arbitrary filesystem paths, execute shell
commands, or run unrestricted generated Python. It selects from a bounded tool
catalogue whose inputs are validated before execution.

## 4. Existing data contracts to reuse

The optimized runtime bundle is resolved through
`india_resilience_tool.data.optimized_bundle` and normally lives at:

```text
IRT_DATA_DIR/processed_optimised/
```

The assistant should understand these artifact families:

| Artifact family | Purpose | Typical analytical use |
|---|---|---|
| `bundle_manifest.json` | Bundle version and available Glance bundles | Catalogue bootstrapping and provenance |
| `metrics/<slug>/masters/` | Period-level district and block values | Rankings, comparisons, summaries |
| `metrics/<slug>/state_values/` | Precomputed area-weighted state values | State and national comparisons |
| `metrics/<slug>/yearly_ensemble/` | Ensemble yearly series | Trend and temporal analyses |
| `metrics/<slug>/yearly_models/` | Per-model yearly series | Model spread and agreement |
| `context/glance/` | Persisted dashboard view models | Dashboard-parity answers |
| `geometry/admin/` | Optimized state, district, and block geometry | Maps and spatial selection |
| `context/` | Exposure, hydrology, overlays, and related context | Contextual explanations and maps |

Canonical admin master identifiers must remain:

- District: `state`, `district`
- Block: `state`, `district`, `block`

Canonical master metric columns follow:

```text
{metric}__{scenario}__{period}__{statistic}
```

The metric registry is the authority for:

- slug, label, aliases, and description
- source type and selection mode
- units and display scale
- whether higher values mean worse outcomes
- fixed or supported scenarios and periods
- supported statistics
- baseline and scenario comparison support
- supported spatial families, levels, and states
- yearly-trend availability

## 5. Proposed package layout

The implementation can grow into the following structure:

```text
assistant/
├── ARCHITECTURE.md
├── __init__.py
├── catalog.py
├── contracts.py
├── resolver.py
├── provenance.py
├── service.py
├── tools/
│   ├── __init__.py
│   ├── describe.py
│   ├── values.py
│   ├── rankings.py
│   ├── comparisons.py
│   ├── trends.py
│   ├── drivers.py
│   ├── uncertainty.py
│   ├── portfolio.py
│   ├── quality.py
│   └── exports.py
└── ui/
    ├── __init__.py
    └── streamlit_chat.py
```

This is a target layout, not a requirement to create every module immediately.
Non-UI catalogue, resolver, and analytical logic should remain Streamlit-free.

## 6. Semantic catalogue

### 6.1 Responsibilities

Build an in-memory catalogue at application startup from:

- `bundle_manifest.json`
- `india_resilience_tool.config.metrics_registry`
- `india_resilience_tool.config.dashboard_bundles`
- `india_resilience_tool.config.composite_metrics`
- the optimized artifact paths that actually exist

The catalogue should expose a stable API such as:

```python
search_metrics(query: str) -> list[MetricSummary]
describe_metric(slug: str) -> MetricDescription
list_available_dimensions(slug: str, level: str) -> AvailableDimensions
list_available_geographies(slug: str, level: str) -> list[GeographyRef]
describe_bundle(slug: str) -> BundleDescription
```

### 6.2 Catalogue records

Each metric record should include:

- canonical slug and label
- searchable aliases
- description
- raw and display units
- display scale
- ranking directionality
- source type
- snapshot or scenario-period selection mode
- supported geography levels
- scenarios, periods, and statistics found on disk
- yearly ensemble and yearly model availability
- composite or dashboard bundle membership
- methodology parameters and baseline years where applicable
- artifact paths represented as internal references, not user-supplied strings
- artifact modification time or bundle build identifier

The catalogue should distinguish configured availability from observed on-disk
availability and report discrepancies.

## 7. Request contracts and resolution

The model should produce a typed request rather than code or SQL. For example:

```json
{
  "analysis": "rank",
  "metric": "composite_heat_risk",
  "level": "district",
  "states": ["Gujarat"],
  "scenario": "ssp585",
  "period": "2040-2060",
  "statistic": "mean",
  "order": "worst_first",
  "limit": 10
}
```

The resolver must:

1. Map labels and aliases to one canonical metric slug.
2. Resolve geography names to deterministic canonical keys.
3. Validate level, scenario, period, and statistic against observed availability.
4. Apply a default only when the product contract defines one.
5. Reject incompatible combinations such as requesting a yearly trend for a
   static snapshot.
6. Return a clarification requirement when more than one interpretation remains.
7. Resolve artifact paths internally through optimized bundle helpers.

Recommended request models include:

- `MetricSelection`
- `GeographySelection`
- `ComparisonScope`
- `AnalysisRequest`
- `AnalysisResult`
- `ProvenanceRecord`

## 8. Governed analytical tools

### 8.1 MVP tools

#### `describe_data`

Describe a metric, composite, bundle, geography level, scenario, period, units,
methodology, and data availability without calculating new values.

#### `get_metric_values`

Retrieve persisted metric values for a validated geography and selection.
Enforce column projection, geography filters, and row limits.

#### `rank_geographies`

Rank districts, blocks, or states using registered higher-is-worse directionality
and existing tie behavior. Return the comparison population and finite-value count.

#### `compare_geographies`

Compare selected units across one or more compatible metrics. Do not combine
incompatible units into a synthetic score unless a registered methodology exists.

#### `compare_scenarios_periods`

Calculate absolute and, when meaningful, percentage differences between two
scenario-period selections. Report the reference selection explicitly.

#### `analyze_trend`

Use yearly ensemble data to calculate period means, first-to-last change, slope
per decade, extremes, and coverage. Define the year window and NaN behavior.

#### `explain_risk_drivers`

Read persisted Glance `drivers.parquet` rows. Do not recompute or reinterpret
official composite weights at runtime.

### 8.2 Later tools

#### `analyze_model_spread`

Use per-model yearly data to report quantiles, range, model count, agreement on
change direction, and models with incomplete coverage.

#### `benchmark_geography`

Report rank, percentile, risk class, state mean difference, national comparison,
and a clearly defined peer-group comparison.

#### `analyze_portfolio`

Summarize a user-selected district/block portfolio. Area-weighting or population-
weighting must be explicit; an unweighted mean must be labelled as such.

#### `check_data_quality`

Report missingness, finite row counts, invalid dimensions, model coverage, all-NaN
selections, duplicate geography keys, and unexpected ranges where contracts exist.

#### `export_analysis`

Write CSV or Excel results outside `processed_optimised/`, including filters,
units, source artifacts, methods, warnings, and bundle version.

#### `render_analysis_map`

Join a validated result to optimized geometry using canonical keys. Geometry is
for visualization and spatial selection; metric values continue to come from
persisted tables.

## 9. Dashboard-parity and exploratory modes

### 9.1 Dashboard-parity mode

Use persisted Glance artifacts for official dashboard answers:

- `state.parquet`
- `district.parquet`
- `block.parquet` when available
- `drivers.parquet`
- `attributes.parquet`
- `distributions.parquet`

These artifacts are authoritative for dashboard scores, rankings, score bands,
drivers, attributes, and distributions. The assistant should not recalculate them
from source metrics during a conversation.

### 9.2 Exploratory mode

Use masters, yearly ensembles, and yearly model tables for new analyses such as:

- period-to-period differences
- trend slopes
- correlations
- model agreement
- portfolio summaries
- custom peer comparisons

Each result must be marked `assistant_analysis` and include its calculation method.
An exploratory result must never be described as an official dashboard score.

## 10. Conversation and orchestration layer

The conversation state should retain structured selections rather than relying
only on chat history:

- active metric or bundle
- geography level
- selected state, district, or block
- scenario, period, and statistic
- comparison population
- prior result identifier

This enables follow-ups such as:

1. "Show the ten highest-risk districts in Gujarat."
2. "Now use SSP2-4.5."
3. "Why did Ahmedabad move?"
4. "Export this comparison."

For each turn, the orchestrator should:

1. Classify the analytical intent.
2. Resolve metrics and geographies.
3. construct and validate a typed request.
4. Ask a concise clarification question if required.
5. Call one or more governed tools.
6. Validate the returned result contract.
7. Generate an explanation grounded only in tool results and semantic metadata.

## 11. Result and provenance contract

Every numerical result should carry:

- result identifier
- analysis type
- metric slug and display label
- units and display scaling
- geography level and selected units
- comparison population
- scenario, period, statistic, and temporal window
- finite, missing, and total row counts
- methodology name and parameters
- dashboard-persisted or assistant-calculated classification
- source artifact identifiers
- bundle artifact version
- warnings and limitations
- execution timestamp

Example response provenance:

```text
Source: composite_heat_risk district Glance artifact
Selection: Gujarat; SSP5-8.5; 2040-2060
Comparison: all finite Gujarat districts
Result type: persisted_dashboard_result
Bundle artifact version: 3
```

Answers should lead with the conclusion, followed by a compact table or chart and
then the provenance and caveats needed to interpret it.

## 12. Missing data, units, and methodology guardrails

### 12.1 Missing data

- Never coerce missing values to zero.
- Return an insufficient-data result when no finite values remain.
- Report partial coverage when some values are missing.
- State whether aggregations exclude NaNs or require a minimum coverage threshold.
- Preserve dashboard-specific coverage gates for persisted composite results.

### 12.2 Units

- Display units with every metric result.
- Apply registry display scaling exactly once.
- Do not calculate percentage change when zero or categorical reference values make
  the result misleading.
- Do not aggregate metrics with incompatible units unless using a registered score.

### 12.3 Spatial aggregation

- Use existing precomputed state values for dashboard-parity state comparisons.
- State whether a new aggregation is area-weighted, population-weighted, or
  unweighted.
- Keep district and block comparison populations separate unless the method
  explicitly reconciles them.

### 12.4 Temporal and scenario comparisons

- Report baseline and projection periods explicitly.
- Do not offer scenario comparisons for metrics that disable them.
- Distinguish static snapshots from climate scenarios.
- For yearly trends, report year coverage and gaps.

### 12.5 Composite methodology

- Never silently change thresholds, weights, normalization, baselines, or ranking
  directionality.
- Use persisted Glance values when answering dashboard score questions.
- Label any hypothetical or sensitivity calculation as non-dashboard analysis.

## 13. Security and execution boundaries

The normal assistant runtime should enforce:

- read-only access to the resolved optimized bundle root
- no general shell access
- no unrestricted generated Python execution
- no arbitrary user-provided file paths
- whitelisted tools and aggregations
- validated metric, geography, scenario, period, and statistic identifiers
- column projection and predicate pushdown for Parquet reads
- row, memory, execution-time, and export-size limits
- output writes only to a dedicated temporary/export root
- audit logs containing request, resolved selection, artifacts read, tool version,
  duration, and warnings
- redaction of internal absolute paths from normal user-facing responses

If notebook-style arbitrary analysis is introduced later, it should run in a
separate sandbox with explicit user authorization and resource limits.

## 14. Performance and caching

- Read only the required state partitions and columns.
- Use Parquet filters for geography, scenario, model, and year where supported.
- Cache the semantic catalogue using the bundle manifest and artifact modification
  signatures as invalidation keys.
- Cache small immutable lookup tables and Glance artifacts.
- Bound conversational result tables and offer exports for larger results.
- Keep heavy computation out of Streamlit reruns.
- Run long exploratory analyses through a job interface with progress and
  cancellation rather than blocking the chat request.

## 15. Testing and evaluation

### 15.1 Unit tests

Test:

- metric and alias resolution
- geography normalization and ambiguity
- request validation
- higher-is-worse and lower-is-worse ranking
- ranking ties
- snapshot versus scenario-period validation
- empty, all-NaN, and partial inputs
- yearly gaps and single-year series
- model-count and agreement calculations
- provenance completeness
- path containment and resource limits

### 15.2 Dashboard-parity tests

For fixed Glance selections, assert that assistant outputs match persisted:

- scores
- ranks and counts
- score bands
- drivers
- attributes
- distributions

Cover district and block artifacts, including block-scoped driver fallback behavior
where applicable.

### 15.3 Golden conversation evaluations

Maintain representative questions covering:

- descriptive metadata
- top/bottom rankings
- geography comparisons
- scenario and period comparisons
- trend analysis
- model uncertainty
- composite drivers
- portfolio summaries
- unsupported combinations
- ambiguous place names
- missing data
- prompt-injection attempts seeking other filesystem paths or arbitrary execution

Evaluation should check numerical correctness, correct tool selection, complete
provenance, appropriate caveats, and absence of unsupported claims.

## 16. Observability

Record structured events for:

- user request identifier
- selected tool and validated arguments
- catalogue and bundle version
- artifacts read
- rows scanned and returned
- execution duration
- cache hits
- warnings and errors
- user-visible result identifier

Do not log sensitive conversational content beyond the project's retention policy.
Logs must be sufficient to reproduce how a numerical answer was produced.

## 17. Delivery sequence

### Phase 1 — Foundation

- Create typed request, result, and provenance contracts.
- Build the semantic catalogue.
- Implement metric and geography resolution.
- Add safe optimized-bundle readers.

### Phase 2 — Minimum viable assistant

- Implement `describe_data`, `get_metric_values`, `rank_geographies`,
  `compare_geographies`, `analyze_trend`, and `explain_risk_drivers`.
- Add a tool-calling conversation service.
- Add a separate Streamlit chat page or panel.
- Include provenance and CSV export in every analytical response.
- Add dashboard-parity and core safety tests.

### Phase 3 — Analytical depth

- Add scenario-period comparison and benchmarking.
- Add per-model uncertainty and agreement.
- Add portfolio analysis and data-quality diagnostics.
- Add charts and maps.

### Phase 4 — Production hardening

- Add job execution for longer analyses.
- Add caching, cancellation, limits, and structured audit logs.
- Expand golden evaluations and adversarial tests.
- Add deployment monitoring and user feedback capture.

## 18. MVP acceptance criteria

The MVP is ready when it can:

1. Resolve natural-language metric and geography references deterministically.
2. Answer at least one metadata, ranking, comparison, trend, and driver question.
3. Match persisted dashboard outputs for a fixed Glance test matrix.
4. Explain units, scenario, period, statistic, geography scope, and missingness.
5. Distinguish dashboard results from new assistant analyses.
6. Export the result and its provenance.
7. Reject arbitrary paths, shell requests, and unsupported analytical dimensions.
8. Return useful insufficient-data responses for empty and all-NaN inputs.

## 19. Explicitly out of scope for the MVP

- Rebuilding `processed_optimised/` or its source pipelines
- Modifying dashboard metric methodology
- General shell access
- Unrestricted Python or SQL generated by the model
- Autonomous writes to repository or data artifacts
- Treating vector retrieval as the numerical query engine
- Claiming causal relationships from correlations
- Creating new official risk scores without an approved methodology change

Document retrieval may later support questions about methodology and guidance, but
structured data tools remain the authority for numerical answers.

## 20. Separation from the CRAVIS case-study evaluator

`assistant/evaluations/cravis/` is an external-product interaction and evaluation
harness, not an implementation component of this proposed IRT assistant. It has
its own Node/Playwright CLI, prompt oracles, human scoring lock, raw-evidence
boundary, and reporting contracts. The evaluator must not import planned IRT
assistant runtime modules, and the future IRT assistant must not depend on CRAVIS
session state, selectors, campaign ledgers, evidence, or scores.

The evaluator's capability mapping may inform roadmap decisions, but an observed
CRAVIS behavior does not change IRT metric definitions or prove a suitable IRT
methodology. In particular, CRAVIS-native RCP 4.5/8.5 outputs are not equivalent
to IRT SSP contracts without documented source support. Legitimate scenario,
source, baseline, aggregation, or uncertainty differences should be recorded as
`methodology_conflict` rather than silently copied or treated automatically as an
IRT capability gap. The case study is `n=1` and is not a general performance
estimate.
