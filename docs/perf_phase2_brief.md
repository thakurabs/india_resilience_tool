# Performance Phase 2 brief — ADM1-first boot

Persistent handoff for the IRT dashboard speed audit. Self-contained: any session
can read this file alone and resume Phase 2 work without rebuilding context.

Author: Abu Bakar Siddiqui Thakur (work driven by user, applied by Claude Code)
Created on: 2026-05-13
Branch at creation: `add_flood_depth@7fb2ff7`

---

## 1. Status as of this brief

Phases 0 and 1 are **applied** (working tree dirty, uncommitted) and validated:
the targeted test files pass; the 8 full-suite failures are pre-existing on
`add_flood_depth@7fb2ff7` (verified by stashing and re-running). The user is
holding the work uncommitted until Phase 2 lands so they can ship in one commit
or stop after Phase 2 and ship together.

CHG ledger at this point:

| CHG | Files | Status |
|-----|-------|--------|
| CHG-0004 | `india_resilience_tool/app/map_layer_runtime.py` | APPLIED — removed `_folium_base_map_cache` and `copy.deepcopy` plumbing |
| CHG-0005 (Option A) | `india_resilience_tool/app/map_layer_runtime.py`, `viz/folium_featurecollection.py`, `app/map_pipeline.py` | APPLIED — dropped `_patched_fc_cache`, `props_map_signature`, `_build_map_render_signature`; cleaned dead params (`session_state`, `render_signature`, `overlay_cache_signature`) |
| CHG-0007 | `india_resilience_tool/app/views/map_view.py` | APPLIED — both `st_folium(...)` branches now use `use_container_width=True` |
| CHG-0008 | `india_resilience_tool/app/perf.py`, `runtime.py` | APPLIED in reduced form — moved perf checkbox out of Developer expander; added cold-load `perf_section("cold: ...")` wrappers around ADM2/ADM3/basin/subbasin load points. The over-map "Total ms" caption was reverted as not useful and slightly misleading. |
| CHG-0013 | `tests/test_app_map_layer_runtime.py`, `tests/test_viz_folium_featurecollection.py`, `tests/test_app_map_pipeline.py` | APPLIED — tests updated for the no-cache contract |

Phase 2 (this brief) is the next block of work.

---

## 2. Why Phase 2 matters

The single largest remaining slowness is **first-paint cold load**. The
dashboard unconditionally reads `districts_4326.geojson` (267 MB on disk; 820
features) before the geography selector is rendered, then dissolves it to ADM1.
That costs ~10–15 s on a fresh `streamlit run` and feeds the user-visible
"painfully long time to load the map." Optimized per-state shards already exist
on disk (`processed_optimised/geometry/admin/district/state=*.geojson`, ~0.5–2.4
MB each) but the boot path can't use them because the state selection itself
depends on the ADM2 already being loaded.

Phase 2 inverts the dependency: load a tiny precomputed ADM1 artifact at boot,
render the geography selector from it, defer ADM2 to "after a state is chosen
AND the map or details actually need it."

Expected result: cold paint of Telangana drops from ~15 s to ~2 s.

---

## 3. Key file:line anchors

These were the precise spots where the boot path currently forces the cold
load. Re-verify before editing — line numbers drift.

### Landing path (runtime.py)

- `india_resilience_tool/app/runtime.py:166-196` — `if bool(st.session_state.get("landing_active", False)):` branch.
  - **L173** (currently wrapped in `with perf_section("cold: load adm2 [landing]"):`): `adm2 = load_local_adm2(str(ADM2_GEOJSON), tolerance=SIMPLIFY_TOL_ADM2)`
  - **L177** (wrapped in `with perf_section("cold: build adm1 [landing]"):`): `adm1 = build_adm1_from_adm2(adm2)`
  - **L179** (wrapped in `with perf_section("cold: enrich adm2 state names [landing]"):`): `adm2 = enrich_adm2_with_state_names(adm2, adm1)`
  - L180-188: `adm3_by_district = build_adm3_geojson_by_district(...)` (also wrapped)
  - L190-195: `render_landing_page(adm1=adm1, adm2=adm2, adm3_by_district=adm3_by_district, data_dir=DATA_DIR)`

### Detail path (runtime.py)

- `india_resilience_tool/app/runtime.py:418-429` — base ADM2 load before the metric ribbon.
  - **L427** (wrapped in `with perf_section("cold: load adm2 [detail]"):`): `adm2 = load_local_adm2(str(ADM2_GEOJSON), tolerance=SIMPLIFY_TOL_ADM2)`
- `india_resilience_tool/app/runtime.py:492-497` — ADM1 build and ADM2 enrichment for the detail flow.
  - **L495** (wrapped in `with perf_section("cold: build adm1 [detail]"):`): `adm1 = build_adm1_from_adm2(adm2)`
  - **L497** (wrapped in `with perf_section("cold: enrich adm2 state names [detail]"):`): `adm2 = enrich_adm2_with_state_names(adm2, adm1)`
- `india_resilience_tool/app/runtime.py:505-509` — `render_geography_and_analysis_focus(...)` call site; takes `adm2` and `adm1` as inputs.
- `india_resilience_tool/app/runtime.py:611-621` — the existing optimized state-shard swap that already runs *after* the state is known. Phase 2 keeps the spirit of this block but moves it earlier in the flow.

### Sub-basin overlay (the narrow remaining gap)

- `india_resilience_tool/app/map_layer_runtime.py:340` — `build_subbasin_geojson_all(...)` call, hit when the crosswalk overlay's `level == "sub_basin"` and `scope_dimension != "basin_name"` (so it falls through to "all"). Scoped overlays already use `build_subbasin_geojson_by_basin` and are tested in `tests/test_app_map_layer_runtime.py::test_build_folium_map_for_selection_uses_scoped_subbasin_overlay_shards`.

### Hydro selector index (already present, lightly used)

- `india_resilience_tool/app/geo_cache.py:262` — `load_hydro_subbasin_selector_index(path)` already exists and reads
  `processed_optimised/context/hydro_subbasin_index.parquet`, returning:
  ```
  {
    "basin_names": [...],
    "subbasins_all": [...],
    "subbasins_by_basin": {basin_name: [subbasin_name, ...]},
    "basin_ids_by_name": {basin_name: basin_id, ...},
  }
  ```
- Currently consumed only at `runtime.py:646-650` to map a selected basin name to its `basin_id` for the sub-basin optimized shard. Phase 2 extends this consumer side.

### Optimized path helpers (already in place)

- `india_resilience_tool/data/optimized_bundle.py:171-197` — `optimized_geometry_path(*, level, state=None, basin_id=None, data_dir=None)`. Levels: `"district"`, `"block"`, `"basin"`, `"sub_basin"`. District/block require `state`. Sub-basin requires `basin_id`. Basin returns `processed_optimised/geometry/hydro/basin.geojson` (a single file).
- `india_resilience_tool/data/optimized_bundle.py:200-202` — `optimized_context_path(name, *, data_dir=None)`.

### ADM2 helpers Phase 2 must mirror or bypass

- `india_resilience_tool/data/adm2_loader.py:215` — `build_adm1_from_adm2(adm2_gdf, *, state_col="state_name") -> gpd.GeoDataFrame`. Returns an ADM1 GDF with columns: `state_name` and `shapeName` (mirror of state_name). EPSG:4326.
- `india_resilience_tool/data/adm2_loader.py:228` — `enrich_adm2_with_state_names(adm2_gdf, adm1_gdf, *, state_col="state_name", adm1_name_col="shapeName")`. Spatial joins centroids to ADM1 to backfill missing state names; downstream code relies on `state_name` being populated.

---

## 4. Phase 2 CHG sequence (execute in this order)

Each step is one commit. Do not interleave. Each has a clear rollback point.

### Step 2a — CHG-0002 — Offline build of `adm1.geojson` artifact

**Why first:** every subsequent step assumes the artifact exists. Without it,
the runtime change can't shed the monolith.

**New file:** `tools/geodata/build_adm1_geojson.py`

Behavior:
- Read `districts_4326.geojson` from `IRT_DATA_DIR` (use `paths.DISTRICTS_PATH`).
- Apply the same ADM2 prep `load_local_adm2` does (CRS→4326, drop Z, simplify, bbox crop, min_area filter). Reuse `simplify_and_filter(...)` from `adm2_loader.py` for parity.
- Dissolve by `state_name` (mirror `build_adm1_from_adm2`).
- Simplify ADM1 at `SIMPLIFY_TOL_ADM1` (already 0.015 in `config/constants.py`).
- Write to `processed_optimised/geometry/admin/adm1.geojson`.
- Include `state_name` and `shapeName` columns (downstream contract from `build_adm1_from_adm2`).
- CLI: `--overwrite`, `--dry-run`, `--out`. Mirror style of `tools/geodata/build_blocks_geojson.py` for conventions.

**Edit:** `india_resilience_tool/data/optimized_bundle.py` — add a path helper:
```python
def optimized_adm1_path(data_dir: Optional[Path] = None) -> Path:
    return (
        resolve_optimized_bundle_root(data_dir=data_dir)
        / _GEOMETRY_DIRNAME / "admin" / "adm1.geojson"
    )
```

**Edit:** `tools/optimized/build_processed_optimised.py` — call the new builder
in the geometry-shard build phase so a regular optimized rebuild produces the
artifact. Look for the existing per-state shard build loop; add an ADM1 build
step alongside it (idempotent, gated by `--overwrite`).

**Tests:** `tests/test_optimized_bundle.py` (Tier 1 — data contract):
- `test_optimized_adm1_path_resolution` — returns a path under the optimized
  root with the expected suffix.
- `test_optimized_adm1_artifact_load_returns_state_polygons` — given a written
  artifact (small fixture, 2 states), loading it returns a GDF with `shapeName`
  and `state_name`, EPSG:4326, one row per state.

**Execution:** after wiring is in, run the builder once locally:
```bash
python -m tools.geodata.build_adm1_geojson --overwrite
```
The user pre-approved this in the prior conversation. Verify output file size
< 5 MB and feature count equals the number of distinct `state_name` values in
`districts_4326.geojson` (35 for India incl. UTs).

**Rollback:** delete the new module, the path helper, the wiring line, and the
artifact file. No production code touches it yet.

---

### Step 2b — CHG-0011 — Refactor boot path (the real work)

**Why second:** with the artifact in place, swap the runtime to consume it.

**Concept:** today, `runtime.py` always loads ADM2 → builds ADM1 → enriches
ADM2 → renders state selector → if state selected, loads optimized state shard.
After Phase 2:
- Load tiny `adm1.geojson` artifact.
- Render state selector immediately.
- If `selected_state != "All"` AND (`include_map` or `details_need_geometry`),
  load only the optimized per-state ADM2 shard.
- Fall back to today's monolith path **with a visible warning** when the artifact
  is missing, so the dashboard never silently regresses.

**Edits:**

`india_resilience_tool/data/adm2_loader.py` — add a small reader:
```python
def load_local_adm1_artifact(path: PathLike) -> gpd.GeoDataFrame:
    """Read the precomputed ADM1 artifact written by build_adm1_geojson.

    Returns GDF in EPSG:4326 with columns `state_name`, `shapeName`.
    Streamlit-free; caller wraps in @st.cache_data.
    """
```

`india_resilience_tool/app/geo_cache.py` — add a Streamlit-cached wrapper next
to `load_local_adm2`:
```python
@st.cache_data(ttl=3600)
def load_local_adm1(path: str) -> gpd.GeoDataFrame:
    return _load_local_adm1_artifact(path)
```

`india_resilience_tool/app/runtime.py` — main edits:

1. **Landing branch (currently L166-196):** before falling back to the monolith,
   try the optimized ADM1 path:
   ```python
   optimized_adm1 = optimized_adm1_path(data_dir=DATA_DIR)
   if optimized_adm1.exists():
       with perf_section("cold: load adm1 artifact [landing]"):
           adm1 = load_local_adm1(str(optimized_adm1))
       # adm2 is no longer needed until a state is clicked. The landing page
       # currently takes both adm1 and adm2, so we need to either lazy-load
       # adm2 inside render_landing_page or pass None and let it lazy-load.
   else:
       # existing path: warn the operator and fall through to today's monolith load.
       st.caption("⚠ adm1.geojson artifact not found; loading the full ADM2 (slow first paint). "
                  "Run `python -m tools.geodata.build_adm1_geojson --overwrite` to fix.")
       # ... existing code
   ```
   **Audit `landing_runtime.render_landing_page` for ADM2 dependencies** —
   it likely needs ADM2 for the India overview clickable polygons. Options:
   - Load ADM2 once the user clicks into a state (drilldown).
   - Or accept the trade-off that India overview still needs ADM2 (the landing
     page lives on this view; the cold cost is unavoidable for the first paint).
   If the second option is the simpler win, keep landing-page ADM2 load as-is
   and only fix the detail flow. **User can be asked here** if the simpler
   landing scope is acceptable; the bigger win is the detail flow regardless.

2. **Detail branch (currently L418-497):** the harder refactor. Sequence:
   - At top of detail flow, load `adm1` from the artifact instead of `adm2`.
   - Pass `adm1` (only) into `render_geography_and_analysis_focus` (look at its
     signature; today it takes both `adm1` and `adm2`). Audit
     `app/geography_controls.py` for ADM2 usage in state-selection rendering.
     The state list today comes from ADM2 → enrich_adm2_with_state_names. With
     the artifact, the state list comes from ADM1 directly (`adm1["shapeName"]`).
   - After state selection, load the per-state ADM2 shard via
     `optimized_geometry_path(level="district", state=selected_state, data_dir=DATA_DIR)`
     **only when `include_map` or `details_need_geometry`**. Today this is at
     L611-621 — move it earlier and make it the single ADM2 load point.
   - Skip `build_adm1_from_adm2` and `enrich_adm2_with_state_names` entirely
     when the artifact is used (adm1 already has `state_name` + `shapeName`;
     the per-state shard already has `state_name` populated).

3. **Geography controls audit:** `app/geography_controls.py` and `app/geography.py`
   contain pre-state-selection logic. Read both end-to-end; find every ADM2
   usage that happens before state selection. Most uses are to populate the
   state dropdown — replaceable by ADM1's `shapeName` column. A few may need
   ADM2 (e.g., district auto-complete in the geography header). For those:
   either defer to after state selection, or use a small precomputed selector
   index (similar to `hydro_subbasin_index.parquet` but for admin).

**Tests:**
- Existing `tests/test_app_runtime_view.py`, `tests/test_app_geography_controls.py`,
  `tests/test_app_landing_runtime.py` need a read-through. Any test that hands
  `runtime.run_app` a synthetic ADM2 GDF will need to also hand it ADM1 (or be
  refactored to use the artifact path). Be surgical — don't churn tests that
  exercise the no-artifact fallback path.
- Add `tests/test_app_runtime_boot.py` (new) with two cases:
  1. Artifact present → ADM2 is *not* loaded during state-selection render.
  2. Artifact absent → ADM2 is loaded with a visible warning caption.

**Risk:** Medium. New boot path. The fallback keeps it safe. Most fragile
points: anything that currently relies on `adm2` being a populated GDF before
state selection. Read `landing_runtime.py`'s `render_landing_page` parameter
list carefully.

**Rollback:** revert the commit; the artifact remains harmless on disk.

---

### Step 2c — CHG-0001 — Collapse to a single ADM2 loader helper

**Why third:** after 2b lands, there are likely two ADM2 load sites still in
play (the new state-aware one + the fallback to the monolith). Collapse them
into one helper for clarity and to ensure future edits don't drift.

**Edit:** `india_resilience_tool/app/runtime.py` — add a module-level helper:
```python
def _load_adm2_for_state(
    *,
    selected_state: str,
    data_dir: Path,
    full_path: Path,
    tolerance: float,
) -> gpd.GeoDataFrame:
    """Return the smallest ADM2 GDF that satisfies the current selection.

    Prefers the optimized per-state shard when a concrete state is selected;
    falls back to the full monolith only when no shard is available.
    """
```

Use this helper at the two remaining ADM2 load points. Single source of truth.

**Tests:** `tests/test_app_runtime_view.py` — a small assertion that the helper
picks the optimized shard when state != "All" and falls back otherwise.

**Risk:** Low — pure consolidation.

---

### Step 2d — CHG-0012 — Sub-basin overlay resolver

**Why fourth:** the only remaining hot path that hits a raw monolith. Independent
of the boot work but cheap to ship alongside.

**Edit:** `india_resilience_tool/app/map_layer_runtime.py` around the existing
`overlay_level == "sub_basin"` block (currently L327-353 after Phase 1 edits).
When `crosswalk_overlay.feature_keys` are known but `scope_dimension` is not
`"basin_name"`:
1. Read `hydro_subbasin_index.parquet` via the existing
   `load_hydro_subbasin_selector_index`.
2. Map each subbasin in `feature_keys` to its parent `basin_id`.
3. For each unique `basin_id`, call `build_subbasin_geojson_by_basin(...)` with
   the optimized per-basin shard path resolved via
   `optimized_geometry_path(level="sub_basin", basin_id=basin_id, data_dir=DATA_DIR)`.
4. Union the per-basin FCs (already have `_union_featurecollections` helper at
   `map_layer_runtime.py:28`).
5. Filter to `feature_keys` via `filter_fc_by_feature_keys`.

**Avoid:** building a generic `sub_basin_all.geojson` unless there's a true
all-subbasin display case. The reviewer specifically called this out — don't
add the bundle artifact unless the resolver path can't cover the scenario.

**Tests:** `tests/test_app_map_layer_runtime.py` — add a case that exercises
the resolver, monkeypatching `load_hydro_subbasin_selector_index` and
`build_subbasin_geojson_by_basin` and asserting the right basin shards are
selected and unioned. The existing
`test_build_folium_map_for_selection_uses_scoped_subbasin_overlay_shards` is
a good pattern to mirror.

**Risk:** Low–medium. New code path with a known input contract.

**Rollback:** revert; the prior fallback behavior is unchanged.

---

### Step 2e — CHG-0014 — Documentation

**Edit:**
- `MANIFEST.md` — under "Boundary inputs expected under `IRT_DATA_DIR`" and the
  optimized bundle section, document the new `adm1.geojson` artifact and the
  runtime preference order.
- `tools/README.md` — add a line for the new builder and its CLI flags.
- `README.md` — only if the operator-facing prep instructions changed (likely
  yes, since `prepare_dashboard` should now write the artifact too).

**Tests:** none. Docs change.

---

## 5. Validation strategy

For each commit:

1. **Targeted tests:**
   ```bash
   python -m pytest -q \
     tests/test_optimized_bundle.py \
     tests/test_app_runtime_view.py \
     tests/test_app_landing_runtime.py \
     tests/test_app_geography_controls.py \
     tests/test_app_map_layer_runtime.py
   ```
2. **Full suite (excluding known Windows-matplotlib crash files):**
   ```bash
   python -m pytest -q \
     --ignore=india_resilience_tool/compute/tests \
     --ignore=tests/test_viz_charts.py \
     --ignore=tests/test_viz_exports.py \
     --ignore=tests/test_viz_trend_spaghetti.py \
     --ignore=tests/test_viz_scenario_yaxis_scaling.py
   ```

**Known pre-existing failures on this branch** (do not treat as regressions):
- `tests/test_app_ribbon.py::test_resolve_admin_master_source_for_all_falls_back_to_legacy_when_optimized_state_coverage_is_partial`
- `tests/test_bundle_scores.py::test_compute_bundle_score_frame_renormalizes_weights_for_available_metrics`
- `tests/test_legacy_dashboard_state_profile_files.py::test_state_profile_files_missing_{true_when_required_files_absent,false_when_required_files_present}` (NameError in `master_freshness.py`)
- `tests/test_metrics_registry.py::test_population_exposure_domain_is_admin_only` (test predates LULC onboarding)
- `tests/test_timeseries_optimized.py::test_load_{block,hydro}_yearly_from_optimized_metric_root` (missing `year` column)
- `tests/test_compute_indices_synthetic_comprehensive.py::TestTierGHIJPrecipitation::test_r95p_exact_bimodal`
- Plus Windows-only matplotlib/numpy fatal exceptions in viz/SPI test files.

3. **Manual smoke test (after Step 2b lands):**
   - Restart `streamlit run main.py`.
   - Enable "Show performance timings" in the sidebar.
   - Open the screenshot path: India → Telangana → district choropleth → enable
     sub-basin reference overlay → enable river overlay → block view → basin view.
   - The new perf rows should show `cold: load adm1 artifact [detail]` at <100 ms
     and `cold: load adm2 [detail]` should appear **only after** a state is
     selected, not on first paint.

---

## 6. Reviewer's specific corrections (do not lose these)

These are the corrections from the second-opinion reviewer that strengthened
the original audit. They are baked into the plan above but recorded here in
case any are dropped during execution:

1. **Both boot paths cold-load ADM2.** `runtime.py:173` (landing) AND
   `runtime.py:427` (detail). Step 2b must address both.
2. **Sub-basin scoped overlays already work.** `build_subbasin_geojson_by_basin`
   is used and tested. Only the unscoped/feature-keys path needs the resolver
   (Step 2d), not the scoped path.
3. **Folium base-map deepcopy cache had tests.** Tests were updated in CHG-0013
   to assert the no-cache contract (each call rebuilds). Don't re-introduce the
   cache without re-thinking the signature design too.
4. **Pyogrio is already the geopandas engine.** Phase 3 (if pursued) should tune
   `columns=`, `bbox=`, and `use_arrow=True` rather than swap backends.
5. **Selected state isn't available pre-render.** Today's controls require
   `adm2` to render the state selector. Phase 2 inverts this by rendering from
   ADM1 + state-name lists. Audit `geography_controls.py` carefully.
6. **CHG-0012 should resolve via `hydro_subbasin_index`, not blanket build a
   sub_basin_all.geojson.** Avoid creating the bundle artifact unless truly
   needed for an all-subbasin display.
7. **Phase 3 parity must be canonical, not byte-byte.** When/if pre-baking
   FeatureCollection shards in Phase 3, equality is "feature-set equal after
   sorting by stable key and rounding coords to 6 decimals."

---

## 7. Decisions already made (do not re-litigate)

- **CHG-0005 chose Option A** (drop the patched-FC cache entirely, not "keep
  with corrected signature"). Reason: the SHA-1 signature cost more than the
  patch step it gated; net negative cache. Reverting to keep-cache requires a
  fundamentally different fingerprint design.
- **The over-map "Total ms" caption was tried and reverted.** The caption fired
  before `render_map_view` and so reported a partial total; the sidebar fired
  at end-of-script and reported the full total. Inconsistency → reverted. The
  sidebar perf panel is the source of truth.
- **Memory rule from CLAUDE.md §6:** `docs/HANDOFF.md` updates require explicit
  `Applied CHG-xxxx` confirmation from the user. This brief lives in `docs/`
  but is not the handoff ledger; the handoff ledger should be updated separately
  when Phase 2 commits land.
- **Tests CLAUDE.md tier table applies.** Data-contract changes (artifact paths,
  identifier columns) → always test. UI/runtime → manual click-path is fine.

---

## 8. Out-of-scope for Phase 2

- Phase 3 (pyogrio tuning, pre-baked FC shards, parity audit).
- Color slider commit-on-release (CHG-0010 was carried in the original plan;
  measurement after Phase 1 showed it's no longer a hot spot, so it's deferred
  to "only if Phase 2 leaves it noticeable").
- `landing_runtime.py`'s India overview clickable polygons — if these require
  ADM2 on first paint, leave them alone in Phase 2 and only ensure the detail
  flow wins. The landing flow may eat ADM2 cold cost; that's acceptable as the
  user's pain is in the detail/click-through path per the screenshots.

---

## 9. Approval gate reminder

CLAUDE.md §0: write code only with explicit `APPROVED: APPLY` or equivalent.
For Phase 2:

- The user **pre-approved** the offline run of `tools/geodata/build_adm1_geojson.py`
  once the module exists. No further confirmation needed for the run itself.
- The user requested `MANIFEST.md` and `tools/README.md` updates **ride inside
  the Phase 2 commits**, not as a follow-up.
- The user did not specify a stop-after-each-step preference. Default to
  shipping all five CHGs in sequence under one approval; surface a status check
  after Step 2b if it materially changes test behavior.

---

## 10. After Phase 2

When all five CHGs are in and validated:

1. Provide the git handoff block per CLAUDE.md §8:
   ```bash
   git add <touched files>
   git commit -m "Phase 2: ADM1-first boot; sub-basin overlay resolver; optimized cold loads"
   ```
2. Wait for user confirmation `Applied CHG-0001, CHG-0002, CHG-0011, CHG-0012, CHG-0014` before updating `docs/HANDOFF.md`.
3. Offer Phase 3 only if the user reports remaining slowness — measurement
   should drive that decision, not preemptive optimization.

End of brief.
