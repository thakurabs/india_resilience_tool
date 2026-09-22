# Flag C Remediation Plan — Versioned JRC RP-100 Source and Fail-Closed Rebuild

## Status

- **Planning snapshot:** `GIT:add_flood_depth@d46c686` — aligned with origin when this plan was amended.
- **Change Ledger:**
  - `CHG-0228` — replace defective RP-100 source and rebuild affected outputs — `SUGGESTED`.
  - `CHG-0229` — add source, coverage, sentinel, and publication validation — `SUGGESTED`.
  - `CHG-0231` — incorporate review fixes for sentinel provenance, coverage VRT gaps, mask alignment, state coverage definition, fallback footprint validation, and snapshot freshness — `APPLIED`.
- **Approval boundary:** this plan does not authorize implementation, downloads, pipelines, publication, or commits.

## Summary

Build a provenance-pinned JRC v2.1.2 RP-100 source from official tiles, preserving tile presence in a separate coverage raster. Rebuild district, block, composite, Glance, and optimized outputs in an isolated staging area, validate them against pinned sentinels and national completeness rules, and promote only during a stopped-application maintenance window.

The implementation will distinguish:

- **Acquisition completeness:** every expected India-intersecting tile must be present and valid; missing tiles fail the run.
- **Polygon source coverage:** partially covered coastal/border polygons remain publishable from their covered portion and are flagged in QA.
- **Hazard value:** covered-but-unflooded support produces zero; no official source coverage produces `NaN`.

RP-10, RP-50, and RP-500 remain unchanged and explicitly retain unresolved legacy provenance.

## Implementation Changes

### 1. Versioned source acquisition

Add `tools.data_acquisition.prepare_jrc_rp100_source` with a resumable CLI:

```text
--dataset-version 2.1.2
--boundary-path <canonical-district-source>
--output-dir <versioned-source-directory>
--base-url <official-JRC-root>
--workers <n>
--resume
--overwrite
--dry-run
```

The command will:

- Download and pin the official README, changelog, RP-100 directory listing, and `tile_extents.geojson`, which is currently published at the [official JRC flood-hazard root](https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/CEMS-GLOFAS/flood_hazard/).
- Select tiles intersecting the canonical India district union buffered by one native pixel, `1/1200°`, to avoid exact-touch seam omissions.
- Derive the expected India tile count at runtime; never treat the global tile count as the India count.
- If `tile_extents.geojson` is unavailable or unreadable, derive nominal 10-degree footprints from official tile filenames. Before relying on the fallback, validate at least one fallback-derived footprint against an actual downloaded raster's bounds. When both sources exist, require official extents, fallback footprints, and downloaded raster bounds to agree.
- Download each tile through a unique `.part` file, then atomically rename it after validation.
- Partition tile IDs uniquely among workers and use an atomic per-tile lock file so separate processes cannot write the same `.part`.
- On resume, retain a tile only when its recorded size and SHA-256 match the local file and the raster still passes validation.

Per-tile validation will require:

- HTTP content length equals the completed local file length when the server provides it.
- Raster opens successfully and every block can be read.
- CRS is EPSG:4326.
- Resolution is exactly three arc-seconds within a small numeric tolerance.
- Raw nodata is `-9999`, never zero.
- Bounds agree with the selected tile footprint.
- Raster dimensions, transform, dtype, and metadata are recorded.

SHA-256 will be documented as local integrity/self-consistency evidence, not upstream authenticity. If JRC publishes a checksum sidecar, acquisition will pin and verify it. Otherwise the manifest will record:

```json
{
  "upstream_checksum_available": false,
  "integrity_basis": [
    "https_content_length",
    "local_sha256",
    "full_blockwise_raster_read",
    "header_and_grid_contract"
  ]
}
```

A suspect tile—size mismatch, interrupted transfer, inconsistent range response, or read failure—will be retried and compared against fresh first/middle/last byte ranges. The remaining risk of a corrupt-but-consistent upstream response will be stated in the source manifest and technical note.

### 2. VRT-based India source and explicit coverage

Avoid materializing a roughly 5.5 GB national float32 mosaic. Produce:

- `RP100_depth.vrt`, referencing official raw tiles by relative path.
- One compressed, tiled `uint8` coverage GeoTIFF per source tile, containing `1` across the verified tile footprint and copying the depth tile's CRS, transform, width, height, and bounds verbatim.
- `RP100_tile_coverage.vrt`, mosaicking those masks on exactly the same grid and resolving gaps outside acquired tile footprints to literal `0`.
- `source_inventory.json` and `source_manifest.json`.

The coverage mask is independent of raw depth values: a present official tile is covered even where its depth raster contains `-9999`.

Coverage masks must never recompute their own grid from nominal bounds. Each mask inherits the corresponding depth tile's grid metadata exactly so strict depth/coverage alignment cannot fail because of sub-pixel rounding.

The manifest will pin:

- JRC version and official URLs.
- Expected, acquired, validated, and rejected tile IDs.
- Per-tile size, SHA-256, transform, bounds, resolution, nodata, and dtype.
- Depth and coverage VRT hashes.
- Canonical boundary file SHA-256.
- Selection buffer and selection method.
- Acquisition timestamp and tool revision.
- Upstream-checksum availability and residual integrity caveat.

All administrative extraction will remain windowed. The overlay exporter will reproject directly from the VRT and retain its existing maximum 4096-pixel output dimension; it must not read the national source into a full-size array.

### 3. Strict RP-100 builder path

Add a strict manifest-driven mode to `build_jrc_flood_depth_admin_masters`:

```text
--source-manifest <source_manifest.json>
--rp100-only
```

Keep the existing multi-return-period validator and zero-nodata behavior unchanged behind the explicit legacy interface:

```text
--source-dir <legacy-directory>
--allow-unversioned-source
```

Strict mode will use a separate validator over only:

- `RP100_depth.vrt`
- `RP100_tile_coverage.vrt`

It will fail unless the two rasters have identical CRS, transform, shape, bounds, and resolution. Depth nodata must be `-9999`; coverage values must be restricted to `0/1`.

Implement a separate strict coverage-statistics path rather than changing the legacy helper:

1. Compute one `geometry_window` from the depth raster.
2. Read both rasters using that exact window.
3. Build one `all_touched=False` in-polygon mask from the shared window transform.
4. Define:

```text
source_covered = (coverage == 1) AND in_polygon
flooded = source_covered AND finite(depth) AND depth > 0
dry = source_covered AND NOT flooded
source_coverage_fraction = source_covered_cells / in_polygon_cells
```

Within a verified tile footprint, raw `-9999`, non-finite, zero, and negative depth values count as dry support for the IRT exposure denominator. This is an explicit methodology caveat: JRC nodata may include permanent water or other non-modelled support, but it must no longer be confused with a missing source tile.

The legacy `_is_zero_nodata` dry-zero override remains intact only for explicitly allowed unversioned builds.

### 4. Polygon coverage and sub-cell behavior

Use exact coverage states:

- `full`: `source_coverage_fraction >= 0.99`
- `partial`: `0 < source_coverage_fraction < 0.99`
- `none`: `source_coverage_fraction == 0`

`0.99` classifies QA coverage; it is not a publication threshold.

Behavior:

- Full and partial polygons publish metrics from the officially covered portion.
- Partial polygons set `partial_coverage=True` and do not halt the batch.
- No-coverage polygons publish explicit `NaN`/no-data values.
- Covered polygons with no positive cells publish `0.0`.
- Covered polygons with positive cells retain the existing positive-cell p95 depth, extent, matrix, and weighted district-rollup arithmetic.

If the primary `all_touched=False` mask has zero cells:

- Sample depth and coverage at `geometry.representative_point()`.
- Coverage `1` creates a one-sample covered result: positive depth is flooded; otherwise it is dry.
- Coverage `0` produces explicit no-data.
- Record `sampling_mode="representative_point_fallback"`; normal rows use `"cell_center_mask"`.

Strict-mode QA will use unambiguous new fields:

- `source_covered_cell_count`
- `source_coverage_fraction`
- `source_coverage_state`
- `partial_coverage`
- `dry_source_cell_count`
- `flooded_source_cell_count`
- `sampling_mode`

Do not silently reuse legacy `coverage_fraction` semantics. Audit block calculations, district rollups, QA exports, thresholds, summaries, and downstream consumers to use the new strict fields. This is a methodology-impacting coverage-contract change; hazard classification formulas themselves remain unchanged.

### 5. Validation contract and sentinels

Create a versioned RP-100 validation contract containing:

- JRC version.
- Source-manifest schema version.
- Exact canonical boundary SHA-256.
- Normalized state/district sentinel keys.
- Expected classified inundation shares and allowed intervals.
- An initially empty reviewed expected-dry state/UT allowlist.
- Coverage and raster-contract thresholds.

Pinned sentinel intervals:

| District | Observed share | Allowed interval |
|---|---:|---:|
| Araria | 0.6121 | 0.55–0.69 |
| Supaul | 0.9166 | 0.85–0.98 |
| Sitamarhi | 0.6484 | 0.58–0.72 |
| Darbhanga | 0.8441 | 0.78–0.91 |
| Patna | 0.7482 | 0.68–0.82 |
| Begusarai | 0.9645 | 0.90–1.00 |
| Bahraich | 0.5549 | 0.49–0.63 |
| Gorakhpur | 0.5261 | 0.46–0.60 |
| Ballia | 0.6551 | 0.59–0.73 |

The observed-share values above are provisional planning anchors until the first authorized corrected build records their provenance. Before they become hard acceptance gates, regenerate them from the corrected v2.1.2 build and freeze a reviewed validation contract that records, per table revision:

- JRC dataset version.
- Source-manifest SHA-256.
- Canonical boundary SHA-256.
- Tool name, command, and code commit used to compute the shares.
- Computation timestamp.
- Reviewer and review note.

Until that freeze occurs, validation may require that sentinel names and intervals are present, but it must not treat the provisional four-decimal values as measured acceptance evidence. If the first authorized corrected build falls outside these provisional intervals, review the generated shares, source manifest, boundary hash, and extraction logs before either revising the intervals or failing the release.

Sentinel rules:

- Missing sentinel state or district fails validation; it is never skipped.
- A boundary-hash mismatch invalidates the complete sentinel contract.
- Boundary changes require recomputation and explicit review of the sentinel table.
- Validate both lower and upper interval bounds to catch source, geometry, or masking drift.
- Sentinel fractions continue to use the primary `all_touched=False` mask.

National gates:

- Expected tile IDs must exactly equal acquired and validated tile IDs.
- No raster/VRT grid mismatch, unknown source version, missing manifest, or zero-nodata strict source is allowed.
- Every canonical district and block must resolve to full, partial, none, or documented representative-point fallback.
- Partial official footprint coverage is reported, not treated as acquisition failure.
- A state/UT with aggregate source coverage of at least `0.99` and no positive RP-100 cell fails unless explicitly reviewed as expected-dry. Aggregate source coverage is area-weighted over the state's canonical admin units using the same area-denominator semantics as the existing valid-supported-area rollups.
- A partially covered all-zero state is a warning requiring QA inspection, not an automatic source-failure conclusion.

Do not guess an expected-dry allowlist. Begin empty and generate an all-zero candidate report during the first corrected build.

Support a narrowly scoped reviewed override:

```text
--dry-state-review-token <reviewed-json>
```

The token must pin dataset version, source-manifest SHA-256, boundary SHA-256, canonical state names, reviewer, and rationale. It may waive only the all-zero-state gate. It cannot waive missing tiles, hashes, raster contracts, sentinel failures, or unknown coverage. Accepted waivers are embedded in the release manifest.

### 6. Resumable national rebuild

Add an orchestration command:

```text
python -m tools.runs.rebuild_jrc_rp100_national \
  --source-manifest <manifest> \
  --staging-dir <same-volume-stage> \
  --resume
```

Optional controlled inputs:

```text
--dry-state-review-token <reviewed-json>
--publish
```

Stages will be independently checkpointed using fingerprints of source manifest, boundary hash, configuration, code revision, and upstream artifact hashes:

1. Validate source inventory and raster contracts.
2. Build strict RP-100 block outputs and QA.
3. Build strict RP-100 district outputs and QA.
4. Validate national coverage and sentinels.
5. Recompute `jrc_flood_depth_rp100`.
6. Recompute `jrc_flood_extent_rp100`.
7. Recompute `jrc_flood_depth_index_rp100`.
8. Recompute `composite_flood_jrc_depth`.
9. Rebuild the Riverine Glance bundle.
10. Rebuild affected optimized metric roots.
11. Generate parity, distribution-audit, and publication reports.

The composite configuration must be asserted unchanged: severity retains weight `1.0`, with raw depth and extent attribute weights remaining `0.0`. Corrected source values are expected to shift Riverine rankings; that is intended input correction, not a formula change.

A stage is reusable only when its checkpoint fingerprint and output hashes still match.

### 7. Optimized artifact isolation and portable manifests

The optimized bundle stores metrics in independent roots. Therefore:

- Only these roots may change:
  - `jrc_flood_depth_rp100`
  - `jrc_flood_extent_rp100`
  - `jrc_flood_depth_index_rp100`
  - `composite_flood_jrc_depth`
- Only the Riverine Glance bundle may change.
- Unrelated metric roots, unrelated Glance bundles, geometry, and context artifacts must be byte-for-byte identical.

Combined global artifacts such as bundle manifests and parity reports may be rewritten. For them, require semantic parity for unrelated entries rather than whole-file byte equality: unrelated metric summaries, hashes, row counts, and Glance entries must remain identical after ignoring allowed timestamp, ordering, and release-path changes.

Make staged Glance manifests portable by recording the root relative to the optimized bundle, e.g. `context/glance`, rather than an absolute staging path. Version the manifest schema and update its validator before using it in the staged publication flow.

### 8. Fail-closed publication and rollback

The current dashboard artifacts remain visible unchanged until the corrected release passes every gate.

Publication requirements:

- Application fully stopped; an idle or open Streamlit process is insufficient.
- Streamlit caches are not relied upon to detect swapped files.
- Staging, canonical runtime directory, and backup are on the same NTFS volume.
- Complete source, QA, parity, sentinel, distribution, and artifact-isolation reports exist.
- No unreviewed validation failure remains.

Promotion:

1. Rename the canonical runtime directory to a timestamped backup.
2. Rename the complete staging directory to the canonical path.
3. If step 2 fails, immediately restore the backup.
4. Restart the application, thereby clearing Streamlit caches.
5. Run runtime smoke tests and post-swap parity checks.
6. If checks fail, stop the application, restore the backup, restart, and record the failed release.
7. Retain the backup until explicit post-release acceptance.

The two-rename operation has a brief non-atomic gap and is accepted only because the application is stopped throughout it.

### 9. Documentation and audit closure

Update the technical guidance, README, and MANIFEST where applicable to document:

- JRC v2.1.2 provenance and acquisition command.
- RP-100 strict source-manifest requirements.
- VRT and tile-coverage-mask contracts.
- Full/partial/none polygon semantics.
- Representative-point fallback.
- In-tile nodata-as-dry caveat.
- Local SHA-256 authenticity limitation.
- Legacy/unresolved RP-10/50/500 provenance.
- Staging, maintenance-window publication, cache restart, and rollback.
- Expected downstream Riverine ranking changes with unchanged composite formula.

After the corrected publication:

- Repeat the national metric distribution review.
- Replace Flag C’s invalid current statistics with corrected national numbers.
- Record confirmed affected scope beyond Bihar/eastern Uttar Pradesh.
- Reassess the separate five-class floor/ceiling saturation concern only after the corrected input distribution is available.
- Do not update `docs/HANDOFF.md` until the user explicitly confirms `Applied CHG-0228` and/or `Applied CHG-0229`.

## Test Plan

### Acquisition and manifest tests

- Tile-index selection and one-pixel seam buffer.
- Filename-footprint fallback and index/filename disagreement failure.
- Missing expected tile, duplicate tile, truncated tile, wrong content length, bad CRS, wrong resolution, nodata zero, unreadable block, and bounds mismatch.
- Resume with valid tile, modified tile, stale `.part`, and concurrent per-tile lock contention.
- Manifest determinism apart from declared timestamps.
- No India-subset test hard-codes the global tile count.

### Strict raster and polygon tests

Use small synthetic depth and coverage rasters without network access:

- Missing tile area remains no-data and cannot become dry zero.
- Covered raw zero and covered raw `-9999` become dry support.
- Positive cells produce the existing p95-positive calculation.
- Full, partial, and no-coverage polygons produce the specified states.
- Coastal/border partial polygons publish and are flagged.
- Zero-cell polygons invoke representative-point fallback.
- Misaligned depth/coverage grids fail before extraction.
- Legacy mode retains existing four-RP validator and zero-nodata behavior.
- Strict mode neither requires nor validates RP-10/50/500.

### Aggregation and methodology regression

- District rollups reproduce current arithmetic from corrected strict block inputs.
- All-zero covered districts publish zero rather than no-data.
- All-no-coverage districts publish `NaN`.
- Bihar and Uttar Pradesh matrix recomputation remains exact within floating-point tolerance.
- Composite weights and formula are byte/value-identical to the pre-remediation configuration.
- Controlled input changes alter Riverine rankings while unrelated metrics remain unchanged.

### National validation tests

- Missing sentinel name fails.
- Boundary-hash mismatch fails.
- Sentinel value below or above its interval fails.
- State with coverage `>=0.99` and no positive cell fails.
- Reviewed static allowlist or valid dry-state token waives only that gate.
- Wrong-version, wrong-source-hash, wrong-boundary-hash, or unrecognized-state token fails.
- Partial all-zero state emits a warning without silently passing as confirmed dry.
- Candidate expected-dry report is deterministic.

### Overlay, optimized bundle, and publication tests

- VRT-backed overlay is capped at 4096 pixels and performs no full-national array allocation.
- Only four affected metric roots and Riverine Glance change.
- Unrelated independent roots remain byte-for-byte identical.
- Unrelated entries in rewritten global manifests remain semantically identical.
- No absolute staging path survives in a published manifest.
- Interrupted second rename restores the backup.
- Post-swap smoke test runs only after application restart/cache clear.
- Rollback restores the prior dashboard and its manifest.

### Acceptance criteria

- Every expected India-intersecting v2.1.2 tile is acquired and validated.
- Bihar/eastern Uttar Pradesh sentinel shares fall within their pinned intervals.
- Every canonical block and district has an explicit full, partial, none, or fallback QA classification.
- No missing source content is interpreted as valid dry support.
- Existing RP-100 depth, extent, severity, composite, and optimized artifacts are replaced only after all gates pass.
- RP-10/50/500 and unrelated metrics remain unchanged.
- The final audit distinguishes input correction from the still-open severity-scale saturation question.

## Assumptions

- JRC v2.1.2 remains the approved authoritative RP-100 source.
- The official `tile_extents.geojson` is preferred, with filename-derived footprints retained as a tested fallback.
- VRTs are the canonical national representation; BigTIFF materialization is not part of this remediation.
- `0.99` is a QA threshold separating full from partial source coverage, not a publication cutoff.
- Partial coastal/border source coverage is legitimate when national tile acquisition is complete.
- The expected-dry allowlist begins empty and requires reviewed evidence.
- Current dashboard outputs remain visible, without warning-label changes, until maintenance-window promotion.
- CHG-0228 and CHG-0229 should be implemented together because publishing corrected data without the new coverage and validation contract would leave the original failure mode reproducible.
