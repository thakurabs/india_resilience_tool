# Route B reference acquisition: bounded sample

CHG-0644, 2026-10-03. Programme:
[Outdoor WBGT Route B](../../wbgt_outdoor_route_b.md).

**Acquisition integrity passed; scientific reference QC remains pending.**
Twelve India-window dates were acquired and independently reopened through the
local verifier. No full calibration archive or QDM pilot was run.

| Measurement | Result |
|---|---:|
| Dates | Jan/Apr/Jul/Oct 15 in 1985, 2000, 2014 |
| Native window | 640 × 600 cells; nominal 68–98°E, 6–38°N |
| Successful-run HTTP body bytes | 221,970,432 (221.97 MB) |
| Additional header probes | 196,608 bytes |
| Stored compressed arrays + coordinates + metadata | 10,391,503 bytes (10.39 MB) |
| Successful-run elapsed time | 72.77 s |
| Retries in successful run | 0 |
| Valid cells, every sampled date | 248,559 of 384,000 |
| Missing cells, every sampled date | 135,441 |
| Valid masks | Identical across all 12 dates |
| Sample window minimum | −39.8552 °C, 2000-01-15, approximately 35.025°N / 68.175°E |
| Sample window maximum | 33.6568 °C, 2014-07-15, approximately 31.075°N / 71.025°E |

These are **bounding-box extrema, not India-admin extrema**. Neighbouring
countries occur in the rectangular subset. The earlier single-day range is
not a valid climatological QC range. Rural/mountain accuracy and suitability for
calibrating IRT remain unestablished.

The live source has no nodata tag and encodes the nominal 0.05° pixel scale as
`0.05000000074505806`. An initial exact-value guard correctly stopped before
reading any rows. The corrected reader accepts only a 1e-9° representation
tolerance and preserves the actual spacing in coordinates and metadata.

[sample_manifest.json](sample_manifest.json) records all dates, source validators,
subset/array/artifact hashes, native resolution, per-day support and extrema.
Data themselves are ignored diagnostic artifacts under `scratch/uhe_daily/`.
No source raster was downloaded in full. Row payload hashes refer to downloaded
full-width strips; artifact hashes refer to local cropped NPZ files.

Straight-line extrapolation from this small sample is about 9.49 GB compressed
storage and 18.5 hours for 10,957 dates. This is a planning estimate only: host
load, pacing, retries, compression and date-dependent source behaviour can vary.
The deterministic no-retry transfer estimate is 202.6775 GB.

## Validation

- `python -m pytest -q tests/test_uhe_daily.py`: **26 passed**, using the existing
  Windows IRT environment. Synthetic tests require no network.
- `python -m tools.data_acquisition.uhe_daily --help`: passed.
- Default/full and `--sample --dry-run`: passed; no network or artifact writes.
- `python -m tools.data_acquisition.uhe_daily --sample --download --output-dir scratch/uhe_daily --max-transfer-gb 0.25`:
  12 acquired, no retries, cap respected.
- `python -m tools.data_acquisition.uhe_daily --sample --verify --output-dir scratch/uhe_daily`:
  12 verified; no network or writes.
- Repeating the sample download command: 0 acquired, 12 verified cached dates;
  completed with network unavailable, confirming resumable local reuse.

## Next scientific checks

1. Check native valid support against the canonical pilot district/block roster,
   including coastal and small-island units; a stable rectangular mask is not
   proof of admin coverage.
2. Confirm the raster's WBGT definition and reproduce CarbonPlan's aggregated
   UHE reference on matched source footprints/weights.
3. Examine seasonal and elevation-stratified values within India, and assess
   independent station evidence and any shared source observations.
4. Record the QC verdict and complete-archive resource decision before the
   1985–2014 acquisition. Then implement and cost-probe the paired pilot paths.
