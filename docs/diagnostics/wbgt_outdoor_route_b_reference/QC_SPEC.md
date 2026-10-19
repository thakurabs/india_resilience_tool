# Route B reference QC — CHG-0649

Frozen before scored execution, 2026-10-03, `e40af88`.
User authority: continue the remainder of the approved Route B programme.
Existing shade-validation code/data are read-only predecessor evidence.

1. Verify all 12 previously acquired UHE sample artifacts and native coordinates.
2. Compute exact native-pixel/polygon intersections in EPSG:6933 for every
   canonical district and block in Kerala, Rajasthan, Himachal Pradesh and the
   Andaman/Nicobar and Lakshadweep UTs. Repair invalid polygon topology explicitly,
   recording original validity; preserve the full polygon-area denominator.
3. Report finite-reference coverage and area-weighted values/extrema on every
   sample date. Empty/all-missing support remains NaN. Report units below 99%
   support as review flags, not automatic scientific failures or acceptance.
4. Reproduce Kochi, Bikaner and Shimla's published UHE reference at these dates,
   using their original CarbonPlan footprints, ESRI:53034 intersections and the
   published **0.05°** population grid. The reference uses that native grid;
   the model chain's 0.25° population weights are not a substitute.
5. Source layout and numerical control gate: all 36 comparisons finite and max
   absolute residual <=0.001 °C. This is a source/aggregation agreement band,
   not a scientific accuracy criterion. Report failures without tuning weights.
6. Fetch missing population chunks only when explicitly enabled, single-stream,
   two-second pacing, cumulative 200 MB body cap, per-object 160 MB cap. Persist
   source hashes and transfer ledger. No full UHE archive acquired by this tool.

Independent Indian WBGT observational accuracy remains unestablished. A control
pass plus documented coverage supports proceeding with a **provisional-reference
diagnostic pilot**, not release or composite membership. Full-period completeness
must be verified after acquisition. Scientific gate failures must be named and
must not be reclassified as engineering successes.

Commands, in order (existing Windows IRT environment):

```bash
python -m pytest -q tests/test_wbgt_route_b_reference.py tests/test_uhe_daily.py
python -m tools.diagnostics.wbgt_route_b_reference --dry-run
python -m tools.diagnostics.wbgt_route_b_reference --run --fetch-population --max-transfer-gb 0.2
```

The first command tests offline; the second only prints a plan; the third writes
QC evidence and downloads missing population chunks into a separate scratch cache.

## Source-wiring investigation (after the frozen native-cell test)

The original test failed at 0.4399656 °C and remains preserved in `qc/`.
Inspection of checksum-pinned `utils.py:ds_to_grid` showed a hardcoded
`grid_spacing = 0.25`, also called by notebook 05 on the 0.05° UHE grid.
The resulting footprints overlap. This was tested as a separate source-wiring
control using the same data, cities, population and 0.001 °C gate, with no fitted
parameters. All 36 values reproduce to <=2.63e-14 °C; see
`qc_published_footprint/`. The reference-source identity is therefore established
for the sample. The original native-footprint comparison was not rerun or erased.

IRT's paired pilot retains **true native footprints** and treats the published
overlapping-footprint calculation only as a fidelity control. Both IRT branches
share the same conservatively coarsened reference for their primary comparison.
This is an explicit methodology difference from the published product; it must
not be called exact CarbonPlan reference replication or observational improvement.
The source-control pass and measured mainland coverage permit the provisional
diagnostic programme to continue. Independent observational validation and island
coverage remain unresolved release constraints.
