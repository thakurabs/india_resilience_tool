# Why Andaman & Nicobar and Lakshadweep carry no outdoor-WBGT values

Investigation of the 23 island administrative units that the national Route B
release cannot serve. Traced against the frozen spatial support and the fitted
ACCESS-ESM1-5 assembly, read-only.

Evidence:

* `scratch/wbgt_route_b_national/islands/island_unit_trace.csv` — all 23 units plus
  two mainland controls, with the coverage numerator and denominator, the cell count,
  the elevation gap, the retained fraction and the per-cell support distribution.
* `scratch/wbgt_route_b_national/islands/island_cell_trace.csv` — every intersecting
  grid cell with its area weight, share of the unit, reference support and eligibility.
* `scratch/wbgt_route_b_national/islands/island_support_map.png` — island boundaries,
  the 0.25° cells that intersect them, each cell's reference support, which cells fail
  the 0.50 support gate, and which have no published elevation.

## 1. The calculation being traced

From `wbgt_route_b_run.run`:

```python
valid = (support >= .50) & np.isfinite(reference).all(axis=0) & np.isfinite(hist["shade"]).all(axis=0)
coverage = areas[:, valid].sum(axis=1) / units.whole_area_m2.to_numpy()
units["eligible"] = coverage >= .95
```

`support` is the per-cell minimum fraction of the NEX cell covered by valid UHE-Daily
reference pixels. `areas` holds exact projected intersection areas in EPSG:6933 between
each administrative polygon and each retained 0.25° cell. The denominator is the whole
polygon area, including any part lying outside the grid — stated in
`wbgt_route_b_reference.polygon_weights` and intended, so a unit cannot earn coverage by
having its uncovered portion silently removed from the denominator.

Island polygons are land only, so the denominator is land area, not sea. Measured
polygon areas track published figures: Nicobars 1,778 km², North and Middle Andaman
3,447 km², South Andamans 2,529 km², Lakshadweep 29.9 km².

## 2. No defect was found in selection, geometry or coordinates

`grid_area_fraction` is the share of each unit's area covered by **retained** cells.

| Check | Result |
|---|---|
| Cells intersect the islands at all | Yes — `polygon_weights` raises if a polygon finds none, and never did |
| A&N units covered by retained cells | 0.986 – 1.000 |
| Longitude wrapping / CRS | No evidence — cells land on the islands in the diagnostic map |
| Multipart island geometries | Handled; `make_valid` applied before intersection |
| Denominator inflated by sea | No — polygon area is land |
| Elevation fabricated at sea level | No — retained island cells carry genuine 0.0, 0.5, 7.5 … 85.5 m |

Two mainland controls behave as expected: Hyderabad district retains 1.000 and is
eligible; Surat district retains 0.927 and is excluded on a single sub-0.50 cell of
fourteen.

## 3. Lakshadweep — source limitation, not a threshold effect

| Unit | retained cells | grid fraction | retained fraction | **highest cell support** |
|---|---|---|---|---|
| Lakshadweep district | 7 | 0.679 | 0.000 | **0.12** |
| Amini, Kadmat, Kalpeni, Minicoy | 1–2 | 1.000 | 0.000 | 0.04 – 0.12 |
| Agatti, Kavaratti | 1–2 | 0.713 / 0.847 | 0.000 | 0.04 – 0.08 |
| Kiltan | 1 | 0.056 | 0.000 | 0.04 |
| Andrott, Bitra, Chetlat | 0 | 0.000 | 0.000 | no cell retained |

**The highest reference support of any Lakshadweep cell is 0.12, against a 0.50 cell
gate.** Lowering the 0.95 unit coverage gate to zero would still publish nothing,
because no cell qualifies to contribute. The atolls are 0.12 – 4.95 km² inside cells of
roughly 700 km² at that latitude; the 0.25° NEX grid and the UHE reference cannot
resolve them. This is a limitation of the method's spatial basis, and no change to a
threshold, a weighting or a mask recovers it.

A secondary contributor, not decisive: seven Lakshadweep cells have no published
CarbonPlan elevation and are dropped before eligibility is computed. That removes 32%
of the district's area and all of Andrott, Bitra and Chetlat. Those cells carry support
of 0.04 and would have failed the cell gate regardless.

## 4. Andaman & Nicobar — threshold exclusion at the retained 0.95 gate

| Unit | retained fraction | cells ≥ 0.50 | highest cell support |
|---|---|---|---|
| Ferrargunj (block) | **0.904** | 4 of 9 | 0.84 |
| Rangat (block) | 0.813 | 4 of 7 | 1.00 |
| Mayabunder (block) | 0.792 | 2 of 4 | 1.00 |
| North and Middle Andaman (district) | 0.787 | 7 of 17 | 1.00 |
| Diglipur (block) | 0.764 | 3 of 9 | 0.92 |
| Campbell Bay (block) | 0.654 | 2 of 5 | 0.68 |
| South Andamans (district) | 0.635 | 5 of 16 | 0.84 |
| Nicobars (district) | 0.420 | 2 of 15 | 0.68 |
| Little Andaman (block) | 0.388 | 1 of 5 | 0.60 |
| Prothrapur (block) | 0.305 | 2 of 7 | 0.76 |
| Car Nicobar, Nancowrie (blocks) | 0.000 | 0 | 0.20 / 0.40 |

Support reaches 1.00 and thirteen A&N cells clear the 0.50 gate, so the reference does
cover the larger islands. The elevation gap costs 0.1 – 1.4% here, not enough to matter.
**A&N fails only because no unit reaches the 0.95 unit coverage gate**, and several come
close. The underlying constraint is real — these are narrow islands whose cells are
mostly sea — but the binding constraint is the threshold, which this release keeps.

Earlier reporting called both territories "entirely unsupported by the UHE reference".
That is accurate for Lakshadweep and misleading for A&N, whose exclusion is a threshold
decision over partial but genuine support.

## 5. Conclusions

| Territory | Verdict |
|---|---|
| **Lakshadweep** | **Source limitation.** No cell reaches the support gate; unrecoverable without changing the method's spatial basis. |
| **Andaman & Nicobar Islands** | **Threshold exclusion.** Genuine partial support, 0.31–0.90 retained, none reaching the retained 0.95 gate. |

No correction is proposed. Nothing here is a defect to fix, so no preparation, fit or
output is invalidated, and no refitting is implied.

Both territories keep their full administrative roster in the published masters, with
missing values, coverage fractions and exclusion reasons carried in `support_coverage`
beside each state's metrics.
