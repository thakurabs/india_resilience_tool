# Coastal coverage: is the 0.95 shortfall real, or a geometry error?

CHG-0668. The user kept the 0.95 unit-coverage gate for this release and asked
for one thing to be ruled out first: that the coastal coverage fractions reflect
"a geometry or coastal masking error" rather than genuine reference support.

**Verdict: the fractions are correct. The shortfall is a real limitation of the
UHE-Daily reference at the coastline, not an IRT geometry or denominator error.**
Keeping 0.95 preserves the current contract. It does not establish that 0.95 is
scientifically optimal, and nothing here argues that it is.

## 1. The denominator is land area, not land plus sea

The coverage fraction is `sum(intersection area with eligible cells) /
whole_area_m2`. If `whole_area_m2` included territorial water, coastal units
would be penalised for sea they should never be judged on. Measured against
published district land areas:

| District | Polygon km² | Published land km² | Ratio |
|---|---|---|---|
| Chennai | 334.4 | 426 | 0.785 |
| Ernakulam | 3,060.0 | 3,068 | 0.997 |
| Kasaragod | 1,992.8 | 1,992 | 1.000 |
| Thiruvananthapuram | 2,193.1 | 2,189 | 1.002 |
| Surat | 4,333.6 | 4,549 | 0.953 |
| Udupi | 3,582.2 | 3,880 | 0.923 |
| Pondicherry | 298.5 | 293 | 1.019 |
| Diu | 38.9 | 40 | 0.972 |
| Lakshadweep | 29.9 | 32 | 0.934 |
| Yanam | 37.8 | 30 | 1.260 |

The polygons track published **land** area, and Chennai's is 21% *smaller* than
its published figure, not larger. An inflated, sea-including denominator would
show ratios well above 1. It does not.

## 2. The shortfall is reference support, not grid coverage

The shortfall decomposes into two independent parts:

- `1 - grid_area_fraction` — area no NEX cell covers at all, which is geometry.
- `grid_area_fraction - retained_fraction` — area a NEX cell covers but which
  fails the cell-level reference gate.

Across the 169 excluded units the mean grid gap is **0.028** and the mean
reference gap is **0.306**. Reference support dominates in **165 of 169** units.
`grid_area_fraction` is exactly 1.000 for Chennai, Diu, Yanam, Pondicherry,
Gir Somnath, Kasaragod, Mayiladuthurai, Udupi and Chengalpattu — the grid covers
them completely, and they are excluded anyway.

For comparison, eligible units have a minimum `grid_area_fraction` of 0.9966 and
a mean of 1.000, so grid coverage is essentially total everywhere.

## 3. The cause, at cell level

Every ineligible coastal cell is a sea-edge cell carrying `elevation_m = 0.0`
with only a minority of valid UHE pixels inside it:

| District | Cell | Elevation | Valid UHE fraction | Share of unit area | Eligible |
|---|---|---|---|---|---|
| Chennai | 13.125°N, 80.125°E | 22.0 m | 1.000 | 0.560 | yes |
| Chennai | 13.125°N, 80.375°E | 0.0 m | **0.280** | 0.382 | no |
| Chennai | 12.875°N, 80.375°E | 0.0 m | **0.080** | 0.017 | no |
| Ernakulam | 10.125°N, 76.125°E | 0.0 m | **0.360** | 0.047 | no |
| Surat | 21.375°N, 72.625°E | 0.0 m | **0.480** | 0.073 | no |
| Surat | 21.125°N, 72.625°E | 0.0 m | 0.520 | 0.051 | yes |

The mechanism is a resolution mismatch between two land masks. Real coastal land
sits inside 0.25° NEX cells that are mostly ocean at UHE's native 0.05°, so those
cells carry 8-52% valid reference and fail the >= 0.50 cell gate. Surat's two
sea-edge cells straddle the gate at 0.48 and 0.52, which is the clearest
demonstration that the gate — not the geometry — is what decides.

This is a property of UHE-Daily, which correctly has no data over ocean. It is
not a masking mistake on IRT's side, and it is not fixable by changing the
denominator.

## 3a. Two territories have zero coverage

Stated plainly, because counting them inside the 169 understates it:

| State / UT | Eligible units | Total units |
|---|---|---|
| Andaman & Nicobar Islands | **0** | 12 |
| Lakshadweep | **0** | 11 |

The outdoor metric cannot serve either territory at all. Their masters carry the
full roster with missing values, so a map shows them as blank rather than wrong,
but no yearly series exists for any unit in either. Eleven other states lose part
of their roster; these two lose all of it.

## 4. Lakshadweep is the one genuine geometry case

Lakshadweep district is the only unit where both parts fail: `grid_area_fraction`
0.679 **and** `retained_fraction` 0.000. Atolls of a few km² sit inside 0.25°
cells that are almost entirely ocean, so there is neither full grid coverage nor
any cell that clears the reference gate. Chetlat, Andrott and Bitra have zero
supported area and Kiltan has 5.6%. No amount of gate adjustment reaches them;
only a reference-side method at atoll scale would, and that is out of scope here.

## 5. What the gate actually trades

Lowering the gate does not change any value a unit already has. It admits units
whose value would be computed from their **supported subset only**. For Chennai
at 0.601 that means scoring a coastal city from its 60% inland area, which will
read warmer and less maritime than the city itself. That is the trade-off, and it
is why the measured value shift would have to be reported per recovered unit
before any such change shipped. The user's ruling for this release is to keep
0.95 and publish the exclusions honestly instead.

## 6. What ships with the release

Per the user's instruction, the complete administrative roster is retained and
the exclusions are made legible rather than silent:

- all 7,921 units present, the 169 excluded ones carrying missing values;
- `retained_fraction` — the share of each unit's area backed by eligible cells;
- `retained_fraction_strict_reference` — the same under a 0.95 cell gate;
- `grid_area_fraction` — so a grid gap can be told from a reference gap;
- an explicit exclusion reason per unit.

These travel with the published metric rather than living only in this document,
so an export or a map hover can say why a unit is blank.
