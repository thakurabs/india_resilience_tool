# Holdout gate ruling (CHG-0700)

**Date:** 2026-10-10. **Decided by:** the project owner, in session.

The frozen holdout rule in [SPEC.md](SPEC.md) blocks automatic publication when any
state × model × metric or national cut is classified *worsened*. The first scored
model (ACCESS-CM2, 2000–2010, 7,752 classified units) produced verdict
`BLOCK_USER_RULING`:

| Cut | Units worse | Median abs. error change | 90% bootstrap |
|---|---|---|---|
| Chhattisgarh, days ≥ 32 °C | 121 / 178 (68%) | +3.95 d/yr | +2.42 … +5.78 |
| Jharkhand, days ≥ 32 °C | 194 / 286 (68%) | +3.76 d/yr | +1.51 … +6.02 |

Nationally the same model improved every metric's median absolute error (annual
mean −3.45 °C; ≥ 28 °C −74.5 d/yr; ≥ 30 °C −43.7 d/yr; ≥ 32 °C −0.26 d/yr with an
interval spanning zero).

**Ruling:** proceed with the CarbonPlan-aligned shade WBGT replacement
*irrespective of these numbers*. The ruling covers worsened cuts found by the
remaining 17 models as well; they are reported in the release evidence but do not
block. The scoring is not altered or re-run to change its verdict, and the
regressions are disclosed in the release notes.

The ruling does not waive any other gate: the sun-adjustment control, staged
parity audit, test-regression gate and dashboard verification still have to pass,
and production writes still need their explicit go-ahead.

## Addendum, 2026-10-11: sun-adjustment control

The ACCESS-CM2 control passed both future scenarios (≤ 3.4e-13 °C) but failed the
historical scenario: mean and max differ from the published Route B values by at
most 2.9e-7 °C, with every day count exact. The project owner accepted this as not
material (the shade metrics do not use the sun term, and the futures establish
the cache identity) and directed that it be disclosed as unexplained. The control
and its tolerance are unchanged. See [RELEASE_NOTES.md](RELEASE_NOTES.md) §3.
