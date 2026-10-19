# Kochi CarbonPlan reproduction contract

Scope: ACCESS-CM2; Kochi processing_id 7886; historical calibration 1985–2014;
primary daily comparison 2005. Written before reproduction scores.

Use CarbonPlan extreme-heat commit f662b37200fe219db912ebd09ceb52fdac979861,
its published city polygon, population raster and elevation raster, UHE-Daily
regional reference, and released shade/sun daily series. Use the pinned MetSim
commit edbd68fecd7decf0f03b32d1226f44327bd297b7. Resolve every coordinate by
value; never treat processing_id as an array index.

Separate reproduction stages: geometry/weights; raw shade; shade QDM;
radiation disaggregation; sun adjustment; released-series comparison.
CarbonPlan's archived intermediates are being checked, not assumed accessible.
Local NEX inputs may differ in version from the 2023 product; retain provenance.

Primary parity target: all 365 days finite, maximum absolute daily difference
<= 1e-6 °C, and identical inclusive counts >=28, >=30, >=32 °C. Report continuous
residuals even if parity fails. Agreement in counts alone is not parity.
No fitting to the released WBGT series or tuning of coefficients, weights,
calendars or calibration period. Published corrected shade may be used to test
the outdoor conversion separately, but is never called an independent full
reproduction. Any source inconsistency is recorded; a diagnostic correction
must be separately labelled and cannot silently replace the literal source.

Single worker; bounded remote chunks, no global archive. All downloads and
intermediate products under scratch/carbonplan_kochi_reproduction; durable
evidence only in this directory. Production, shade artifacts, predecessor
diagnostic evidence and source climate files remain untouched. No deployment.

Pre-score source finding: the pinned MetSim constants define SW_RAD_DT=30 s,
while notebook 07 passes SW_RAD_DT=3600 s to shortwave. The literal source is the
primary run. A separately labelled diagnostic uses the same geometry and sets
the wrapper to 30 s so it integrates the actual geometry intervals. This is a
source-consistency check, not a fitted candidate or an accepted upstream fix.

Source audit extension after the first numerical run: also test the other
internally consistent interpretation, geometry=3600 s and wrapper=3600 s. These
are the two explicit values in the pinned sources, not a searched parameter
range. This post-score diagnostic is not promoted to a predeclared primary.
During implementation, retain original NEX noon timestamps in QDM training;
normalizing them to the UHE midnight timestamps would alter the upstream
rolling-window alignment. Normalize dates only for final daily comparisons.

## Scope extension to Bikaner and Shimla (written before the extended run scored)

Add Bikaner (processing_id 6558, 2 climate cells, semi-arid plain) and Shimla
(6822, 1 cell, ~1.5 km elevation) to the existing Kochi case, and extend the
daily comparison from 2005 alone to 2005, 2007 and 2009. These three cities and
three years are exactly the subset of the predecessor five-city W1 comparison
for which local ACCESS-CM2 rsds and sfcWind exist (1990-2010 only), chosen for
input availability before any new score. Hyderabad and Kolkata are in range but
are deliberately left out of this run; adding them is follow-up work, not a
result withheld after scoring.

Bikaner and Shimla are selected because W1's disagreement with published
CarbonPlan has opposite sign at them (-1.01 degC and +2.42 degC annual mean)
while Kochi sits between (+0.46 degC). If one unchanged chain reproduces the
published product at all three, the disagreement is attributable to the method
W1 omits rather than to a location-specific input or geometry artefact. Failure
at one city is a real negative result and is reported as such.

Calibration stays 1985-2014 against UHE-Daily, resolved by coordinate value in
every store: the UHE processing_id axis has 39,171 entries against the released
products' 39,168, so positional indexing is prohibited. Weights, elevation,
polygons and the radiation and sun stages are unchanged from the Kochi run; no
per-city coefficient, weight, calendar or calibration period is introduced.

Declared parity gate stays the SPEC's original maximum absolute daily
difference <= 1e-6 degC with identical >=28/30/32 degC counts. The Kochi run
measured 4.0e-5 degC, which fails that gate while matching all counts exactly.
That gate is NOT relaxed. A second, separately named band,
`within_input_version_tolerance` at 1e-3 degC, is reported alongside it to
express the residual expected from local NEX files differing in version from
CarbonPlan's 2023 download. Both are reported for every city-year; neither
replaces the other, and the 1e-3 band is an assumption about input lineage, not
a reproduction criterion that has been met.

The run verdict is keyed to the self-consistent hourly radiation
interpretation, with the literal inconsistent source reported in its own
separate field. The Kochi run proved the literal source cannot reproduce the
product; continuing to key the overall verdict to it reports a defect in
CarbonPlan's published wiring as a failure of this reproduction.

## Post-score reporting change (recorded after the extended run, no gate moved)

The extended run met neither declared continuous gate and reproduced every
threshold count exactly at all nine city-years. A single-token verdict
misreports that in both directions, so the verdict became compound:
`COUNTS_EXACT_CONTINUOUS_PARITY_NOT_MET`, with the declared 1e-6 degC and the
1e-3 degC band each reported as its own boolean. Neither threshold was changed,
and no score was recomputed; only the label is new.

Measured residual structure, reported rather than corrected: 32,869 of 32,871
calibration days fall within 1e-3 degC. The two exceptions are both at Kochi,
both on 7 August (1989 and 2007), both 0.0168 degC. Those two days have raw
shade values 3e-6 degC apart inside one day-of-year QDM group, and their
adjustment factors are exchanged: our 1989 factor equals CarbonPlan's 2007
factor and vice versa, each to about 1e-5 degC. The rank order of two
indistinguishable days is float-level ambiguous, so this is an ordering
artefact between tied days, not a difference in method. It is reported in
`tie_break_pairs.csv` and left in every score; no re-pairing is applied.
