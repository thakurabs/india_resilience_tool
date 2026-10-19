# Grid-first outdoor WBGT working pilot — CHG-0654

Open `index.html` directly in a browser. No server, network or Python environment is required.

ACCESS-CM2 only; Kerala, Rajasthan and Himachal Pradesh; 75 districts and 581 blocks. Historical 1990–2010; SSP245/SSP585 2020–2080. Select a place, metric and period; inspect annual values and baseline differences, or download CSVs.

Computation: daily outdoor WBGT per cell → annual metric per cell → fixed area-weighted admin mean. Counts are area-average cell exceedance days; annual maximum is an area-average of cell annual maxima, not a polygon-wide simultaneous extreme. Complete 365-day cell-years are required; missing support stays missing.

630/656 units meet the 95% retained-area screen. The 26 exclusions are in Kerala. This is a single-model diagnostic pilot, not an ensemble or production release. UHE-Daily is a provisional shade calibration reference. Rajasthan ≥32 °C held-out errors worsen; future QDM application-window sensitivity remains material.

`pilot_yearly.csv` retains full-precision annual values and roster metadata; `manifest.json` records source and output hashes. No further climate calculations are launched by this viewer or builder. Rebuild with `python -m tools.diagnostics.wbgt_route_b_pilot --build`.
