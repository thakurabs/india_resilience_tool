# India Resilience Tool — Technical Guidance Note
## Climate Risk Methodology: Data, Metrics, and Bundle Construction

**Status:** DRAFT — all sections drafted; figures (§3.4) and final review pending  
**Scope:** Data sources → downscaling → grid-first compute → individual metrics → thematic and sectoral bundle construction → composite score output  
**Out of scope:** Exposure layers, vulnerability, adaptive capacity, dashboard UI/UX, pipeline tooling  
**Primary audience:** Technical peers (climate scientists, GIS specialists) and policy/planning stakeholders  
**Tone:** Technically rigorous throughout; mathematical derivations included with plain-language explanations alongside

---

> **Notation and conventions:**
> - Mathematical notation uses standard LaTeX-style inline and display math where rendered.
> - Tables are preferred over prose for metric lists, weights, and parameter values.
> - Cross-references between sections are marked `(→ §N.M)` or `(§N.M)`.
> - The note speaks in two registers: mathematical derivations for technical peers, set alongside plain-language statements of what each result means for planning. A reader who wants only the policy reading can skip the display-math blocks — the surrounding prose states each result in words.

---

## 1. Introduction and Framing

Climate adaptation in India is planned and financed at the subnational level — by states, districts, and increasingly by blocks — yet the climate-projection products that usually inform those decisions are global, spatially coarse, and aggregated across hazards. The India Resilience Tool (IRT) is built to close that resolution-and-specificity gap: it turns downscaled CMIP6 projections into district- and block-level, multi-hazard climate **hazard-pressure** scores that a planner can read at the administrative unit they actually govern. This note documents the methodology end to end — data provenance, downscaling, grid-first computation, individual climate-index definitions, and the assembly of those indices into thematic and sectoral bundles and a single composite score.

### 1.1 The subnational climate-planning problem

India spans a subcontinent of climatic regimes — the arid northwest, the monsoon core, the Himalayan north, the peninsular plateau, and long eastern and western coastlines. A single national figure, or a value read off a coarse global grid, therefore tells a district little about the hazard it specifically faces. The relevant hazards are also multiple and co-occurring: extreme daytime and night-time heat, humid heat, meteorological drought, extreme rainfall and flash flooding, winter cold, and riverine flooding, each with its own seasonality and geography. Adaptation decisions against these hazards are made for multi-decade horizons under genuine scenario and model uncertainty, which means planners need projections that are (a) resolved to the administrative units they manage, (b) available for more than one emissions future, and (c) available for more than one time horizon. IRT is organised around exactly those three needs.

### 1.2 What the tool provides

The tool's design choices, stated plainly:

- **Spatial resolution.** Scores are produced at **district (ADM2)** and **block (ADM3)** level, computed *grid-first* from a 0.25° (~25 km) downscaled grid and then area-aggregated to administrative polygons (→ §3, §4).
- **Climate inputs.** Projections come from **NASA-NEX GDDP-CMIP6**, a statistically downscaled, bias-corrected product over a multi-model CMIP6 ensemble (→ §2.1, §2.3).
- **Scenarios and horizons.** Two emissions pathways — **SSP2-4.5** and **SSP5-8.5** — across multiple future periods (2020–2040 through 2060–2080), measured against a **1990–2010** historical baseline (→ §2.2).
- **Hazards and indices.** A library of standard (ETCCDI, SPI) and India-context climate indices spanning heat, humid heat, cold, drought, and extreme rainfall, plus a static riverine-flood layer from global flood-hazard data (→ §5).
- **Aggregation.** Indices are normalized and combined into **six thematic hazard bundles** and **eight sectoral hazard-pressure bundles**, each emitting a single 0–100 *higher-is-worse* composite (→ §6, §7, §8).

IRT's contribution is to operate at the administrative resolution — district (ADM2) and block (ADM3) — at which Indian adaptation is actually planned, using India-context indices and thresholds and explicit per-hazard decomposition rather than a single aggregate index value.

### 1.3 Scope of this note and how to read it

**In scope:** data provenance (§2), the downscaling context (§3), grid-first computation and spatial/temporal aggregation (§4), individual metric definitions (§5), thematic bundle construction (§6), sectoral bundle construction (§7), and the composite output (§8), with a complete metric reference and impact-band catalogue in the appendices.

**Out of scope:** the web dashboard and its UI/UX; **exposure** layers (population, land use, built-up area); groundwater and other hydrological-context layers; and **vulnerability / adaptive-capacity** components. Because exposure and vulnerability are excluded, the scores in this note are climate **hazard-pressure** indices, not full risk scores in the IPCC sense — the word "Risk" in a bundle name denotes *hazard pressure relevant to that sector*, and the full caveat is set out in §7.1 and §8.1.

**Section roadmap:**

| Section | What it covers |
|---|---|
| §2 Climate Data Sources | CMIP6 ensemble and scenarios, temporal coverage, the NASA-NEX downscaled product, and the JRC flood data |
| §3 Downscaling | What statistical downscaling is, the BCSD method NASA-NEX uses, grid resolution, and reproducibility |
| §4 Grid-First Compute | Why indices are computed per grid cell first, then area-aggregated to districts/blocks, with period and ensemble handling |
| §5 Metric Definitions | Definition, units, baseline, and derivation of each temperature, precipitation, drought, humid-heat, and flood index |
| §6 Thematic Bundles | The six hazard-family bundles: per-period normalization and fixed-weight compositing |
| §7 Sectoral Bundles | The eight sector hazard-pressure bundles: the lens-based blended-rule framework and impact bands |
| §8 Composite Score and Output | What the 0–100 composite is and is not, and how to read it across scenario, period, and spatial level |
| Appendix A / B | Complete metric reference; sectoral impact-band derivations |

### 1.4 How to use these scores — and how not to

These bundles are decision-support inputs for **relative prioritisation**: comparing districts or blocks against one another, or one scenario/period against another, to flag where a given hazard pressure is high or rising. They are designed to *screen and rank* — to direct attention and further assessment.

They are **not** measurements of realised risk or impact. A "Health Risk 80" denotes high hazard pressure on the health sector at that location — not that 80 % of people will be harmed, nor a probability of any specific outcome. Because exposure and vulnerability are out of scope (→ §1.3), a high score does not by itself establish that people or assets are actually at risk; it must be combined with exposure and vulnerability information before it can support resource-allocation or investment decisions. Scores are also relative within the tool's ensemble and normalization, so absolute values should not be read as physical quantities or compared across unrelated indices (→ §8.1).

---

## 2. Climate Data Sources

### 2.1 CMIP6: Model Ensemble and Scenarios

The Coupled Model Intercomparison Project Phase 6 (CMIP6) is the sixth generation of coordinated climate model experiments, providing the primary basis for the IPCC Sixth Assessment Report (AR6) future climate projections. IRT draws on CMIP6 outputs from 24 general circulation models (GCMs), using the variant label **r1i1p1f1** — denoting the first realisation (r1), initialisation method (i1), physics configuration (p1), and forcing (f1) — for all models and scenarios.

**Scenarios**

Two Shared Socioeconomic Pathways (SSPs) are used:

| Scenario | Label | Description |
|----------|-------|-------------|
| SSP2-4.5 | Middle-of-the-road | Social, economic, and technological trends evolve without dramatic departure from historical patterns; radiative forcing stabilises at approximately 4.5 W m⁻² by 2100. |
| SSP5-8.5 | Fossil-fuelled development | Rapid economic growth driven largely by fossil fuels; radiative forcing reaches approximately 8.5 W m⁻² by 2100. |

**GCM ensemble**

| # | Model | Modelling Centre | Country |
|---|-------|-----------------|---------|
| 1 | ACCESS-CM2 | CSIRO / Bureau of Meteorology (BOM) | Australia |
| 2 | ACCESS-ESM1-5 | CSIRO | Australia |
| 3 | BCC-CSM2-MR | Beijing Climate Center (BCC), China Meteorological Administration | China |
| 4 | CMCC-CM2-SR5 | Centro Euro-Mediterraneo sui Cambiamenti Climatici (CMCC) | Italy |
| 5 | CMCC-ESM2 | CMCC | Italy |
| 6 | CanESM5 | Canadian Centre for Climate Modelling and Analysis (CCCma) | Canada |
| 7 | EC-Earth3 | EC-Earth Consortium | Europe |
| 8 | EC-Earth3-Veg-LR | EC-Earth Consortium | Europe |
| 9 | GFDL-CM4 | NOAA Geophysical Fluid Dynamics Laboratory (GFDL) | USA |
| 10 | GFDL-ESM4 | NOAA GFDL | USA |
| 11 | IITM-ESM | Indian Institute of Tropical Meteorology (IITM) | India |
| 12 | INM-CM4-8 | Institute of Numerical Mathematics (INM), Russian Academy of Sciences | Russia |
| 13 | INM-CM5-0 | INM, Russian Academy of Sciences | Russia |
| 14 | IPSL-CM6A-LR | Institut Pierre-Simon Laplace (IPSL) | France |
| 15 | KACE-1-0-G | National Institute of Meteorological Sciences / Korea Meteorological Administration (NIMS-KMA) | South Korea |
| 16 | KIOST-ESM | Korea Institute of Ocean Science and Technology (KIOST) | South Korea |
| 17 | MIROC6 | JAMSTEC / AORI / NIES / R-CCS | Japan |
| 18 | MPI-ESM1-2-HR | Max Planck Institute for Meteorology (MPI-M) | Germany |
| 19 | MPI-ESM1-2-LR | MPI-M | Germany |
| 20 | MRI-ESM2-0 | Meteorological Research Institute (MRI) | Japan |
| 21 | NESM3 | Nanjing University of Information Science and Technology (NUIST) | China |
| 22 | NorESM2-LM | Norwegian Climate Centre (NCC) | Norway |
| 23 | NorESM2-MM | NCC | Norway |
| 24 | TaiESM1 | Research Center for Environmental Changes (RCEC), Academia Sinica | Taiwan |

### 2.2 Temporal Coverage and Analysis Periods

**Raw data temporal span**

| Scenario | Period |
|----------|--------|
| Historical | 1950–2014 |
| SSP2-4.5 | 2015–2100 |
| SSP5-8.5 | 2015–2100 |

Historical and projection files are contiguous across models: each model contributes one annual NetCDF file per variable per year spanning 1950–2100 across the historical and SSP runs.

**Analysis periods**

IRT aggregates individual-year climate indices into the following multi-year windows:

| Label | Period | Role |
|-------|--------|------|
| Historical baseline | 1990–2010 | Reference for per-period normalisation (→ §6.2) |
| Near-term | 2020–2040 | Near-term projection |
| Mid-century | 2040–2060 | Mid-century projection |
| End-century | 2060–2080 | End-of-century projection |

Each future window is an inclusive 21-year mean (e.g. 2020–2040 covers the years 2020 through 2040). The endpoint years 2040 and 2060 are each shared by two adjacent windows, so the windows abut rather than leaving gaps between them.

The anchor period (1990–2010) falls entirely within the historical simulation run (1950–2014); no splicing of historical and SSP files is required (→ §4.3).

A fifth period label, **`Current`**, is reserved for static present-day layers that have no climate-projection time dimension — in this note, the **Riverine Flood severity index** (→ §2.4, §5.5). It is paired with the **Snapshot** scenario rather than an SSP pathway, represents a single externally modelled present-day state rather than a multi-year climate average, and carries no averaging window.

### 2.3 NASA-NEX GDDP-CMIP6: The Downscaled Product

IRT uses the **NASA Earth Exchange Global Daily Downscaled Projections, CMIP6** (NEX-GDDP-CMIP6) as its primary climate input. All spatial disaggregation and bias correction was applied by NASA prior to data release. The downscaling method is described in §3.

The following variables were obtained from the NEX-GDDP-CMIP6 product:

| Variable | CF Standard Name | Description | Native Units |
|----------|-----------------|-------------|-------------|
| `tas` | air_temperature | Daily mean near-surface air temperature | K |
| `tasmin` | air_temperature | Daily minimum near-surface air temperature | K |
| `tasmax` | air_temperature | Daily maximum near-surface air temperature | K |
| `pr` | precipitation_flux | Daily total precipitation | kg m⁻² s⁻¹ |
| `huss` | specific_humidity | Near-surface specific humidity | kg kg⁻¹ |
| `hurs` | relative_humidity | Near-surface relative humidity | % |

Temperature variables are converted from kelvin to degrees Celsius (°C) and precipitation is converted from kg m⁻² s⁻¹ to mm day⁻¹ (× 86400) during pre-processing (→ §4.1).

**Spatial resolution and domain:** NEX-GDDP-CMIP6 is provided at **0.25° × 0.25°** horizontal resolution (~25 km at the equator). All data are clipped to the India domain: **68.0°E–97.5°E, 5.0°N–45.0°N**.

**Citation:** Thrasher, B., Wang, W., Michaelis, A., Melton, F., Lee, T., and Nemani, R. (2022). NASA Global Daily Downscaled Projections, CMIP6. *Scientific Data*, 9, 262. https://doi.org/10.1038/s41597-022-01393-4

### 2.4 JRC Global Flood Data

Riverine flood metrics (→ §5.5) are derived from the **CEMS-GloFAS Global River Flood Hazard Maps** (Version 2.1), a product of the Copernicus Emergency Management Service (CEMS) published by the European Commission Joint Research Centre. IRT uses the **RP-100** (1-in-100-year return period) raster layers: flood depth (metres) and flood extent (binary inundation mask).

Because this is a static, observationally-derived snapshot product rather than a future climate projection, the Riverine Flood bundle carries a "Snapshot" scenario label in the tool and is not available under SSP2-4.5 or SSP5-8.5 (→ §6.1).

**Citation:** Baugh, C., Colonese, J., D'Angelo, C., Dottori, F., Neal, J., Prudhomme, C., and Salamon, P. (2024). Global river flood hazard maps (Version 2.1) [Dataset]. European Commission, Joint Research Centre (JRC). https://doi.org/10.2905/JRC.VD32YWG

---

## 3. Downscaling: What It Is and How NASA-NEX Applies It

### 3.1 What Statistical Downscaling Means

GCMs are designed to simulate the large-scale dynamics of the climate system — atmospheric circulation, ocean–atmosphere coupling, and radiative transfer — at a global scale. Their native grid spacing is typically 100–300 km, meaning a single model grid cell covers an area comparable to a large Indian state. At this resolution, GCMs cannot resolve the terrain-driven heterogeneity, coastal gradients, or local land–surface feedbacks that determine temperature and rainfall patterns at the district or block level.

**Downscaling** is the process of translating GCM output from its native coarse resolution to a finer spatial scale more appropriate for regional and local analysis. Two broad approaches exist:

- **Dynamic downscaling** nests a higher-resolution regional climate model (RCM) within the GCM domain, simulating regional atmospheric dynamics explicitly. It is computationally intensive and not available at the pan-India, 24-model scale required here.
- **Statistical downscaling** uses the statistical relationship between coarse-resolution GCM output and observed fine-resolution climatology to correct and spatially disaggregate GCM fields. It is tractable across large multi-model ensembles.

NASA-NEX GDDP-CMIP6 applies a statistical downscaling approach. IRT uses the NASA-NEX product as published online.

### 3.2 The BCSD Method in NASA-NEX GDDP

The NASA-NEX GDDP-CMIP6 product employs **Bias Correction and Spatial Disaggregation (BCSD)** (Wood et al. 2002; Maurer et al. 2010), a two-step statistical procedure:

**Step 1 — Bias correction**

For each GCM, each variable, and each calendar month, the empirical cumulative distribution function (CDF) of the model's monthly-mean output is mapped to match the empirical CDF of a reference observational climatology over the historical period. This quantile mapping adjusts systematic biases in both the mean and the distribution tails while preserving the model's interannual variability and long-term trend signal. The reference climatology used by NASA-NEX is the Global Meteorological Forcing Dataset (GMFD; Sheffield et al. 2006), as described in Thrasher et al. (2022).

Formally, let $F_{\text{obs}}$ and $F_{\text{mod}}$ denote the empirical CDFs of the observed and modelled monthly distributions over the bias-correction reference period. The bias-corrected value $x'$ for a raw model value $x$ is:

$$x' = F_{\text{obs}}^{-1}\!\bigl(F_{\text{mod}}(x)\bigr)$$

This transfer function is derived on the coarse GCM grid and then applied to all years, including future projections (where the model's distribution shift due to climate change is preserved relative to the corrected historical distribution).

**Step 2 — Spatial disaggregation**

Bias-corrected monthly anomalies at the coarse GCM resolution are spatially interpolated to the target 0.25° × 0.25° grid using bilinear interpolation. Daily sub-monthly variability is disaggregated by preserving the daily anomaly patterns from the bias-corrected monthly fields.

**What BCSD corrects and what it does not**

BCSD corrects the marginal distribution of temperature and precipitation at the monthly scale. It does not alter the GCM's large-scale atmospheric dynamics, synoptic circulation patterns, or temporal sequencing. Biases in monsoon onset timing, intraseasonal oscillations, or the frequency of extreme daily events are only partially addressed by the monthly-scale distribution-matching step; residual dynamical biases remain (→ §3.4).

### 3.3 Grid Resolution and Spatial Domain

The NEX-GDDP-CMIP6 product is provided at **0.25° × 0.25°** horizontal resolution, corresponding to approximately 25 km at the equator and ~27 km at 25°N (typical central India latitude). IRT clips the global product to the India domain — **68.0°E–97.5°E, 5.0°N–45.0°N** — yielding a domain of 118 × 160 grid cells (29.5° ÷ 0.25° = 118 columns, 40.0° ÷ 0.25° = 160 rows).

**Resolution implications at district vs block level**

India has 784 districts (mean area ~4,171 km²) and 7,137 sub-district blocks (mean area ~458 km²). At 0.25° resolution (~625 km² per cell):

- A typical district contains **4–20** 0.25° cells, providing adequate spatial sampling for area-weighted aggregation.
- A typical block may contain **fewer than 4** cells, and smaller blocks in densely sub-divided states may fall within a single cell. Block-level composites therefore carry higher spatial uncertainty than district-level composites, and cross-block variation in small block groups may partly reflect grid-cell boundaries rather than true sub-district heterogeneity.

Spatial aggregation from the 0.25° grid to administrative units is described in §4.2.

### 3.4 Provenance and Reproducibility Notes

**Dataset access and version**

NEX-GDDP-CMIP6 is publicly available via the NASA Center for Climate Simulation (https://www.nccs.nasa.gov/services/data-collections/land-based-products/nex-gddp-cmip6). The IRT pipeline ingests the product as released (all 24 GCMs, variant r1i1p1f1, both SSPs). The authoritative dataset description and processing documentation is:

> Thrasher, B., Wang, W., Michaelis, A., Melton, F., Lee, T., and Nemani, R. (2022). NASA Global Daily Downscaled Projections, CMIP6. *Scientific Data*, 9, 262. https://doi.org/10.1038/s41597-022-01393-4

> Wood, A. W., Maurer, E. P., Kumar, A., and Lettenmaier, D. P. (2002). Long-range experimental hydrologic forecasting for the eastern United States. *Journal of Geophysical Research: Atmospheres*, 107(D20), 4429. https://doi.org/10.1029/2001JD000659

> Sheffield, J., Goteti, G., and Wood, E. F. (2006). Development of a 50-year high-resolution global dataset of meteorological forcings for land surface modeling. *Journal of Climate*, 19(13), 3088–3111.

**Internal validation: Telangana domain**

As part of dataset QA, a validation analysis was conducted comparing NEX-GDDP-CMIP6 historical daily output against ERA5 reanalysis and IMD gridded observations over the Telangana domain for the period 1980–1985, using polygon-overlap area weighting for spatial aggregation. Results should be interpreted in the context of this limited spatial and temporal sample; they are illustrative of the dataset's bias characteristics rather than a full pan-India evaluation.

*Temperature*: The 24-model ensemble reproduces ERA5 mean near-surface temperature over Telangana with good fidelity. The ERA5 domain-mean daily temperature is 27.3°C; most individual GCMs fall within ±0.5°C of this value (model range approximately 26.8–28.0°C), with tight clustering in normalised standard deviation and spatial correlation visible in the Taylor diagram.

*Precipitation*: Area-mean daily precipitation for ERA5 (2.47 mm day⁻¹) and IMD (2.65 mm day⁻¹) agree closely at the seasonal mean scale. Spatial correlations between district-level precipitation metrics and ERA5 for the best-performing models range from 0.81 to 0.90, indicating moderate spatial skill at district resolution. However, the ensemble systematically underestimates peak daily rainfall intensities: the IMD Rx1day (mean annual maximum 1-day rainfall) for Adilabad district is approximately 77 mm day⁻¹, while the 24 CMIP6 models range from approximately 33 to 55 mm day⁻¹ — a consistent dry bias in extreme events that persists after BCSD.

This arises because BCSD applies monthly-scale quantile mapping and then bilinear spatial disaggregation to 0.25°; neither step alters the GCM's underlying atmospheric dynamics. The convective processes that generate intense short-duration rainfall events are parameterised at each GCM's native grid spacing — ranging from approximately 70 km (EC-Earth3, the finest in the ensemble) to 310 km (CanESM5, the coarsest), with most models at 100–200 km; these are approximate physical grid spacings (grid interval × ~111 km), not the coarser CMIP6 nominal-resolution attributes (e.g. 500 km for CanESM5) — at which mesoscale convective systems responsible for high-intensity daily rainfall in the Indian region cannot be explicitly resolved. The BCSD spatial disaggregation resamples these coarse fields to the 0.25° output grid but adds no new sub-grid meteorological information.

The extreme-rainfall underestimation is therefore a structural limitation of the GCM ensemble, not a downscaling artefact. Users interpreting the Extreme Rainfall | Flash Flood bundle (→ §5.2) should note that absolute metric values likely understate observed extreme rainfall intensities.

*Comparative context*: Jain et al. (2019) evaluated the NEX-GDDP (CMIP5-era) product against IMD gridded observations over the Indian subcontinent for the summer monsoon season (1975–2005), benchmarking it against multi-model means from 28 raw CMIP5 models and 10 CORDEX regional models. NEX-GDDP surpassed both CMIP5 and CORDEX in reproducing seasonal mean temperature and precipitation patterns (spatial pattern correlation ~0.8; RMSE ~4.25°C for temperature and ~2.48 mm day⁻¹ for precipitation), inter-annual variability, and annual cycle characteristics. The simulation of extremes was also found to be more realistic in NEX-GDDP relative to raw CMIP5 and CORDEX output, with reduced inter-model spread — supporting the use of the NEX-GDDP product for climate change impact assessment. Although these findings pertain to the CMIP5-era version of NEX-GDDP, the BCSD methodology is common to both the CMIP5 and CMIP6 versions; the results are therefore informative about the relative improvement that the downscaling procedure confers over raw GCM output.

> Jain, S., Salunke, P., Mishra, S. K., Sahany, S., and Choudhary, N. (2019). Advantage of NEX-GDDP over CMIP5 and CORDEX data: Indian Summer Monsoon. *Atmospheric Research*, 228, 152–160. https://doi.org/10.1016/j.atmosres.2019.05.026

> **[FIGURES TO INSERT]** The following validation figures from the notebook analysis will be incorporated here to provide visual evidence of the bias characterisation. Until they are inserted, the specific quantitative values cited in this subsection (ERA5 and model domain-mean temperatures, the Adilabad Rx1day comparison, and the per-model resolutions) should be read as **provisional** pending the supporting figures. Source notebooks: `notebooks/era5_vs_cmip_clean_tel_1980_1985.ipynb` and `notebooks/rainfall_metrics_imd_cmip6_tel_box_1980_1985.ipynb`.
> 1. Taylor diagram — near-surface temperature (tas) vs ERA5, Telangana 1980–1985
> 2. Taylor diagram — daily precipitation (pr) vs ERA5, Telangana 1980–1985
> 3. Taylor diagram — daily precipitation (pr) vs IMD, Telangana 1980–1985
> 4. nRMSE heatmap — precipitation metrics vs ERA5 and vs IMD across 24 models
> 5. Rx1day comparison bar chart — IMD vs 24 CMIP6 models, Adilabad district 1980–1985

**Known limitations relevant to India**

Three classes of systematic limitation are relevant to users interpreting IRT outputs:

1. **Monsoon dynamics.** The Indian Summer Monsoon (ISM) is influenced by complex land–sea thermal gradients, orographic lifting, and large-scale teleconnections (ENSO, IOD). Most CMIP6 GCMs simulate the broad seasonal cycle of ISM precipitation but exhibit systematic biases in onset date, spatial distribution of the monsoon core and break phases, and sub-seasonal variability. BCSD corrects the monthly distribution but preserves the GCM's underlying dynamical representation of monsoon structure.

2. **Himalayan terrain.** The 0.25° grid (~25 km) cannot resolve the elevation gradients of the Hindu Kush–Himalayan arc, where elevations change by 3,000–5,000 m over tens of kilometres. Temperature and precipitation are subject to large interpolation errors at high altitudes; outputs for Himalayan districts and blocks in Uttarakhand, Himachal Pradesh, Jammu & Kashmir, Sikkim, and Arunachal Pradesh should be interpreted with particular caution.

3. **Coastal resolution.** At 0.25° resolution, coastal grid cells blend land and ocean surface conditions. This can introduce artefacts in temperature and humidity fields for coastal and island districts (including the Kerala coast, Tamil Nadu coast, Lakshadweep, and Andaman & Nicobar Islands).

---

## 4. Grid-First Compute and Post-Processing

### 4.1 Architecture: Why Grid-First

All climate index computations in IRT are performed at the native 0.25° grid resolution before any aggregation to administrative boundaries. This "grid-first" design reflects a methodological choice rooted in the structure of the indices being computed.

An alternative — the "admin-first" approach — would average the raw daily GCM values over each administrative unit before computing indices. This is appropriate for linear statistics such as mean temperature, but introduces bias for any non-linear index. Consider a district that straddles a dense urban area and a river valley: one 0.25° cell (the city) records five consecutive days at 36–38°C, while an adjacent cell (the valley) records those same days at 28–30°C. The admin-first approach averages the two cells first, producing a district-mean of 32–34°C — below the 35°C threshold on every day — and consequently reports **zero** extreme-heat days for the district. The grid-first approach computes five hot days for the city cell and zero for the valley cell, then takes the area-weighted mean: 2.5 hot days for the district. The admin-first result is not merely less precise — it erases a multi-day extreme heat event that affected half the district. The distortion compounds further for non-linear indices: spell-length metrics, percentile-exceedance fractions, and the SPI gamma transform all produce systematically biased outputs when applied to pre-averaged spatial means.

Operating grid-first also preserves within-unit spatial heterogeneity through the final aggregation step and allows the per-cell index fields to be re-aggregated to any future boundary revision without repeating the (computationally intensive) index computation.

The pipeline accordingly computes each annual climate index as a per-cell 2D field on the 0.25° grid, one year at a time, for each GCM and scenario. These annual index fields are then area-weighted and averaged to the administrative boundary set as described in §4.2.

### 4.2 Spatial Aggregation to District and Block

**Aggregation method**

Spatial aggregation from the 0.25° grid to administrative polygons uses **fractional area overlap**: for each (polygon, cell) pair, the area of intersection between the cell tile and the polygon is computed, and the resulting intersection area is used as the weight in a weighted average.

Formally, let $v_j$ denote the index value at grid cell $j$, and let $a_{ij}$ denote the intersection area (in m²) between administrative unit $i$ and cell $j$. The aggregated value for unit $i$ is:

$$\bar{v}_i = \frac{\sum_j a_{ij}\, v_j}{\sum_j a_{ij}}$$

where the sum runs over all cells $j$ that intersect unit $i$. Cells with no intersection or with missing ($\text{NaN}$) index values are excluded from both numerator and denominator.

Grid cell tile boundaries are defined as midpoints between adjacent cell centres: a cell at latitude $\phi$ and longitude $\lambda$ occupies the tile $[\phi - \delta/2,\, \phi + \delta/2] \times [\lambda - \delta/2,\, \lambda + \delta/2]$ where $\delta = 0.25°$. Both the administrative boundary polygons (stored in EPSG:4326, the universal geographic coordinate standard) and these grid cell tile boxes are reprojected to the **EPSG:6933 Equal-Area Cylindrical** projected coordinate reference system before any intersection or area calculation is performed. This reprojection is necessary because a 0.25° × 0.25° angular tile covers a larger physical area near the equator than near the Himalayas; computing intersection weights in degree-space would therefore over-represent equatorial cells. In EPSG:6933 the intersection areas are in m² and are geometrically correct across all latitudes.

This approach — sometimes called rasterize-to-polygon area weighting or exact polygon-cell intersection — is more accurate than centroid-in-polygon methods for small or irregularly shaped units, where a cell centroid may fall outside the polygon even though a substantial fraction of the cell area overlaps it.

**Administrative boundary set**

The canonical boundary set is the **LGD (Local Government Directory)** boundary dataset at two levels:

- **District (ADM2):** 784 units, India-wide
- **Block / sub-district (ADM3):** 7,137 units, India-wide

District-level and block-level composite scores are both computed directly from the 0.25° grid: district scores are not derived by aggregating block scores, nor are block scores disaggregated from district scores. Concretely: the 784 district polygons are intersected with the 0.25° grid to produce a district-level lookup table recording, for each (district, cell) pair, the fraction of the cell area that falls within that district. Separately, the 7,137 block polygons are intersected with the same grid to produce an equivalent block-level lookup. When a climate index field is computed on the grid, it is aggregated to district values using the district lookup and to block values using the block lookup — two independent aggregation passes over the same underlying grid field. As a result, a district's composite score is not necessarily equal to the area-weighted average of its constituent blocks' composite scores — the two values are independently derived from the same underlying grid and will generally differ slightly due to the difference in polygon geometry at each level.

### 4.3 Period Aggregation and Ensemble Handling

**Temporal aggregation chain**

For each metric, each GCM, and each scenario, the pipeline applies a three-stage temporal aggregation:

1. **Daily → annual index.** For each calendar year $y$, the daily gridded data for that year (and, for some metrics, the preceding year) are reduced to a single annual index value per cell. The specific reduction depends on the metric family — annual mean, exceedance count, peak intensity, SPI transform, and so on — and is defined metric by metric in §5. The output is a per-cell annual index field.

2. **Annual → period mean.** For each analysis period (e.g. 2020–2040), the per-cell annual index values for all years within that period are averaged to produce a single per-cell period-mean field. The averaging is an unweighted arithmetic mean over years.

3. **Period mean → ensemble mean.** For each (scenario, period) combination, the 24 per-model period means are averaged to produce the ensemble-mean field. In addition to the ensemble mean, the pipeline retains ensemble spread statistics — standard deviation, median, and 5th and 95th percentile across models. The composite and bundle scores described in §6 and §7 use the **ensemble mean** only; ensemble spread is retained for diagnostic and uncertainty-characterisation purposes but is not surfaced in the current composite outputs.

This three-stage chain is designed to separate climate signal from noise at two distinct scales. Averaging annual index values over each multi-decadal window (stage 2) filters out interannual variability driven by modes such as ENSO and the Indian Ocean Dipole, dampening the imprint of any particular sequence of years and bringing out the underlying forced climate change signal. Santer et al. (2011) demonstrate that signal-to-noise ratios in *global-mean* atmospheric temperature trends are below 1 at 10-year timescales but exceed 3.9 at 32-year trends, and that at least 17 years of data are required to reliably distinguish the forced climate change signal from internal variability noise — a *trend-detection* timescale invoked here to motivate *period-mean* estimation. These detection results are derived for global-mean lower-tropospheric temperature, where internal variability is heavily averaged down; at the district and block scale interannual noise is proportionally larger and the time of emergence correspondingly later (§3.4), so multi-decadal averaging *dampens* — rather than fully isolates — the interannual variability in the local indices. Hawkins and Sutton (2012) formalise this as the *time of emergence* — the point at which the forced signal rises detectably above the background noise of natural variability — which multi-decadal period averaging is designed to approach.

Averaging across 24 GCMs (stage 3) reduces sensitivity to the structural biases of any individual model; Tebaldi and Knutti (2007) provide the foundational treatment of this argument, showing that multi-model ensemble means systematically outperform individual model projections because model-specific errors arising from different structural choices are partially uncorrelated across the ensemble and therefore partially cancel in the mean. The two operations are applied in sequence — time-averaging first, then ensemble-averaging — so that each model's period mean contributes equally to the ensemble average regardless of its interannual variance.

> Santer, B. D., Mears, C., Doutriaux, C., Caldwell, P., Gleckler, P. J., Wigley, T. M. L., Solomon, S., Gillett, N. P., Ivanova, D., Karl, T. R., Lanzante, J. R., Meehl, G. A., Stott, P. A., Taylor, K. E., Thorne, P. W., McCarthy, M. P., and Wehner, M. F. (2011). Separating signal and noise in atmospheric temperature changes: The importance of timescale. *Journal of Geophysical Research: Atmospheres*, 116, D22105. https://doi.org/10.1029/2011JD016263

> Hawkins, E. and Sutton, R. (2012). Time of emergence of climate signals. *Geophysical Research Letters*, 39, L01702. https://doi.org/10.1029/2011GL050087

> Tebaldi, C. and Knutti, R. (2007). The use of the multi-model ensemble in probabilistic climate projections. *Philosophical Transactions of the Royal Society A*, 365, 2053–2075. https://doi.org/10.1098/rsta.2007.2076

**Analysis periods**

| Scenario | Period |
|----------|--------|
| Historical | 1990–2010 |
| SSP2-4.5 and SSP5-8.5 | 2020–2040 |
| SSP2-4.5 and SSP5-8.5 | 2040–2060 |
| SSP2-4.5 and SSP5-8.5 | 2060–2080 |

Each future window is an inclusive 21-year mean (e.g. 2020–2040 covers 2020 through 2040), with the endpoint years 2040 and 2060 each shared by two adjacent windows (→ §2.2).

**Historical scenario: no splice required**

The historical anchor period (1990–2010) falls entirely within the historical simulation run (1950–2014). No splicing of historical and SSP files is required for the anchor period. SSP projection files begin in 2015 and are used exclusively for the 2020–2040, 2040–2060, and 2060–2080 windows.

**Composite normalization (per-period spatial ranking)**

For each (scenario, period) combination, the ensemble-mean metric values across all administrative units are normalised onto a [0, 100] scale using the **spatial minimum and maximum** of that same (scenario, period) slice. Let $v_i$ be the ensemble-mean value for unit $i$, and let $v_{\min}$ and $v_{\max}$ be the minimum and maximum of $v_i$ across all units with finite values in that slice. The normalised score is:

$$S_i = \operatorname{clip}\!\left(\frac{v_i - v_{\min}}{v_{\max} - v_{\min}},\; 0,\; 1\right) \times 100$$

For metrics where a lower value indicates greater hazard, the score is inverted: $S_i = (1 - \text{scaled}) \times 100$ before clipping. If all units share an identical value ($v_{\max} = v_{\min}$), all receive a score of 50.

This per-period spatial normalization means that a score of, say, 70 for a given unit in the 2040–2060 SSP5-8.5 period indicates that this unit sits at the 70th percentile of the national distribution in that period — not that it is 70% above its own historical baseline. Scores are not directly comparable across different metrics or bundles unless the normalization ranges are understood. The weighted bundle composite (→ §6.3) aggregates these per-metric scores into a single bundle-level value on the same [0, 100] scale.

---

## 5. Individual Metric Definitions

The five subsections below correspond to distinct data sources and hazard families. Derivations are given for IRT-specific and non-standard indices; for canonical ETCCDI indices, the definition is cited and only IRT-specific parameter choices are noted. A complete metric reference table — slug, label, variable(s), definition, units, baseline period, and bundle memberships — is given in Appendix A. Metrics are grouped here by the thematic bundle they feed (Heat Risk, Heat Stress, Cold Risk, Extreme Rainfall, Drought Risk, Riverine Flood); the bundles themselves — their membership, normalization, and weighting — are defined in §6.

### 5.1 Temperature and Heat Metrics

Sustained temperature extremes drive three of the six thematic bundles: Heat Risk (daytime and nocturnal extremes, heatwave characteristics), Heat Stress (humid-heat metrics defined in §5.4, combined with two dry-heat persistence metrics — WSDI and TN90p — that it shares with Heat Risk and which are defined below), and Cold Risk (winter cold extremes and cold-spell persistence). All temperature metrics are derived from daily mean (`tas`), maximum (`tasmax`), or minimum (`tasmin`) temperature, each converted from Kelvin to Celsius before index calculation.

Metrics are organised in five groups across the three bundles:

- **Background means.** Annual and seasonal mean temperature. Summer is defined as **March–May (MAM)**; winter as **December–January–February (DJF)**.
- **Absolute extremes.** TXx (annual maximum of tasmax), TNx (warmest night of tasmin), TNn (coldest night of tasmin), and DJF minimum of tasmin — standard ETCCDI indices (Zhang et al. 2011) recording the most extreme individual day of the year.
- **Threshold-frequency indices.** Counts of days crossing fixed thresholds. Hot-day thresholds (TX ≥ 30°C, TX ≥ 35°C) follow IMD operational criteria; tropical-night thresholds (TN > 25°C for Heat Risk; TN > 28°C for Heat Stress) are India-adapted. Cold thresholds (TN ≤ 10°C, TN ≤ 5°C, TX ≤ 15°C) are calibrated to the cold-season climate of the Indian plains.
- **Percentile-relative indices.** TX90p, TN90p, TX10p, TN10p express the fraction of days exceeding or falling below a locally calibrated day-of-year (DOY) percentile threshold. These capture relative shifts in the local temperature distribution regardless of absolute temperature level.
- **Heatwave and cold-spell characteristics.** WSDI (warm spell days), CSDI (cold spell days), hwfi (heatwave spell days), hwfi_events (heatwave event count), hwa (heatwave amplitude), and tnle10_consecutive (longest consecutive cold-night run).

**DOY percentile threshold framework**

All percentile-relative and spell metrics share a common threshold derivation aligned with the ETCCDI TX90p method (Zhang et al. 2011). For each grid cell and calendar day $d = 1, \ldots, 365$ (February 29 excluded), all baseline-period daily values falling within a symmetric ±2-day window centred on $d$ are pooled. For the 21-year baseline this yields approximately 105 values per day-of-year. The $p$-th percentile of this pooled set is the threshold $\tau_d$:

$$\tau_d = \text{quantile}_p\!\bigl(\{x_{y,d'} : y \in [y_1, y_2],\; |d' - d| \leq 2\}\bigr)$$

where $d'$ is measured on the 365-day no-leap calendar and $[y_1, y_2]$ is the baseline period. During evaluation, day $t$ with value $x_t$ is classified as an exceedance when $x_t > \tau_{d(t)}$ (strict greater-than throughout). The threshold vector $\tau_d$ is computed once per (model, variable, baseline configuration) and applied unchanged to all evaluation years including SSP projections.

The percentile differs between warm and cold index families, but the baseline period is held fixed:

| Index family | Percentile | 
|---|---|
| TX90p, TN90p, WSDI, hwfi, hwa | 90th | 
| TX10p, TN10p, CSDI | 10th | 

**Baseline period.** A single reference period — **1990–2010** (21 years) — is used throughout for every percentile threshold and distribution fit: the temperature percentile and spell indices above, the precipitation percentile indices of §5.2, and the SPI calibration of §5.3. The same window also serves downstream as the **historical reference period** (§2.2/§4.3): the period-over-period change signals in the sectoral rules of §7, and the historical state reported alongside each projection, are measured against 1990–2010. These two uses share one set of years but act at different stages — as a *baseline* it calibrates each index's internal thresholds and fits (the subject of this section); as a *reference period* it anchors change comparisons after the indices are computed. The thematic composites of §6 are a separate case: they are normalized within each period against the spatial spread of units, **not** against the 1990–2010 anchor (see §6.2). Holding one window fixed across these roles keeps the relative indices and cross-period comparisons mutually consistent.

**Spells.** Several indices count *spells* — maximal runs of consecutive days that satisfy an exceedance condition. The minimum qualifying run length differs by index: WSDI and CSDI require ≥6 consecutive days (ETCCDI convention), whereas the heatwave indices hwfi and hwa require ≥5 consecutive days — an IRT/ETCCDI-style design choice, not an IMD criterion. (IMD's own heat-wave declaration uses a *two*-consecutive-day duration criterion, declared on the second qualifying day; the 5-day minimum here is the tool's own spell-length convention.) Spells are evaluated within a calendar year; a run is not carried across the year boundary.

**Worked example — TX90p for one cell.** Consider a single 0.25° grid cell and the calendar day 1 May (day-of-year 121). Pooling all daily `tasmax` values from 29 April–3 May across the 21 baseline years 1990–2010 gives 5 × 21 = 105 values; their 90th percentile is the threshold $\tau_{121}$ (say, 41.2°C). Repeating this for every calendar day traces the smooth seasonal threshold curve $\tau_d$. In any evaluation year, TX90p is the percentage of days whose `tasmax` exceeds that day's threshold:

$$\text{TX90p} = 100 \times \frac{1}{N}\sum_t \mathbb{1}\!\left[x_t > \tau_{d(t)}\right]$$

By construction this is ≈ 10% under the baseline climate; a warming year pushes it well above 10%, which is the relative-shift signal the index is designed to capture.

> **[FIGURE TO INSERT]** Day-of-year 90th-percentile threshold curve $\tau_d$ (annual cycle) for one cell, with a single year's daily `tasmax` overlaid and exceedance days marked — illustrating the TX90p / WSDI / hwa threshold framework shared across §5.1.

**Heatwave amplitude (hwa)**

Heatwave amplitude is an IRT-specific index. For each year and grid cell, the DOY-90th-percentile framework (baseline 1990–2010, applied to tasmax, minimum spell length 5 consecutive days) identifies all heatwave spells $s$ within the year. For each spell, the mean daily exceedance above the per-day threshold is:

$$\bar{\epsilon}_s = \frac{1}{|s|} \sum_{t \in s} \bigl(x_t - \tau_{d(t)}\bigr)$$

The spell with the largest mean exceedance $\bar{\epsilon}_{s^*}$ is selected as the "worst" heatwave of the year. The amplitude is the peak daily maximum temperature within that spell:

$$\text{hwa} = \max_{t \in s^*}\, x_t \quad ({}^\circ\text{C})$$

This captures both the persistence and the intensity of the strongest annual heatwave event as a single value in absolute Celsius — not an anomaly relative to a threshold.

> Zhang, X., Alexander, L., Hegerl, G. C., Jones, P., Klein Tank, A., Peterson, T. C., Trewin, B., and Zwiers, F. W. (2011). Indices for monitoring changes in extremes based on daily temperature and precipitation data. *WIREs Climate Change*, 2(6), 851–870. https://doi.org/10.1002/wcc.147

### 5.2 Precipitation and Extreme Rainfall Metrics

The precipitation indices characterise rainfall intensity and accumulation. All six Extreme Rainfall | Flash Flood Risk bundle metrics are derived from the daily precipitation variable `pr` (converted from kg m⁻² s⁻¹ to mm day⁻¹). Four are canonical ETCCDI indices with no IRT-specific departures: **Rx1day** (annual maximum 1-day total), **Rx5day** (annual maximum 5-day running total), **R20mm** (count of days with precipitation ≥ 20 mm), and **CWD** (maximum consecutive wet days, where a wet day is any day with precipitation ≥ 1 mm). See Appendix A for parameter details.

The two percentile-based indices require a baseline:

**R95p** is the annual total precipitation contributed by very wet days — days whose daily precipitation exceeds the 95th percentile of the wet-day precipitation distribution in the baseline period 1990–2010. Wet days are defined as days with precipitation ≥ 1 mm. The 95th-percentile threshold is a single per-cell scalar computed from pooling all wet-day values across the full baseline (not DOY-specific). For each evaluation year, precipitation is accumulated over all days that exceed this threshold.

**R95pTOT** expresses the fraction of annual wet-day precipitation contributed by very wet days:

$$\text{R95pTOT} = \frac{\text{R95p}}{\text{PRCPTOT}} \times 100 \quad (\%)$$

where PRCPTOT is the annual total precipitation on wet days (≥ 1 mm). Both indices use baseline period 1990–2010.

### 5.3 Drought Indices (SPI)

Where §5.2 captures rainfall excess, drought is its slow, accumulated counterpart. The Drought Risk bundle uses the **Standardised Precipitation Index** (SPI; McKee et al. 1993), a dimensionless probabilistic drought index that expresses accumulated monthly precipitation as a standard-normal departure from the long-term fitted distribution. IRT computes SPI at three accumulation timescales — SPI-3 (seasonal), SPI-6 (meteorological), and SPI-12 (long-term) — all on the 0.25° grid before spatial aggregation. Longer timescales carry higher bundle weight, reflecting the greater agricultural and hydrological impact of sustained multi-month drought.

**Derivation**

Monthly precipitation totals are accumulated over rolling $k$-month windows (where $k = 3$, $6$, or $12$). The resulting monthly series for each cell is fitted to a two-parameter **Gamma distribution** over the calibration period 1990–2010 using the Method of Moments estimator:

$$f(x;\, \alpha, \beta) = \frac{x^{\alpha-1}\, e^{-x/\beta}}{\beta^\alpha\, \Gamma(\alpha)}, \quad x > 0$$

Zero-precipitation months occur with probability $q = P(x = 0)$, estimated as the fraction of zero months in the baseline. The Gamma fit applies to positive-precipitation months only. The mixed cumulative distribution function is:

$$H(x) = q + (1 - q)\, G(x;\, \alpha, \beta)$$

SPI is obtained by mapping this CDF through the standard normal quantile function:

$$\text{SPI} = \Phi^{-1}\!\bigl(H(x)\bigr)$$

The Gamma parameters ($\alpha$, $\beta$, $q$) are estimated once from the 1990–2010 historical run and applied unchanged to SSP future data, preserving cross-period comparability of SPI values. The implementation uses the open-source `climate-indices` package (Monocongo 2021) with Method of Moments fitting.

**Bundle metrics**

The monthly SPI series is not used directly in composites. Two annual aggregation statistics are derived per cell, per year:

- **Count of drought events** (`spi{k}_count_events_lt_minus1`): number of contiguous episodes per year during which SPI remains continuously below −1, averaged over the 20-year analysis period. SPI < −1 corresponds to the 15.9th percentile of the standard normal — conditions that occur in approximately one year in six under the baseline climatology.
- **Maximum drought spell** (`spi{k}_max_spell_lt_minus1`): longest consecutive-month period per year during which SPI is continuously below −1, expressed as the period maximum over the analysis window (not the mean).

These per-cell annual metric fields are then area-weighted and aggregated to administrative units following the procedure in §4.2.


> McKee, T. B., Doesken, N. J., and Kleist, J. (1993). The relationship of drought frequency and duration to time scales. *Proceedings of the 8th Conference on Applied Climatology*, 17–22 January, Anaheim, California. American Meteorological Society, 179–183.

### 5.4 Wet-Bulb Temperature and Humid Heat Metrics

Humid-heat metrics couple temperature with humidity. The body's primary cooling mechanism under heat stress is evaporative sweat loss; at high humidity this mechanism is impaired, generating physiological strain at air temperatures well below those dangerous in dry conditions. Wet-bulb temperature ($T_{wb}$) integrates both air temperature and ambient humidity into a quantity directly proportional to the ambient evaporative cooling capacity. Sherwood and Huber (2010) established 35°C wet-bulb as the theoretical limit of human thermoregulation — the level above which the body can no longer shed metabolic heat even at rest in shade — and Raymond et al. (2020) documented that this limit has begun to be approached, and briefly exceeded, in parts of South Asia and the Persian Gulf. Subsequent empirical work has revised the practical survivability ceiling downward (to roughly 31°C for young, healthy adults under exertion; Vecellio et al. 2022), a revision that §7.4 treats as a cautionary precedent for any threshold the tool adopts. IRT uses working thresholds of 28°C and 30°C, corresponding to severe and very severe occupational heat stress relevant to India's outdoor labour conditions.

**Stull (2011) approximation**

$T_{wb}$ is computed from daily near-surface air temperature ($T$ in °C, from `tas`) and near-surface relative humidity ($RH$ in %, from `hurs`) using the empirical approximation of Stull (2011):

$$T_{wb} = T \cdot \arctan\!\bigl(0.151977\,\sqrt{RH + 8.313659}\,\bigr) + \arctan(T + RH) - \arctan(RH - 1.676331) + 0.00391838 \cdot RH^{1.5} \cdot \arctan(0.023101\,RH) - 4.686035$$

This approximation has a mean absolute error below 0.3°C relative to the psychrometric wet-bulb across its validity range ($-20°\text{C} \leq T \leq 50°\text{C}$, $5\% \leq RH \leq 99\%$, excepting jointly low-humidity and cold conditions), covering the $0°\text{C}$–$50°\text{C}$ range of tropical and subtropical conditions in India. $T_{wb}$ is computed day-by-day from the daily `tas` and `hurs` fields before spatial aggregation, consistent with the grid-first architecture (→ §4.1). The summer season for the summer-mean wet-bulb metric is March–May (MAM), consistent with the summer temperature means in §5.1.

> Stull, R. (2011). Wet-bulb temperature from relative humidity and air temperature. *Journal of Applied Meteorology and Climatology*, 50(11), 2267–2269. https://doi.org/10.1175/JAMC-D-11-0143.1

> Sherwood, S. C., and Huber, M. (2010). An adaptability limit to climate change due to heat stress. *Proceedings of the National Academy of Sciences*, 107(21), 9552–9555. https://doi.org/10.1073/pnas.0913352107

> Raymond, C., Matthews, T., and Horton, R. M. (2020). The emergence of heat and humidity too severe for human tolerance. *Science Advances*, 6(19), eaaw1838. https://doi.org/10.1126/sciadv.aaw1838

> Vecellio, D. J., Wolf, S. T., Cottle, R. M., and Kenney, W. L. (2022). Evaluating the 35°C wet-bulb temperature adaptability threshold for young, healthy subjects (PSU HEAT Project). *Journal of Applied Physiology*, 132(2), 340–345. https://doi.org/10.1152/japplphysiol.00738.2021

### 5.5 Riverine Flood Metrics (JRC)

Riverine flood metrics depart from the climate grid entirely. They are derived from the CEMS-GloFAS RP-100 raster layers (→ §2.4) and are static snapshots with no SSP scenario dimension. All three metrics are computed directly on the raster and then aggregated to administrative polygons; they do not pass through the 0.25° climate grid.

**jrc_flood_depth_rp100** — Mean peak flood depth. At block level: the 95th percentile of positive flooded-cell depth values within the polygon, capturing the severe tail of the inundation depth distribution. At district level: the flooded-area-weighted mean of constituent block p95 values. Units: metres.

**jrc_flood_extent_rp100** — Share of the polygon's total area covered by positive modelled flood depth, displayed as a percentage. Units: fraction (shown as %).

**jrc_flood_depth_index_rp100** — Composite severity class (ordinal 1–5: Very Low, Low, Moderate, High, Extreme). Each block's RP-100 flood depth and flood extent are first binned independently into 1–5 classes, then combined through a fixed 5×5 lookup matrix.

Depth classes (metres): ≤ 0.2 → 1; ≤ 0.5 → 2; ≤ 1.0 → 3; ≤ 2.5 → 4; > 2.5 → 5.
Extent classes (flooded fraction of polygon): ≤ 0.01 → 1; ≤ 0.05 → 2; ≤ 0.15 → 3; ≤ 0.25 → 4; > 0.25 → 5.

The severity class is read from the matrix (rows = extent class, columns = depth class):

| Extent ↓ \ Depth → | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| **1** | 1 | 2 | 2 | 3 | 4 |
| **2** | 2 | 2 | 3 | 4 | 4 |
| **3** | 2 | 3 | 4 | 4 | 5 |
| **4** | 3 | 4 | 4 | 5 | 5 |
| **5** | 4 | 5 | 5 | 5 | 5 |

Severity is scored at **block** level. District severity is the **flooded-area-weighted mean of constituent block severity classes** (and is therefore generally non-integer), computed bottom-up because directly classifying district-scale depth and extent collapses most districts to the lowest classes — district polygons are far larger than the block scale at which the bins were calibrated.

---

## 6. Thematic Bundle Construction

Each of the six hazard families defined in §5 is condensed into a single composite **bundle score** on a 0–100 scale, computed independently for every geography, scenario, and time period. Two steps produce that score: each component metric is first normalized onto a common 0–100 *higher-is-worse* scale (§6.2), and the normalized components are then combined with fixed weights (§6.3). This thematic framework — a weighted average of co-normalized climate metrics — is distinct from the sectoral hazard-pressure framework of §7, which scores exposure to curated hazard rules rather than compositing metrics directly.

### 6.1 Bundle Taxonomy and Grouping Rationale

The six thematic bundles and the hazard dimension each captures:

| Bundle | Hazard dimension | Component metric families (§5) |
|---|---|---|
| Heat Risk | Daytime/nocturnal thermal extremes and background heat | Background means, absolute & percentile extremes, threshold-frequency, heatwave characteristics (§5.1) |
| Heat Stress | Humid-heat physiological stress | Wet-bulb means/extremes (§5.4) + shared dry-heat persistence WSDI, TN90p (§5.1) |
| Cold Risk | Winter cold extremes and cold-spell persistence | Background cold, absolute extremes, cold-day thresholds, percentile-relative cold, cold-spell characteristics (§5.1) |
| Drought Risk | Meteorological drought across timescales | SPI-3/6/12 event counts and maximum spell lengths (§5.3) |
| Extreme Rainfall \| Flash Flood Risk | Extreme precipitation and wet-spell persistence | Peak intensity, heavy-rain frequency, very-wet contribution, wet-spell persistence (§5.2) |
| Riverine Flood | Static RP-100 inundation severity | JRC flood severity index (§5.5) |

The grouping logic is consistent across bundles: the members of a bundle measure **complementary facets of one hazard** — magnitude (e.g. TXx), frequency (e.g. hot-day counts), persistence (e.g. WSDI), and percentile-relative shift (e.g. TX90p) — rather than redundant restatements of the same signal. Compositing these facets dampens the idiosyncratic noise of any single index and yields a more stable hazard ranking. **Riverine Flood is the structural exception**: it carries a single scored metric (the JRC severity index, weight 1.0), so its "composite" is a pass-through of that one index; the two companion JRC fields (depth, extent) are retained as display attributes at weight 0 (§6.4).

### 6.2 Normalization: Per-Period Spatial Scaling

Every component metric is normalized **independently within each scenario–period column**, across the set of geographies in the computed frame — the districts, or the blocks, of one state at the chosen level. The shipped method is a cross-sectional **min–max rescaling** onto 0–100, oriented so that higher always means worse.

For a metric with finite values $v_i$ over the geography set $G$ in a given scenario–period:

$$v_{\min} = \min_{i\in G} v_i, \qquad v_{\max} = \max_{i\in G} v_i$$

$$S_i = \operatorname{clip}\!\left(\frac{v_i - v_{\min}}{v_{\max} - v_{\min}},\; 0,\; 1\right)\times 100$$

For metrics whose directionality is *lower-is-worse* (e.g. winter-temperature means, where colder is the hazard), the numerator is replaced by $v_{\max}-v_i$ so that the worst tail still maps to 100. Two degenerate cases are handled explicitly: if every geography shares one finite value ($v_{\max}=v_{\min}$), all rows receive **50**; if no finite value exists, the score is NaN.

Three consequences follow from normalizing **per period**:

- A bundle score is **relative, not absolute**. A district scoring 90 is among the most exposed *of its state's districts for that scenario and period* — it is not a physical magnitude, and it is **not** a change-versus-history signal.
- Because each scenario–period is rescaled on its own min/max, scores are comparable *within* a period across space; absolute score differences *between* periods reflect the shifting spatial spread, not only the change in the underlying hazard.
- The 1990–2010 anchor period plays **no role** in this normalization. A baseline-anchored variant — scaling every period against the fixed 1990–2010 historical range to produce a true change signal — exists as a dormant capability but is **not** used by any shipped thematic bundle. It is a dormant capability, noted here only to avoid confusion with the change-based sectoral rules of §7.

The **Riverine Flood** bundle is a static snapshot: the JRC RP-100 severity index has no scenario or future-period dimension, so it is normalized once over the geography set on the same min–max scale (higher severity → worse), with no period anchoring.

### 6.3 Weighted Composite Methodology

Within a bundle the normalized component scores are combined as a **weighted mean**, renormalized per row over the components actually present:

$$\text{Composite}_g = \frac{\sum_{m \in A_g} w_m\, S_{g,m}}{\sum_{m \in A_g} w_m}$$

where $A_g$ is the set of component metrics with a valid (non-NaN) normalized score for geography $g$, and $w_m$ are the fixed bundle weights (§6.4). Because each $S_{g,m}\in[0,100]$ and the weights are renormalized to sum to 1 over $A_g$, the composite is itself bounded in $[0,100]$ — no separate clipping is required. The count of contributing metrics ($\lvert A_g\rvert$) is persisted alongside each score for transparency.

Per-row renormalization means a geography missing one metric is scored on its remaining metrics rather than being penalized or dropped. The only completeness gate for the shipped per-period bundles is that **at least one** component must be present: a row with every component missing yields NaN. (The stricter "≥ 4 anchored components" floor applies *only* to the dormant baseline-anchored mode of §6.2 and is inactive for the shipped composites.)

Weights are drawn from the approved bundle-weight schedule and sum to 1.0 per bundle. They are organised into **weight groups** that gather related facets together; within a group the weight is, in most bundles, split equally across members, so the group subtotal encodes the relative emphasis placed on that facet of the hazard.

### 6.4 Bundle-by-Bundle Metric Weights

The tables below give the full component weighting for each thematic bundle. Each metric's final weight is the product of its **group weight** (shown in the sub-header) and its **share of that group**:

$$w_m = (\text{group weight}) \times (\text{share of group})$$

In every bundle except Drought Risk the group is split *equally* among its members, so the share is simply $1/n$ for a group of $n$ metrics (e.g. each of the three metrics in Heat Risk's 0.200 "Mean & Background Heat" group takes a $1/3$ share → $0.200 \times \tfrac13 = 0.0667$). Drought Risk is the one exception, with an unequal $2/5$–$3/5$ split inside each timescale (below). Every bundle's final weights sum to 1.000.

#### Heat Risk

| Weight group | Metric | Share of group | Weight |
|---|---|---|---|
| **Mean & Background Heat (0.200)** | Annual Mean Temperature (TM) | 1/3 | 0.0667 |
| | Summer Max Temperature (MAM) | 1/3 | 0.0667 |
| | Summer Mean Temperature (MAM) | 1/3 | 0.0667 |
| **Extremes (0.250)** | Annual Maximum Temperature (TXx) | 1/3 | 0.0833 |
| | Warm Nights (TN90p) | 1/3 | 0.0833 |
| | Heatwave Amplitude | 1/3 | 0.0833 |
| **Threshold-based Frequency (0.200)** | Hot Days (TX ≥ 30°C) | 1/3 | 0.0667 |
| | Extreme Heat Days (TX ≥ 35°C) | 1/3 | 0.0667 |
| | Tropical Nights (TN > 25°C) | 1/3 | 0.0667 |
| **Percentile Extremes (0.150)** | Heat Wave Frequency Index (days) | 1/2 | 0.0750 |
| | Heat Wave Frequency (events) | 1/2 | 0.0750 |
| **Heatwave Characteristics (0.200)** | Warm Spell Duration Index (WSDI) | 1/3 | 0.0667 |
| | Warmest Night (TNx) | 1/3 | 0.0667 |
| | Hot Days (TX90p) | 1/3 | 0.0667 |

#### Heat Stress

| Weight group | Metric | Share of group | Weight |
|---|---|---|---|
| **Background humid heat (0.200)** | Wet-Bulb Temperature (Annual Mean) | 1/2 | 0.1000 |
| | Wet-Bulb Temperature (Summer Mean, MAM) | 1/2 | 0.1000 |
| **Extreme / threshold humid heat (0.400)** | Wet-Bulb Temperature (Annual Max) | 1/3 | 0.1333 |
| | Heat Stress Days (Twb ≥ 28°C) | 1/3 | 0.1333 |
| | Wet-Bulb Days (Twb ≥ 30°C) | 1/3 | 0.1333 |
| **Night-time recovery stress (0.200)** | Tropical Nights (TN > 28°C) | 1/2 | 0.1000 |
| | Warm Nights (TN90p) | 1/2 | 0.1000 |
| **Persistence (0.200)** | Warm Spell Duration Index (WSDI) | 1/1 | 0.2000 |

#### Cold Risk

| Weight group | Metric | Share of group | Weight |
|---|---|---|---|
| **Background Cold (0.200)** | Winter Mean Temperature (DJF) | 1/2 | 0.1000 |
| | Winter Min Temperature (DJF) | 1/2 | 0.1000 |
| **Absolute Extremes (0.200)** | Annual Minimum of Tmin (TNn) | 1/2 | 0.1000 |
| | Winter Minimum Tmin (DJF) | 1/2 | 0.1000 |
| **Threshold-based Cold Days (0.250)** | Cold Nights (TN ≤ 10°C) | 1/3 | 0.0833 |
| | Severe Cold Nights (TN ≤ 5°C) | 1/3 | 0.0833 |
| | Cold Days (TX ≤ 15°C) | 1/3 | 0.0833 |
| **Relative Cold (0.150)** | Cool Days (TX10p) | 1/2 | 0.0750 |
| | Cool Nights (TN10p) | 1/2 | 0.0750 |
| **Cold Spell Characteristics (0.200)** | Cold Spell Duration Index (CSDI) | 1/2 | 0.1000 |
| | Consecutive Cold Nights (TN ≤ 10°C) | 1/2 | 0.1000 |

#### Drought Risk

Drought Risk is the only bundle with an unequal within-group split: inside each SPI timescale the maximum-spell metric takes a **3/5** share and the event count **2/5**, so duration outweighs frequency. The group weights themselves rise with accumulation window (SPI-12 > SPI-6 > SPI-3), reflecting the greater impact of sustained, long-accumulation drought.

| Weight group | Metric | Share of group | Weight |
|---|---|---|---|
| **Seasonal Drought — SPI-3 (0.200)** | SPI-3 drought event count (SPI < −1) | 2/5 | 0.0800 |
| | SPI-3 maximum drought spell | 3/5 | 0.1200 |
| **Meteorological Drought — SPI-6 (0.300)** | SPI-6 drought event count (SPI < −1) | 2/5 | 0.1200 |
| | SPI-6 maximum drought spell | 3/5 | 0.1800 |
| **Long-term Drought — SPI-12 (0.500)** | SPI-12 drought event count (SPI < −1) | 2/5 | 0.2000 |
| | SPI-12 maximum drought spell | 3/5 | 0.3000 |

#### Extreme Rainfall | Flash Flood Risk

| Weight group | Metric | Share of group | Weight |
|---|---|---|---|
| **Peak Intensity (0.250)** | Maximum 1-day Precipitation (Rx1day) | 1/2 | 0.1250 |
| | Maximum 5-day Precipitation (Rx5day) | 1/2 | 0.1250 |
| **Heavy Rain Frequency (0.250)** | Very Heavy Precipitation Days (R20mm) | 1/1 | 0.2500 |
| **Very Wet Contribution (0.250)** | Very Wet Day Precipitation (R95p) | 1/2 | 0.1250 |
| | Very Wet Day Contribution (R95pTOT) | 1/2 | 0.1250 |
| **Wet-spell Persistence (0.250)** | Consecutive Wet Days (CWD) | 1/1 | 0.2500 |

#### Riverine Flood

| Weight group | Metric | Share of group | Weight |
|---|---|---|---|
| **Inundation Severity (1.000)** | Flood Severity Index (RP-100) | 1/1 | 1.0000 |
| **Inundation Depth** (display attribute) | RP-100 Flood Depth | — | 0.0000 |
| **Inundation Extent** (display attribute) | RP-100 Flood Extent | — | 0.0000 |

The Riverine Flood composite is fully determined by the single JRC severity index (§5.5); the depth and extent fields are carried at weight 0 for display and drill-down only and do not affect the score.

---

## 7. Sectoral Bundle Construction

The eight **sectoral bundles** answer a different question from the thematic bundles of §6. A thematic bundle co-normalizes the metrics of *one hazard family* and averages them. A sectoral bundle instead scores a sector's exposure to a **curated set of hazard pressures drawn from across families**, and evaluates each pressure through a *rule* that fuses three readings of the same metric: its absolute magnitude, its projected change versus the historical baseline, and — where a defensible danger threshold exists — its position within a fixed harm **impact band**. The output is still a 0–100 *higher-is-worse* score per geography, scenario, and period, but the construction is a blended-rule pipeline rather than a weighted average of co-normalized metrics.

> **Scope caveat (Phase 1).** These are **sector climate hazard-pressure** scores. They characterise the climatic *hazard* a sector faces; they do **not** yet incorporate exposure, vulnerability, or adaptive capacity, and are therefore not full sectoral risk scores in the technical sense. This boundary is intrinsic to the current compute path.

### 7.1 Sector Hazard-Pressure Framework

The eight sectoral bundles and their rule counts:

| Bundle | Rules |
|---|---|
| Agricultural Risk | 7 |
| Health Risk | 5 |
| Industrial Risk | 4 |
| Investment / Financial Risk | 5 |
| Infrastructure Risk | 3 |
| Asset Risk (Thermal Power Plants) | 3 |
| Asset Risk (Hydropower Plants) | 3 |
| Life & Livelihood Loss Risk | 4 |

Every bundle is an **ordered set of rules**; each rule binds exactly one source metric (§5) to a scoring recipe and an explicit *rule weight*. All eight bundles use explicit, normalized rule weights that sum to 1.0 (§7.5). All are computed at both district and block level, for scenarios **SSP2-4.5** and **SSP5-8.5**, over the future periods **2020–2040, 2040–2060, 2060–2080**; the historical 1990–2010 window enters only as the change-lens baseline, not as a published period.

A rule's selection encodes a deliberate sector judgement — that a given hazard *matters to that sector* — so the same metric can appear in several bundles under different rule weights and different impact bands, reflecting the sector-specific consequence rather than a single universal harm.

### 7.2 The Blended Rule: Absolute Pressure + Change + Impact Band

A rule produces up to three **lens scores**, each on a 0–100 *higher-is-worse* scale, which are then weighted into a single rule score. The lens weights ($\omega_{\text{abs}}, \omega_{\text{chg}}, \omega_{\text{imp}}$) are declared per rule and sum to 1.0; a lens with weight 0 is simply not evaluated. Throughout, all shipped rules are *higher-worse* (the lower-is-worse direction is supported but unused in the current catalog).

The three lenses are not an arbitrary decomposition: each answers a different, complementary question about the same metric, and each draws on an established methodological tradition. The lens framework is a structured combination of these traditions rather than a novel scoring invention.

| Lens | Question it answers | Anchored to | Methodological tradition |
|---|---|---|---|
| **Absolute** | How extreme is the projected value relative to its peers? | the peer cohort's spatial distribution (relative) | composite-indicator normalization (OECD/JRC *Handbook on Constructing Composite Indicators*, 2008) |
| **Change** | How much worse is the projected value than its own history? | the 1990–2010 baseline (anomaly) | delta / change-factor method (Anandhi et al. 2011) |
| **Impact** | How far into a physically dangerous range is the value? | a fixed, externally justified threshold band (absolute) | threshold / dose–response impact functions (Gasparrini et al. 2015) |

The **cohort** for the two relative lenses is one `state × level × scenario × period` group: a district is scored against the other districts *of its state*, and a block against the other blocks *of its state* — not only the blocks of its own district, since a single district rarely holds enough blocks for stable deciles. The impact lens needs no cohort; it reads each value against a fixed band.

**Absolute lens — $S_{\text{abs}}$.** The current-period metric value is scaled across the geography set $G$ (the districts, or blocks, of one state) by a **robust p10–p90 rescaling**, not the full min–max of §6. With $q_{10}, q_{90}$ the 10th and 90th percentiles of the finite values over $G$:

$$S_{\text{abs},i} = \operatorname{clip}\!\left(\frac{v_i - q_{10}}{q_{90} - q_{10}},\; 0,\; 1\right)\times 100$$

Clipping at the deciles damps the influence of single-cell outliers on the spatial scale. If $q_{90}\approx q_{10}$ (a spatially flat field) every valid row receives **50**; rows with no finite value are NaN. This robust-quantile choice is the chief normalization difference between the sectoral and thematic frameworks: §6 uses $p_0$–$p_{100}$ (min–max), §7 uses $p_{10}$–$p_{90}$.

**Change lens — $S_{\text{chg}}$.** The lens first forms a per-geography change of the future value against its 1990–2010 baseline column, then scales those *change magnitudes* across $G$ with the same robust p10–p90 scaler. The change mode is metric-dependent:

$$\Delta_i = \begin{cases} v_i^{\text{fut}} - v_i^{\text{base}} & \text{absolute\_delta (temperature-like metrics)}\\[4pt] \dfrac{v_i^{\text{fut}} - v_i^{\text{base}}}{\lvert v_i^{\text{base}}\rvert}\times 100 & \text{relative\_pct (counts, rainfall, spells)} \end{cases}$$

The mode is selected automatically: the absolute delta is used for temperature-like metrics (the daily-mean, daily-maximum, and daily-minimum temperature families and other heat indices) and relative percent otherwise. Relative-percent change guards against tiny denominators ($\lvert v^{\text{base}}\rvert < 10^{-6}\Rightarrow$ NaN) so a near-zero baseline cannot explode the score. Because $S_{\text{chg}}$ is itself a spatial p10–p90 rank of the change, it measures *where a unit sits in the distribution of projected change* — not an absolute warming or wetting magnitude. A missing baseline column drops this lens to NaN (with a build warning) and the rule is scored on its remaining lenses.

**Impact lens — $S_{\text{imp}}$.** This is the only **absolute, non-spatial** lens: it maps the raw metric value onto a fixed physical harm band $[a, b]$ — $a$ the onset of concern, $b$ the saturation/severe threshold — independent of how other geographies score:

$$S_{\text{imp},i} = \operatorname{clip}\!\left(\frac{v_i - a}{b - a},\; 0,\; 1\right)\times 100$$

A value at or below onset scores 0; at or above saturation, 100. The lens is evaluated only when the rule declares a band; regime/proxy metrics with no defensible threshold omit it (impact weight 0). The bands, their provenance, and their confidence grading are the subject of §7.4.

**Rule score.** The lens scores present for a rule are combined as a renormalized weighted mean over the lenses actually available (i.e. non-NaN), exactly mirroring the per-row renormalization of §6.3:

$$S_r = \frac{\sum_{\ell \in L_r}\omega_\ell\, S_{\ell}}{\sum_{\ell \in L_r}\omega_\ell}, \qquad L_r = \{\ell \in \{\text{abs},\text{chg},\text{imp}\} : \omega_\ell > 0 \text{ and } S_\ell \text{ finite}\}$$

so a rule whose change lens is unavailable is scored on absolute (+impact) alone rather than being voided. All three lens scores are persisted alongside the blended rule score for transparency.

**Bundle aggregation and the 0.70 completeness gate.** A bundle composite is the renormalized weighted mean of its rule scores over the rules with a finite score, using the rule weights $W_r$:

$$\text{Composite}_b = \frac{\sum_{r\in R_b} W_r\, S_r}{\sum_{r\in R_b} W_r}, \qquad f_b = \!\!\sum_{r:\,S_r\text{ finite}}\!\! W_r$$

where $f_b$ is the **available-rule-weight fraction** (the share of total rule weight that resolved to a valid score). A composite is published only if $f_b \ge 0.70$ **and** at least one rule is present; otherwise it is set to NaN. The 0.70 floor prevents a sector score from being asserted when nearly a third of its weighted evidence base is missing — a stricter posture than the thematic "≥ 1 component" gate of §6.3, appropriate because sectoral rules are fewer and individually more consequential. Both $f_b$ and the available-rule count are persisted with every composite.

> A second rule type — a **trend** rule, scoring an adverse yearly slope within the future window — is specified but unused, and **no shipped bundle uses it**; every current rule is of the blended type above. It is noted here only for completeness and is excluded from the per-bundle tables.

**Inputs and two standing caveats.** Each metric is reduced to one value per geography from the 24-model ensemble using the **ensemble mean** (the same ensemble-mean statistic used for the thematic scores and the displayed statistic). The multi-model **median** is the methodologically preferred central estimate — robust to a single divergent model, per IPCC practice — but adopting it is a tool-wide change pending its own approval and tests. Separately, the change lens is only as trustworthy as its baseline: it must read the reconciled **1990–2010** historical window, the same period used for the displayed historical-delta columns. The residual code-gap in which some indices still calibrate on 1981–2010 (§5.1) applies to the change lens too; it is flagged here, not silent. Ensemble spread (std, p05/p95, model count) is deliberately **not** folded into the 0–100 score — it is surfaced separately as a confidence annotation, so a "70 with wide model disagreement" and a "70 with tight agreement" remain distinguishable.

### 7.3 Reading the Score: What Each Lens Lets You Compare

Because two of the three lenses are cohort-relative and only the impact lens is absolute, the blended score is a **hybrid** of relative ranking and absolute danger, and not every comparison of it is valid. The absolute lens carries a specific structural blind spot: since its cohort is rebuilt for each scenario–period, a *uniform* escalation — every unit in a state warming by the same amount between two periods — shifts $q_{10}$ and $q_{90}$ together and leaves the normalized scores unchanged. The absolute lens can rank places against one another; it cannot, on its own, show that the future is worse than the present. The change and impact lenses fill that gap, and the **impact lens is the only carrier of absolute escalation** across periods and across states.

| Comparison | Absolute | Change | Impact | Net interpretation |
|---|:--:|:--:|:--:|---|
| Units **within** one `state × level × scenario × period` | ✓ | ✓ | ✓ | fully comparable — a true ranking of units |
| **Across periods**, same state | re-normalized each period | re-normalized | ✓ (fixed band) | only the impact component is comparable; a flat absolute trend means "same rank", not "no warming" |
| **Across states** | each state normalized to itself | each to itself | ✓ (fixed band) | only the impact component is cross-state comparable |

**Worked example — why the blend beats pure-absolute ranking.** Two districts in one state, scenario SSP5-8.5, period 2060–2080, metric **TXx** (annual-maximum daytime temperature), scored through the Health Risk TXx rule (lens weights 0.40 / 0.25 / 0.35, impact band 40–45 °C). Suppose across this cohort projected TXx spans $q_{10}=41$ °C to $q_{90}=46$ °C, and the warming anomaly versus 1990–2010 spans $q_{10}=+1.0$ °C to $q_{90}=+3.5$ °C:

| District | TXx 2060–80 | Anomaly | $S_{\text{abs}}$ | $S_{\text{chg}}$ | $S_{\text{imp}}$ | **Blended** | Pure-absolute |
|---|---:|---:|---:|---:|---:|---:|---:|
| **A** — already hot | 45.5 °C | +1.5 °C | 90 | 20 | 100 | **76** | 90 |
| **B** — fast-warming | 42.0 °C | +3.5 °C | 20 | 100 | 40 | **47** | 20 |

District A is the established heat hazard — hottest among its peers and past the 45 °C heat-wave declaration threshold — and is correctly rated high by either method. District B is the case pure-absolute ranking *hides*: it looks unremarkable relative to peers ($S_{\text{abs}}=20$), but it is warming faster than any of them ($S_{\text{chg}}=100$) and has just crossed the 40 °C heatwave-declaration floor ($S_{\text{imp}}=40$). The blend lifts it to 47 — a mid-range hazard warranting attention — whereas pure-absolute scoring leaves it at 20, mislabelling a fast-warming, newly dangerous district as low priority. The blend thus preserves three decision-relevant signals — current severity, trajectory, and absolute danger-threshold crossing — that a relative ranking alone discards. The cost is interpretability: one number now mixes three signals, which is precisely why the per-lens decomposition is persisted with every rule (§7.2) — a user can read District B's 47 back as $S_{\text{abs}}$ 20 / $S_{\text{chg}}$ 100 / $S_{\text{imp}}$ 40.

### 7.4 The Impact Lens: Bands, Provenance, and Confidence

The impact lens is what distinguishes the sectoral framework from a purely relative ranking: it injects an **absolute, externally meaningful** reading of harm. Where the absolute and change lenses only say *how a unit compares to its neighbours*, the impact lens says *how close the unit is to a recognised danger threshold*, on a fixed scale that does not move with the spatial distribution. Each band is a pair $[a,b]$ — onset $a$ (harm begins) and saturation $b$ (harm is near-complete or sector-dominant) — and the linear interpolation of §7.2 converts the raw value into a 0–100 harm-proximity score.

Bands are graded by the strength of their evidentiary support:

- **HIGH** — anchored on a published, institutionally recognised threshold (e.g. the IMD plains heatwave criterion, or IMD daily-rainfall categories). Treated as external and zone-invariant.
- **MEDIUM** — derived from literature combined with reasoned judgement, often with an institutionally anchored onset and a self-derived saturation.
- **LOW** — self-derived from first principles or indirect evidence where no categorical institutional band exists at the relevant scale. By design these rules carry a **small impact weight** (typically 0.15), so a weakly supported band contributes little to the score.

**Provenance, by tier.** The catalog leans on a small number of **externally anchored, high-confidence** bands that recur across sectors and carry the heaviest impact weight: extreme daytime heat (TXx, IMD plains heatwave **40–45 °C**) and one-day rainfall (Rx1day, IMD very-heavy-to-extremely-heavy **115.6–204.5 mm**). These appear in Health, Industrial, Infrastructure, Asset, and Life-&-Livelihood bundles, each time with the same physical band but a sector-specific consequence. The remaining bands are **self-derived**: a MEDIUM tier derived from literature and reasoned judgement, sometimes with an institutionally anchored cut point — multi-week dry spells (CDD, IMD Agricultural-Drought-anchored **30–90** / **60–120 days**), crop reproductive heat (TXx **35–45 °C**, research-derived onset), warmest-night stress (TNx **28–32 °C**, research-derived onset) — and a LOW tier for indices with no institutional category at all: multi-day rainfall (Rx5day **250–500 mm**, anchored on Kerala 2018 / Mumbai 2005), warm spells (WSDI **6–18 days**), damaging-heat-day counts (**15–60 days**), SPI drought episode/spell counts (**3–12**), peninsular chilling nights (**10–30 days**), consecutive wet days (**7–15 days**), and heatwave-frequency days (**5–15 days**). Finally, three **regime/proxy metrics carry no impact lens** — R99p extreme-wet concentration, the SPI-3 low-flow cooling proxy, and R95p interannual variability — because no defensible danger threshold exists for them; they are scored on absolute and change only.

**Provenance discipline.** Five principles govern how a band may be admitted: (1) a band scores *danger, not unusualness* — emergence-versus-history is the change lens's job, so an impact band may never be built from a percentile or a standardized anomaly, which would duplicate the change lens; (2) external institutional thresholds are preferred, and a self-derived band is admitted only where none exists, through a documented protocol (harm mechanism → nearest external anchors → cut points → confidence → dated provenance); (3) confidence sets the impact weight, so a low-confidence band cannot drive a rule; (4) a borrowed standard may be used *only* in the construction its source defines — the IMD warm-night "+4.5 to +6.4 °C above normal" departure criterion was **rejected** for TNx because it is defined jointly with a same-day Tmax ≥ 40 °C co-condition against a *daily climatological* normal, neither of which holds for an annual-maximum value, so TNx instead uses an absolute 28–32 °C level band; (5) **no phantom thresholds** — a slug naming a number (e.g. `..._ge_45`) must implement that number as a real band with provenance or be renamed. Every band is versioned, dated, and revisable; the downward revision of the once-canonical 35 °C wet-bulb survivability limit is the cautionary precedent, and any band change is itself a methodology change.

The full onset/saturation derivation, source, zone caveat, and confidence grade for every distinct band are catalogued in **Appendix B**, deduplicated across the bundles that share them.

### 7.5 Bundle-by-Bundle Rule Tables and Weights

Two weight layers govern a sectoral score: the **lens split** *within* each rule (§7.2) and the **rule weight** *within* each bundle. The lens splits are not arbitrary per rule — they fall into a handful of recurring **archetypes** tied to band provenance:

| Lens archetype (abs / chg / imp) | Typical use | Rationale |
|---|---|---|
| **0.40 / 0.25 / 0.35** | Rules on an external, HIGH-confidence IMD band (TXx 40–45, Rx1day, TNx) | The trusted danger threshold earns the largest impact weight |
| **0.40 / 0.30 / 0.30** | Rules on an IMD-anchored MEDIUM band (CDD dry spells, crop-heat TXx, livelihood Rx5day/WSDI) | Balanced; the band is defensible but not externally categorical |
| **0.45 / 0.40 / 0.15** | Secondary rules on a self-derived LOW band (WSDI, SPI counts, Rx5day, chilling nights, HWFI) | Small impact weight by design — a weak band contributes little |
| **0.70 / 0.30 / 0.00** | Regime/proxy metrics, no band (SPI low-flow proxy, R95p variability) | Absolute level dominant; emergence supplies a secondary signal |
| **0.40 / 0.60 / 0.00** | Change-dominant regime metric, no band (R99p concentration) | Emergence of tail concentration vs baseline is the decision-relevant signal |

The per-bundle tables below give each rule's source metric, its **rule weight** (summing to 1.0 per bundle), its lens archetype, and its impact band (or "—" where no band applies). Band derivations are in Appendix B.

#### Agricultural Risk

Selects the agronomic stressors of a kharif–rabi cropping system: reproductive-stage heat, damaging heat-day and warm-spell burden, short-window (SPI-3) drought episodes and their longest spell, kharif-waterlogging rainfall, and — peninsular default — horticultural chilling nights. The heaviest weights sit on 5-day rainfall (0.20) and the two drought rules (0.15 each), reflecting the dominance of water extremes in rainfed agriculture.

| Rule | Source metric | Rule weight | Lens archetype | Impact band |
|---|---|---|---|---|
| Peak crop heat | Annual max temperature (TXx) | 0.15 | 0.40 / 0.30 / 0.30 | 35–45 °C |
| Damaging heat days | Extreme heat days (TX ≥ 35 °C) | 0.10 | 0.45 / 0.40 / 0.15 | 15–60 days |
| Persistent heat | Warm spell duration (WSDI) | 0.10 | 0.45 / 0.40 / 0.15 | 6–18 days |
| Drought episodes | SPI-3 drought events | 0.15 | 0.45 / 0.40 / 0.15 | 3–12 events |
| Longest drought spell | SPI-3 max drought spell | 0.15 | 0.45 / 0.40 / 0.15 | 3–12 months |
| 5-day heavy rainfall | Max 5-day precipitation (Rx5day) | 0.20 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Cold nights | Cold nights (TN ≤ 10 °C) | 0.15 | 0.45 / 0.40 / 0.15 | 10–30 days |

#### Health Risk

Targets the climatic drivers of heat mortality and waterborne/vector disruption: extreme daytime heat (the dominant rule at 0.30, on the HIGH-confidence IMD band), night-time heat that denies physiological recovery, warm-spell duration, and the rainfall/standing-water pathway for disease and disruption.

| Rule | Source metric | Rule weight | Lens archetype | Impact band |
|---|---|---|---|---|
| Extreme daytime heat pressure | Annual max temperature (TXx) | 0.30 | 0.40 / 0.25 / 0.35 | 40–45 °C |
| Warm-spell duration pressure | Warm spell duration (WSDI) | 0.12 | 0.45 / 0.40 / 0.15 | 6–18 days |
| Night-time heat pressure | Warmest night (TNx) | 0.18 | 0.40 / 0.25 / 0.35 | 28–32 °C |
| 1-day rainfall disruption pressure | Max 1-day precipitation (Rx1day) | 0.25 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| Consecutive wet-day pressure | Consecutive wet days (CWD) | 0.15 | 0.45 / 0.40 / 0.15 | 7–15 days |

#### Industrial Risk

Scores process-disruption and water/heat-derating pressures on industry: extreme operational heat (the dominant rule at 0.40), one- and five-day rainfall disruption, and prolonged dry spells stressing process-water supply.

| Rule | Source metric | Rule weight | Lens archetype | Impact band |
|---|---|---|---|---|
| 1-day rainfall disruption pressure | Max 1-day precipitation (Rx1day) | 0.25 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| 5-day rainfall disruption pressure | Max 5-day precipitation (Rx5day) | 0.15 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Dry-spell water-stress pressure | Consecutive dry days (CDD) | 0.20 | 0.40 / 0.30 / 0.30 | 30–90 days |
| Extreme heat operations pressure | Annual max temperature (TXx) | 0.40 | 0.40 / 0.25 / 0.35 | 40–45 °C |

#### Investment / Financial Risk

Emphasises *emergence vs the historical baseline* — the signal an investor cares about. It pairs the rainfall-disruption rules with two change-weighted regime metrics: extreme-wet concentration (R99p, change-dominant) and heatwave persistence (HWFI), plus chronic dry-spell water stress for water-intensive assets.

| Rule | Source metric | Rule weight | Lens archetype | Impact band |
|---|---|---|---|---|
| 1-day rainfall disruption pressure | Max 1-day precipitation (Rx1day) | 0.25 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| 5-day rainfall accumulation pressure | Max 5-day precipitation (Rx5day) | 0.15 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Extreme wet precipitation concentration | Extremely wet-day precip (R99p) | 0.10 | 0.40 / 0.60 / 0.00 | — |
| Dry-spell water-stress pressure | Consecutive dry days (CDD) | 0.25 | 0.40 / 0.30 / 0.30 | 30–90 days |
| Heatwave persistence pressure | Heatwave spell days (HWFI) | 0.25 | 0.45 / 0.40 / 0.15 | 5–15 days |

#### Infrastructure Risk

A compact three-rule design centred on rainfall design loads: one-day design rainfall dominates (0.45), followed by five-day accumulation (0.30) and extreme-heat asset stress (0.25).

| Rule | Source metric | Rule weight | Lens archetype | Impact band |
|---|---|---|---|---|
| 1-day rainfall design pressure | Max 1-day precipitation (Rx1day) | 0.45 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| 5-day rainfall design pressure | Max 5-day precipitation (Rx5day) | 0.30 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Extreme heat asset pressure | Annual max temperature (TXx) | 0.25 | 0.40 / 0.25 / 0.35 | 40–45 °C |

#### Asset Risk — Thermal Power Plants

Targets the two climate vulnerabilities of thermal generation: cooling-water availability (dry-spell CDD and an SPI-3 low-flow proxy) and cooling-efficiency loss under extreme heat, weighted roughly evenly (0.35 / 0.35 / 0.30).

| Rule | Source metric | Rule weight | Lens archetype | Impact band |
|---|---|---|---|---|
| Dry-spell cooling-water pressure | Consecutive dry days (CDD) | 0.35 | 0.40 / 0.30 / 0.30 | 30–90 days |
| Extreme heat cooling-efficiency pressure | Annual max temperature (TXx) | 0.35 | 0.40 / 0.25 / 0.35 | 40–45 °C |
| Low-flow drought proxy pressure | SPI-3 moderate-drought months | 0.30 | 0.70 / 0.30 / 0.00 | — |

#### Asset Risk — Hydropower Plants

Scores inflow-driven generation risk: heavy 5-day rainfall stressing spillway/operations (dominant at 0.45), prolonged dry spells cutting reservoir inflow, and the inflow-predictability signal from R95p **interannual variability** — a helper metric (the coefficient of variation of yearly very-wet precipitation, §7.2/Appendix A) sharing the Rx5day/CDD baseline epoch.

| Rule | Source metric | Rule weight | Lens archetype | Impact band |
|---|---|---|---|---|
| 5-day rainfall operations pressure | Max 5-day precipitation (Rx5day) | 0.45 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Dry-spell flow pressure | Consecutive dry days (CDD) | 0.35 | 0.40 / 0.30 / 0.30 | 30–90 days |
| Very wet precipitation variability pressure | R95p inter-annual variability (CV) | 0.20 | 0.70 / 0.30 / 0.00 | — |

#### Life & Livelihood Loss Risk

Captures the direct human-exposure hazards: extreme one- and five-day rainfall (flood exposure), prolonged dry spells driving livelihood/crop failure, and warm-spell heat mortality. One-day rainfall carries the largest weight (0.30).

| Rule | Source metric | Rule weight | Lens archetype | Impact band |
|---|---|---|---|---|
| 1-day rainfall exposure pressure | Max 1-day precipitation (Rx1day) | 0.30 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| 5-day rainfall exposure pressure | Max 5-day precipitation (Rx5day) | 0.25 | 0.40 / 0.30 / 0.30 | 250–500 mm |
| Dry-spell livelihood pressure | Consecutive dry days (CDD) | 0.20 | 0.40 / 0.30 / 0.30 | 60–120 days |
| Warm-spell livelihood pressure | Warm spell duration (WSDI) | 0.25 | 0.40 / 0.30 / 0.30 | 6–18 days |

---

## 8. Composite Score and Output

The **composite score** is the published output of every bundle, thematic and sectoral alike: one 0–100 *higher-is-worse* number per admin unit, scenario, and period. The two construction engines of §6 and §7 differ internally but emit the same object, so a dashboard or export treats all bundle scores uniformly. This section states what that number is, what it is not, and how to read it across the scenario, period, and spatial-level dimensions.

### 8.1 Composite Derivation

For a given admin unit, scenario, and period the composite is produced by whichever engine owns the bundle:

- **Thematic bundles (§6.3):** a per-row-renormalized weighted mean of the bundle's component metric scores, each metric first normalized by **per-period spatial min–max** ($p_0$–$p_{100}$) across the state's units (§6.2). The only completeness gate is "≥ 1 component present."
- **Sectoral bundles (§7.2):** a per-row-renormalized weighted mean of the bundle's **blended rule scores**, where each rule is itself a weighted mean of up to three lenses — absolute (robust $p_{10}$–$p_{90}$), change-vs-baseline, and a fixed-band impact lens. A composite is published only if its rules cover ≥ 70 % of total rule weight.

Both engines bound the result in [0, 100] by construction (the renormalization makes the weights sum to 1 over the present components), so no separate clipping is applied, and both persist the count of contributing components alongside the score. For interpretation the 0–100 range is banded into three tiers — **low (0–33.3), moderate (33.3–66.6), high (66.6–100)** — used consistently wherever the score is classified.

**What the composite is not.** The score is a **multi-metric hazard-pressure index**, not a risk estimate. It is *not* a probability, *not* an annualized expected loss, and *not* a risk score in the IPCC AR6 sense, because it models the **hazard** determinant only — exposure (population, assets, cropped area in the hazard's path) and vulnerability / adaptive capacity are deliberately out of scope in this compute path (§7.1). A bundle labelled "Health Risk" or "Agricultural Risk" should therefore be read as *"climate hazard pressure relevant to that sector,"* not as realised risk. It is also, in part, a **relative** index: the thematic composite and the sectoral absolute/change lenses rank a unit against its cohort rather than against an absolute physical scale (§8.2).

### 8.2 Scenario and Period Handling

Composites are computed **independently for each `(scenario, period)` combination** and persisted to master files that the tool reloads on selection; no scores are recomputed at view time. The published combinations differ between the two engines:

| Engine | Scenarios | Periods |
|---|---|---|
| **Thematic** (§6) | SSP2-4.5, SSP5-8.5, plus **Snapshot** (the static Riverine bundle) | `1990-2010` (historical), `Current`, `2020-2040`, `2040-2060`, `2060-2080` |
| **Sectoral** (§7) | SSP2-4.5, SSP5-8.5 | `2020-2040`, `2040-2060`, `2060-2080` |

The sectoral engine does **not** publish a historical or `Current` period: the 1990–2010 window enters sectoral scores only as the change-lens *baseline* (§7.2), never as an output column. The **Riverine Flood** bundle is the lone static case — a single `Snapshot` published under the `Current` period label (§2.2), with no scenario or future-period dimension (§5.5/§6.2).

**Comparability — read carefully.** Because both engines normalize *within* a cohort, composite scores are **not** directly comparable across periods or states on an absolute scale, despite sharing the 0–100 range:

- **Thematic** composites are re-normalized on each period's own spatial min–max. A unit scoring 70 in `2040-2060` and 70 in `2060-2080` is "near the top of its state's spread *in each of those periods*" — it is **not** a statement that the hazard is unchanged between them, nor that the two 70s denote the same physical magnitude. Only the *within-period* ranking of units is strictly valid.
- **Sectoral** composites blend relative lenses (absolute, change) with the absolute impact lens, so only the **impact component** carries genuine cross-period and cross-state meaning; the blended number mixes ranking and danger and must be read with the lens decomposition (§7.3) when comparing across periods or states.
- A true cross-period change signal for the thematic bundles would require the **baseline-anchored** normalization that exists but is dormant (§6.2); it is noted here as the intended future path, not a current property.

This corrects an earlier framing that treated all `(scenario, period)` scores as sharing a common 1990–2010 normalization anchor and therefore "directly comparable." They do not, and they are not — within-period ranking (both engines) and the impact lens (sectoral) are the comparisons the methodology actually supports.

### 8.3 District vs Block Resolution Behaviour

Both district (ADM2) and block (ADM3) composites are computed **independently from the grid-first index pipeline** — block scores are *not* aggregated down from district scores, nor districts up from blocks; each level area-weights the 0.25° grid to its own polygons (§4.2). All bundles, thematic and sectoral, support both levels.

Two resolution effects follow:

- **Grid coverage.** A district overlaps many 0.25° cells; a small block may overlap only one or two. Block-level scores therefore inherit more spatial variability and are more sensitive to individual grid-cell values and to fractional-overlap artefacts near administrative boundaries. This is a property of the ~25 km native grid, not a defect of the aggregation.
- **Cohort separation.** The normalization cohort (per-period for thematic, the absolute/change lens cohort for sectoral) is the set of units *at that level within the state* (§6.2/§7.2). District scores and block scores are thus normalized against **different cohorts**: a district scoring 80 and a block scoring 80 are not on the same scale, and the two levels should not be cross-compared unit-to-unit. Each level is internally consistent; they are parallel views, not a single nested hierarchy of scores.

---

## Appendix A: Complete Metric Reference

The tables below list every metric that feeds a bundle. Sections A.1–A.5 cover the metrics that appear in a thematic bundle weight entry; **A.6** adds the source metrics used only by the sectoral bundles (§7). Metrics shared across bundles appear once with all bundles noted. Columns: **Slug** (canonical pipeline identifier), **Label** (display name), **Variable(s)** (NEX-GDDP-CMIP6 input or external source), **Definition** (how the annual value is computed), **Units**, **Baseline** (period for percentile/distribution fitting, where applicable), **Bundle(s)**.

Abbreviations: DOY = day-of-year percentile threshold; MAM = March–May; DJF = December–January–February; MoM = Method of Moments.

### A.1 Heat Risk and Heat Stress

| Label | Variable(s) | Definition | Units | Baseline | Bundle(s) |
|---|---|---|---|---|---|
| Annual mean temperature | tas | Arithmetic mean of daily mean temperature | °C | — | Heat Risk |
| Summer (MAM) max temp | tasmax | Mean of daily max temperature in months [3,4,5] | °C | — | Heat Risk |
| Summer (MAM) mean temp | tas | Mean of daily mean temperature in months [3,4,5] | °C | — | Heat Risk |
| Annual max daily max temp (TXx) | tasmax | Annual maximum of daily maximum temperature | °C | — | Heat Risk |
| Warmest night (TNx) | tasmin | Annual maximum of daily minimum temperature | °C | — | Heat Risk |
| Heatwave amplitude | tasmax | Peak daily max temp within the heatwave spell with highest mean exceedance above DOY 90th-pct threshold; min 5 consecutive exceedance days | °C | 1990–2010 | Heat Risk |
| Hot days (TX ≥ 30°C) | tasmax | Count of days where tasmax ≥ 30°C | days | — | Heat Risk |
| Extreme heat days (TX ≥ 35°C) | tasmax | Count of days where tasmax ≥ 35°C | days | — | Heat Risk |
| Tropical nights (TN > 25°C) | tasmin | Count of days where tasmin > 25°C | days | — | Heat Risk |
| Heatwave spell days | tas | Total days inside spells of ≥ 5 consecutive days where tas > DOY 90th-pct threshold | days | 1990–2010 | Heat Risk |
| Heatwave event count | tasmax | Count of distinct spells of ≥ 5 consecutive days where tasmax > DOY 90th-pct threshold | events | 1990–2010 | Heat Risk |
| Warm spell days (WSDI) | tasmax | Count of days inside warm spells of ≥ 6 consecutive days where tasmax > DOY 90th-pct threshold | days | 1990–2010 | Heat Risk, Heat Stress |
| Hot days % (TX90p) | tasmax | Fraction of days where tasmax > DOY 90th-pct threshold; 5-day window | % | 1990–2010 | Heat Risk |
| Warm nights % (TN90p) | tasmin | Fraction of days where tasmin > DOY 90th-pct threshold; 5-day window | % | 1990–2010 | Heat Risk, Heat Stress |
| Annual mean wet-bulb temp | tas, hurs | Annual mean of daily Twb (Stull 2011) | °C | — | Heat Stress |
| Summer (MAM) mean wet-bulb | tas, hurs | Mean of daily Twb (Stull 2011) in months [3,4,5] | °C | — | Heat Stress |
| Annual max wet-bulb temp | tas, hurs | Annual maximum of daily Twb (Stull 2011) | °C | — | Heat Stress |
| Heat stress days (Twb ≥ 28°C) | tas, hurs | Count of days where Twb (Stull 2011) ≥ 28°C | days | — | Heat Stress |
| Severe heat stress days (Twb ≥ 30°C) | tas, hurs | Count of days where Twb (Stull 2011) ≥ 30°C | days | — | Heat Stress |
| Tropical nights (TN > 28°C) | tasmin | Count of days where tasmin > 28°C | days | — | Heat Stress |

### A.2 Cold Risk

| Label | Variable(s) | Definition | Units | Baseline | Bundle(s) |
|---|---|---|---|---|---|
| Winter (DJF) mean temp | tas | Mean of daily mean temperature in months [12,1,2] | °C | — | Cold Risk |
| Winter (DJF) mean min temp | tasmin | Mean of daily minimum temperature in months [12,1,2] | °C | — | Cold Risk |
| Coldest night (TNn) | tasmin | Annual minimum of daily minimum temperature | °C | — | Cold Risk |
| Winter (DJF) min of min temp | tasmin | Minimum of daily minimum temperature in months [12,1,2] | °C | — | Cold Risk |
| Cold nights (TN ≤ 10°C) | tasmin | Count of days where tasmin ≤ 10°C | days | — | Cold Risk |
| Severe cold nights (TN ≤ 5°C) | tasmin | Count of days where tasmin ≤ 5°C | days | — | Cold Risk |
| Cold days (TX ≤ 15°C) | tasmax | Count of days where tasmax ≤ 15°C | days | — | Cold Risk |
| Cool days % (TX10p) | tasmax | Fraction of days where tasmax < DOY 10th-pct threshold; 5-day window | % | 1990–2010 | Cold Risk |
| Cool nights % (TN10p) | tasmin | Fraction of days where tasmin < DOY 10th-pct threshold; 5-day window | % | 1990–2010 | Cold Risk |
| Cold spell days (CSDI) | tasmin | Count of days inside cold spells of ≥ 6 consecutive days where tasmin < DOY 10th-pct threshold | days | 1990–2010 | Cold Risk |
| Longest cold-night run (TN ≤ 10°C) | tasmin | Maximum consecutive run of days where tasmin ≤ 10°C | days | — | Cold Risk |

### A.3 Drought Risk

All SPI metrics use Gamma distribution fitted by MoM over the calibration period 1990–2010. Event/spell metrics apply the SPI < −1 threshold (moderate drought onset).

| Label | Scale | Definition | Units | Period rollup | Bundle(s) |
|---|---|---|---|---|---|
| SPI-3 drought events | 3 months | Mean annual count of contiguous SPI episodes below −1 | events/yr | Period mean | Drought Risk |
| SPI-6 drought events | 6 months | As above at 6-month scale | events/yr | Period mean | Drought Risk |
| SPI-12 drought events | 12 months | As above at 12-month scale | events/yr | Period mean | Drought Risk |
| SPI-3 max drought spell | 3 months | Period maximum of within-year longest SPI episode below −1 | months | Period max | Drought Risk |
| SPI-6 max drought spell | 6 months | As above at 6-month scale | months | Period max | Drought Risk |
| SPI-12 max drought spell | 12 months | As above at 12-month scale | months | Period max | Drought Risk |

### A.4 Extreme Rainfall | Flash Flood Risk

All metrics derived from `pr` (mm day⁻¹). ETCCDI standard: Zhang et al. (2011).

| Label | Definition | Units | Baseline | Bundle(s) |
|---|---|---|---|---|
| Max 1-day precipitation (Rx1day) | Annual maximum of daily precipitation total | mm | — | Extreme Rainfall |
| Max 5-day precipitation (Rx5day) | Annual maximum of consecutive 5-day precipitation total | mm | — | Extreme Rainfall |
| Very heavy rain days (R20mm) | Count of days where precipitation ≥ 20 mm | days | — | Extreme Rainfall |
| Very wet day total (R95p) | Annual total precipitation on days exceeding p95 of baseline wet-day distribution (wet day ≥ 1 mm) | mm | 1990–2010 | Extreme Rainfall |
| Very wet day fraction (R95pTOT) | R95p as a fraction of annual wet-day total × 100 | % | 1990–2010 | Extreme Rainfall |
| Consecutive wet days (CWD) | Maximum consecutive days with precipitation ≥ 1 mm | days | — | Extreme Rainfall |

### A.5 Riverine Flood

Source: CEMS-GloFAS Global River Flood Hazard Maps Version 2.1 (RP-100 layers). Static snapshot; no SSP scenario dimension.

| Label | Definition | Units | Role |
|---|---|---|---|
| Flood severity index | 5×5 depth-by-extent scoring matrix; ordinal class 1–5 (Very Low to Extreme) | severity class | Scored (weight 1.0) |
| RP-100 flood depth | Block: p95 of positive flooded-cell depths within polygon. District: flooded-area-weighted mean of block p95 values | m | Display attribute |
| RP-100 flood extent | Share of polygon area with positive modelled flood depth | fraction (%) | Display attribute |

### A.6 Sectoral-only source metrics

These metrics feed the sectoral bundles (§7) but are not part of any thematic bundle, so they do not appear in A.1–A.5. Definitions, units, and baselines are given here so §7's quantitative basis is reconstructable from this note alone. **CDD** is the highest-leverage of these — it drives a dry-spell rule in five of the eight sectoral bundles. All four are derived from the daily precipitation variable `pr`.

| Label | Definition | Units | Baseline | Bundle(s) |
|---|---|---|---|---|
| Consecutive dry days (CDD) | Maximum run of consecutive days with precipitation < 1 mm in the year (a "dry day" is `pr` < 1 mm; Climdex CDD) | days | — | Industrial, Investment, Asset (Thermal), Asset (Hydropower), Life & Livelihood |
| Extremely wet-day precipitation (R99p) | Annual total precipitation on days exceeding the 99th percentile of baseline wet-day precipitation (wet day ≥ 1 mm; ETCCDI R99p) | mm | 1990–2010 | Investment |
| SPI-3 moderate-drought months | Annual count of calendar months with 3-month SPI below −1, period-mean rolled up. A persistence/low-flow proxy, distinct from the SPI-3 *event-count* and *max-spell* metrics in A.3 | months | 1990–2010 (SPI calibration) | Asset (Thermal) |
| R95p inter-annual variability (CV) | Coefficient of variation (σ ⁄ μ) of annual R95p very-wet-day totals across the years within the selected future period; an inflow-predictability proxy. The R95p p95 threshold uses the baseline distribution, but the CV itself is computed across the future-period years, not against the baseline | ratio (dimensionless) | 1990–2010 (R95p threshold) | Asset (Hydropower) |

---

## Appendix B: Sectoral Impact-Band Derivations

The distinct impact bands used by the §7 rules, deduplicated across the bundles that share them. **Onset** ($a$) is the harm threshold; **saturation** ($b$) the severe/sector-dominant threshold; the impact lens interpolates linearly between them (§7.2). "External" provenance denotes an institutionally published threshold; "self-derived" denotes a first-principles band where no categorical institutional value exists at the relevant scale.

| Band | Metric(s) | Onset → saturation rationale | Provenance | Confidence | Used by |
|---|---|---|---|---|---|
| **40–45 °C** | Annual max temperature (TXx) | IMD plains heatwave: ≥ 40 °C plains consideration floor (onset), ≥ 45 °C absolute heat-wave declaration threshold — declared irrespective of normal (saturation) | External (IMD) | HIGH | Health, Industrial, Infrastructure, Asset (Thermal) |
| **35–45 °C** | Annual max temperature (TXx) | Onset 35 (rice/wheat reproductive-stage heat-sterility threshold); saturation 45 (IMD absolute heat-wave declaration threshold / documented crop-failure regime) | Self-derived | MEDIUM | Agricultural |
| **28–32 °C** | Warmest night (TNx) | Onset 28 (hot-night minimum associated with elevated nocturnal heat-mortality risk in India); saturation 32 (upper envelope of Indian warmest nights) | Self-derived | MEDIUM | Health |
| **115.6–204.5 mm/day** | Max 1-day precipitation (Rx1day) | IMD daily-rainfall categories: very heavy 115.6–204.4 (onset) to extremely heavy ≥ 204.5 (saturation) | External (IMD) | HIGH | Health, Industrial, Investment, Infrastructure, Life & Livelihood |
| **250–500 mm/5 days** | Max 5-day precipitation (Rx5day) | Onset 250 (drainage-failure regime, ≈ 5× IMD heavy-rain floor); saturation 500 (regional flood-event regime, Kerala 2018 / Mumbai 2005). No external categorical band exists at 5-day scale | Self-derived | LOW | Agricultural, Industrial, Investment, Infrastructure, Asset (Hydropower), Life & Livelihood |
| **30–90 days** | Consecutive dry days (CDD) | Onset 30 (IMD Agricultural Drought: four consecutive Drought Weeks ≈ 28 days); saturation 90 (¾ of JJAS monsoon; SPI severe-drought territory) | Self-derived (IMD-anchored onset) | MEDIUM | Industrial, Investment, Asset (Thermal), Asset (Hydropower) |
| **60–120 days** | Consecutive dry days (CDD) | Onset 60 (IMD agro-met 4-week prolonged dry spell + ICAR-CRIDA rainfed-kharif critical-water-deficit); saturation 120 (full kharif-to-early-rabi system failure; NDMA framing) | Self-derived | MEDIUM | Life & Livelihood |
| **6–18 days** | Warm spell duration (WSDI) | Onset 6 (WSDI minimum qualifying spell, past the ≈ 4-day added-mortality threshold); saturation 18 (multi-spell / season-dominant warm-spell regime) | Self-derived | LOW (Ag/Health), MEDIUM (Life & Livelihood) | Agricultural, Health, Life & Livelihood |
| **15–60 days** | Extreme heat days (TX ≥ 35 °C) | Onset 15 (complete anthesis-window exposure + second window); saturation 60 (≈ 2 months damaging heat, season-dominant) | Self-derived | LOW | Agricultural |
| **3–12 events / months** | SPI-3 drought events, SPI-3 max drought spell | Onset 3 (natural SPI < −1 baseline frequency / one SPI-3 window); saturation 12 (near-continuous drought / year-long sustained moderate drought) | Self-derived | LOW | Agricultural |
| **10–30 days** | Cold nights (TN ≤ 10 °C) | Onset 10 (chilling-injury exposure for sensitive peninsular horticulture); saturation 30 (≈ 1 month sustained cold-night stress). **Peninsular default — over-applies in the northern wheat belt where cold nights are beneficial (vernalization)** | Self-derived | LOW | Agricultural |
| **7–15 days** | Consecutive wet days (CWD) | Onset 7 (week of standing water spans mosquito aquatic cycle / waterlogging onset); saturation 15 (prolonged saturation). Local-hydrology dependent | Self-derived | LOW | Health |
| **5–15 days/yr** | Heatwave spell days (HWFI) | Onset 5 (HWFI minimum qualifying spell); saturation 15 (high annual burden implying multiple qualifying spells) | Self-derived | LOW | Investment |
| **— (no band)** | Extremely wet-day precip (R99p), SPI-3 moderate-drought months, R95p inter-annual variability (CV) | Regime/concentration/variability metrics with no defensible danger threshold; scored on absolute + change lenses only | — | — | Investment, Asset (Thermal), Asset (Hydropower) |

**Zone-dependence caveat.** Several self-derived bands are calibrated to a national/plains or peninsular default and are known to mis-apply in specific agro-climatic zones — most notably the 10–30 day cold-night band (beneficial in the northern wheat belt) and the heat bands in non-plains terrain. Zone-specific refinement is tracked as deferred work; the present bands are the documented Phase-1 defaults.

---

*Document last updated: 2026-06-24*  
*Maintained by: Abu Bakar Siddiqui Thakur*
