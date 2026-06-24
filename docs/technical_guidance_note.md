# India Resilience Tool — Technical Guidance Note
## Climate Risk Methodology: Data, Metrics, and Bundle Construction

**Status:** DRAFT — structure agreed, sections pending  
**Scope:** Data sources → downscaling → grid-first compute → individual metrics → thematic and sectoral bundle construction → composite score output  
**Out of scope:** Exposure layers, vulnerability, adaptive capacity, dashboard UI/UX, pipeline tooling  
**Primary audience:** Technical peers (climate scientists, GIS specialists) and policy/planning stakeholders  
**Tone:** Technically rigorous throughout; mathematical derivations included with plain-language explanations alongside

---

> **Working convention for this document:**
> - Each section carries a `<!-- WRITING GUIDE -->` block summarising what to cover, what sources to draw from, and key constraints. Remove the guide block when the section is finalized.
> - Do not defend design choices — state them explicitly and move on.
> - Mathematical notation uses standard LaTeX-style inline math where rendered.
> - Tables are preferred over prose for metric lists, weights, and parameter values.
> - Cross-references between sections are marked `(→ §N.M)`.

---

## 1. Introduction and Framing

<!-- WRITING GUIDE
PURPOSE: Orient both technical and policy readers. Establish why a subnational climate risk index for India is necessary and what gap this tool fills relative to global products.

COVER:
- India's climate risk context: scale, diversity of hazards (heat, drought, extreme rainfall, cold, riverine flood), and the governance challenge of subnational planning under climate uncertainty.
- What is missing from existing global tools (e.g. Aqueduct, ND-GAIN): coarser spatial resolution, limited multi-hazard thematic and sectoral decomposition, limited India-specific downscaled projections.
- What this tool provides: district- and block-level multi-hazard risk scores derived from CMIP6 projections for two SSP scenarios and multiple future periods.
- Scope of this note: covers data provenance, downscaling context, post-processing compute, metric definitions, and bundle/composite construction. Does NOT cover the web dashboard, exposure layers (population, LULC, built-up area), groundwater context, or adaptive capacity.

CONSTRAINTS:
- Do not defend choices. State them: "We use NASA-NEX GDDP-CMIP6 at 0.25° resolution. We compute at district and block administrative levels. We cover SSP2-4.5 and SSP5-8.5."
- Keep framing brief (~0.5–1 page). The bulk of the document is methods.
- Do not make normative claims about hazard severity or policy prescriptions.

CROSS-REFERENCES: Forward-reference §2 (data), §4 (compute), §6–7 (bundles).
-->

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
| Historical | 1951–2014 |
| SSP2-4.5 | 2015–2100 |
| SSP5-8.5 | 2015–2100 |

Historical and projection files are contiguous across models: each model contributes one annual NetCDF file per variable per year spanning 1951–2100 across the historical and SSP runs.

**Analysis periods**

IRT aggregates individual-year climate indices into the following multi-year windows:

| Label | Period | Role |
|-------|--------|------|
| Historical baseline | 1990–2010 | Reference for per-period normalisation (→ §6.2) |
| Near-term | 2021–2040 | Near-term projection |
| Mid-century | 2041–2060 | Mid-century projection |
| End-century | 2061–2080 | End-of-century projection |

The anchor period (1990–2010) falls entirely within the historical simulation run (1951–2014); no splicing of historical and SSP files is required (→ §4.3).

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

**Citation:** European Commission, Joint Research Centre. (2026). CEMS-GloFAS Global River Flood Hazard Maps Version 2.1. Copernicus Emergency Management Service (CEMS). https://data.jrc.ec.europa.eu

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

The NEX-GDDP-CMIP6 product is provided at **0.25° × 0.25°** horizontal resolution, corresponding to approximately 25 km at the equator and ~27 km at 25°N (typical central India latitude). IRT clips the global product to the India domain — **68.0°E–97.5°E, 5.0°N–45.0°N** — yielding a domain of 119 × 160 grid cells.

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

*Precipitation*: Area-mean daily precipitation for ERA5 (2.47 mm day⁻¹) and IMD (2.65 mm day⁻¹) agree closely at the seasonal mean scale. Spatial correlations between district-level precipitation metrics and ERA5 for the best-performing models range from 0.81 to 0.90, indicating moderate spatial skill at district resolution. However, the ensemble systematically underestimates peak daily rainfall intensities: the IMD Rx1day (mean annual maximum 1-day rainfall) for Adilabad district is approximately 77 mm day⁻¹, while the 24 CMIP6 models range from approximately 33 to 55 mm day⁻¹ — a consistent dry bias in extreme events that persists after BCSD. This arises because BCSD applies monthly-scale quantile mapping and then bilinear spatial disaggregation to 0.25°; neither step alters the GCM's underlying atmospheric dynamics. The convective processes that generate intense short-duration rainfall events are parameterised at each GCM's native grid spacing — ranging from approximately 70 km (EC-Earth3, the finest in the ensemble) to 310 km (CanESM5, the coarsest), with most models at 100–200 km — at which mesoscale convective systems responsible for high-intensity daily rainfall in the Indian region cannot be explicitly resolved. The BCSD spatial disaggregation resamples these coarse fields to the 0.25° output grid but adds no new sub-grid meteorological information. The extreme-rainfall underestimation is therefore a structural limitation of the GCM ensemble, not a downscaling artefact. Users interpreting the Extreme Rainfall | Flash Flood bundle (→ §5.2) should note that absolute metric values likely understate observed extreme rainfall intensities.

*Comparative context*: Jain et al. (2019) evaluated the NEX-GDDP (CMIP5-era) product against IMD gridded observations over the Indian subcontinent for the summer monsoon season (1975–2005), benchmarking it against multi-model means from 28 raw CMIP5 models and 10 CORDEX regional models. NEX-GDDP surpassed both CMIP5 and CORDEX in reproducing seasonal mean temperature and precipitation patterns (spatial pattern correlation ~0.8; RMSE ~4.25°C for temperature and ~2.48 mm day⁻¹ for precipitation), inter-annual variability, and annual cycle characteristics. Crucially, the simulation of extremes was found to be more realistic in NEX-GDDP relative to raw CMIP5 and CORDEX output, with reduced inter-model spread — supporting the use of the NEX-GDDP product for climate change impact assessment. Although these findings pertain to the CMIP5-era version of NEX-GDDP, the BCSD methodology is common to both the CMIP5 and CMIP6 versions; the results are therefore informative about the relative improvement that the downscaling procedure confers over raw GCM output.

> Jain, S., Salunke, P., Mishra, S. K., Sahany, S., and Choudhary, N. (2019). Advantage of NEX-GDDP over CMIP5 and CORDEX data: Indian Summer Monsoon. *Atmospheric Research*, 228, 152–160. https://doi.org/10.1016/j.atmosres.2019.05.026

> **[FIGURES TO INSERT]** The following validation figures from the notebook analysis will be incorporated here to provide visual evidence of the bias characterisation. Source notebooks: `notebooks/era5_vs_cmip_clean_tel_1980_1985.ipynb` and `notebooks/rainfall_metrics_imd_cmip6_tel_box_1980_1985.ipynb`.
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

All climate index computations in IRT are performed at the native 0.25° grid resolution before any aggregation to administrative boundaries. This "grid-first" design reflects a deliberate methodological choice rooted in the structure of the indices being computed.

An alternative — the "admin-first" approach — would average the raw daily GCM values over each administrative unit before computing indices. This is appropriate for linear statistics such as mean temperature, but introduces bias for any non-linear index. Consider a district that straddles a dense urban area and a river valley: one 0.25° cell (the city) records five consecutive days at 36–38°C, while an adjacent cell (the valley) records those same days at 28–30°C. The admin-first approach averages the two cells first, producing a district-mean of 32–34°C — below the 35°C threshold on every day — and consequently reports **zero** extreme-heat days for the district. The grid-first approach computes five hot days for the city cell and zero for the valley cell, then takes the area-weighted mean: 2.5 hot days for the district. The admin-first result is not merely less precise — it completely erases a genuine multi-day extreme heat event that affected half the district. The distortion compounds further for non-linear indices: spell-length metrics, percentile-exceedance fractions, and the SPI gamma transform all produce systematically biased outputs when applied to pre-averaged spatial means.

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

This three-stage chain is designed to separate climate signal from noise at two distinct scales. Averaging annual index values over a 20-year window (stage 2) filters out interannual variability driven by modes such as ENSO and the Indian Ocean Dipole, isolating the underlying forced climate change signal rather than the characteristics of any particular sequence of years. Santer et al. (2011) demonstrate that signal-to-noise ratios in atmospheric temperature trends are below 1 at 10-year timescales but exceed 3.9 at 32-year trends, and that at least 17 years of data are required to reliably distinguish the forced climate change signal from internal variability noise. Hawkins and Sutton (2012) formalise this as the *time of emergence* — the point at which the forced signal rises detectably above the background noise of natural variability — which multi-decadal period averaging is designed to approach. Averaging across 24 GCMs (stage 3) reduces sensitivity to the structural biases of any individual model; Tebaldi and Knutti (2007) provide the foundational treatment of this argument, showing that multi-model ensemble means systematically outperform individual model projections because model-specific errors arising from different structural choices are partially uncorrelated across the ensemble and therefore partially cancel in the mean. The two operations are applied in sequence — time-averaging first, then ensemble-averaging — so that each model's period mean contributes equally to the ensemble average regardless of its interannual variance.

> Santer, B. D., Mears, C., Doutriaux, C., Caldwell, P., Gleckler, P. J., Wigley, T. M. L., Solomon, S., Gillett, N. P., Ivanova, D., Karl, T. R., Lanzante, J. R., Meehl, G. A., Stott, P. A., Taylor, K. E., Thorne, P. W., McCarthy, M. P., and Wehner, M. F. (2011). Separating signal and noise in atmospheric temperature changes: The importance of timescale. *Journal of Geophysical Research: Atmospheres*, 116, D22105. https://doi.org/10.1029/2011JD016263

> Hawkins, E. and Sutton, R. (2012). Time of emergence of climate signals. *Geophysical Research Letters*, 39, L01702. https://doi.org/10.1029/2011GL050087

> Tebaldi, C. and Knutti, R. (2007). The use of the multi-model ensemble in probabilistic climate projections. *Philosophical Transactions of the Royal Society A*, 365, 2053–2075. https://doi.org/10.1098/rsta.2007.2076

**Analysis periods**

| Scenario | Period |
|----------|--------|
| Historical | 1990–2010 |
| SSP2-4.5 and SSP5-8.5 | 2021–2040 |
| SSP2-4.5 and SSP5-8.5 | 2041–2060 |
| SSP2-4.5 and SSP5-8.5 | 2061–2080 |

**Historical scenario: no splice required**

The historical anchor period (1990–2010) falls entirely within the historical simulation run (1951–2014). No splicing of historical and SSP files is required for the anchor period. SSP projection files begin in 2015 and are used exclusively for the 2021–2040, 2041–2060, and 2061–2080 windows.

**Composite normalization (per-period spatial ranking)**

For each (scenario, period) combination, the ensemble-mean metric values across all administrative units are normalised onto a [0, 100] scale using the **spatial minimum and maximum** of that same (scenario, period) slice. Let $v_i$ be the ensemble-mean value for unit $i$, and let $v_{\min}$ and $v_{\max}$ be the minimum and maximum of $v_i$ across all units with finite values in that slice. The normalised score is:

$$S_i = \operatorname{clip}\!\left(\frac{v_i - v_{\min}}{v_{\max} - v_{\min}},\; 0,\; 1\right) \times 100$$

For metrics where a lower value indicates greater hazard (i.e. `higher_is_worse = False` in the registry), the score is inverted: $S_i = (1 - \text{scaled}) \times 100$ before clipping. If all units share an identical value ($v_{\max} = v_{\min}$), all receive a score of 50.

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

**Spells.** Several indices count *spells* — maximal runs of consecutive days that satisfy an exceedance condition. The minimum qualifying run length differs by index: WSDI and CSDI require ≥6 consecutive days (ETCCDI convention), whereas the heatwave indices hwfi and hwa require ≥5 consecutive days, aligning with India Meteorological Department heatwave criteria. Spells are evaluated within a calendar year; a run is not carried across the year boundary.

**Worked example — TX90p for one cell.** Consider a single 0.25° grid cell and the calendar day 1 May (day-of-year 121). Pooling all daily `tasmax` values from 29 April–3 May across the 21 baseline years 1990–2010 gives 5 × 21 = 105 values; their 90th percentile is the threshold $\tau_{121}$ (say, 41.2°C). Repeating this for every calendar day traces the smooth seasonal threshold curve $\tau_d$. In any evaluation year, TX90p is the percentage of days whose `tasmax` exceeds that day's threshold:

$$\text{TX90p} = 100 \times \frac{1}{N}\sum_t \mathbb{1}\!\left[x_t > \tau_{d(t)}\right]$$

By construction this is ≈ 10% under the baseline climate; a warming year pushes it well above 10%, which is precisely the relative-shift signal the index is designed to capture.

> **[FIGURE TO INSERT]** Day-of-year 90th-percentile threshold curve $\tau_d$ (annual cycle) for one cell, with a single year's daily `tasmax` overlaid and exceedance days marked — illustrating the TX90p / WSDI / hwa threshold framework shared across §5.1.

**Heatwave amplitude (hwa)**

`hwa_heatwave_amplitude` is an IRT-specific index. For each year and grid cell, the DOY-90th-percentile framework (baseline 1990–2010, applied to tasmax, minimum spell length 5 consecutive days) identifies all heatwave spells $s$ within the year. For each spell, the mean daily exceedance above the per-day threshold is:

$$\bar{\epsilon}_s = \frac{1}{|s|} \sum_{t \in s} \bigl(x_t - \tau_{d(t)}\bigr)$$

The spell with the largest mean exceedance $\bar{\epsilon}_{s^*}$ is selected as the "worst" heatwave of the year. The amplitude is the peak daily maximum temperature within that spell:

$$\text{hwa} = \max_{t \in s^*}\, x_t \quad ({}^\circ\text{C})$$

This captures both the persistence and the intensity of the strongest annual heatwave event as a single value in absolute Celsius — not an anomaly relative to a threshold.

> Zhang, X., Alexander, L., Hegerl, G. C., Jones, P., Klein Tank, A., Peterson, T. C., Trewin, B., and Zwiers, F. W. (2011). Indices for monitoring changes in extremes based on daily temperature and precipitation data. *WIREs Climate Change*, 2(6), 851–870. https://doi.org/10.1002/wcc.147

### 5.2 Precipitation and Extreme Rainfall Metrics

Turning from temperature to the water cycle, the precipitation indices characterise rainfall intensity and accumulation. All six Extreme Rainfall | Flash Flood Risk bundle metrics are derived from the daily precipitation variable `pr` (converted from kg m⁻² s⁻¹ to mm day⁻¹). Four are canonical ETCCDI indices with no IRT-specific departures: **Rx1day** (annual maximum 1-day total), **Rx5day** (annual maximum 5-day running total), **R20mm** (count of days with precipitation ≥ 20 mm), and **CWD** (maximum consecutive wet days, where a wet day is any day with precipitation ≥ 1 mm). See Appendix A for parameter details.

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
- **Maximum drought spell** (`spi{k}_max_spell_lt_minus1`): longest consecutive-month period per year during which SPI is continuously below −1, expressed as the period maximum over the 20-year window (not the mean).

These per-cell annual metric fields are then area-weighted and aggregated to administrative units following the procedure in §4.2.


> McKee, T. B., Doesken, N. J., and Kleist, J. (1993). The relationship of drought frequency and duration to time scales. *Proceedings of the 8th Conference on Applied Climatology*, 17–22 January, Anaheim, California. American Meteorological Society, 179–183.

### 5.4 Wet-Bulb Temperature and Humid Heat Metrics

Returning to heat, but now coupling temperature with humidity: the body's primary cooling mechanism under heat stress is evaporative sweat loss; at high humidity this mechanism is impaired, generating physiological strain at air temperatures well below those dangerous in dry conditions. Wet-bulb temperature ($T_{wb}$) integrates both air temperature and ambient humidity into a quantity directly proportional to the ambient evaporative cooling capacity. Raymond et al. (2020) demonstrated that wet-bulb temperatures above 35°C are incompatible with sustained human activity even for acclimatised individuals; IRT uses working thresholds of 28°C and 30°C, corresponding to severe and very severe occupational heat stress relevant to India's outdoor labour conditions.

**Stull (2011) approximation**

$T_{wb}$ is computed from daily near-surface air temperature ($T$ in °C, from `tas`) and near-surface relative humidity ($RH$ in %, from `hurs`) using the empirical approximation of Stull (2011):

$$T_{wb} = T \cdot \arctan\!\bigl(0.151977\,\sqrt{RH + 8.313659}\,\bigr) + \arctan(T + RH) - \arctan(RH - 1.676331) + 0.00391838 \cdot RH^{1.5} \cdot \arctan(0.023101\,RH) - 4.686035$$

This approximation has a mean absolute error of 0.28°C relative to the psychrometric wet-bulb for the range $0°\text{C} \leq T \leq 50°\text{C}$, $5\% \leq RH \leq 99\%$, covering the full range of tropical and subtropical conditions in India. $T_{wb}$ is computed day-by-day from the daily `tas` and `hurs` fields before spatial aggregation, consistent with the grid-first architecture (→ §4.1). The summer season for `twb_summer_mean` is March–May (MAM), consistent with the summer temperature means in §5.1.

> Stull, R. (2011). Wet-bulb temperature from relative humidity and air temperature. *Journal of Applied Meteorology and Climatology*, 50(11), 2267–2269. https://doi.org/10.1175/JAMC-D-11-0143.1

> Raymond, C., Matthews, T., and Horton, R. M. (2020). The emergence of heat and humidity too severe for human tolerance. *Science Advances*, 6(19), eaaw1838. https://doi.org/10.1126/sciadv.aaw1838

### 5.5 Riverine Flood Metrics (JRC)

The final hazard family departs from the climate grid entirely. Riverine flood metrics are derived from the CEMS-GloFAS RP-100 raster layers (→ §2.4) and are static snapshots with no SSP scenario dimension. All three metrics are computed directly on the raster and then aggregated to administrative polygons; they do not pass through the 0.25° climate grid.

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
<!-- This index alone carries a non-zero bundle weight (1.0); `jrc_flood_depth_rp100` and `jrc_flood_extent_rp100` are retained as inline display attributes (weight = 0.0). -->

---

## 6. Thematic Bundle Construction

Each of the six hazard families defined in §5 is condensed into a single composite **bundle score** on a 0–100 scale, computed independently for every geography, scenario, and time period. Two steps produce that score: each component metric is first normalized onto a common 0–100 *higher-is-worse* scale (§6.2), and the normalized components are then combined with fixed weights (§6.3). This thematic framework — a weighted average of co-normalized climate metrics — is distinct from the sectoral hazard-pressure framework of §7, which scores exposure to curated hazard rules rather than compositing metrics directly.

### 6.1 Bundle Taxonomy and Grouping Rationale

The six thematic bundles and the hazard dimension each captures:

| Bundle | Hazard dimension | Component metric families (§5) | Composite slug |
|---|---|---|---|
| Heat Risk | Daytime/nocturnal thermal extremes and background heat | Background means, absolute & percentile extremes, threshold-frequency, heatwave characteristics (§5.1) | `composite_heat_risk` |
| Heat Stress | Humid-heat physiological stress | Wet-bulb means/extremes (§5.4) + shared dry-heat persistence WSDI, TN90p (§5.1) | `composite_heat_stress` |
| Cold Risk | Winter cold extremes and cold-spell persistence | Background cold, absolute extremes, cold-day thresholds, percentile-relative cold, cold-spell characteristics (§5.1) | `composite_cold_risk` |
| Drought Risk | Meteorological drought across timescales | SPI-3/6/12 event counts and maximum spell lengths (§5.3) | `composite_drought_risk` |
| Extreme Rainfall \| Flash Flood Risk | Extreme precipitation and wet-spell persistence | Peak intensity, heavy-rain frequency, very-wet contribution, wet-spell persistence (§5.2) | `composite_flood_extreme_rainfall_risk` |
| Riverine Flood | Static RP-100 inundation severity | JRC flood severity index (§5.5) | `composite_flood_jrc_depth` |

The grouping logic is consistent across bundles: the members of a bundle measure **complementary facets of one hazard** — magnitude (e.g. TXx), frequency (e.g. hot-day counts), persistence (e.g. WSDI), and percentile-relative shift (e.g. TX90p) — rather than redundant restatements of the same signal. Compositing these facets dampens the idiosyncratic noise of any single index and yields a more stable hazard ranking. **Riverine Flood is the structural exception**: it carries a single scored metric (the JRC severity index, weight 1.0), so its "composite" is a pass-through of that one index; the two companion JRC fields (depth, extent) are retained as display attributes at weight 0 (§6.4).

### 6.2 Normalization: Per-Period Spatial Scaling

Every component metric is normalized **independently within each scenario–period column**, across the set of geographies in the computed frame — the districts, or the blocks, of one state at the chosen level. The shipped method is a cross-sectional **min–max rescaling** onto 0–100, oriented so that higher always means worse.

For a metric with finite values $v_i$ over the geography set $G$ in a given scenario–period:

$$v_{\min} = \min_{i\in G} v_i, \qquad v_{\max} = \max_{i\in G} v_i$$

$$S_i = \operatorname{clip}\!\left(\frac{v_i - v_{\min}}{v_{\max} - v_{\min}},\; 0,\; 1\right)\times 100$$

For metrics whose registry directionality is *lower-is-worse* (e.g. winter-temperature means, where colder is the hazard), the numerator is replaced by $v_{\max}-v_i$ so that the worst tail still maps to 100. Two degenerate cases are handled explicitly: if every geography shares one finite value ($v_{\max}=v_{\min}$), all rows receive **50**; if no finite value exists, the score is NaN.

Three consequences follow from normalizing **per period**:

- A bundle score is **relative, not absolute**. A district scoring 90 is among the most exposed *of its state's districts for that scenario and period* — it is not a physical magnitude, and it is **not** a change-versus-history signal.
- Because each scenario–period is rescaled on its own min/max, scores are comparable *within* a period across space; absolute score differences *between* periods reflect the shifting spatial spread, not only the change in the underlying hazard.
- The 1990–2010 anchor period plays **no role** in this normalization. A baseline-anchored variant — scaling every period against the fixed 1990–2010 historical range to produce a true change signal — exists in the codebase but is **not** used by any shipped thematic bundle. It is a dormant capability, noted here only to avoid confusion with the change-based sectoral rules of §7.

The **Riverine Flood** bundle is a static snapshot: the JRC RP-100 severity index has no scenario or future-period dimension, so it is normalized once over the geography set on the same min–max scale (higher severity → worse), with no period anchoring.

### 6.3 Weighted Composite Methodology

Within a bundle the normalized component scores are combined as a **weighted mean**, renormalized per row over the components actually present:

$$\text{Composite}_g = \frac{\sum_{m \in A_g} w_m\, S_{g,m}}{\sum_{m \in A_g} w_m}$$

where $A_g$ is the set of component metrics with a valid (non-NaN) normalized score for geography $g$, and $w_m$ are the fixed bundle weights (§6.4). Because each $S_{g,m}\in[0,100]$ and the weights are renormalized to sum to 1 over $A_g$, the composite is itself bounded in $[0,100]$ — no separate clipping is required. The count of contributing metrics ($\lvert A_g\rvert$) is persisted alongside each score for transparency.

Per-row renormalization means a geography missing one metric is scored on its remaining metrics rather than being penalized or dropped. The only completeness gate for the shipped per-period bundles is that **at least one** component must be present: a row with every component missing yields NaN. (The stricter "≥ 4 anchored components" floor referenced in some internal configuration applies *only* to the dormant baseline-anchored mode of §6.2 and is inactive for the shipped composites.)

Weights are drawn from the approved `Bundles_comp_Score.xlsx` workbook and sum to 1.0 per bundle. They are organised into **weight groups** that gather related facets together; within a group the weight is, in most bundles, split equally across members, so the group subtotal encodes the relative emphasis placed on that facet of the hazard.

### 6.4 Bundle-by-Bundle Metric Weights

The tables below give the full component weighting for each thematic bundle. Each metric's final weight is the product of its **group weight** (shown in the sub-header) and its **share of that group**:

$$w_m = (\text{group weight}) \times (\text{share of group})$$

In every bundle except Drought Risk the group is split *equally* among its members, so the share is simply $1/n$ for a group of $n$ metrics (e.g. each of the three metrics in Heat Risk's 0.200 "Mean & Background Heat" group takes a $1/3$ share → $0.200 \times \tfrac13 = 0.0667$). Drought Risk is the one exception, with an unequal $2/5$–$3/5$ split inside each timescale (below). Every bundle's final weights sum to 1.000.

#### Heat Risk (`composite_heat_risk`)

| Weight group | Metric | Slug | Share of group | Weight |
|---|---|---|---|---|
| **Mean & Background Heat (0.200)** | Annual Mean Temperature (TM) | `tas_annual_mean` | 1/3 | 0.0667 |
| | Summer Max Temperature (MAM) | `tasmax_summer_mean` | 1/3 | 0.0667 |
| | Summer Mean Temperature (MAM) | `tas_summer_mean` | 1/3 | 0.0667 |
| **Extremes (0.250)** | Annual Maximum Temperature (TXx) | `txx_annual_max` | 1/3 | 0.0833 |
| | Warm Nights (TN90p) | `tn90p_warm_nights_pct` | 1/3 | 0.0833 |
| | Heatwave Amplitude | `hwa_heatwave_amplitude` | 1/3 | 0.0833 |
| **Threshold-based Frequency (0.200)** | Hot Days (TX ≥ 30°C) | `txge30_hot_days` | 1/3 | 0.0667 |
| | Extreme Heat Days (TX ≥ 35°C) | `txge35_extreme_heat_days` | 1/3 | 0.0667 |
| | Tropical Nights (TN > 25°C) | `tasmin_tropical_nights_gt25` | 1/3 | 0.0667 |
| **Percentile Extremes (0.150)** | Heat Wave Frequency Index (days) | `hwfi_tmean_90p` | 1/2 | 0.0750 |
| | Heat Wave Frequency (events) | `hwfi_events_tmean_90p` | 1/2 | 0.0750 |
| **Heatwave Characteristics (0.200)** | Warm Spell Duration Index (WSDI) | `wsdi_warm_spell_days` | 1/3 | 0.0667 |
| | Warmest Night (TNx) | `tnx_annual_max` | 1/3 | 0.0667 |
| | Hot Days (TX90p) | `tx90p_hot_days_pct` | 1/3 | 0.0667 |

#### Heat Stress (`composite_heat_stress`)

| Weight group | Metric | Slug | Share of group | Weight |
|---|---|---|---|---|
| **Background humid heat (0.200)** | Wet-Bulb Temperature (Annual Mean) | `twb_annual_mean` | 1/2 | 0.1000 |
| | Wet-Bulb Temperature (Summer Mean, MAM) | `twb_summer_mean` | 1/2 | 0.1000 |
| **Extreme / threshold humid heat (0.400)** | Wet-Bulb Temperature (Annual Max) | `twb_annual_max` | 1/3 | 0.1333 |
| | Heat Stress Days (Twb ≥ 28°C) | `twb_days_ge_28` | 1/3 | 0.1333 |
| | Wet-Bulb Days (Twb ≥ 30°C) | `twb_days_ge_30` | 1/3 | 0.1333 |
| **Night-time recovery stress (0.200)** | Tropical Nights (TN > 28°C) | `tasmin_tropical_nights_gt28` | 1/2 | 0.1000 |
| | Warm Nights (TN90p) | `tn90p_warm_nights_pct` | 1/2 | 0.1000 |
| **Persistence (0.200)** | Warm Spell Duration Index (WSDI) | `wsdi_warm_spell_days` | 1/1 | 0.2000 |

#### Cold Risk (`composite_cold_risk`)

| Weight group | Metric | Slug | Share of group | Weight |
|---|---|---|---|---|
| **Background Cold (0.200)** | Winter Mean Temperature (DJF) | `tas_winter_mean` | 1/2 | 0.1000 |
| | Winter Min Temperature (DJF) | `tasmin_winter_mean` | 1/2 | 0.1000 |
| **Absolute Extremes (0.200)** | Annual Minimum of Tmin (TNn) | `tnn_annual_min` | 1/2 | 0.1000 |
| | Winter Minimum Tmin (DJF) | `tasmin_winter_min` | 1/2 | 0.1000 |
| **Threshold-based Cold Days (0.250)** | Cold Nights (TN ≤ 10°C) | `tnle10_cold_nights` | 1/3 | 0.0833 |
| | Severe Cold Nights (TN ≤ 5°C) | `tnle5_severe_cold_nights` | 1/3 | 0.0833 |
| | Cold Days (TX ≤ 15°C) | `txle15_cold_days` | 1/3 | 0.0833 |
| **Relative Cold (0.150)** | Cool Days (TX10p) | `tx10p_cool_days_pct` | 1/2 | 0.0750 |
| | Cool Nights (TN10p) | `tn10p_cool_nights_pct` | 1/2 | 0.0750 |
| **Cold Spell Characteristics (0.200)** | Cold Spell Duration Index (CSDI) | `csdi_cold_spell_days` | 1/2 | 0.1000 |
| | Consecutive Cold Nights (TN ≤ 10°C) | `tnle10_consecutive_cold_nights` | 1/2 | 0.1000 |

#### Drought Risk (`composite_drought_risk`)

Drought Risk is the only bundle with an unequal within-group split: inside each SPI timescale the maximum-spell metric takes a **3/5** share and the event count **2/5**, so duration outweighs frequency. The group weights themselves rise with accumulation window (SPI-12 > SPI-6 > SPI-3), reflecting the greater impact of sustained, long-accumulation drought.

| Weight group | Metric | Slug | Share of group | Weight |
|---|---|---|---|---|
| **Seasonal Drought — SPI-3 (0.200)** | SPI-3 drought event count (SPI < −1) | `spi3_count_events_lt_minus1` | 2/5 | 0.0800 |
| | SPI-3 maximum drought spell | `spi3_max_spell_lt_minus1` | 3/5 | 0.1200 |
| **Meteorological Drought — SPI-6 (0.300)** | SPI-6 drought event count (SPI < −1) | `spi6_count_events_lt_minus1` | 2/5 | 0.1200 |
| | SPI-6 maximum drought spell | `spi6_max_spell_lt_minus1` | 3/5 | 0.1800 |
| **Long-term Drought — SPI-12 (0.500)** | SPI-12 drought event count (SPI < −1) | `spi12_count_events_lt_minus1` | 2/5 | 0.2000 |
| | SPI-12 maximum drought spell | `spi12_max_spell_lt_minus1` | 3/5 | 0.3000 |

#### Extreme Rainfall | Flash Flood Risk (`composite_flood_extreme_rainfall_risk`)

| Weight group | Metric | Slug | Share of group | Weight |
|---|---|---|---|---|
| **Peak Intensity (0.250)** | Maximum 1-day Precipitation (Rx1day) | `pr_max_1day_precip` | 1/2 | 0.1250 |
| | Maximum 5-day Precipitation (Rx5day) | `pr_max_5day_precip` | 1/2 | 0.1250 |
| **Heavy Rain Frequency (0.250)** | Very Heavy Precipitation Days (R20mm) | `r20mm_very_heavy_precip_days` | 1/1 | 0.2500 |
| **Very Wet Contribution (0.250)** | Very Wet Day Precipitation (R95p) | `r95p_very_wet_precip` | 1/2 | 0.1250 |
| | Very Wet Day Contribution (R95pTOT) | `r95ptot_contribution_pct` | 1/2 | 0.1250 |
| **Wet-spell Persistence (0.250)** | Consecutive Wet Days (CWD) | `cwd_consecutive_wet_days` | 1/1 | 0.2500 |

#### Riverine Flood (`composite_flood_jrc_depth`)

| Weight group | Metric | Slug | Share of group | Weight |
|---|---|---|---|---|
| **Inundation Severity (1.000)** | Flood Severity Index (RP-100) | `jrc_flood_depth_index_rp100` | 1/1 | 1.0000 |
| **Inundation Depth** (display attribute) | RP-100 Flood Depth | `jrc_flood_depth_rp100` | — | 0.0000 |
| **Inundation Extent** (display attribute) | RP-100 Flood Extent | `jrc_flood_extent_rp100` | — | 0.0000 |

The Riverine Flood composite is fully determined by the single JRC severity index (§5.5); the depth and extent fields are carried at weight 0 for display and drill-down only and do not affect the score.

---

## 7. Sectoral Bundle Construction

<!-- WRITING GUIDE
PURPOSE: Explain the distinct "proposal bundle" (sector hazard-pressure) framework used for the 8 sectoral bundles, which is methodologically different from the thematic weighted average.

SUBSECTION BREAKDOWN:

### 7.1 Sector Hazard-Pressure Framework
- The 8 sectoral bundles are:
  1. Agricultural Risk
  2. Health Risk
  3. Industrial Risk
  4. Investment / Financial Risk
  5. Infrastructure Risk
  6. Asset Risk (Thermal Power Plants)
  7. Asset Risk (Hydropower Plants)
  8. Life & Livelihood Loss Risk
- Clarify the key conceptual difference from thematic bundles: sectoral bundles do not directly composite climate metrics — they score each sector's exposure to a curated set of climate hazard pressures, where each hazard is evaluated through a "rule" that combines current magnitude, projected change, and known impact thresholds.
- Important caveat: these are Phase-1 sector climate hazard-pressure scores. They do not yet include exposure, vulnerability, or adaptive capacity components.

### 7.2 The Blended Rule: Absolute Pressure + Change + Impact Band
- Each sectoral bundle is composed of N rules. Each rule is tied to one source metric.
- A rule score has three components:
  1. Absolute pressure score (S_abs): how high is the metric value in absolute terms?
  2. Change score (S_change): how much does the metric change relative to the historical baseline? Change can be expressed as absolute delta (absolute_delta) or relative percent change (relative_pct), depending on the metric.
  3. Impact band score (S_impact): does the metric value fall within a known harm-onset–to–saturation range? The impact band [impact_low, impact_high] maps linearly from 0 (at impact_low) to 1 (at impact_high).
- The blended rule score for rule r:
  S_r = absolute_weight × S_abs + change_weight × S_change + impact_weight × S_impact
  where absolute_weight + change_weight + impact_weight = 1 per rule.
- Write out the full mathematical definitions of S_abs, S_change, S_impact. Confirm the normalization method for S_abs and S_change from proposal_bundles.py and the build logic.
- Component scores are [0, 1]; the rule score S_r is [0, 1]; the bundle composite is scaled to [0, 100].

### 7.3 Impact Bands — Derivation and Confidence
- Impact bands define the metric value range within which damage or harm is expected to occur.
- impact_low = onset threshold (harm begins); impact_high = saturation threshold (harm is near-complete or dominant).
- The linear interpolation within the band: S_impact = clip((value − impact_low) / (impact_high − impact_low), 0, 1).
- Confidence grades: impact bands are internally graded HIGH / MEDIUM / LOW based on literature support. Briefly describe what each grade means.
  - HIGH: based on a published threshold with strong evidence (e.g. IMD heatwave declaration criterion).
  - MEDIUM: derived from a combination of literature and expert judgement.
  - LOW: self-derived from first principles or indirect evidence; used with a small impact weight by design.
- [DECISION PENDING: whether to publish the per-band confidence grades in this note. Leave a placeholder table if not yet decided.]

### 7.4 Bundle-by-Bundle Rule Tables and Weights
- For each of the 8 sectoral bundles, provide:
  - Table: Rule Slug | Display Label | Source Metric | Rule Weight | Absolute / Change / Impact Weights | Impact Band [Low, High] | Change Mode | Confidence
  - Narrative paragraph: 2–4 sentences on the scientific rationale for the hazard selection for that sector.
- Weight mode: all current sectoral bundles use explicit_normalized weights (rule_weight values sum to 1.0 per bundle).
- Note the min_available_rule_weight_fraction = 0.70 floor: a sectoral composite is only computed if at least 70% of the total rule weight has valid source data. State why.

CONSTRAINTS:
- S_abs, S_change, S_impact derivations must be confirmed from the codebase (proposal_bundle_builder or equivalent). Mark [TO CONFIRM FROM CODE] where uncertain.
- The "blended" rule type and "trend" rule type both exist in the codebase. The trend rule is not currently used in shipped bundles — mention briefly, exclude from detailed derivation.
- This section will be long. Use consistent table formatting and keep the per-bundle narrative tight (2–4 sentences maximum per bundle).
-->

---

## 8. Composite Score and Output

<!-- WRITING GUIDE
PURPOSE: Describe what the final composite score is, how it is produced, what range it takes, and how it varies by scenario, period, and spatial level.

SUBSECTION BREAKDOWN:

### 8.1 Composite Derivation
- The composite score is the final output of the bundle construction process (§6.3 for thematic, §7.2 for sectoral).
- It is a single number per (admin unit × scenario × period) tuple, on a [0, 100] scale, where higher values indicate greater hazard pressure.
- For thematic bundles: it is the weighted sum of per-period normalized component metric scores (→ §6.3).
- For sectoral bundles: it is the weighted sum of blended rule scores (→ §7.2).
- State clearly: the composite score is NOT a probability, NOT an annualized loss estimate, and NOT a risk score in the technical sense (it does not include exposure, vulnerability, or adaptive capacity). It is a multi-metric hazard-pressure index.

### 8.2 Scenario and Period Handling
- Composite scores are computed independently for each (scenario, period) combination.
- Available combinations: Historical (1995–2014, 1979–2019) · SSP2-4.5 (2021–2040, 2041–2060, 2061–2080, 2081–2100) · SSP5-8.5 (same future periods).
- Exception: Riverine Flood carries only the "Snapshot" scenario (no future projections).
- The tool allows users to select any valid (scenario, period) pair; the composite is reloaded from pre-computed persisted master files.
- Note: composite scores across different (scenario, period) pairs are comparable because they share a common normalization anchor (1990–2010 historical). Explain the implication: a score of 70 in 2041–2060 SSP5-8.5 is directly comparable to 55 in 2041–2060 SSP2-4.5.

### 8.3 District vs Block Resolution Behaviour
- Both district (ADM2) and block (ADM3) level composites are independently computed from the grid-first index pipeline — blocks are not aggregated from districts.
- Note any resolution-specific limitations: block-level scores have fewer 0.25° cells contributing per unit than districts, which may increase spatial variability and sensitivity to grid-cell artefacts near administrative boundaries.
- State the supported levels for each bundle (both thematic and sectoral bundles support district and block).

CONSTRAINTS:
- Be explicit that this is a hazard index, not a full risk score. Do not use "risk" loosely.
- Do not describe the dashboard UI or how scores are displayed — this is a methods note.
- If there are known score distribution characteristics (e.g. typical range, skewness across India), these can be noted briefly, but are not required.
-->

---

---

## Appendix A: Complete Metric Reference

The table below lists every metric that appears in a thematic bundle weight entry in `bundle_weights.py`. Metrics shared across bundles appear once with all bundles noted. Columns: **Slug** (canonical pipeline identifier), **Label** (display name), **Variable(s)** (NEX-GDDP-CMIP6 input or external source), **Definition** (how the annual value is computed), **Units**, **Baseline** (period for percentile/distribution fitting, where applicable), **Bundle(s)**.

Abbreviations: DOY = day-of-year percentile threshold; MAM = March–May; DJF = December–January–February; MoM = Method of Moments.

### A.1 Heat Risk and Heat Stress

| Slug | Label | Variable(s) | Definition | Units | Baseline | Bundle(s) |
|---|---|---|---|---|---|---|
| `tas_annual_mean` | Annual mean temperature | tas | Arithmetic mean of daily mean temperature | °C | — | Heat Risk |
| `tasmax_summer_mean` | Summer (MAM) max temp | tasmax | Mean of daily max temperature in months [3,4,5] | °C | — | Heat Risk |
| `tas_summer_mean` | Summer (MAM) mean temp | tas | Mean of daily mean temperature in months [3,4,5] | °C | — | Heat Risk |
| `txx_annual_max` | Annual max daily max temp (TXx) | tasmax | Annual maximum of daily maximum temperature | °C | — | Heat Risk |
| `tnx_annual_max` | Warmest night (TNx) | tasmin | Annual maximum of daily minimum temperature | °C | — | Heat Risk |
| `hwa_heatwave_amplitude` | Heatwave amplitude | tasmax | Peak daily max temp within the heatwave spell with highest mean exceedance above DOY 90th-pct threshold; min 5 consecutive exceedance days | °C | 1990–2010 | Heat Risk |
| `txge30_hot_days` | Hot days (TX ≥ 30°C) | tasmax | Count of days where tasmax ≥ 30°C | days | — | Heat Risk |
| `txge35_extreme_heat_days` | Extreme heat days (TX ≥ 35°C) | tasmax | Count of days where tasmax ≥ 35°C | days | — | Heat Risk |
| `tasmin_tropical_nights_gt25` | Tropical nights (TN > 25°C) | tasmin | Count of days where tasmin > 25°C | days | — | Heat Risk |
| `hwfi_tmean_90p` | Heatwave spell days | tas | Total days inside spells of ≥ 5 consecutive days where tas > DOY 90th-pct threshold | days | 1990–2010 | Heat Risk |
| `hwfi_events_tmean_90p` | Heatwave event count | tasmax | Count of distinct spells of ≥ 5 consecutive days where tasmax > DOY 90th-pct threshold | events | 1990–2010 | Heat Risk |
| `wsdi_warm_spell_days` | Warm spell days (WSDI) | tasmax | Count of days inside warm spells of ≥ 6 consecutive days where tasmax > DOY 90th-pct threshold | days | 1990–2010 | Heat Risk, Heat Stress |
| `tx90p_hot_days_pct` | Hot days % (TX90p) | tasmax | Fraction of days where tasmax > DOY 90th-pct threshold; 5-day window | % | 1990–2010 | Heat Risk |
| `tn90p_warm_nights_pct` | Warm nights % (TN90p) | tasmin | Fraction of days where tasmin > DOY 90th-pct threshold; 5-day window | % | 1990–2010 | Heat Risk, Heat Stress |
| `twb_annual_mean` | Annual mean wet-bulb temp | tas, hurs | Annual mean of daily Twb (Stull 2011) | °C | — | Heat Stress |
| `twb_summer_mean` | Summer (MAM) mean wet-bulb | tas, hurs | Mean of daily Twb (Stull 2011) in months [3,4,5] | °C | — | Heat Stress |
| `twb_annual_max` | Annual max wet-bulb temp | tas, hurs | Annual maximum of daily Twb (Stull 2011) | °C | — | Heat Stress |
| `twb_days_ge_28` | Heat stress days (Twb ≥ 28°C) | tas, hurs | Count of days where Twb (Stull 2011) ≥ 28°C | days | — | Heat Stress |
| `twb_days_ge_30` | Severe heat stress days (Twb ≥ 30°C) | tas, hurs | Count of days where Twb (Stull 2011) ≥ 30°C | days | — | Heat Stress |
| `tasmin_tropical_nights_gt28` | Tropical nights (TN > 28°C) | tasmin | Count of days where tasmin > 28°C | days | — | Heat Stress |

### A.2 Cold Risk

| Slug | Label | Variable(s) | Definition | Units | Baseline | Bundle(s) |
|---|---|---|---|---|---|---|
| `tas_winter_mean` | Winter (DJF) mean temp | tas | Mean of daily mean temperature in months [12,1,2] | °C | — | Cold Risk |
| `tasmin_winter_mean` | Winter (DJF) mean min temp | tasmin | Mean of daily minimum temperature in months [12,1,2] | °C | — | Cold Risk |
| `tnn_annual_min` | Coldest night (TNn) | tasmin | Annual minimum of daily minimum temperature | °C | — | Cold Risk |
| `tasmin_winter_min` | Winter (DJF) min of min temp | tasmin | Minimum of daily minimum temperature in months [12,1,2] | °C | — | Cold Risk |
| `tnle10_cold_nights` | Cold nights (TN ≤ 10°C) | tasmin | Count of days where tasmin ≤ 10°C | days | — | Cold Risk |
| `tnle5_severe_cold_nights` | Severe cold nights (TN ≤ 5°C) | tasmin | Count of days where tasmin ≤ 5°C | days | — | Cold Risk |
| `txle15_cold_days` | Cold days (TX ≤ 15°C) | tasmax | Count of days where tasmax ≤ 15°C | days | — | Cold Risk |
| `tx10p_cool_days_pct` | Cool days % (TX10p) | tasmax | Fraction of days where tasmax < DOY 10th-pct threshold; 5-day window | % | 1990–2010 | Cold Risk |
| `tn10p_cool_nights_pct` | Cool nights % (TN10p) | tasmin | Fraction of days where tasmin < DOY 10th-pct threshold; 5-day window | % | 1990–2010 | Cold Risk |
| `csdi_cold_spell_days` | Cold spell days (CSDI) | tasmin | Count of days inside cold spells of ≥ 6 consecutive days where tasmin < DOY 10th-pct threshold | days | 1990–2010 | Cold Risk |
| `tnle10_consecutive_cold_nights` | Longest cold-night run (TN ≤ 10°C) | tasmin | Maximum consecutive run of days where tasmin ≤ 10°C | days | — | Cold Risk |

### A.3 Drought Risk

All SPI metrics use Gamma distribution fitted by MoM over the calibration period 1990–2010. Event/spell metrics apply the SPI < −1 threshold (moderate drought onset).

| Slug | Label | Scale | Definition | Units | Period rollup | Bundle(s) |
|---|---|---|---|---|---|---|
| `spi3_count_events_lt_minus1` | SPI-3 drought events | 3 months | Mean annual count of contiguous SPI episodes below −1 | events/yr | Period mean | Drought Risk |
| `spi6_count_events_lt_minus1` | SPI-6 drought events | 6 months | As above at 6-month scale | events/yr | Period mean | Drought Risk |
| `spi12_count_events_lt_minus1` | SPI-12 drought events | 12 months | As above at 12-month scale | events/yr | Period mean | Drought Risk |
| `spi3_max_spell_lt_minus1` | SPI-3 max drought spell | 3 months | Period maximum of within-year longest SPI episode below −1 | months | Period max | Drought Risk |
| `spi6_max_spell_lt_minus1` | SPI-6 max drought spell | 6 months | As above at 6-month scale | months | Period max | Drought Risk |
| `spi12_max_spell_lt_minus1` | SPI-12 max drought spell | 12 months | As above at 12-month scale | months | Period max | Drought Risk |

### A.4 Extreme Rainfall | Flash Flood Risk

All metrics derived from `pr` (mm day⁻¹). ETCCDI standard: Zhang et al. (2011).

| Slug | Label | Definition | Units | Baseline | Bundle(s) |
|---|---|---|---|---|---|
| `pr_max_1day_precip` | Max 1-day precipitation (Rx1day) | Annual maximum of daily precipitation total | mm | — | Extreme Rainfall |
| `pr_max_5day_precip` | Max 5-day precipitation (Rx5day) | Annual maximum of consecutive 5-day precipitation total | mm | — | Extreme Rainfall |
| `r20mm_very_heavy_precip_days` | Very heavy rain days (R20mm) | Count of days where precipitation ≥ 20 mm | days | — | Extreme Rainfall |
| `r95p_very_wet_precip` | Very wet day total (R95p) | Annual total precipitation on days exceeding p95 of baseline wet-day distribution (wet day ≥ 1 mm) | mm | 1990–2010 | Extreme Rainfall |
| `r95ptot_contribution_pct` | Very wet day fraction (R95pTOT) | R95p as a fraction of annual wet-day total × 100 | % | 1990–2010 | Extreme Rainfall |
| `cwd_consecutive_wet_days` | Consecutive wet days (CWD) | Maximum consecutive days with precipitation ≥ 1 mm | days | — | Extreme Rainfall |

### A.5 Riverine Flood

Source: CEMS-GloFAS Global River Flood Hazard Maps Version 2.1 (RP-100 layers). Static snapshot; no SSP scenario dimension.

| Slug | Label | Definition | Units | Role |
|---|---|---|---|---|
| `jrc_flood_depth_index_rp100` | Flood severity index | 5×5 depth-by-extent scoring matrix; ordinal class 1–5 (Very Low to Extreme) | severity class | Scored (weight 1.0) |
| `jrc_flood_depth_rp100` | RP-100 flood depth | Block: p95 of positive flooded-cell depths within polygon. District: flooded-area-weighted mean of block p95 values | m | Display attribute |
| `jrc_flood_extent_rp100` | RP-100 flood extent | Share of polygon area with positive modelled flood depth | fraction (%) | Display attribute |

---

*Document last updated: 2026-06-23*  
*Maintained by: Abu Bakar Siddiqui Thakur*
