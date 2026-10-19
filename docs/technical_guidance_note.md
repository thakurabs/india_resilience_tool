# India Resilience Tool — Technical Guidance Note
## Climate Risk Methodology: Data and Individual Metrics

**Status:** DRAFT — all sections drafted; final review pending
**Scope:** Data sources → downscaling → grid-first compute → individual metrics and their interpretation
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

Climate adaptation in India is planned and financed at the subnational level — by states, districts, and increasingly by blocks — yet the climate-projection products that usually inform those decisions are global, spatially coarse, and aggregated across hazards. India's national policy has directed adaptation planning at this level since the National Action Plan on Climate Change (Government of India 2008), under which every state prepared a State Action Plan on Climate Change. Those first-generation plans were widely criticised: they leaned on heterogeneous, often generic vulnerability assessments and were oriented more toward listing implementable actions than toward redirecting development onto a climate-resilient path (Dubash & Jogesh 2014; Singh et al. 2017). Subsequent national efforts have moved toward a standardised, scientifically grounded basis for this planning — notably the Department of Science and Technology's common framework for climate vulnerability and risk assessment across Indian states and districts (DST 2021).

The demand for better climate-risk data is also visible among practitioners. In a 2022–23 Reserve Bank of India survey, roughly 95 per cent of responding financial institutions reported lacking adequate data to assess climate risk, and respondents specifically called for a national database of climate scenarios at a disaggregated geographic level to assess physical hazards (RBI 2023). The India Resilience Tool (IRT) addresses the part of that gap concerned with spatial resolution and hazard specificity: it turns downscaled CMIP6 projections into district- and block-level, multi-hazard climate metrics that planners can read at the administrative unit they govern.

[FIGURE: fig_02_hazard_exposure_vulnerability_scope.svg | IRT supplies the climate hazard-pressure layer within the broader hazard, exposure, and vulnerability framing used for full risk assessment.]

India spans a subcontinent of climatic regimes — the arid northwest, the monsoon core, the Himalayan north, the peninsular plateau, and long eastern and western coastlines. A single national figure cannot tell a district what it needs to know: one place may be facing rising heat stress, another more intense rainfall, and another longer dry spells. The practical problem for planners is therefore not only whether India is becoming more climate-stressed, but where each hazard is intensifying and which administrative units should be prioritised for further assessment. IRT is designed around that need: it translates climate-projection data into district- and block-level hazard metrics that preserve local contrast while remaining simple enough to compare, rank, and use in planning.

The hazards themselves are not isolated. Extreme heat, humid heat, drought, heavy rainfall, winter cold, and riverine flooding can overlap in the same state or district, but their importance varies sharply across India's regions. A district in the Indo-Gangetic plains may need to plan for heat and river flooding together; a coastal district may be more concerned with intense rainfall and flood disruption; a central-Indian district may face drought, heat, and rainfall variability in the same planning cycle. IRT is therefore organised as a multi-hazard tool: it lets planners compare hazard pressures across places, scenarios, and time horizons instead of relying on a single national risk number or a single-hazard map.

IRT sits within a broader climate-risk information landscape. Existing assessments provide important national, state, district, sectoral, or vulnerability-focused views, but planners still need a way to read projected physical hazard pressure at the administrative scale where many adaptation decisions are made. IRT is one layer in that larger assessment stack: it describes climate hazard pressure that can be combined with exposure, vulnerability, adaptive capacity, local knowledge, and sector-specific analysis.

The administrative-unit framing is deliberate. District-level climate analysis already has precedent in Indian policy work: the Government of India's *Economic Survey 2017–18* used district-level percentile thresholds to estimate how temperature and rainfall extremes affect crop yields (Government of India 2018). IRT extends that planning logic across multiple hazards and to both district and block geographies. The sections that follow document how the tool moves from climate data to administrative-unit metrics, and how those metrics should be interpreted.

In the standard hazard × exposure × vulnerability decomposition (IPCC 2022), IRT supplies the **hazard** term. Its individual metrics describe physical conditions, event frequencies, persistence, or departures from local climate. They do not estimate realised loss or the probability that people or assets will be harmed. Interpret each metric in its stated units and combine it with exposure, vulnerability, adaptive capacity, and local knowledge for a full risk assessment.

The rest of this note covers data provenance (§2), downscaling context (§3), grid-first computation and spatial/temporal aggregation (§4), and individual metric definitions (§5). Appendix A provides the metric reference.

---

## 2. Climate Data Sources

IRT combines future climate projections from CMIP6/NEX-GDDP-CMIP6 with a static riverine flood hazard layer from JRC. This section identifies the source datasets, scenarios, time periods, variables, and spatial domain used before the note turns to downscaling and computation.

### 2.1 CMIP6: Model Ensemble and Scenarios

The Coupled Model Intercomparison Project Phase 6 (CMIP6) is the sixth generation of coordinated global climate model experiments and provides the primary projection basis for the IPCC Sixth Assessment Report (AR6). IRT draws on outputs from 24 general circulation models (GCMs), using the **r1i1p1f1** variant for all models and scenarios. This variant denotes the first realisation (r1), initialisation method (i1), physics configuration (p1), and forcing configuration (f1).

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

[FIGURE: fig_05_temporal_coverage_analysis_windows.svg | Raw historical and SSP files are reduced into the 1990–2010 baseline plus three inclusive future analysis windows.]

| Label | Period | Role |
|-------|--------|------|
| Historical baseline | 1990–2010 | Historical indicator reference |
| Near-term | 2020–2040 | Near-term projection |
| Mid-century | 2040–2060 | Mid-century projection |
| End-century | 2060–2080 | End-of-century projection |

Each future window is an inclusive 21-year mean (e.g. 2020–2040 covers the years 2020 through 2040). The endpoint years 2040 and 2060 are each shared by two adjacent windows, so the windows abut rather than leaving gaps between them.

The anchor period (1990–2010) falls entirely within the historical simulation run (1950–2014); no splicing of historical and SSP files is required (→ §4.3).

A fifth period label, **`Current`**, is reserved for static present-day layers that have no climate-projection time dimension — in this note, the **Riverine Flood severity index** (→ §2.4, §5.5). It is paired with the **Snapshot** scenario rather than an SSP pathway, represents a single externally modelled present-day state rather than a multi-year climate average, and carries no averaging window.

### 2.3 NASA-NEX GDDP-CMIP6: The Downscaled Product

IRT uses the **NASA Earth Exchange Global Daily Downscaled Projections, CMIP6** (NEX-GDDP-CMIP6) as its primary climate input. NASA applied the spatial disaggregation and bias correction before data release; the downscaling method is described in §3.

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

**Spatial resolution and domain:** NEX-GDDP-CMIP6 is provided at **0.25° × 0.25°** horizontal resolution (~25 km at the equator). All data are clipped to the India domain: **68.0°E–98.0°E, 6.0°N–38.0°N** (grid dimensions and resolution implications for district vs block analysis are detailed in §3.3).

**Source:** Thrasher et al. (2022) — full citation in References.

### 2.4 JRC Global Flood Data

Riverine flood metrics (→ §5.5) are derived from the **CEMS-GloFAS Global River Flood Hazard Maps** (Version 2.1), a product of the Copernicus Emergency Management Service (CEMS) published by the European Commission Joint Research Centre. IRT uses the **RP-100** (1-in-100-year return period) raster layers: flood depth (metres) and flood extent (binary inundation mask).

Because this is a static, observationally-derived snapshot product rather than a future climate projection, the Riverine Flood domain carries a "Snapshot" scenario label in the tool and is not available under SSP2-4.5 or SSP5-8.5.

**Source:** Baugh et al. (2024) — full citation in References.

---

## 3. Downscaling: What It Is and How NASA-NEX Applies It

IRT does not perform its own statistical downscaling. It uses the NASA-NEX GDDP-CMIP6 product after NASA has bias-corrected and spatially disaggregated the original CMIP6 model outputs. This section explains what that means, why the 0.25° product is appropriate for district-level screening, and what limitations remain when using it for Indian districts and blocks.

### 3.1 What Statistical Downscaling Means

GCMs are designed to simulate the large-scale dynamics of the climate system — atmospheric circulation, ocean–atmosphere coupling, and radiative transfer — at a global scale. Their native grid spacing is typically 100–300 km, meaning a single model grid cell covers an area comparable to a large Indian state. At this resolution, GCMs cannot resolve the terrain-driven heterogeneity, coastal gradients, or local land–surface feedbacks that determine temperature and rainfall patterns at the district or block level.

**Downscaling** is the process of translating GCM output from its native coarse resolution to a finer spatial scale more appropriate for regional and local analysis. Two broad approaches exist:

- **Dynamic downscaling** nests a higher-resolution regional climate model (RCM) within the GCM domain, simulating regional atmospheric dynamics explicitly. It is computationally intensive and not available at the pan-India, 24-model scale required here.
- **Statistical downscaling** uses the statistical relationship between coarse-resolution GCM output and observed fine-resolution climatology to correct and spatially disaggregate GCM fields. It is tractable across large multi-model ensembles.

NASA-NEX GDDP-CMIP6 applies the statistical approach. IRT uses the NASA-NEX product as published online, without modifying the NASA downscaling method.

### 3.2 The BCSD Method in NASA-NEX GDDP

The NASA-NEX GDDP-CMIP6 product employs a **daily variant of Bias Correction and Spatial Disaggregation (BCSD)** (Wood et al. 2002; Maurer et al. 2010; Thrasher et al. 2022). The method combines distributional bias correction with spatial disaggregation to a 0.25° grid:

[FIGURE: fig_06_bcsd_schematic.svg | BCSD bias-corrects model distributions against reference climatology, then combines GCM change signals with observed spatial detail on the 0.25° grid.]

**Step 1 — Bias correction**

For each GCM and variable, the method compares historical model output with corresponding observationally derived reference data over a common reference period. Quantile mapping is then used to adjust the modelled distribution so that the corrected historical values are more consistent with the observed reference distribution. The reference forcing dataset used by NASA-NEX is the Global Meteorological Forcing Dataset (GMFD; Sheffield et al. 2006), as described in Thrasher et al. (2022).

Formally, let $F_{\text{obs}}$ and $F_{\text{mod}}$ denote the empirical CDFs of the observed and modelled distributions over the bias-correction reference period. The bias-corrected value $x'$ for a raw model value $x$ is:

$$x' = F_{\text{obs}}^{-1}\!\bigl(F_{\text{mod}}(x)\bigr)$$

The transfer function is derived from the historical overlap between model and reference data and then applied to both historical and future model output. In the future period, the model's climate-change signal is therefore retained relative to the bias-corrected historical distribution.

**Step 2 — Spatial disaggregation**

After bias correction, the GCM fields are spatially disaggregated to the 0.25° GMFD grid. In simplified terms, the method factors out the observed fine-grid climatology, interpolates the model residual or anomaly fields from the coarse GCM grid to the 0.25° grid, and then factors the fine-grid climatology back in. Temperature variables use additive residuals; precipitation and related moisture variables use multiplicative ratios.

**What BCSD corrects and what it does not**

BCSD corrects important marginal distribution biases and adds observed historical spatial detail to the GCM fields. It does not alter the GCM's large-scale atmospheric dynamics, synoptic circulation patterns, or temporal sequencing, and it does not create new meteorological information below the source-model scale. Biases in monsoon onset timing, intraseasonal oscillations, or the frequency and sequencing of extreme daily events may therefore remain after downscaling (→ §3.4).

### 3.3 Grid Resolution and Spatial Domain

The NEX-GDDP-CMIP6 product is provided at **0.25° × 0.25°** horizontal resolution, corresponding to roughly **25-28 km over India** depending on latitude and direction: 0.25° latitude is about 27.8 km, while 0.25° longitude narrows from about 27.6 km near 6°N to about 21.9 km near 38°N. IRT clips the global product to the India domain — **68.0°E–98.0°E, 6.0°N–38.0°N** — yielding **120 longitude × 128 latitude cells**. Cell centres span **68.125°E–97.875°E and 6.125°N–37.875°N**; the domain bounds above describe the download box and outer cell edges, not the centres.

[FIGURE: fig_08_district_block_resolution_zoom.svg | At district scale, several climate-grid cells contribute to the area-weighted mean; at block scale, multiple small blocks can fall within the same 0.25° cell and inherit the same underlying climate value.]

**Resolution implications at district vs block level**

India has 784 districts (mean area ~4,171 km²) and 7,137 sub-district blocks (mean area ~458 km²). The 0.25° grid is therefore much better matched to district screening than to fine within-district comparison. A grid cell over much of India is on the order of **600-750 km²**, varying by latitude:

- A typical district intersects **4–20** 0.25° cells, so its value is usually an area-weighted summary across several independent grid-cell values.
- A typical block may intersect **fewer than 4** cells, and smaller blocks in densely subdivided states may fall mostly or entirely within a single cell. In that case, neighbouring blocks inside the same cell can receive nearly identical climate-derived values, while a nearby block crossing into the next cell can differ abruptly.

Block-level outputs are therefore useful for screening and for locating broad within-state patterns, but they should not be read as resolving micro-climate variation below the ~25 km source-grid scale. Cross-block contrasts in small or densely subdivided areas may partly reflect grid-cell boundaries rather than true sub-district heterogeneity.

Spatial aggregation from the 0.25° grid to administrative units is described in §4.2.

### 3.4 Provenance and Reproducibility Notes

**Dataset access and version**

NEX-GDDP-CMIP6 is publicly available via the NASA Center for Climate Simulation (https://www.nccs.nasa.gov/services/data-collections/land-based-products/nex-gddp-cmip6). The IRT pipeline ingests the product as released (all 24 GCMs, variant r1i1p1f1, both SSPs). The authoritative dataset description and processing documentation are Thrasher et al. (2022) for the product itself, with Wood et al. (2002) and Sheffield et al. (2006) documenting the underlying BCSD method and reference forcing dataset respectively.

**Internal validation: Telangana domain**

As part of dataset QA, a validation analysis was conducted comparing NEX-GDDP-CMIP6 historical daily output against ERA5 reanalysis and IMD gridded observations over the Telangana domain for the period 1980–1985, using polygon-overlap area weighting for spatial aggregation. Results should be interpreted in the context of this limited spatial and temporal sample; they are illustrative of the dataset's bias characteristics over one state and six historical years, not a full pan-India validation of all models, hazards, or seasons.

*Temperature*: The 24-model ensemble reproduces ERA5 mean near-surface temperature over Telangana with good fidelity. The ERA5 domain-mean daily temperature is 27.3°C; most individual GCMs fall within ±0.5°C of this value (model range approximately 26.8–28.0°C), with tight clustering in normalised standard deviation and spatial correlation visible in the Taylor diagram.

*Precipitation*: Area-mean daily precipitation for ERA5 (2.47 mm day⁻¹) and IMD (2.65 mm day⁻¹) agree closely at the seasonal mean scale. Spatial correlations between district-level precipitation metrics and ERA5 for the best-performing models range from 0.81 to 0.90, indicating moderate spatial skill at district resolution. However, the ensemble systematically underestimates peak daily rainfall intensities: the IMD Rx1day (mean annual maximum 1-day rainfall) for Adilabad district is approximately 77 mm day⁻¹, while the 24 CMIP6 models range from approximately 33 to 55 mm day⁻¹ — a consistent dry bias in extreme events that persists after BCSD.

This underestimation is expected because BCSD applies monthly-scale quantile mapping and then bilinear spatial disaggregation to 0.25°; neither step alters the GCM's underlying atmospheric dynamics. The convective processes that generate intense short-duration rainfall events are parameterised at each GCM's native grid spacing — ranging from approximately 70 km (EC-Earth3, the finest in the ensemble) to 310 km (CanESM5, the coarsest), with most models at 100–200 km; these are approximate physical grid spacings (grid interval × ~111 km), not the coarser CMIP6 nominal-resolution attributes (e.g. 500 km for CanESM5) — at which mesoscale convective systems responsible for high-intensity daily rainfall in the Indian region cannot be explicitly resolved. The BCSD spatial disaggregation resamples these coarse fields to the 0.25° output grid but adds no new sub-grid meteorological information.

The extreme-rainfall underestimation is therefore a structural limitation of the GCM ensemble, not a downscaling artefact. Users interpreting the Extreme Rainfall | Flash Flood metrics (→ §5.2) should note that absolute metric values likely understate observed extreme rainfall intensities.

*Comparative context*: Jain et al. (2019) evaluated the NEX-GDDP (CMIP5-era) product against IMD gridded observations over the Indian subcontinent for the summer monsoon season (1975–2005), benchmarking it against multi-model means from 28 raw CMIP5 models and 10 CORDEX regional models. NEX-GDDP surpassed both CMIP5 and CORDEX in reproducing seasonal mean temperature and precipitation patterns (spatial pattern correlation ~0.8; RMSE ~4.25°C for temperature and ~2.48 mm day⁻¹ for precipitation), inter-annual variability, and annual cycle characteristics. The simulation of extremes was also found to be more realistic in NEX-GDDP relative to raw CMIP5 and CORDEX output, with reduced inter-model spread — supporting the use of the NEX-GDDP product for climate change impact assessment. Although these findings pertain to the CMIP5-era version of NEX-GDDP, the BCSD methodology is common to both the CMIP5 and CMIP6 versions; the results are therefore informative about the relative improvement that the downscaling procedure confers over raw GCM output.

[FIGURE: FIG-V1V2V3_taylor.png | Taylor diagrams summarise Telangana 1980–1985 model skill for temperature and precipitation against ERA5 and IMD references.]

[FIGURE: FIG-V5_rx1day_adilabad.png | Adilabad Rx1day comparison shows observed IMD annual-maximum daily rainfall above the CMIP6 model range for 1980–1985.]

**Known limitations relevant to India**

Three classes of systematic limitation are relevant to users interpreting IRT outputs:

1. **Monsoon dynamics.** The Indian Summer Monsoon (ISM) is influenced by complex land–sea thermal gradients, orographic lifting, and large-scale teleconnections (ENSO, IOD). Most CMIP6 GCMs simulate the broad seasonal cycle of ISM precipitation but exhibit systematic biases in onset date, spatial distribution of the monsoon core and break phases, and sub-seasonal variability. BCSD corrects the monthly distribution but preserves the GCM's underlying dynamical representation of monsoon structure.

2. **Himalayan terrain.** The 0.25° grid (~25 km) cannot resolve the elevation gradients of the Hindu Kush–Himalayan arc, where elevations change by 3,000–5,000 m over tens of kilometres. Temperature and precipitation are subject to large interpolation errors at high altitudes; outputs for Himalayan districts and blocks in Uttarakhand, Himachal Pradesh, Jammu & Kashmir, Sikkim, and Arunachal Pradesh should be interpreted with particular caution.

3. **Coastal resolution.** At 0.25° resolution, coastal grid cells blend land and ocean surface conditions. This can introduce artefacts in temperature and humidity fields for coastal and island districts (including the Kerala coast, Tamil Nadu coast, Lakshadweep, and Andaman & Nicobar Islands).

---

## 4. Grid-First Compute and Post-Processing


This section describes the transformation from gridded climate data to administrative-unit values. The sequence is: compute each climate index on the 0.25° grid, aggregate those grid-cell values to districts and blocks using area-overlap weights, average annual values into multi-year periods, combine the available valid model estimates with equal weights.

### 4.1 Architecture: Why Grid-First

All climate index computations in IRT are performed at the native 0.25° grid resolution before any aggregation to administrative boundaries. This "grid-first" design reflects a methodological choice rooted in the structure of the indices being computed.

[FIGURE: fig_09_admin_first_vs_grid_first.svg | Computing non-linear indices on grid cells before aggregation preserves events that an admin-mean-first workflow can erase.]

An alternative — the "admin-first" approach — would average the raw daily GCM values over each administrative unit before computing indices. This is appropriate for linear statistics such as mean temperature, but introduces bias for any non-linear index.

Consider a district that straddles a dense urban area and a river valley. One 0.25° cell (the city) records five consecutive days at 36–38°C, while an adjacent cell (the valley) records those same days at 28–30°C. The admin-first approach averages the two cells first, producing a district mean of 32–34°C — below the 35°C threshold on every day — and consequently reports **zero** extreme-heat days for the district. The grid-first approach computes five hot days for the city cell and zero for the valley cell, then takes the area-weighted mean: 2.5 hot days for the district.

The admin-first result is not merely less precise — it erases a multi-day extreme heat event that affected half the district. The distortion compounds further for non-linear indices: spell-length metrics, percentile-exceedance fractions, and the SPI gamma transform all produce systematically biased outputs when applied to pre-averaged spatial means.

Operating grid-first also preserves within-unit spatial heterogeneity through the final aggregation step and allows the per-cell index fields to be re-aggregated to any future boundary revision without repeating the (computationally intensive) index computation.

The pipeline accordingly computes each annual climate index as a per-cell 2D field on the 0.25° grid, one year at a time, for each GCM and scenario. These annual index fields are then area-weighted and averaged to the administrative boundary set as described in §4.2.

### 4.2 Spatial Aggregation to District and Block

**Aggregation method**

Spatial aggregation from the 0.25° grid to administrative polygons uses **fractional area overlap**: for each (polygon, cell) pair, the area of intersection between the cell tile and the polygon is computed, and the resulting intersection area is used as the weight in a weighted average.

[FIGURE: fig_10_fractional_area_overlap_weights.svg | Fractional area-overlap weights assign each grid-cell value to polygons in proportion to actual intersecting area.]

Formally, let $v_j$ denote the index value at grid cell $j$, and let $a_{ij}$ denote the intersection area (in m²) between administrative unit $i$ and cell $j$. The aggregated value for unit $i$ is:

$$\bar{v}_i = \frac{\sum_j a_{ij}\, v_j}{\sum_j a_{ij}}$$

where the sum runs over all cells $j$ that intersect unit $i$. Cells with no intersection or with missing ($\text{NaN}$) index values are excluded from both numerator and denominator.

Grid cell tile boundaries are defined as midpoints between adjacent cell centres: a cell at latitude $\phi$ and longitude $\lambda$ occupies the tile $[\phi - \delta/2,\, \phi + \delta/2] \times [\lambda - \delta/2,\, \lambda + \delta/2]$ where $\delta = 0.25°$.

Both the administrative boundary polygons (stored in EPSG:4326, the universal geographic coordinate standard) and these grid cell tile boxes are reprojected to the **EPSG:6933 Equal-Area Cylindrical** projected coordinate reference system before any intersection or area calculation is performed. This reprojection is necessary because a 0.25° × 0.25° angular tile covers a larger physical area near the equator than near the Himalayas; computing intersection weights in degree-space would therefore over-represent equatorial cells. In EPSG:6933 the intersection areas are in m² and are geometrically correct across all latitudes.

This approach — sometimes called rasterize-to-polygon area weighting or exact polygon-cell intersection — is more accurate than centroid-in-polygon methods for small or irregularly shaped units, where a cell centroid may fall outside the polygon even though a substantial fraction of the cell area overlaps it.

**Administrative boundary set**

The canonical boundary set is the **LGD (Local Government Directory)** boundary dataset at two levels:

- **District (ADM2):** 784 units, India-wide
- **Block / sub-district (ADM3):** 7,137 units, India-wide

District-level and block-level climate metrics are computed independently from the same 0.25° grid, using the polygons at each administrative level.

Concretely, the 784 district polygons are intersected with the 0.25° grid to produce a district-level lookup table recording, for each (district, cell) pair, the fraction of the cell area that falls within that district. Separately, the 7,137 block polygons are intersected with the same grid to produce an equivalent block-level lookup. When a climate index field is computed on the grid, it is aggregated to district values using the district lookup and to block values using the block lookup — two independent aggregation passes over the same underlying grid field.

For a linear area-weighted mean, district values and area-weighted block rollups agree when boundaries partition the same area and valid-cell support is identical. Missing-data support and nonlinear reductions can break this equivalence.

### 4.3 Period Aggregation and Ensemble Handling

**Temporal aggregation chain**

For each metric, each GCM, and each scenario, the pipeline applies a three-stage temporal aggregation:

[FIGURE: fig_11_temporal_aggregation_ensemble_chain.svg | Daily fields are reduced to annual cell indices, then period means, then ensemble summaries before publication.]

1. **Daily → annual index.** For each calendar year $y$, the daily gridded data for that year (and, for some metrics, the preceding year) are reduced to a single annual index value per cell. The specific reduction depends on the metric family — annual mean, exceedance count, peak intensity, SPI transform, and so on — and is defined metric by metric in §5. The output is a per-cell annual index field.

2. **Annual → period mean.** For each analysis period (e.g. 2020–2040), the per-cell annual index values for all years within that period are averaged to produce a single per-cell period-mean field. The averaging is an unweighted arithmetic mean over years.

3. **Period mean → ensemble mean.** For each geography, metric, scenario, and period, the ensemble mean gives **equal weight to each available valid per-model period mean**. Missing model values are excluded, not replaced with zero. The contributing model count can vary with the metric's required variables and data coverage; the archive roster is not a guarantee that all models contribute to every value. Ensemble spread statistics include standard deviation, median, and 5th and 95th percentiles where available.

This three-stage chain is designed to separate climate signal from noise at two distinct scales. Averaging annual index values over each multi-decadal window (stage 2) dampens interannual variability driven by modes such as ENSO and the Indian Ocean Dipole, reducing the imprint of any particular sequence of years and bringing out the underlying forced climate change signal. The 21-year windows should therefore be read as period-mean estimates, not forecasts of any single year within the period.

This averaging does not eliminate local uncertainty. Santer et al. (2011) show that forced climate signals become easier to distinguish from internal variability as the averaging or trend-detection window lengthens, while Hawkins and Sutton (2012) formalise this as the *time of emergence* — the point at which the forced signal rises detectably above natural variability. Those results motivate multi-decadal averaging, but district- and block-scale indices still retain more local variability than global-mean temperature series.

Averaging across 24 GCMs (stage 3) reduces sensitivity to the structural biases of any individual model; Tebaldi and Knutti (2007) provide the foundational treatment of this argument, showing that multi-model ensemble means systematically outperform individual model projections because model-specific errors arising from different structural choices are partially uncorrelated across the ensemble and therefore partially cancel in the mean. The two operations are applied in sequence — time-averaging first, then ensemble-averaging — so that each model's period mean contributes equally to the ensemble average regardless of its interannual variance.

**Analysis periods**

| Scenario | Period |
|----------|--------|
| Historical | 1990–2010 |
| SSP2-4.5 and SSP5-8.5 | 2020–2040 |
| SSP2-4.5 and SSP5-8.5 | 2040–2060 |
| SSP2-4.5 and SSP5-8.5 | 2060–2080 |

Each future window is an inclusive 21-year mean (e.g. 2020–2040 covers 2020 through 2040), with the endpoint years 2040 and 2060 each shared by two adjacent windows (→ §2.2).


## 5. Individual Metric Definitions

The five subsections below describe individual hazard metrics. Derivations are given for IRT-specific and non-standard indices; for canonical ETCCDI indices, the definition is cited and IRT-specific parameter choices are noted. Appendix A lists definitions, units, and baseline periods.

### 5.1 Temperature and Heat Metrics

Temperature-related metrics are grouped into three domains: Heat Risk (daytime and nocturnal extremes, heatwave characteristics), Heat Stress (humid-heat metrics defined in §5.4, combined with two dry-heat persistence metrics — WSDI and TN90p — that it shares with Heat Risk and which are defined below), and Cold Risk (winter cold extremes and cold-spell persistence). All temperature metrics are derived from daily mean (`tas`), maximum (`tasmax`), or minimum (`tasmin`) temperature, each converted from Kelvin to Celsius before index calculation.

Metrics are organised in five groups across these domains:

- **Background means.** Annual and seasonal mean temperature. Summer is defined as **March–May (MAM)**; winter as **December–January–February (DJF)**.
- **Absolute extremes.** TXx (annual maximum of tasmax), TNx (warmest night of tasmin), TNn (coldest night of tasmin), and DJF minimum of tasmin — standard ETCCDI indices (Zhang et al. 2011) recording the most extreme individual day of the year.
- **Threshold-frequency indices.** Counts of days crossing fixed thresholds. Hot-day thresholds (TX ≥ 30°C, TX ≥ 35°C) follow IMD operational criteria; tropical-night thresholds (TN > 25°C for Heat Risk; TN > 28°C for Heat Stress) are India-adapted. Cold thresholds (TN ≤ 10°C, TN ≤ 5°C, TX ≤ 15°C) are calibrated to the cold-season climate of the Indian plains.
- **Percentile-relative indices.** TX90p, TN90p, TX10p, TN10p express the fraction of days exceeding or falling below a locally calibrated day-of-year (DOY) percentile threshold. These capture relative shifts in the local temperature distribution regardless of absolute temperature level.
- **Heatwave and cold-spell characteristics.** WSDI (warm spell days), CSDI (cold spell days), hwfi (heatwave spell days), hwfi_events (heatwave event count), hwa (heatwave amplitude), and tnle10_consecutive (longest consecutive cold-night run).

**DOY percentile threshold framework**

All percentile-relative and spell metrics share a common threshold derivation aligned with the ETCCDI TX90p method (Zhang et al. 2011). For each grid cell and calendar day $d = 1, \ldots, 365$ (February 29 excluded), all baseline-period daily values falling within a symmetric ±2-day window centred on $d$ are pooled. For the 21-year baseline this yields approximately 105 values per day-of-year. The $p$-th percentile of this pooled set is the threshold $\tau_d$:

[FIGURE: fig_12_doy_percentile_threshold_curve.svg | The day-of-year percentile threshold is built from a moving five-day baseline window and reused for all evaluation years.]

$$\tau_d = \text{quantile}_p\!\bigl(\{x_{y,d'} : y \in [y_1, y_2],\; |d' - d| \leq 2\}\bigr)$$

where $d'$ is measured on the 365-day no-leap calendar and $[y_1, y_2]$ is the baseline period. During evaluation, day $t$ with value $x_t$ is classified as an exceedance when $x_t > \tau_{d(t)}$ (strict greater-than throughout). The threshold vector $\tau_d$ is computed once per (model, variable, baseline configuration) and applied unchanged to all evaluation years including SSP projections.

The percentile differs between warm and cold index families, but the baseline period is held fixed:

| Index family | Percentile | 
|---|---|
| TX90p, TN90p, WSDI, hwfi, hwa | 90th | 
| TX10p, TN10p, CSDI | 10th | 

**Baseline period.** A single reference period **1990–2010** is used throughout for every percentile threshold and distribution fit: the temperature percentile and spell indices above, the precipitation percentile indices of §5.2, and the SPI calibration of §5.3. The same window also serves downstream as the **historical reference period** (§2.2/§4.3): the historical values reported alongside each projection are measured against 1990–2010. These two uses share one set of years but act at different stages — as a *baseline* it calibrates each index's internal thresholds and fits (the subject of this section); as a *reference period* it anchors change comparisons after the indices are computed. Holding one window fixed across these roles keeps the relative indices and cross-period comparisons mutually consistent.

**Spells.** Several indices count *spells* — maximal runs of consecutive days that satisfy an exceedance condition. The minimum qualifying run length differs by index: WSDI and CSDI require ≥6 consecutive days (ETCCDI convention), whereas the heatwave indices hwfi and hwa require ≥5 consecutive days — an IRT/ETCCDI-style design choice, not an IMD criterion. (IMD's own heat-wave declaration uses a *two*-consecutive-day duration criterion, declared on the second qualifying day; the 5-day minimum here is the tool's own spell-length convention.) Spells are evaluated within a calendar year; a run is not carried across the year boundary.

**Worked example — TX90p for one cell.** Consider a single 0.25° grid cell and the calendar day 1 May (day-of-year 121). Pooling all daily `tasmax` values from 29 April–3 May across the 21 baseline years 1990–2010 gives 5 × 21 = 105 values; their 90th percentile is the threshold $\tau_{121}$ (say, 41.2°C). Repeating this for every calendar day traces the smooth seasonal threshold curve $\tau_d$. In any evaluation year, TX90p is the percentage of days whose `tasmax` exceeds that day's threshold:

$$\text{TX90p} = 100 \times \frac{1}{N}\sum_t \mathbb{1}\!\left[x_t > \tau_{d(t)}\right]$$

By construction this is ≈ 10% under the baseline climate; a warming year pushes it well above 10%, which is the relative-shift signal the index is designed to capture.

**Heatwave amplitude (hwa)**

Heatwave amplitude is an IRT-specific index. For each year and grid cell, the DOY-90th-percentile framework (baseline 1990–2010, applied to tasmax, minimum spell length 5 consecutive days) identifies all heatwave spells $s$ within the year. For each spell, the mean daily exceedance above the per-day threshold is:

$$\bar{\epsilon}_s = \frac{1}{|s|} \sum_{t \in s} \bigl(x_t - \tau_{d(t)}\bigr)$$

The spell with the largest mean exceedance $\bar{\epsilon}_{s^*}$ is selected as the "worst" heatwave of the year. The amplitude is the peak daily maximum temperature within that spell:

$$\text{hwa} = \max_{t \in s^*}\, x_t \quad ({}^\circ\text{C})$$

This captures both the persistence and the intensity of the strongest annual heatwave event as a single value in absolute Celsius — not an anomaly relative to a threshold.

### 5.2 Precipitation and Extreme Rainfall Metrics

The precipitation indices characterise rainfall intensity and accumulation. All six Extreme Rainfall | Flash Flood Risk metrics are derived from the daily precipitation variable `pr` (converted from kg m⁻² s⁻¹ to mm day⁻¹). Four are canonical ETCCDI indices with no IRT-specific departures: **Rx1day** (annual maximum 1-day total), **Rx5day** (annual maximum 5-day running total), **R20mm** (count of days with precipitation ≥ 20 mm), and **CWD** (maximum consecutive wet days, where a wet day is any day with precipitation ≥ 1 mm). See Appendix A for parameter details.

The two percentile-based indices require a baseline:

**R95p** is the annual total precipitation contributed by very wet days — days whose daily precipitation exceeds the 95th percentile of the wet-day precipitation distribution in the baseline period 1990–2010. Wet days are defined as days with precipitation ≥ 1 mm. The 95th-percentile threshold is a single per-cell scalar computed from pooling all wet-day values across the full baseline (not DOY-specific). For each evaluation year, precipitation is accumulated over all days that exceed this threshold.

**R95pTOT** expresses the fraction of annual wet-day precipitation contributed by very wet days:

$$\text{R95pTOT} = \frac{\text{R95p}}{\text{PRCPTOT}} \times 100 \quad (\%)$$

where PRCPTOT is the annual total precipitation on wet days (≥ 1 mm). Both indices use baseline period 1990–2010.

### 5.3 Drought Indices: Dry Spells, Aridity, and SPI

Where §5.2 captures rainfall excess, drought concerns sustained dry conditions and rainfall deficits. Drought is not a single quantity, and IRT computes three families that answer different physical questions:

| Family | Physical question | Frame |
|---|---|---|
| Consecutive dry days (CDD) | How long does the longest rainless run last? | Absolute — days, meaning the same everywhere |
| Aridity index (P/PET) | Over a year, how much water does the sky supply against how much the atmosphere can evaporate? | Absolute — a dimensionless ratio, meaning the same everywhere |
| SPI-3 / SPI-6 / SPI-12 | How unusual is this accumulation for *this* place? | Relative — a departure from local climatology |

The first two are absolute statements about the physical world: 152 dry days, or a supply-to-demand ratio of 0.07, mean the same thing in Rajasthan and in Kerala. SPI is a departure from each unit's own history, so a humid place having an unusually dry year and a desert having an unusually dry year register at similar SPI values. Read the absolute indicators and locally standardized SPI as complementary measures with different reference scales.

**Consecutive dry days (CDD)**

CDD is the longest run of consecutive days with precipitation below 1 mm within the year, averaged over the analysis period (Climdex definition; Zhang et al. 2011 — full catalogue entry in A.6). It measures *dry-spell persistence*: how long a place must go without resupply.

Its known limitation is that it conflates aridity with seasonality. A sharply seasonal but adequately watered regime records a long dry spell for the same reason a desert does, so Nagpur (≈128 days) sits near the Thar (≈152 days) despite receiving several times the rainfall. Closing that gap is the purpose of the aridity index below.

**Aridity index (P/PET)**

The aridity index states the water balance directly: annual precipitation divided by annual **potential evapotranspiration** (PET), both in millimetres, so the ratio is dimensionless. PET is the water a continuously moist surface *could* evaporate given the available energy — the atmosphere's demand, set against precipitation's supply. A ratio of 1 means a year's rainfall exactly meets a year's demand; below 1 the atmosphere demands more than the sky delivers.

*Potential evapotranspiration.* PET is computed with the **Hargreaves–Samani** equation as published in FAO-56 (Hargreaves and Samani 1985; Allen et al. 1998, eq. 52). FAO recommends it for precisely this data-limited case — daily maximum and minimum temperature and latitude, with no wind, humidity, or measured radiation — which is what the downscaled archive carries:

$$\mathrm{PET} = 0.0023\,\bigl(T_{\text{mean}} + 17.8\bigr)\,\sqrt{T_{\max} - T_{\min}}\;R_a$$

with $T_{\text{mean}} = (T_{\max} + T_{\min})/2$ by the FAO-56 definition (not daily mean temperature), and PET in mm day⁻¹.

Every term is physical. $R_a$ is **extraterrestrial radiation**, the solar energy arriving at the top of the atmosphere, which depends only on latitude and day of year and is therefore computed from orbital geometry rather than from any model field (FAO-56 eqs. 21–25):

$$R_a=\frac{24(60)}{\pi}\,G_{sc}\,d_r\bigl[\omega_s\sin\varphi\,\sin\delta+\cos\varphi\,\cos\delta\,\sin\omega_s\bigr]$$

where $G_{sc}=0.0820$ MJ m⁻² min⁻¹ is the solar constant, $\varphi$ the latitude in radians, $\delta$ the solar declination, $d_r$ the inverse relative Earth–Sun distance, and $\omega_s$ the sunset hour angle. $R_a$ emerges in MJ m⁻² day⁻¹ and is converted into the depth of water that energy could evaporate by dividing through the latent heat of vaporisation, 2.45 MJ kg⁻¹ — a factor of 0.408. FAO-56 eq. 52 expects $R_a$ in mm day⁻¹, and omitting this conversion would inflate PET by a factor of about 2.45.

The term $(T_{\text{mean}}+17.8)$ scales demand with temperature. The $\sqrt{T_{\max}-T_{\min}}$ term uses the **diurnal temperature range as a proxy for cloudiness**: under clear skies the surface heats strongly by day and radiates freely by night, so a wide daily range implies that a large share of $R_a$ actually reached the ground, while an overcast day has a narrow range. This is how the method recovers a radiation signal from temperature alone, and it is why no measured radiation field is required.

Days with $T_{\max}<T_{\min}$ are invalid temperature pairs and produce missing PET, not zero PET. They count against the monthly daily-coverage requirement. This matters at coastal/island cells too: clipping such ranges to zero previously created near-zero annual PET and extreme P/PET values in two models over Lakshadweep. Exactly equal valid temperatures still produce zero PET. PET itself remains floored at zero for valid temperature pairs when the equation would otherwise give negative demand below −17.8 °C.

The implementation is verified against the FAO-56 worked examples: $R_a$ at 20° S on day 246 computes to 32.19 MJ m⁻² day⁻¹ against the published 32.2 (Example 8), and Hargreaves reference evapotranspiration for Lyon in July to 5.04 mm day⁻¹ against the published ≈5.0 (Example 19).

*Forming the ratio.* Daily PET is summed to calendar-month totals under the same ≥90% daily-coverage rule applied to precipitation in the SPI derivation below, so a month too sparse to yield a precipitation total is also too sparse to yield a PET total, and the ratio never pairs a full month of supply with a partial month of demand. Both monthly series are placed on a contiguous month axis and trimmed to whole January–December years; a year contributes only where it carries twelve finite months on both sides:

$$\text{P/PET}_{y}=\frac{\sum_{m=1}^{12} P_{m,y}}{\sum_{m=1}^{12} \mathrm{PET}_{m,y}}$$

The ratio is formed **within each grid cell, before any spatial aggregation** (→ §4.1). PET is a non-linear function of temperature and latitude, so averaging temperature across a district and then computing PET would answer a different — and incorrect — question from averaging the per-cell ratios. Aggregating the finished ratio also keeps the two administrative levels mutually consistent: an area-weighted rollup of a district's block values reproduces the district value to within numerical precision (→ §4.2).

*Reading the value.* Because PET is the demand of a permanently moist surface, a condition that rarely obtains, the dryland boundary sits well below 1: rainfall is seasonal and soil stores it, so a place receiving two-thirds of its annual demand still supports rainfed cropping and closed-canopy forest. The index carries the absolute UNEP/FAO desertification classes (Middleton and Thomas 1992), which provide a common physical interpretation across locations:

| Class | P/PET |
|---|---|
| Hyper-arid | < 0.05 |
| Arid | 0.05 – 0.20 |
| Semi-arid | 0.20 – 0.50 |
| Dry sub-humid | 0.50 – 0.65 |
| Humid | ≥ 0.65 |

The index is oriented **lower-is-worse** — a low ratio is a water-scarce climate — and it is unbounded above, with humid districts running past 1 (Kerala districts fall between roughly 1.3 and 1.9). It separates regimes that CDD cannot: Nagpur at ≈0.62 (dry sub-humid) against Jaisalmer at ≈0.07 (arid) is close to a ninefold separation, where the same two places differ by a factor of 1.2 in CDD.

*Two caveats, both material to interpretation.* First, **a rising annual P/PET is not a falling drought hazard.** Across the ensemble, projected Indian precipitation rises considerably faster than projected PET, because CMIP6 robustly intensifies the summer monsoon; the annual water balance therefore improves under both SSPs, including over the Thar. That is what the driving models project and the index reports it faithfully, but the projected change in Indian drought is largely *intra-seasonal* — longer dry spells within a wetter year — and no annual supply-versus-demand statistic can represent it. The index should be read as a statement of *where* water is scarce, with CDD carrying the complementary *how long without rain* signal.

Second, **PET-based indices are known to overstate future drying.** PET treats atmospheric demand as purely energy-driven and takes no account of stomatal closure under elevated CO₂, which reduces actual transpiration for a given evaporative demand (Roderick et al. 2015; Milly and Dunne 2016). Any drying trend read from a PET-based index should be treated as an upper bound.

*Ensemble note.* PET requires both `tasmax` and `tasmin`. One of the 24 models (IITM-ESM, §2.1) publishes neither, so the aridity index is a 23-model ensemble mean where the precipitation-only indices in this section use 24 (→ §4.3).

**Standardised Precipitation Index (SPI)**

IRT also computes the **Standardised Precipitation Index** (SPI; McKee et al. 1993), which expresses accumulated monthly precipitation relative to its locally fitted distribution. SPI-3, SPI-6, and SPI-12 describe deficits over different accumulation timescales relative to each location’s own climatology.

**SPI derivation**

For each grid cell, the daily precipitation field (`pr`, converted to mm) is first summed to calendar-month totals — a month is retained only if at least 90% of its days carry finite values, otherwise it is set missing — yielding a contiguous monthly precipitation series trimmed to whole calendar (January–December) years. This monthly series is the input to the **Standardised Precipitation Index** computation, which IRT performs with the open-source `climate_indices` Python package (Adams 2021). Monthly totals are accumulated over rolling $k$-month windows (where $k = 3$, $6$, or $12$), and the resulting series for each cell is fitted to a two-parameter **Gamma distribution** over the calibration period 1990–2010 using the Method of Moments estimator:

[FIGURE: fig_14_spi_derivation.svg | SPI converts monthly precipitation totals into accumulated anomalies, fits a Gamma distribution, and maps the result to a standard-normal drought index.]

$$f(x;\, \alpha, \beta) = \frac{x^{\alpha-1}\, e^{-x/\beta}}{\beta^\alpha\, \Gamma(\alpha)}, \quad x > 0$$

Zero-precipitation months occur with probability $q = P(x = 0)$, estimated as the fraction of zero months in the baseline. The Gamma fit applies to positive-precipitation months only. The mixed cumulative distribution function is:

$$H(x) = q + (1 - q)\, G(x;\, \alpha, \beta)$$

SPI is obtained by mapping this CDF through the standard normal quantile function:

$$\text{SPI} = \Phi^{-1}\!\bigl(H(x)\bigr)$$

The Gamma parameters ($\alpha$, $\beta$, $q$) are estimated once from the 1990–2010 historical run and applied unchanged to SSP future data, preserving cross-period comparability of SPI values.

**SPI annual metrics**

Two annual aggregation statistics are derived from the monthly SPI series per cell, per year:

- **Count of drought events**: number of contiguous episodes per year during which SPI remains continuously below −1, averaged over the analysis period. 
- **Maximum drought spell**: longest consecutive-month period per year during which SPI is continuously below −1, expressed as the period maximum over the analysis window (not the mean).

These per-cell annual metric fields are then area-weighted and aggregated to administrative units following the procedure in §4.2.

### 5.4 Wet-Bulb Temperature and Humid Heat Metrics

Humid-heat metrics couple temperature with humidity. The body's primary cooling mechanism under heat stress is evaporative sweat loss; at high humidity this mechanism is impaired, generating physiological strain at air temperatures well below those dangerous in dry conditions. Wet-bulb temperature ($T_{wb}$) integrates both air temperature and ambient humidity into a quantity directly proportional to the ambient evaporative cooling capacity. Sherwood and Huber (2010) established 35°C wet-bulb as the theoretical limit of human thermoregulation — the level above which the body can no longer shed metabolic heat even at rest in shade — and Raymond et al. (2020) documented that this limit has begun to be approached, and briefly exceeded, in parts of South Asia and the Persian Gulf. Subsequent empirical work has revised the practical survivability ceiling downward (to roughly 31°C for young, healthy adults under exertion; Vecellio et al. 2022), a revision the tool treats as a cautionary precedent for any threshold it adopts. IRT uses working thresholds of 28°C and 30°C, corresponding to severe and very severe occupational heat stress relevant to India's outdoor labour conditions.

**Stull (2011) approximation**

$T_{wb}$ is computed from daily near-surface air temperature ($T$ in °C, from `tas`) and near-surface relative humidity ($RH$ in %, from `hurs`) using the empirical approximation of Stull (2011):

$$T_{wb} = T \cdot \arctan\!\bigl(0.151977\,\sqrt{RH + 8.313659}\,\bigr) + \arctan(T + RH) - \arctan(RH - 1.676331) + 0.00391838 \cdot RH^{1.5} \cdot \arctan(0.023101\,RH) - 4.686035$$

This approximation has a mean absolute error below 0.3°C relative to the psychrometric wet-bulb across its validity range ($-20°\text{C} \leq T \leq 50°\text{C}$, $5\% \leq RH \leq 99\%$, excepting jointly low-humidity and cold conditions), covering the $0°\text{C}$–$50°\text{C}$ range of tropical and subtropical conditions in India. $T_{wb}$ is computed day-by-day from the daily `tas` and `hurs` fields before spatial aggregation, consistent with the grid-first architecture (→ §4.1). The summer season for the summer-mean wet-bulb metric is March–May (MAM), consistent with the summer temperature means in §5.1.

### 5.5 Riverine Flood Metrics (JRC)

Riverine flood metrics depart from the climate grid entirely. They are derived from the CEMS-GloFAS RP-100 raster layers (→ §2.4) and are static snapshots with no SSP scenario dimension. All three metrics are computed directly on the raster and then aggregated to administrative polygons; they do not pass through the 0.25° climate grid.

**jrc_flood_depth_rp100** — Mean peak flood depth. At block level: the 95th percentile of positive flooded-cell depth values within the polygon, capturing the severe tail of the inundation depth distribution. At district level: the flooded-area-weighted mean of constituent block p95 values. Units: metres.

**jrc_flood_extent_rp100** — Share of the polygon's total area covered by positive modelled flood depth, displayed as a percentage. Units: fraction (shown as %).

**jrc_flood_depth_index_rp100** — Flood severity class (ordinal 1–5: Very Low, Low, Moderate, High, Extreme). Each block's RP-100 flood depth and flood extent are first binned independently into 1–5 classes, then combined through a fixed 5×5 lookup matrix.

[FIGURE: fig_15_jrc_rp100_severity_lookup_matrix.svg | The RP-100 flood severity class is read from a fixed depth-by-extent matrix at block level.]

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

Severity is scored at **block** level. District severity is the **flooded-area-weighted mean of constituent block severity classes**, computed bottom-up because directly classifying district-scale depth and extent collapses most districts to the lowest classes — district polygons are far larger than the block scale at which the bins were calibrated.

---

## Appendix A: Complete Metric Reference

The tables below define individual hazard metrics and their units. Sections A.1–A.5 cover the main hazard families; A.6 records additional source metrics.

Abbreviations: DOY = day-of-year percentile threshold; MAM = March–May; DJF = December–January–February; MoM = Method of Moments.

### A.1 Heat Risk and Heat Stress

| Label | Variable(s) | Definition | Units | Baseline | Domain(s) |
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

| Label | Variable(s) | Definition | Units | Baseline | Domain(s) |
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

All SPI metrics use a Gamma distribution fitted by MoM over the calibration period 1990–2010. Event/spell metrics apply the SPI < −1 threshold (moderate drought onset). The aridity index is the exception in this table: it is an absolute ratio and is standardised against no baseline at all. CDD is catalogued in A.6.

| Label | Scale | Definition | Units | Period rollup | Domain(s) |
|---|---|---|---|---|---|
| Aridity index (P/PET) | Annual | Annual precipitation total divided by annual Hargreaves–Samani potential evapotranspiration total, formed per grid cell before aggregation (§5.3); lower is drier | dimensionless | Period mean | Drought Risk |
| SPI-3 drought events | 3 months | Mean annual count of contiguous SPI episodes below −1 | events/yr | Period mean | Drought Risk |
| SPI-6 drought events | 6 months | As above at 6-month scale | events/yr | Period mean | Drought Risk |
| SPI-12 drought events | 12 months | As above at 12-month scale | events/yr | Period mean | Drought Risk |
| SPI-3 max drought spell | 3 months | Period maximum of within-year longest SPI episode below −1 | months | Period max | Drought Risk |
| SPI-6 max drought spell | 6 months | As above at 6-month scale | months | Period max | Drought Risk |
| SPI-12 max drought spell | 12 months | As above at 12-month scale | months | Period max | Drought Risk |

### A.4 Extreme Rainfall | Flash Flood Risk

All metrics derived from `pr` (mm day⁻¹). ETCCDI standard: Zhang et al. (2011).

| Label | Definition | Units | Baseline | Domain(s) |
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
| Flood severity index | 5×5 depth-by-extent scoring matrix; ordinal class 1–5 (Very Low to Extreme) | severity class | Ordinal severity metric |
| RP-100 flood depth | Block: p95 of positive flooded-cell depths within polygon. District: flooded-area-weighted mean of block p95 values | m | Physical metric |
| RP-100 flood extent | Share of polygon area with positive modelled flood depth | fraction (%) | Physical metric |

### A.6 Additional source metrics

These additional metrics describe dry-spell persistence, extreme-rainfall concentration, drought-month frequency, and rainfall variability. All four are derived from daily precipitation.

| Label | Definition | Units | Baseline | Domain(s) |
|---|---|---|---|---|
| Consecutive dry days (CDD) | Maximum run of consecutive days with precipitation < 1 mm in the year (a dry day is `pr` < 1 mm; Climdex CDD) | days | — | Drought Risk; Industrial, Investment, Asset (Thermal), Asset (Hydropower), Life & Livelihood |
| Extremely wet-day precipitation (R99p) | Annual total precipitation on days exceeding the 99th percentile of baseline wet-day precipitation (wet day ≥ 1 mm; ETCCDI R99p) | mm | 1990–2010 | Investment |
| SPI-3 moderate-drought months | Annual count of calendar months with 3-month SPI below −1, period-mean rolled up. A persistence/low-flow proxy, distinct from the SPI-3 *event-count* and *max-spell* metrics in A.3 | months | 1990–2010 (SPI calibration) | Asset (Thermal) |
| R95p inter-annual variability (CV) | Coefficient of variation (σ ⁄ μ) of annual R95p very-wet-day totals across the years within the selected future period; an inflow-predictability proxy. The R95p p95 threshold uses the baseline distribution, but the CV itself is computed across the future-period years, not against the baseline | ratio (dimensionless) | 1990–2010 (R95p threshold) | Asset (Hydropower) |

---

## References

Adams, J. (2021). *climate_indices: An open source Python library providing reference implementations of commonly used climate indices* [Computer software]. https://github.com/monocongo/climate_indices

Allen, R. G., Pereira, L. S., Raes, D., and Smith, M. (1998). *Crop evapotranspiration — Guidelines for computing crop water requirements.* FAO Irrigation and Drainage Paper 56. Food and Agriculture Organization of the United Nations, Rome.

Anandhi, A., Frei, A., Pierson, D. C., Schneiderman, E. M., Zion, M. S., Lounsbury, D., and Matonse, A. H. (2011). Examination of change factor methodologies for climate change impact assessment. *Water Resources Research*, 47, W03501. https://doi.org/10.1029/2010WR009104

Baugh, C., Colonese, J., D'Angelo, C., Dottori, F., Neal, J., Prudhomme, C., and Salamon, P. (2024). Global river flood hazard maps (Version 2.1) [Dataset]. European Commission, Joint Research Centre (JRC). https://doi.org/10.2905/JRC.VD32YWG

Department of Science and Technology (DST) (2021). *Climate Vulnerability Assessment for Adaptation Planning in India Using a Common Framework.* DST, Government of India — IIT Mandi, IIT Guwahati and IISc Bengaluru.

Dubash, N. K., and Jogesh, A. (2014). *From Margins to Mainstream? State Climate Change Planning in India.* Centre for Policy Research, New Delhi.

Gasparrini, A., Guo, Y., Hashizume, M., Lavigne, E., Zanobetti, A., Schwartz, J., et al. (2015). Mortality risk attributable to high and low ambient temperature: a multicountry observational study. *The Lancet*, 386(9991), 369–375. https://doi.org/10.1016/S0140-6736(14)62114-0

Government of India (2008). *National Action Plan on Climate Change.* Prime Minister's Council on Climate Change, New Delhi.

Government of India, Ministry of Finance (2018). *Economic Survey 2017–18, Volume I*, Chapter 6: "Climate, Climate Change, and Agriculture: Coping with Climate Change." Department of Economic Affairs, New Delhi.

Hargreaves, G. H., and Samani, Z. A. (1985). Reference crop evapotranspiration from temperature. *Applied Engineering in Agriculture*, 1(2), 96–99. https://doi.org/10.13031/2013.26773

Hawkins, E., and Sutton, R. (2012). Time of emergence of climate signals. *Geophysical Research Letters*, 39, L01702. https://doi.org/10.1029/2011GL050087

IPCC (2022). *Summary for Policymakers.* In *Climate Change 2022: Impacts, Adaptation and Vulnerability* (Working Group II contribution to the Sixth Assessment Report). Cambridge University Press.

Jain, S., Salunke, P., Mishra, S. K., Sahany, S., and Choudhary, N. (2019). Advantage of NEX-GDDP over CMIP5 and CORDEX data: Indian Summer Monsoon. *Atmospheric Research*, 228, 152–160. https://doi.org/10.1016/j.atmosres.2019.05.026

Konda, G., and Vissa, N. K. (2023). Evaluation of CMIP6 models for simulations of surplus/deficit summer monsoon conditions over India. *Climate Dynamics*, 60, 1023–1042. https://doi.org/10.1007/s00382-022-06367-1

Maurer, E. P., Hidalgo, H. G., Das, T., Dettinger, M. D., and Cayan, D. R. (2010). The utility of daily large-scale climate data in the assessment of climate change impacts on daily streamflow in California. *Hydrology and Earth System Sciences*, 14(6), 1125–1138. https://doi.org/10.5194/hess-14-1125-2010

McKee, T. B., Doesken, N. J., and Kleist, J. (1993). The relationship of drought frequency and duration to time scales. *Proceedings of the 8th Conference on Applied Climatology*, 17–22 January, Anaheim, California. American Meteorological Society, 179–184.

Middleton, N., and Thomas, D. S. G. (eds.) (1992). *World Atlas of Desertification.* United Nations Environment Programme / Edward Arnold, London.

Milly, P. C. D., and Dunne, K. A. (2016). Potential evapotranspiration and continental drying. *Nature Climate Change*, 6, 946–949. https://doi.org/10.1038/nclimate3046

OECD and European Commission, Joint Research Centre (2008). *Handbook on Constructing Composite Indicators: Methodology and User Guide.* OECD Publishing, Paris. https://doi.org/10.1787/9789264043466-en

Raymond, C., Matthews, T., and Horton, R. M. (2020). The emergence of heat and humidity too severe for human tolerance. *Science Advances*, 6(19), eaaw1838. https://doi.org/10.1126/sciadv.aaw1838

Reserve Bank of India (RBI) (2023). *Report on Currency and Finance 2022–23: Towards a Greener Cleaner India.* RBI, Mumbai.

Roderick, M. L., Greve, P., and Farquhar, G. D. (2015). On the assessment of aridity with changes in atmospheric CO₂. *Water Resources Research*, 51(7), 5450–5463.

Santer, B. D., Mears, C., Doutriaux, C., Caldwell, P., Gleckler, P. J., Wigley, T. M. L., Solomon, S., Gillett, N. P., Ivanova, D., Karl, T. R., Lanzante, J. R., Meehl, G. A., Stott, P. A., Taylor, K. E., Thorne, P. W., McCarthy, M. P., and Wehner, M. F. (2011). Separating signal and noise in atmospheric temperature changes: The importance of timescale. *Journal of Geophysical Research: Atmospheres*, 116, D22105. https://doi.org/10.1029/2011JD016263

Sheffield, J., Goteti, G., and Wood, E. F. (2006). Development of a 50-year high-resolution global dataset of meteorological forcings for land surface modeling. *Journal of Climate*, 19(13), 3088–3111.

Sherwood, S. C., and Huber, M. (2010). An adaptability limit to climate change due to heat stress. *Proceedings of the National Academy of Sciences*, 107(21), 9552–9555. https://doi.org/10.1073/pnas.0913352107

Singh, C., Deshpande, T., and Basu, R. (2017). How do we assess vulnerability to climate change in India? A systematic review of literature. *Regional Environmental Change*, 17(2), 527–538. https://doi.org/10.1007/s10113-016-1043-y

Stull, R. (2011). Wet-bulb temperature from relative humidity and air temperature. *Journal of Applied Meteorology and Climatology*, 50(11), 2267–2269. https://doi.org/10.1175/JAMC-D-11-0143.1

Tebaldi, C., and Knutti, R. (2007). The use of the multi-model ensemble in probabilistic climate projections. *Philosophical Transactions of the Royal Society A*, 365, 2053–2075. https://doi.org/10.1098/rsta.2007.2076

Thrasher, B., Wang, W., Michaelis, A., Melton, F., Lee, T., and Nemani, R. (2022). NASA Global Daily Downscaled Projections, CMIP6. *Scientific Data*, 9, 262. https://doi.org/10.1038/s41597-022-01393-4

Vecellio, D. J., Wolf, S. T., Cottle, R. M., and Kenney, W. L. (2022). Evaluating the 35°C wet-bulb temperature adaptability threshold for young, healthy subjects (PSU HEAT Project). *Journal of Applied Physiology*, 132(2), 340–345. https://doi.org/10.1152/japplphysiol.00738.2021

Wood, A. W., Maurer, E. P., Kumar, A., and Lettenmaier, D. P. (2002). Long-range experimental hydrologic forecasting for the eastern United States. *Journal of Geophysical Research: Atmospheres*, 107(D20), 4429. https://doi.org/10.1029/2001JD000659

Zhang, X., Alexander, L., Hegerl, G. C., Jones, P., Klein Tank, A., Peterson, T. C., Trewin, B., and Zwiers, F. W. (2011). Indices for monitoring changes in extremes based on daily temperature and precipitation data. *WIREs Climate Change*, 2(6), 851–870. https://doi.org/10.1002/wcc.147

---

*Document last updated: 2026-10-19*
*Maintained by: Abu Bakar Siddiqui Thakur*
