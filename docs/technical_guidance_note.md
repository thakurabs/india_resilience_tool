# India Resilience Tool — Technical Guidance Note
## Climate Risk Methodology: Data, Metrics, and Bundle Construction

**Status:** DRAFT — all sections drafted; final review pending
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

Climate adaptation in India is planned and financed at the subnational level — by states, districts, and increasingly by blocks — yet the climate-projection products that usually inform those decisions are global, spatially coarse, and aggregated across hazards. India's national policy has directed adaptation planning at this level since the National Action Plan on Climate Change (Government of India 2008), under which every state prepared a State Action Plan on Climate Change. Those first-generation plans were widely criticised: they leaned on heterogeneous, often generic vulnerability assessments and were oriented more toward listing implementable actions than toward redirecting development onto a climate-resilient path (Dubash & Jogesh 2014; Singh et al. 2017). Subsequent national efforts have moved toward a standardised, scientifically grounded basis for this planning — notably the Department of Science and Technology's common framework for climate vulnerability and risk assessment across Indian states and districts (DST 2021).

The demand for better climate-risk data is also visible among practitioners. In a 2022–23 Reserve Bank of India survey, roughly 95 per cent of responding financial institutions reported lacking adequate data to assess climate risk, and respondents specifically called for a national database of climate scenarios at a disaggregated geographic level to assess physical hazards (RBI 2023). The India Resilience Tool (IRT) addresses the part of that gap concerned with spatial resolution and hazard specificity: it turns downscaled CMIP6 projections into district- and block-level, multi-hazard climate **hazard-pressure** scores that planners can read at the administrative unit they govern.

[FIGURE: fig_02_hazard_exposure_vulnerability_scope.svg | IRT supplies the climate hazard-pressure layer within the broader hazard, exposure, and vulnerability framing used for full risk assessment.]

India spans a subcontinent of climatic regimes — the arid northwest, the monsoon core, the Himalayan north, the peninsular plateau, and long eastern and western coastlines. A single national figure cannot tell a district what it needs to know: one place may be facing rising heat stress, another more intense rainfall, and another longer dry spells. The practical problem for planners is therefore not only whether India is becoming more climate-stressed, but where each hazard is intensifying and which administrative units should be prioritised for further assessment. IRT is designed around that need: it translates climate-projection data into district- and block-level hazard-pressure scores that preserve local contrast while remaining simple enough to compare, rank, and use in planning.

The hazards themselves are not isolated. Extreme heat, humid heat, drought, heavy rainfall, winter cold, and riverine flooding can overlap in the same state or district, but their importance varies sharply across India's regions. A district in the Indo-Gangetic plains may need to plan for heat and river flooding together; a coastal district may be more concerned with intense rainfall and flood disruption; a central-Indian district may face drought, heat, and rainfall variability in the same planning cycle. IRT is therefore organised as a multi-hazard tool: it lets planners compare hazard pressures across places, scenarios, and time horizons instead of relying on a single national risk number or a single-hazard map.

IRT sits within a broader climate-risk information landscape. Existing assessments provide important national, state, district, sectoral, or vulnerability-focused views, but planners still need a way to read projected physical hazard pressure at the administrative scale where many adaptation decisions are made. IRT is one layer in that larger assessment stack: it describes climate hazard pressure that can be combined with exposure, vulnerability, adaptive capacity, local knowledge, and sector-specific analysis.

The administrative-unit framing is deliberate. District-level climate analysis already has precedent in Indian policy work: the Government of India's *Economic Survey 2017–18* used district-level percentile thresholds to estimate how temperature and rainfall extremes affect crop yields (Government of India 2018). IRT extends that planning logic across multiple hazards and to both district and block geographies. The sections that follow document how the tool moves from climate data to administrative-unit metrics, then to thematic and sectoral hazard-pressure scores.

In the standard hazard × exposure × vulnerability decomposition (IPCC 2022), IRT supplies the **hazard** term. The scores are climate **hazard-pressure** indices, not full risk scores in the IPCC sense; the word "Risk" in a bundle name denotes climate hazard pressure relevant to that sector, not a sectoral impact model. They are meant for **relative prioritisation**: comparing districts or blocks, and comparing scenario-period choices, to flag where hazard pressure is high or rising and where further assessment should begin. A "Health Risk 80" means high climate hazard pressure relevant to health at that location; it does not mean that 80% of people will be harmed, nor does it estimate realised impact, loss, or probability.

The rest of this note explains that hazard-pressure layer in the order it is built. It covers data provenance (§2), downscaling context (§3), grid-first computation and spatial/temporal aggregation (§4), individual metric definitions (§5), thematic bundle construction (§6), sectoral bundle construction (§7), and composite output (§8). The appendices provide the complete metric reference and the sectoral impact-band catalogue.

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
| Historical baseline | 1990–2010 | Historical indicator reference; also included in the climate ruler fitting population (→ §6.2) |
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

**Spatial resolution and domain:** NEX-GDDP-CMIP6 is provided at **0.25° × 0.25°** horizontal resolution (~25 km at the equator). All data are clipped to the India domain: **68.0°E–97.5°E, 5.0°N–45.0°N** (grid dimensions and resolution implications for district vs block analysis are detailed in §3.3).

**Source:** Thrasher et al. (2022) — full citation in References.

### 2.4 JRC Global Flood Data

Riverine flood metrics (→ §5.5) are derived from the **CEMS-GloFAS Global River Flood Hazard Maps** (Version 2.1), a product of the Copernicus Emergency Management Service (CEMS) published by the European Commission Joint Research Centre. IRT uses the **RP-100** (1-in-100-year return period) raster layers: flood depth (metres) and flood extent (binary inundation mask).

Because this is a static, observationally-derived snapshot product rather than a future climate projection, the Riverine Flood bundle carries a "Snapshot" scenario label in the tool and is not available under SSP2-4.5 or SSP5-8.5 (→ §6.1).

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

The NEX-GDDP-CMIP6 product is provided at **0.25° × 0.25°** horizontal resolution, corresponding to roughly **25-28 km over India** depending on latitude and direction: 0.25° latitude is about 27.8 km, while 0.25° longitude narrows from about 27.6 km near 5°N to about 19.7 km near 45°N. IRT clips the global product to the India domain — **68.0°E–97.5°E, 5.0°N–45.0°N** — yielding a domain of 118 × 160 grid cells (29.5° ÷ 0.25° = 118 columns, 40.0° ÷ 0.25° = 160 rows).

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

The extreme-rainfall underestimation is therefore a structural limitation of the GCM ensemble, not a downscaling artefact. Users interpreting the Extreme Rainfall | Flash Flood bundle (→ §5.2) should note that absolute metric values likely understate observed extreme rainfall intensities.

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

[FIGURE: fig_01_pipeline_flow.svg | The IRT pipeline runs from source climate and hazard data through grid-first metric computation, spatial aggregation, bundle scoring, and dashboard output.]

This section describes the transformation from gridded climate data to administrative-unit values. The sequence is: compute each climate index on the 0.25° grid, aggregate those grid-cell values to districts and blocks using area-overlap weights, average annual values into multi-year periods, combine the 24-model ensemble, and finally normalise metric values for bundle construction.

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

District-level and block-level composite scores are both computed directly from the 0.25° grid. District scores are not derived by aggregating block scores, and block scores are not disaggregated from district scores.

Concretely, the 784 district polygons are intersected with the 0.25° grid to produce a district-level lookup table recording, for each (district, cell) pair, the fraction of the cell area that falls within that district. Separately, the 7,137 block polygons are intersected with the same grid to produce an equivalent block-level lookup. When a climate index field is computed on the grid, it is aggregated to district values using the district lookup and to block values using the block lookup — two independent aggregation passes over the same underlying grid field.

As a result, a district's composite score is not necessarily equal to the area-weighted average of its constituent blocks' composite scores. The two values are independently derived from the same underlying grid and will generally differ slightly due to the difference in polygon geometry at each level.

### 4.3 Period Aggregation and Ensemble Handling

**Temporal aggregation chain**

For each metric, each GCM, and each scenario, the pipeline applies a three-stage temporal aggregation:

[FIGURE: fig_11_temporal_aggregation_ensemble_chain.svg | Daily fields are reduced to annual cell indices, then period means, then ensemble summaries before publication.]

1. **Daily → annual index.** For each calendar year $y$, the daily gridded data for that year (and, for some metrics, the preceding year) are reduced to a single annual index value per cell. The specific reduction depends on the metric family — annual mean, exceedance count, peak intensity, SPI transform, and so on — and is defined metric by metric in §5. The output is a per-cell annual index field.

2. **Annual → period mean.** For each analysis period (e.g. 2020–2040), the per-cell annual index values for all years within that period are averaged to produce a single per-cell period-mean field. The averaging is an unweighted arithmetic mean over years.

3. **Period mean → ensemble mean.** For each (scenario, period) combination, the 24 per-model period means are averaged to produce the ensemble-mean field. In addition to the ensemble mean, the pipeline retains ensemble spread statistics — standard deviation, median, and 5th and 95th percentile across models. The composite and bundle scores described in §6 and §7 use the **ensemble mean** only; ensemble spread is retained for diagnostic and uncertainty-characterisation purposes but is not surfaced in the current composite outputs.

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


**Composite normalization (per-period spatial ranking)**

The final post-processing step before bundle construction is metric normalization.

For each (scenario, period) combination, the ensemble-mean metric values across all administrative units are normalised onto a [0, 100] scale using the **spatial minimum and maximum** of that same (scenario, period) slice. Let $v_i$ be the ensemble-mean value for unit $i$, and let $v_{\min}$ and $v_{\max}$ be the minimum and maximum of $v_i$ across all units with finite values in that slice. The normalised score is:

$$
S_i =
\begin{cases}
0, & \text{if } v_i < v_{\min} \\[6pt]

\frac{v_i - v_{\min}}{v_{\max} - v_{\min}} \times 100, 
& \text{if } v_{\min} \le v_i \le v_{\max} \\[10pt]

100, & \text{if } v_i > v_{\max}
\end{cases}
$$

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

[FIGURE: fig_12_doy_percentile_threshold_curve.svg | The day-of-year percentile threshold is built from a moving five-day baseline window and reused for all evaluation years.]

$$\tau_d = \text{quantile}_p\!\bigl(\{x_{y,d'} : y \in [y_1, y_2],\; |d' - d| \leq 2\}\bigr)$$

where $d'$ is measured on the 365-day no-leap calendar and $[y_1, y_2]$ is the baseline period. During evaluation, day $t$ with value $x_t$ is classified as an exceedance when $x_t > \tau_{d(t)}$ (strict greater-than throughout). The threshold vector $\tau_d$ is computed once per (model, variable, baseline configuration) and applied unchanged to all evaluation years including SSP projections.

The percentile differs between warm and cold index families, but the baseline period is held fixed:

| Index family | Percentile | 
|---|---|
| TX90p, TN90p, WSDI, hwfi, hwa | 90th | 
| TX10p, TN10p, CSDI | 10th | 

**Baseline period.** A single reference period **1990–2010** is used throughout for every percentile threshold and distribution fit: the temperature percentile and spell indices above, the precipitation percentile indices of §5.2, and the SPI calibration of §5.3. The same window also serves downstream as the **historical reference period** (§2.2/§4.3): the period-over-period change signals in the sectoral rules of §7, and the historical state reported alongside each projection, are measured against 1990–2010. These two uses share one set of years but act at different stages — as a *baseline* it calibrates each index's internal thresholds and fits (the subject of this section); as a *reference period* it anchors change comparisons after the indices are computed. The thematic composites of §6 are a separate case: they are normalized within each period against the spatial spread of units, **not** against the 1990–2010 anchor (see §6.2). Holding one window fixed across these roles keeps the relative indices and cross-period comparisons mutually consistent.

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

The precipitation indices characterise rainfall intensity and accumulation. All six Extreme Rainfall | Flash Flood Risk bundle metrics are derived from the daily precipitation variable `pr` (converted from kg m⁻² s⁻¹ to mm day⁻¹). Four are canonical ETCCDI indices with no IRT-specific departures: **Rx1day** (annual maximum 1-day total), **Rx5day** (annual maximum 5-day running total), **R20mm** (count of days with precipitation ≥ 20 mm), and **CWD** (maximum consecutive wet days, where a wet day is any day with precipitation ≥ 1 mm). See Appendix A for parameter details.

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

The first two are absolute statements about the physical world: 152 dry days, or a supply-to-demand ratio of 0.07, mean the same thing in Rajasthan and in Kerala. SPI is a departure from each unit's own history, so a humid place having an unusually dry year and a desert having an unusually dry year register at similar SPI values. That distinction governs which indicators can carry a national composite score (→ §6.1, §6.2).

**Consecutive dry days (CDD)**

CDD is the longest run of consecutive days with precipitation below 1 mm within the year, averaged over the analysis period (Climdex definition; Zhang et al. 2011 — full catalogue entry in A.6). It measures *dry-spell persistence*: how long a place must go without resupply. It is the current published Drought Risk score (§6.4).

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

The ratio is formed **within each grid cell, before any spatial aggregation** (→ §4.1). PET is a non-linear function of temperature and latitude, so averaging temperature across a district and then computing PET would answer a different — and incorrect — question from averaging the per-cell ratios. Aggregating the finished ratio also keeps the two administrative levels mutually consistent: an area-weighted rollup of a district's block values reproduces the district value to within numerical precision (→ §8.2).

*Reading the value.* Because PET is the demand of a permanently moist surface, a condition that rarely obtains, the dryland boundary sits well below 1: rainfall is seasonal and soil stores it, so a place receiving two-thirds of its annual demand still supports rainfed cropping and closed-canopy forest. The index carries the absolute UNEP/FAO desertification classes (Middleton and Thomas 1992), and it is this fixed, externally defined class structure — rather than any property of the Indian distribution — that makes the index poolable onto a national reference scale (§6.2), in the same way that 1 mm of rain or 35 °C wet-bulb anchors other metrics in this note:

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

IRT also computes the **Standardised Precipitation Index** (SPI; McKee et al. 1993), which expresses accumulated monthly precipitation relative to its locally fitted distribution. SPI-3, SPI-6, and SPI-12 describe deficits over different accumulation timescales; they remain available indicators but do not enter the current thematic composite (§6.4), for the reason given in §6.1 — being referenced to each unit's own climatology, they cannot place two places on one national scale.

**SPI derivation**

For each grid cell, the daily precipitation field (`pr`, converted to mm) is first summed to calendar-month totals — a month is retained only if at least 90% of its days carry finite values, otherwise it is set missing — yielding a contiguous monthly precipitation series trimmed to whole calendar (January–December) years. This monthly series is the input to the **Standardised Precipitation Index** computation, which IRT performs with the open-source `climate_indices` Python package (Adams 2021). Monthly totals are accumulated over rolling $k$-month windows (where $k = 3$, $6$, or $12$), and the resulting series for each cell is fitted to a two-parameter **Gamma distribution** over the calibration period 1990–2010 using the Method of Moments estimator:

[FIGURE: fig_14_spi_derivation.svg | SPI converts monthly precipitation totals into accumulated anomalies, fits a Gamma distribution, and maps the result to a standard-normal drought index.]

$$f(x;\, \alpha, \beta) = \frac{x^{\alpha-1}\, e^{-x/\beta}}{\beta^\alpha\, \Gamma(\alpha)}, \quad x > 0$$

Zero-precipitation months occur with probability $q = P(x = 0)$, estimated as the fraction of zero months in the baseline. The Gamma fit applies to positive-precipitation months only. The mixed cumulative distribution function is:

$$H(x) = q + (1 - q)\, G(x;\, \alpha, \beta)$$

SPI is obtained by mapping this CDF through the standard normal quantile function:

$$\text{SPI} = \Phi^{-1}\!\bigl(H(x)\bigr)$$

The Gamma parameters ($\alpha$, $\beta$, $q$) are estimated once from the 1990–2010 historical run and applied unchanged to SSP future data, preserving cross-period comparability of SPI values.

**SPI bundle metrics**

The monthly SPI series is not used directly in composites. Two annual aggregation statistics are derived per cell, per year:

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

**jrc_flood_depth_index_rp100** — Composite severity class (ordinal 1–5: Very Low, Low, Moderate, High, Extreme). Each block's RP-100 flood depth and flood extent are first binned independently into 1–5 classes, then combined through a fixed 5×5 lookup matrix.

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

## 6. Thematic Bundle Construction

Each of the six hazard families defined in §5 is condensed into a single composite **bundle score** on a 0–100 scale, computed independently for every geography, scenario, and time period. Each component metric is first normalized onto a common 0–100 *higher-is-worse* scale (§6.2), and the normalized components are then combined with fixed weights (§6.3). 
<!-- This thematic framework — a weighted average of co-normalized climate metrics — is distinct from the sectoral hazard-pressure framework of §7, which scores exposure to curated hazard rules rather than compositing metrics directly. -->

### 6.1 Bundle Taxonomy and Grouping Rationale

The six thematic bundles and the hazard dimension each captures:

| Bundle | Hazard dimension | Component metric families (§5) |
|---|---|---|
| Heat Risk | Daytime/nocturnal thermal extremes and background heat | Background means, absolute & percentile extremes, threshold-frequency, heatwave characteristics |
| Heat Stress | Humid-heat physiological stress | Wet-bulb means/extremes and dry-heat persistence |
| Cold Risk | Winter cold extremes and cold-spell persistence | Background cold, absolute extremes, cold-day thresholds, percentile-relative cold, cold-spell characteristics |
| Drought Risk | Dry-spell persistence and annual water balance | CDD contributes to the composite; the aridity index P/PET describes supply against atmospheric demand and SPI-3/6/12 describe rainfall deficits separately |
| Extreme Rainfall \| Flash Flood Risk | Extreme precipitation and wet-spell persistence | Peak intensity, heavy-rain frequency, very-wet contribution, wet-spell persistence |
| Riverine Flood | Static RP-100 inundation severity | Riverine flood severity, depth and extent|

The indicator catalogue describes the available measures of each hazard; §6.4 identifies the indicators combined to calculate each thematic score. A bundle can contain indicators of magnitude, frequency, persistence, and departure from local climatic conditions without every indicator contributing to its composite. The contributing set is fixed for the published reference version.

**Drought Risk** currently uses consecutive dry days (CDD) to describe dry-spell persistence on a common physical basis. The CDD score should therefore be read as dry-spell pressure, not as a comprehensive measure of drought or water availability; in particular it does not distinguish a seasonal climate from an arid one (§5.3). The **aridity index P/PET** is the complementary absolute measure of that missing dimension and is computed for the same geographies and slices; a forthcoming revision of this composite is expected to combine the two, and §6.4 states the contributing set for the published reference version. **SPI** indicators describe rainfall deficits relative to local climatic conditions and remain part of the indicator catalogue, but do not contribute to this composite: because each unit is scored against its own history rather than against a common physical scale, SPI cannot rank two places on the single national reference distribution that §6.2 requires. **Riverine Flood** also uses one contributing indicator: its published score is the JRC severity value transformed through the national ruler. Depth and extent provide additional context without contributing directly to the score.

### 6.2 Normalization: Frozen National Reference Distributions

The six thematic composites described here use **fixed national reference distributions** to translate their contributing indicators into scores on a common 0–100 scale. Each indicator has its own mapping, called a *ruler*. The mapping is fitted once and applied unchanged across states, scenarios, periods, and administrative levels. Selecting a different state or period therefore changes the values being evaluated, not the scale against which they are evaluated.

For the climate bundles, the reference population pools district-level indicator values across India over the historical 1990–2010 window and the six future combinations of two scenarios and three periods. The Riverine Flood ruler uses the national district population for its single static snapshot. These are distributions of administrative-unit indicator values, not distributions of individual daily observations. Each finite district–slice observation contributes equally to an indicator's reference distribution; the fitting population is not weighted by district area or population.

The mapping uses the **empirical mid-rank cumulative distribution**. At each distinct reference value $x_k$, its position on the scale is:

$$R_m(x_k)=100\,\frac{b_k+c_k/2}{N_m}$$

Here, $b_k$ is the number of reference observations below $x_k$, $c_k$ is the number equal to it, and $N_m$ is the total number of reference observations for indicator $m$. Values between reference points are scored by linear interpolation. The direction is set so that higher scores always indicate greater hazard pressure:

$$S_m(x)=\begin{cases}R_m(x), & \text{higher values indicate greater hazard},\\100-R_m(x), & \text{lower values indicate greater hazard}.\end{cases}$$

For example, higher maximum temperatures increase a heat score, while lower winter temperatures increase a cold score. Both districts and blocks use the same district-fitted mapping: the same physical indicator value receives the same component score wherever it occurs.

**Worked illustration.** Consider a synthetic higher-is-worse reference sample of 10, 20, 20, and 30 units. The stored scores are 12.5 at 10, 50 at 20, and 87.5 at 30. A value of 25 receives 68.75 by interpolation. A district and a block with that value both receive 68.75; if the district's value later rises from 20 to 25, its score rises from 50 to 68.75 on the unchanged reference scale.

A component score describes position against this fixed reference, not a physical danger threshold or an event probability. The weighted composite in §6.3 is an average of component scores; it is not itself a percentile rank of composite hazard. A score difference is measured in score points, not degrees, millimetres, or expected loss.

The committed reference version, currently `cdf_v1` for these six bundles, identifies the mappings, contributing indicators, weights, reference slices, and coverage requirements used to produce the scores. Comparisons require the same bundle definition and reference version. This reference population is distinct from the historical baseline used to derive an individual indicator: it includes future values as well as historical values, and is not refitted when a new geography or period is selected.

### 6.3 Weighted Composite Methodology

Within a bundle the component scores listed in §6.4 are combined as a **weighted mean**, with weights redistributed over available contributing indicators where partial coverage is permitted:

$$\text{Composite}_g = \frac{\sum_{m \in A_g} w_m\, S_{g,m}}{\sum_{m \in A_g} w_m}$$

where $A_g$ is the set of component metrics with a valid (non-NaN) normalized score for geography $g$, and $w_m$ are the fixed bundle weights (§6.4). Because each $S_{g,m}\in[0,100]$ and the weights are renormalized to sum to 1 over $A_g$, the composite is itself bounded in $[0,100]$ — no separate clipping is required. 
<!-- The count of contributing metrics ($\lvert A_g\rvert$) is persisted alongside each score for transparency. -->

Scores are reported only when the required indicator coverage is available. Coverage is the share of the full configured contributing weight represented by valid component scores, rather than the fraction of indicator columns present. Heat Risk requires at least 70% coverage; the other five composites described here require 100%. Below the requirement, no composite score is reported. When comparing Heat Risk scores based on partial inputs, the contributing indicators should also be considered.

The weights express the relative contribution of the selected indicators to each composite.

### 6.4 Bundle-by-Bundle Metric Weights

The tables give the effective weights of the indicators used in each composite when all contributing values are available. They are obtained by dividing each configured contributing weight by the total configured contributing weight for that bundle. Percentages are rounded for display; the calculations retain full precision. Indicators defined elsewhere in this note but absent from these tables do not contribute to these thematic scores.

#### Heat Risk

| Contributing indicator | Effective weight |
|---|---:|
| Annual Mean Temperature (TM) | 10.5263% |
| Summer Max Temperature (MAM) | 10.5263% |
| Summer Mean Temperature (MAM) | 10.5263% |
| Annual Maximum Temperature (TXx) | 13.1579% |
| Heatwave Amplitude | 13.1579% |
| Hot Days (TX ≥ 30°C) | 10.5263% |
| Extreme Heat Days (TX ≥ 35°C) | 10.5263% |
| Tropical Nights (TN > 25°C) | 10.5263% |
| Warmest Night (TNx) | 10.5263% |

#### Heat Stress

| Contributing indicator | Effective weight |
|---|---:|
| Wet-Bulb Temperature (Annual Mean) | 14.2857% |
| Wet-Bulb Temperature (Summer Mean, MAM) | 14.2857% |
| Wet-Bulb Temperature (Annual Max) | 19.0476% |
| Heat Stress Days (Twb ≥ 28°C) | 19.0476% |
| Wet-Bulb Days (Twb ≥ 30°C) | 19.0476% |
| Tropical Nights (TN > 28°C) | 14.2857% |

#### Cold Risk

| Contributing indicator | Effective weight |
|---|---:|
| Winter Mean Temperature (DJF) | 13.3333% |
| Winter Mean of Tmin (DJF) | 13.3333% |
| Annual Minimum of Tmin (TNn) | 13.3333% |
| Winter Minimum Tmin (DJF) | 13.3333% |
| Cold Nights (TN ≤ 10°C) | 11.1111% |
| Severe Cold Nights (TN ≤ 5°C) | 11.1111% |
| Cold Days (TX ≤ 15°C) | 11.1111% |
| Consecutive Cold Nights (TN ≤ 10°C) | 13.3333% |

#### Drought Risk

| Contributing indicator | Effective weight |
|---|---:|
| Consecutive dry days (precipitation < 1 mm/day) | 100% |

The published score is CDD transformed through its frozen national ruler. The aridity index P/PET and the SPI event and spell indicators do not enter this composite in the current reference version (§6.1). Any change to the contributing set is published as a new reference version, and the version identifier carried with the scores is what distinguishes the two (§6.2).

#### Extreme Rainfall | Flash Flood Risk

| Contributing indicator | Effective weight |
|---|---:|
| Maximum 1-day Precipitation (Rx1day) | 16.6667% |
| Maximum 5-day Precipitation (Rx5day) | 16.6667% |
| Very Heavy Precipitation Days (R20mm) | 33.3333% |
| Consecutive Wet Days (CWD) | 33.3333% |

#### Riverine Flood

| Contributing indicator | Effective weight |
|---|---:|
| Flood Severity Index (RP-100) | 100% |

The Riverine Flood composite is the single JRC severity index (§5.5) transformed through its frozen national ruler. Depth and extent remain available for context and do not contribute directly to the score.

---

## 7. Sectoral Bundle Construction

The eight **sectoral bundles** answer a different question from the thematic bundles of §6. A thematic bundle co-normalizes the metrics of *one hazard family* and averages them. A sectoral bundle instead scores a sector's exposure to a **curated set of hazard pressures drawn from across families**, and evaluates each pressure through a *rule* that fuses three readings of the same metric: its absolute magnitude, its projected change versus the historical baseline, and — where a defensible danger threshold exists — its position within a fixed harm **impact band**. The output is still a 0–100 *higher-is-worse* score per geography, scenario, and period, but the construction is a blended-rule pipeline rather than a weighted average of co-normalized metrics.

> **Scope caveat.** These are **sector climate hazard-pressure** scores: they characterise the climatic *hazard* a sector faces and do not incorporate exposure, vulnerability, or adaptive capacity, so they are not full sectoral risk scores in the IPCC sense (→ §1, §8). The word "Risk" in a bundle name denotes *hazard pressure relevant to that sector*.

### 7.1 Sector Hazard-Pressure Framework

The eight sectoral bundles are **Agricultural Risk**, **Health Risk**, **Industrial Risk**, **Investment / Financial Risk**, **Infrastructure Risk**, **Asset Risk (Thermal Power Plants)**, **Asset Risk (Hydropower Plants)**, and **Life & Livelihood Loss Risk**. Each bundle's full rule set — source metrics, rule weights, lens weights, and impact bands — is given in §7.4.

Every bundle is an **ordered set of rules**; each rule binds exactly one source metric (§5) to a scoring recipe and an explicit *rule weight*. All eight bundles use explicit, normalized rule weights that sum to 1.0 (§7.4). All are computed at both district and block level, for scenarios **SSP2-4.5** and **SSP5-8.5**, over the future periods **2020–2040, 2040–2060, 2060–2080**; the historical 1990–2010 window enters only as the change-lens baseline, not as a published period.

A rule's selection encodes a deliberate sector judgement — that a given hazard *matters to that sector* — so the same metric can appear in several bundles under different rule weights and different impact bands, reflecting the sector-specific consequence rather than a single universal harm.

### 7.2 The Blended Rule: Absolute Pressure + Change + Impact Band

A rule produces up to three **lens scores**, each on a 0–100 *higher-is-worse* scale, which are then weighted into a single rule score. The lens weights ($\omega_{\text{abs}}, \omega_{\text{chg}}, \omega_{\text{imp}}$) are declared per rule and sum to 1.0; a lens with weight 0 is simply not evaluated. Throughout, all shipped rules are *higher-worse* (the lower-is-worse direction is supported but unused in the current catalog).

[FIGURE: fig_18_three_lens_blended_rule.svg | Sectoral rules blend absolute pressure, change from history, and fixed impact-band proximity into one rule score.]

[FIGURE: fig_19_impact_band_ramp.svg | The impact lens maps raw metric values onto a fixed onset-to-saturation hazard band independent of peer rankings.]

The three lenses are not an arbitrary decomposition: each answers a different, complementary question about the same metric, and each draws on an established methodological tradition. The lens framework is a structured combination of these traditions rather than a novel scoring invention.

| Lens | Question it answers | Anchored to | Methodological tradition |
|---|---|---|---|
| **Absolute** | How extreme is the projected value relative to its peers? | the peer cohort's spatial distribution (relative) | composite-indicator normalization (OECD/JRC *Handbook on Constructing Composite Indicators*, 2008) |
| **Change** | How much worse is the projected value than its own history? | the 1990–2010 baseline (anomaly) | delta / change-factor method (Anandhi et al. 2011) |
| **Impact** | How far into a physically dangerous range is the value? | a fixed, externally justified threshold band (absolute) | threshold / dose–response impact functions (Gasparrini et al. 2015) |

The **cohort** for the two relative lenses is one `state × level × scenario × period` group: a district is scored against the other districts *of its state*, and a block against the other blocks *of its state* — not only the blocks of its own district, since a single district rarely holds enough blocks for stable deciles. The impact lens needs no cohort; it reads each value against a fixed band.

**Absolute lens — $S_{\text{abs}}$.** The current-period metric value is scaled across the geography set $G$ (the districts, or blocks, of one state) by a **robust p10–p90 rescaling**. This sectoral lens uses a local cohort, unlike the frozen national mappings for the six thematic composites in §6. With $q_{10}, q_{90}$ the 10th and 90th percentiles of the finite values over $G$:

$$S_{\text{abs},i} = \operatorname{clip}\!\left(\frac{v_i - q_{10}}{q_{90} - q_{10}},\; 0,\; 1\right)\times 100$$

Clipping at the deciles damps the influence of outliers on this spatial scale. The sectoral absolute lens is relative to the selected cohort; the thematic reference in §6 remains fixed across states and periods.

**Change lens — $S_{\text{chg}}$.** The lens first forms a per-geography change of the future value against its 1990–2010 baseline column, then scales those *change magnitudes* across $G$ with the same robust p10–p90 scaler. The change mode is metric-dependent:

$$\Delta_i = \begin{cases} v_i^{\text{fut}} - v_i^{\text{base}} & \text{absolute\_delta (temperature-like metrics)}\\[4pt] \dfrac{v_i^{\text{fut}} - v_i^{\text{base}}}{\lvert v_i^{\text{base}}\rvert}\times 100 & \text{relative\_pct (counts, rainfall, spells)} \end{cases}$$

The absolute delta is used for temperature-like metrics (the daily-mean, daily-maximum, and daily-minimum temperature families and other heat indices) and relative percent otherwise. Relative-percent change guards against tiny denominators ($\lvert v^{\text{base}}\rvert < 10^{-6}\Rightarrow$ NaN) so a near-zero baseline cannot explode the score. Because $S_{\text{chg}}$ is itself a spatial p10–p90 rank of the change, it measures *where a unit sits in the distribution of projected change* — not an absolute warming or wetting magnitude. A missing baseline column drops this lens to NaN (with a build warning) and the rule is scored on its remaining lenses.

**Impact lens — $S_{\text{imp}}$.** This is the only **absolute, non-spatial** lens: it maps the raw metric value onto a fixed physical harm band $[a, b]$ — $a$ the onset of concern, $b$ the saturation/severe threshold — independent of how other geographies score:

$$S_{\text{imp},i} = \operatorname{clip}\!\left(\frac{v_i - a}{b - a},\; 0,\; 1\right)\times 100$$

A value at or below onset scores 0; at or above saturation, 100. The lens is evaluated only when the rule declares a band; regime/proxy metrics with no defensible threshold omit it (impact weight 0). 
<!-- The bands, their provenance, and their confidence grading are the subject of §7.4. -->

The impact lens is what distinguishes the sectoral framework from a purely relative ranking. Where the absolute and change lenses only say how a unit compares to its neighbours, the impact lens says how close it is to a recognised danger threshold — an absolute, externally meaningful reading of harm that does not move with the spatial distribution.

**Rule score.** The lens scores present for a rule are combined as a renormalized weighted mean over the lenses actually available (i.e. non-NaN), exactly mirroring the per-row renormalization of §6.3:

$$S_r = \frac{\sum_{\ell \in L_r}\omega_\ell\, S_{\ell}}{\sum_{\ell \in L_r}\omega_\ell}, \qquad L_r = \{\ell \in \{\text{abs},\text{chg},\text{imp}\} : \omega_\ell > 0 \text{ and } S_\ell \text{ finite}\}$$

so a rule whose change lens is unavailable is scored on absolute (+impact) alone rather than being voided. All three lens scores are persisted alongside the blended rule score for transparency.

<!-- **Bundle aggregation and the 0.70 completeness gate.** A bundle composite is the renormalized weighted mean of its rule scores over the rules with a finite score, using the rule weights $W_r$:

$$\text{Composite}_b = \frac{\sum_{r\in R_b} W_r\, S_r}{\sum_{r\in R_b} W_r}, \qquad f_b = \!\!\sum_{r:\,S_r\text{ finite}}\!\! W_r$$

where $f_b$ is the available-rule-weight fraction. The sectoral coverage policy is separate from the thematic indicator-coverage requirements in §6.3. -->

### 7.3 Reading the Score: What Each Lens Lets You Compare

Because two of the three lenses are cohort-relative and only the impact lens is absolute, the blended score is a **hybrid** of relative ranking and absolute danger: it compares units cleanly *within* a cohort, but only partly across periods or states. The reason is the absolute lens. It re-scores every unit against its own cohort — rebuilt for each scenario and period — so it only ever tells you which places are worse off than their neighbours *right now*. It is blind to warming that lifts a whole state together: if every unit heats by the same amount between two periods, the top and bottom of the range ($q_{10}$ and $q_{90}$) rise in step, and the scores don't move. The other two lenses fill this gap. The **change** lens tracks each place's own trend from one period to the next, and the **impact** lens scores every place against a fixed real-world danger scale that never shifts. Because that scale stays put, the impact lens is the only one you can compare across periods and across states and read as genuine worsening rather than a reshuffled ranking.

[FIGURE: fig_20_district_a_b_lens_example.svg | A two-district example shows how the blended score preserves current severity, fast change, and absolute threshold crossing.]

**Worked example — why the blend beats pure-absolute ranking.** Two districts in one state, scenario SSP5-8.5, period 2060–2080, metric **TXx** (annual-maximum daytime temperature), scored through the Health Risk TXx rule (lens weights 0.40 / 0.25 / 0.35, impact band 40–45 °C). Suppose across this cohort projected TXx spans $q_{10}=41$ °C to $q_{90}=46$ °C, and the warming anomaly versus 1990–2010 spans $q_{10}=+1.0$ °C to $q_{90}=+3.5$ °C:

| District | TXx 2060–80 | Anomaly | $S_{\text{abs}}$ | $S_{\text{chg}}$ | $S_{\text{imp}}$ | **Blended** | Pure-absolute |
|---|---:|---:|---:|---:|---:|---:|---:|
| **A** — already hot | 45.5 °C | +1.5 °C | 90 | 20 | 100 | **76** | 90 |
| **B** — fast-warming | 42.0 °C | +3.5 °C | 20 | 100 | 40 | **47** | 20 |

District A is the established heat hazard — hottest among its peers and past the 45 °C heat-wave declaration threshold — and is correctly rated high by either method. District B is the case pure-absolute ranking *hides*: it looks unremarkable relative to peers ($S_{\text{abs}}=20$), but it is warming faster than any of them ($S_{\text{chg}}=100$) and has just crossed the 40 °C heatwave-declaration floor ($S_{\text{imp}}=40$). The blend lifts it to 47 — a mid-range hazard warranting attention — whereas pure-absolute scoring leaves it at 20, mislabelling a fast-warming, newly dangerous district as low priority. The blend thus preserves three decision-relevant signals — current severity, trajectory, and absolute danger-threshold crossing — that a relative ranking alone discards. The cost is interpretability: one number now mixes three signals, which is precisely why the per-lens decomposition is persisted with every rule (§7.2) — a user can read District B's 47 back as $S_{\text{abs}}$ 20 / $S_{\text{chg}}$ 100 / $S_{\text{imp}}$ 40.

<!-- ### 7.4 The Impact Lens: Bands, Provenance, and Confidence

The impact lens is what distinguishes the sectoral framework from a purely relative ranking: it injects an **absolute, externally meaningful** reading of harm. Where the absolute and change lenses only say *how a unit compares to its neighbours*, the impact lens says *how close the unit is to a recognised danger threshold*, on a fixed scale that does not move with the spatial distribution. Each band is a pair $[a,b]$ — onset $a$ (harm begins) and saturation $b$ (harm is near-complete or sector-dominant) — and the linear interpolation of §7.2 converts the raw value into a 0–100 harm-proximity score. -->

<!-- Bands are graded by the strength of their evidentiary support:

- **HIGH** — anchored on a published, institutionally recognised threshold (e.g. the IMD plains heatwave criterion, or IMD daily-rainfall categories). Treated as external and zone-invariant.
- **MEDIUM** — derived from literature combined with reasoned judgement, often with an institutionally anchored onset and a self-derived saturation.
- **LOW** — self-derived from first principles or indirect evidence where no categorical institutional band exists at the relevant scale. By design these rules carry a **small impact weight** (typically 0.15), so a weakly supported band contributes little to the score. -->

<!-- **Provenance, by tier.** The catalog leans on a small number of **externally anchored, high-confidence** bands that recur across sectors and carry the heaviest impact weight: extreme daytime heat (TXx, IMD plains heatwave **40–45 °C**) and one-day rainfall (Rx1day, IMD very-heavy-to-extremely-heavy **115.6–204.5 mm**). These appear in Health, Industrial, Infrastructure, Asset, and Life-&-Livelihood bundles, each time with the same physical band but a sector-specific consequence. The remaining bands are **self-derived**: a MEDIUM tier derived from literature and reasoned judgement, sometimes with an institutionally anchored cut point — multi-week dry spells (CDD, IMD Agricultural-Drought-anchored **30–90** / **60–120 days**), crop reproductive heat (TXx **35–45 °C**, research-derived onset), warmest-night stress (TNx **28–32 °C**, research-derived onset) — and a LOW tier for indices with no institutional category at all: multi-day rainfall (Rx5day **250–500 mm**, anchored on Kerala 2018 / Mumbai 2005), warm spells (WSDI **6–18 days**), damaging-heat-day counts (**15–60 days**), SPI drought episode/spell counts (**3–12**), peninsular chilling nights (**10–30 days**), consecutive wet days (**7–15 days**), and heatwave-frequency days (**5–15 days**). Finally, three **regime/proxy metrics carry no impact lens** — R99p extreme-wet concentration, the SPI-3 low-flow cooling proxy, and R95p interannual variability — because no defensible danger threshold exists for them; they are scored on absolute and change only.

**Provenance discipline.** Five principles govern how a band may be admitted: (1) a band scores *danger, not unusualness* — emergence-versus-history is the change lens's job, so an impact band may never be built from a percentile or a standardized anomaly, which would duplicate the change lens; (2) external institutional thresholds are preferred, and a self-derived band is admitted only where none exists, through a documented protocol (harm mechanism → nearest external anchors → cut points → confidence → dated provenance); (3) confidence sets the impact weight, so a low-confidence band cannot drive a rule; (4) a borrowed standard may be used *only* in the construction its source defines — the IMD warm-night "+4.5 to +6.4 °C above normal" departure criterion was **rejected** for TNx because it is defined jointly with a same-day Tmax ≥ 40 °C co-condition against a *daily climatological* normal, neither of which holds for an annual-maximum value, so TNx instead uses an absolute 28–32 °C level band; (5) **no phantom thresholds** — a slug naming a number (e.g. `..._ge_45`) must implement that number as a real band with provenance or be renamed. Every band is versioned, dated, and revisable; the downward revision of the once-canonical 35 °C wet-bulb survivability limit is the cautionary precedent, and any band change is itself a methodology change.

The full onset/saturation derivation, source, zone caveat, and confidence grade for every distinct band are catalogued in **Appendix B**, deduplicated across the bundles that share them. -->

### 7.4 Bundle-by-Bundle Rule Tables and Weights

<!-- Two weight layers govern a sectoral score: the **lens split** *within* each rule (§7.2) and the **rule weight** *within* each bundle. The lens splits are not arbitrary per rule — they fall into a handful of recurring **archetypes** tied to band provenance:

| Lens archetype (abs / chg / imp) | Typical use | Rationale |
|---|---|---|
| **0.40 / 0.25 / 0.35** | Rules on an external, HIGH-confidence IMD band (TXx 40–45, Rx1day, TNx) | The trusted danger threshold earns the largest impact weight |
| **0.40 / 0.30 / 0.30** | Rules on an IMD-anchored MEDIUM band (CDD dry spells, crop-heat TXx, livelihood Rx5day/WSDI) | Balanced; the band is defensible but not externally categorical |
| **0.45 / 0.40 / 0.15** | Secondary rules on a self-derived LOW band (WSDI, SPI counts, Rx5day, chilling nights, HWFI) | Small impact weight by design — a weak band contributes little |
| **0.70 / 0.30 / 0.00** | Regime/proxy metrics, no band (SPI low-flow proxy, R95p variability) | Absolute level dominant; emergence supplies a secondary signal |
| **0.40 / 0.60 / 0.00** | Change-dominant regime metric, no band (R99p concentration) | Emergence of tail concentration vs baseline is the decision-relevant signal |

The per-bundle tables below give each rule's source metric, its **rule weight** (summing to 1.0 per bundle), its lens archetype, and its impact band (or "—" where no band applies). Band derivations are in Appendix B. -->

Two weight layers govern a sectoral score: the lens weights (abs / chg / imp) within each rule (§7.2) and the rule weight within each bundle. The lens weights are not arbitrary — the impact weight tracks band confidence: largest (~0.35) for rules on an external, HIGH-confidence IMD band, ~0.30 for IMD-anchored MEDIUM bands, smallest (0.15) for self-derived LOW bands, and 0 for regime/proxy metrics that carry no band at all. The per-bundle tables below give each rule's source metric, its rule weight (summing to 1.0 per bundle), its lens weights, and its impact band (or "—" where none applies). Band derivations are in Appendix B.

#### Agricultural Risk

Selects the agronomic stressors of a kharif–rabi cropping system: reproductive-stage heat, damaging heat-day and warm-spell burden, short-window (SPI-3) drought episodes and their longest spell, kharif-waterlogging rainfall, and — peninsular default — horticultural chilling nights. The heaviest weights sit on 5-day rainfall (0.20) and the two drought rules (0.15 each), reflecting the dominance of water extremes in rainfed agriculture.

| Rule | Source metric | Rule weight | Lens weights (abs/chg/imp) | Impact band |
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

| Rule | Source metric | Rule weight | Lens weights (abs/chg/imp) | Impact band |
|---|---|---|---|---|
| Extreme daytime heat pressure | Annual max temperature (TXx) | 0.30 | 0.40 / 0.25 / 0.35 | 40–45 °C |
| Warm-spell duration pressure | Warm spell duration (WSDI) | 0.12 | 0.45 / 0.40 / 0.15 | 6–18 days |
| Night-time heat pressure | Warmest night (TNx) | 0.18 | 0.40 / 0.25 / 0.35 | 28–32 °C |
| 1-day rainfall disruption pressure | Max 1-day precipitation (Rx1day) | 0.25 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| Consecutive wet-day pressure | Consecutive wet days (CWD) | 0.15 | 0.45 / 0.40 / 0.15 | 7–15 days |

#### Industrial Risk

Scores process-disruption and water/heat-derating pressures on industry: extreme operational heat (the dominant rule at 0.40), one- and five-day rainfall disruption, and prolonged dry spells stressing process-water supply.

| Rule | Source metric | Rule weight | Lens weights (abs/chg/imp) | Impact band |
|---|---|---|---|---|
| 1-day rainfall disruption pressure | Max 1-day precipitation (Rx1day) | 0.25 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| 5-day rainfall disruption pressure | Max 5-day precipitation (Rx5day) | 0.15 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Dry-spell water-stress pressure | Consecutive dry days (CDD) | 0.20 | 0.40 / 0.30 / 0.30 | 30–90 days |
| Extreme heat operations pressure | Annual max temperature (TXx) | 0.40 | 0.40 / 0.25 / 0.35 | 40–45 °C |

#### Investment / Financial Risk

Emphasises *emergence vs the historical baseline* — the signal an investor cares about. It pairs the rainfall-disruption rules with two change-weighted regime metrics: extreme-wet concentration (R99p, change-dominant) and heatwave persistence (HWFI), plus chronic dry-spell water stress for water-intensive assets.

| Rule | Source metric | Rule weight | Lens weights (abs/chg/imp) | Impact band |
|---|---|---|---|---|
| 1-day rainfall disruption pressure | Max 1-day precipitation (Rx1day) | 0.25 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| 5-day rainfall accumulation pressure | Max 5-day precipitation (Rx5day) | 0.15 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Extreme wet precipitation concentration | Extremely wet-day precip (R99p) | 0.10 | 0.40 / 0.60 / 0.00 | — |
| Dry-spell water-stress pressure | Consecutive dry days (CDD) | 0.25 | 0.40 / 0.30 / 0.30 | 30–90 days |
| Heatwave persistence pressure | Heatwave spell days (HWFI) | 0.25 | 0.45 / 0.40 / 0.15 | 5–15 days |

#### Infrastructure Risk

A compact three-rule design centred on rainfall design loads: one-day design rainfall dominates (0.45), followed by five-day accumulation (0.30) and extreme-heat asset stress (0.25).

| Rule | Source metric | Rule weight | Lens weights (abs/chg/imp) | Impact band |
|---|---|---|---|---|
| 1-day rainfall design pressure | Max 1-day precipitation (Rx1day) | 0.45 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| 5-day rainfall design pressure | Max 5-day precipitation (Rx5day) | 0.30 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Extreme heat asset pressure | Annual max temperature (TXx) | 0.25 | 0.40 / 0.25 / 0.35 | 40–45 °C |

#### Asset Risk — Thermal Power Plants

Targets the two climate vulnerabilities of thermal generation: cooling-water availability (dry-spell CDD and an SPI-3 low-flow proxy) and cooling-efficiency loss under extreme heat, weighted roughly evenly (0.35 / 0.35 / 0.30).

| Rule | Source metric | Rule weight | Lens weights (abs/chg/imp) | Impact band |
|---|---|---|---|---|
| Dry-spell cooling-water pressure | Consecutive dry days (CDD) | 0.35 | 0.40 / 0.30 / 0.30 | 30–90 days |
| Extreme heat cooling-efficiency pressure | Annual max temperature (TXx) | 0.35 | 0.40 / 0.25 / 0.35 | 40–45 °C |
| Low-flow drought proxy pressure | SPI-3 moderate-drought months | 0.30 | 0.70 / 0.30 / 0.00 | — |

#### Asset Risk — Hydropower Plants

Scores inflow-driven generation risk: heavy 5-day rainfall stressing spillway/operations (dominant at 0.45), prolonged dry spells cutting reservoir inflow, and the inflow-predictability signal from R95p **interannual variability** — a helper metric (the coefficient of variation of yearly very-wet precipitation, §7.2/Appendix A) sharing the Rx5day/CDD baseline epoch.

| Rule | Source metric | Rule weight | Lens weights (abs/chg/imp) | Impact band |
|---|---|---|---|---|
| 5-day rainfall operations pressure | Max 5-day precipitation (Rx5day) | 0.45 | 0.45 / 0.40 / 0.15 | 250–500 mm |
| Dry-spell flow pressure | Consecutive dry days (CDD) | 0.35 | 0.40 / 0.30 / 0.30 | 30–90 days |
| Very wet precipitation variability pressure | R95p inter-annual variability (CV) | 0.20 | 0.70 / 0.30 / 0.00 | — |

#### Life & Livelihood Loss Risk

Captures the direct human-exposure hazards: extreme one- and five-day rainfall (flood exposure), prolonged dry spells driving livelihood/crop failure, and warm-spell heat mortality. One-day rainfall carries the largest weight (0.30).

| Rule | Source metric | Rule weight | Lens weights (abs/chg/imp) | Impact band |
|---|---|---|---|---|
| 1-day rainfall exposure pressure | Max 1-day precipitation (Rx1day) | 0.30 | 0.40 / 0.25 / 0.35 | 115.6–204.5 mm |
| 5-day rainfall exposure pressure | Max 5-day precipitation (Rx5day) | 0.25 | 0.40 / 0.30 / 0.30 | 250–500 mm |
| Dry-spell livelihood pressure | Consecutive dry days (CDD) | 0.20 | 0.40 / 0.30 / 0.30 | 60–120 days |
| Warm-spell livelihood pressure | Warm spell duration (WSDI) | 0.25 | 0.40 / 0.30 / 0.30 | 6–18 days |

---

## 8. Composite Score and Output

The **composite score** is the published output of every bundle, thematic and sectoral alike: one 0–100 *higher-is-worse* number per admin unit, scenario, and period. The two construction methods of §6 and §7 differ internally but emit the same object, so a dashboard or export treats all bundle scores uniformly. It is a **hazard-pressure index, not a risk estimate** — that framing and its consequences are set out in §1 and §7 and not repeated here. For interpretation the 0–100 range is banded into three tiers — **low (0–33.3), moderate (33.3–66.6), high (66.6–100)** — used consistently wherever the score is classified. This section covers how to read the number across the scenario, period, and spatial-level dimensions.

**Why two construction methods?** The thematic composites summarise selected indicators of one hazard family using fixed national mappings and a weighted mean (§6). This supports comparisons on a consistent reference scale. Sectoral bundles combine pressures from several hazard families and evaluate each through the absolute, change, and impact lenses described in §7. Both return a 0–100 hazard-pressure score, but their reference scales and interpretation differ; sharing a numeric range does not make scores from different bundles equivalent.

### 8.1 Scenario and Period Handling

Composites are computed **independently for each `(scenario, period)` combination**. 
<!-- and persisted to master files that the tool reloads on selection; no scores are recomputed at view time.  -->
The published combinations differ between the Thematic and Sectoral bundles:

| Bundle type | Scenarios | Periods |
|---|---|---|
| **Thematic — projected** (§6) | SSP2-4.5, SSP5-8.5 | `2020-2040`, `2040-2060`, `2060-2080` |
| **Thematic — static** (Riverine Flood, §6.2) | Snapshot | `Current` (a single static snapshot) |
| **Sectoral** (§7) | SSP2-4.5, SSP5-8.5 | `2020-2040`, `2040-2060`, `2060-2080` |

`Current` is **not** a modeled near-present period — it is the fixed label under which a single *static* snapshot is filed, and only the **Riverine Flood** bundle uses it: one `Snapshot` value with no scenario and no future-period dimension (§2.2, §5.5/§6.2). The climate (SSP) thematic bundles have no `Current` output; their scores exist only for the three future windows. The sectoral method likewise publishes no historical or `Current` period — for it, the 1990–2010 window enters only as the change-lens *baseline* (§7.2), never as an output column.

**Comparability:** The thematic and sectoral methods have different comparison rules:

- **Thematic** scores for the six frozen-ruler bundles share a fixed scale across states and periods when the bundle definition and reference version are unchanged. A change in score reflects a change in evaluated inputs or their availability, rather than a refitted state-level scale. Equal composite scores can arise from different combinations of indicators, and partial Heat Risk coverage should be considered (§6.3). A score of 80 in one bundle is not equivalent to 80 in another; neither denotes an event probability or expected loss.
- **Sectoral** composites blend relative lenses (absolute, change) with the absolute impact lens, so only the **impact component** carries genuine cross-period and cross-state meaning; the blended number mixes ranking and danger and must be read with the lens decomposition (§7.3) when comparing across periods or states.

### 8.2 District vs Block Resolution Behaviour

Both district (ADM2) and block (ADM3) composites are computed **independently from the grid-first index pipeline**; each level combines the 0.25° grid cells into its own polygons in proportion to how much of each cell falls inside them (§4.2). All bundles, thematic and sectoral, support both levels.

Two resolution effects follow:

- **Grid coverage.** A district overlaps many 0.25° cells; a small block may overlap only one or two. Each grid point is treated as a 0.25° square *tile*, and these tiles cover the whole map with no gaps — so a block is never left "between" grid points and always receives a value. A block smaller than one cell sits entirely inside a single tile and simply **takes that one cell's value**; any other small blocks falling inside the same cell take the *same* value. Block-level scores therefore cannot resolve contrast finer than the ~25 km cell: they inherit more spatial variability, are more sensitive to individual grid-cell values, and are more exposed to distortions where a cell straddles a boundary and only partly covers the unit. This is a property of the native grid, not a defect of the aggregation.
- **Scoring across levels.** For the six frozen-ruler thematic composites, districts and blocks use the same component mappings. Equal physical indicator values receive equal component scores. However, the mappings are nonlinear, so an area-weighted average of block scores generally does not reproduce the district score obtained from district-level physical inputs. The levels share a reference scale without their composite scores forming an additive hierarchy. Sectoral absolute/change lenses retain their separate state-and-level cohorts (§7.2).

---

## Appendix A: Complete Metric Reference

The tables below describe the available thematic and sectoral indicators. Inclusion in this catalogue does not imply contribution to a thematic composite: §6.4 defines those contributing sets. Sections A.1–A.5 cover the thematic indicator families; A.6 lists additional sectoral inputs, including CDD, which also determines the current thematic Drought Risk score. Metrics shared across bundles retain their uses in the Bundle(s) column.

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

All SPI metrics use a Gamma distribution fitted by MoM over the calibration period 1990–2010. Event/spell metrics apply the SPI < −1 threshold (moderate drought onset). The aridity index is the exception in this table: it is an absolute ratio and is standardised against no baseline at all. CDD, which determines the current published Drought Risk score, is catalogued in A.6 because it is also a sectoral input.

| Label | Scale | Definition | Units | Period rollup | Bundle(s) |
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
| Consecutive dry days (CDD) | Maximum run of consecutive days with precipitation < 1 mm in the year (a dry day is `pr` < 1 mm; Climdex CDD) | days | — | Drought Risk; Industrial, Investment, Asset (Thermal), Asset (Hydropower), Life & Livelihood |
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

*Document last updated: 2026-09-18*  
*Maintained by: Abu Bakar Siddiqui Thakur*
