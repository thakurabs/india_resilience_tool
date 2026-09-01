# India Needs a First Climate Screen for Infrastructure: A Data-Centre Pilot Shows Why

*Just 150 kilometres separate Mumbai Suburban and Pune City, but the India Resilience Tool finds a 7.5-fold difference in projected humid-heat days—showing why block-level hazard evidence belongs at the start of infrastructure planning.*

Mumbai Suburban and Pune City are roughly 150 kilometres apart. They sit in the same state, within the orbit of the same broad market and policy environment. Yet under a high-emissions pathway at mid-century, the block containing the screened Mumbai sites records 69 days a year at or above 28°C wet-bulb, compared with just nine days in Pune City. That is a 7.5-fold difference over a distance of just 150 kilometres.

<!-- This is not an argument that Mumbai is “bad” and Pune is “good”. Nor is it a verdict on any individual campus.  -->
This is a block-level climate hazard signal that should change the questions asked before a long-lived asset is conceptualised, financed, or designed. Data centres depend on cooling headroom, continuous and reliable power supply, water or other heat-rejection capacity, drainage, access roads and resilient off-site infrastructure. Climate pressure on those systems can shift sharply even where the commercial geography appears continuous.

That stark difference over a 150-kilometre distance is the case for making climate-hazard screening an ordinary first gate in infrastructure planning.

> **Visual 1 — The 150-kilometre divide.** Two-bar chart comparing Mumbai Suburban (69.1) and Pune City (9.2) on projected 24-model ensemble-mean days per year at or above 28°C wet-bulb. Block level; SSP5-8.5; mid-century, 2040–2060; humid-heat coverage: 18 blocks across seven states.

## From national climate concern to local infrastructure decisions

India does not lack climate plans or national-scale assessments. The harder problem is translating climate science into evidence that can be used at the scale where land, infrastructure and investment decisions are actually made. States, districts and, even blocks govern very different physical geographies, yet climate information often reaches decision-makers as a national map, a coarse grid, a generic vulnerability label or an opaque composite score. The scale of the evidence and the scale of the decision do not always meet.

The demand for better information is visible among the institutions expected to finance the transition. In a 2022–23 Reserve Bank of India survey, roughly 95% of responding financial institutions reported lacking adequate data to assess climate risk. Respondents specifically called for a national database of climate scenarios at a disaggregated geographic level for assessing physical hazards.

This is the resolution-and-specificity gap that the [India Resilience Tool](https://irt.resilience.org.in) is designed to address. IRT turns downscaled CMIP6 projections and selected hazard layers into district- and block-level hazard-pressure evidence. Users can explore places, hazards, scenarios and time horizons, and compare physical indicators across geographies.

The data resolution matters in a country spanning the arid northwest, Himalayan north, monsoon core, peninsular plateau and long eastern and western coastlines. One place may be facing rising humid heat, another more intense rainfall and another longer dry spells. A single national figure cannot tell a district or infrastructure planner where each pressure is intensifying or which locations should be prioritised for further assessment.

<!-- The distinction between *hazard pressure* and *risk* matters. Risk in the fuller sense also depends on exposure, vulnerability, adaptive capacity, engineering design, operating controls and the consequences of failure. IRT does not estimate realised losses or business interruption, and it does not replace a site survey or engineering study. -->

IRT supplies an early, practical screen of physical hazard pressure. It helps identify where hazardpressure is high or rising, which mechanism is driving it and where exposure, vulnerability, engineering and financial analysis should begin—before capital, land and policy lock in the next generation of infrastructure.

## A data-centre pilot of the first screen

Data centers are purpose-built infrastructure that run large numbers of computers reliably, securely and around the clock. Essentially, they are physical facilities that store business critical applications and data and house  houses groups of computer systems, servers, and data storage equipment. They are intensive, interconnected infrastructure systems whose operational continuity depends not only on servers and fibre, but also on uninterrupted power supply, cooling systems, availability of water or alternative heat rejection, substations, drainage, roads and backup-fuel.

India’s digital market is expanding quickly and remains concentrated in a handful of clusters: Mumbai Metropolitan Region (MMR), Chennai, Hyderabad, Bengaluru, Delhi NCR and Pune, alongside several emerging locations such as Vizag. In March 2026, the Government of India reported that total data-centre capacity had increased from about 375 MW in 2020 to around 1,500 MW by 2025. Cushman & Wakefield’s *India Data Centre Update H1 2025* estimated that Mumbai alone accounted for around 46% of operational capacity. Such concentration reflects fibre connectivity, access to power, land, customers, cloud regions and state incentives. The question is whether climate-hazard pressure can be brought alongside those established criteria before new capacity is locked in.

That expansion is now being reinforced by national policy. The Union Budget 2026–27 proposes a tax holiday through 2047 for eligible foreign cloud providers using notified India-based data-centre infrastructure for global operations. The government has separately estimated that electricity demand from data centres could reach 13.56 GW by 2031–32. This is nearly a 37 fold increase from the installed capacity at the start of the decade; this provides a clear indication of the power and cooling demand for this scale of operation. 
<!-- Although electricity demand and installed data-centre capacity are not identical measures, 13.56 GW is about 36 times the 375 MW capacity reported for 2020—an indication of the scale of supporting power infrastructure that may be required, not a like-for-like capacity forecast. -->

The Government of India has also explicitly recognised the gargantuan electricity and water requirements for the sector, while also noting the industry's adoption of direct-to-chip liquid cooling, adiabatic cooling and immersion cooling technologies to reduce water use. Though these measures are important, they do not remove the need to understand how heat, water, rainfall and flood pressures vary between potential locations.

<!-- That concentration is not irrational. Data-centre locations reflect fibre connectivity, access to power, land, customers, cloud regions and state incentives. The question is whether climate-hazard pressure can be brought alongside those established criteria before new capacity is locked in. -->

The sector therefore offers a pilot study opportunity for IRT’s central proposition: can spatially diaggregated multi-hazard evidence reveal decision-relevant insights?

## How we screened the portfolio

An operating portfolio of 33 campuses represented by 28 unique locality-level coordinate points was assembled. We uploaded those points to IRT’s portfolio workflow, which resolved them to block-level administrative units. We then applied extreme-rainfall, riverine-flood and humid-heat hazard pressure analysis to these points.
<!-- runs resolved the operating points to 22 distinct blocks. The humid-heat analysis covered 18 blocks across seven states. -->

<!-- We kept the comparison deliberately physical and hazard-specific. All projected climate values reported below are 24-model ensemble means: -->

- **Humid heat:** projected days per year at or above 28°C and 30°C wet-bulb under SSP5-8.5, using a 1990–2010 baseline and a 2040–2060 mid-century horizon.
- **Extreme rainfall:** projected maximum one-day rainfall (Rx1day), very-heavy-rain days (R20mm) and consecutive wet days under the same scenario.
- **Riverine flood:** present-day mapped extent within each block of a modelled river flood with a 1% annual exceedance probability, commonly called RP-100, based on JRC flood layers. Exported extent fractions are presented below as percentages of block area.

<!-- We excluded composite scores from the findings. In these case-study exports, relative composite scores are comparable only within the same state and period. Using them across states or through time could create a misleading ranking. Physical units permit like-for-like comparison within the same indicator, scenario and horizon, while preserving the mechanism behind each signal. -->

> **What this screening covers—and what it does not:** The operating portfolio comprised 33 campuses represented by 28 unique locality-level points. Rainfall and riverine-flood runs covered 22 blocks, while humid heat covered 18 blocks across seven states. Heat and rainfall are projected 24-model ensemble-mean indicators, while RP-100 riverine flood is a present-day snapshot. Rainfall values are model-ensemble screening estimates. Since we are dealing with climate model projections, absolute extreme-rainfall intensity may be understated, so comparisons are more defensible than treating individual values as source of truth design rainfall. Results describe block-level climate pressure; coordinates are locality-level, so block assignments are screening-grade.

> **Visual 2 — The screened portfolio.** IRT portfolio heatmap using a physical metric rather than a composite score. Caption the chosen metric and units; projected climate values are 24-model ensemble means at block level under SSP5-8.5 for 2040–2060; humid-heat coverage: 18 blocks, rainfall coverage: 22 blocks. If mapped RP-100 flood extent is shown, present it as a percentage and label it as a present-day snapshot covering 22 blocks.

## Three ways geography changes the story

### 1. Maharashtra: the Western Ghats divide

The Mumbai–Pune comparison highlighted in the beginning is part of a wider Maharashtra pattern. Coastal MMR forms a tight high-humid-heat group: for the mid-century period, Mumbai Suburban records about 69.1 humid-heat days at or above 28°C wet-bulb, Thane 68.6 and Panvel 67.2. On the Pune and Nashik side of the Western Ghats, Pune City records about 9.2 days, while Mulshi and Nashik each record about 6.8.

Rainfall strengthens the geographic interpretation. Mid-century maximum one-day rainfall reaches about 101.8 mm in Thane, 99.4 mm in Mumbai Suburban and 98.5 mm in Panvel. Pune City records 46.1 mm, the lowest value in the 22-block rainfall set.

Mulshi, a block in the west of the Pune district, supplies the important complication. At about 6.8 humid-heat days, it resembles the plateau group. But its maximum one-day rainfall reaches about 66.8 mm, consistent with its position near the Ghats crest rather than Pune City’s rain-shadow conditions. This highlights that even a district-level aggregation can therefore blur distinct hazard mechanisms.

<!-- The lesson is to recognise that physical geography can divide an apparently coherent infrastructure market and that different hazards can draw that divide in different places. -->

### 2. Delhi NCR: one market, different hazard pressure

Delhi NCR is often treated as a single infrastructure geography. However, our analysis reveals otherwise. 

Projected mid-century humid-heat days at or above 28°C wet-bulb rise from about 51.5 in Gurgaon to 59.0 in New Delhi, 63.7 in South East Delhi, 65.4 in Bisrakh and 66.8 in Dankaur. Days at or above the more severe 30°C threshold rise in the same west-to-east order, from about 12.0 in Gurgaon to 17.7 in Dankaur.

That is roughly a 15-day spread in humid-heat pressure across the broader NCR geography. The pattern crosses Haryana, Delhi and Uttar Pradesh, which is precisely why a state-normalised score would miss it.

The riverine-flood snapshot adds a second signal. Bisrakh and Dankaur have the greatest mapped RP-100 flood extent among all screened blocks, at about 53% and 39% of block area respectively. The corresponding values are 14% in Gurgaon and 1% in New Delhi.

Eastern NCR therefore shows both greater measured humid-heat pressure and greater mapped riverine flood extent in this portfolio. This then becomes strong evidence to direct the next round of due diligence.

> **Visual 3 — NCR’s west-to-east gradient.** Dot or slope chart for Gurgaon, New Delhi, South East Delhi, Bisrakh and Dankaur. Show projected 24-model ensemble-mean days per year at or above 28°C and, optionally, 30°C wet-bulb. Block level; SSP5-8.5; mid-century, 2040–2060; humid-heat coverage: five NCR blocks within an 18-block, seven-state heat sample.

### 3. Chennai: riverine exposure is not rainfall intensity

Chennai offers the clearest example of why hazards must be separated by mechanism.

The Chennai and Thiruporur blocks are in the high humid-heat group, with about 74.5 and 64.2 mid-century days at or above 28°C wet-bulb. Their mapped RP-100 flood extents are also comparatively high in the screened set, at about 35% and 32% of block area.

Yet Chennai is not exceptional among the screened blocks on the selected block-level rainfall indicators. Mid-century maximum one-day rainfall is about 64.1 mm in Chennai and 63.5 mm in Thiruporur—well below MMR and close to the NCR range in this sample. Chennai records about 16.2 very-heavy-rain days, compared with roughly 48 in the screened MMR blocks.

A single “flood risk” label could obscure this distinction. Rainfall intensity, mapped riverine extent, local drainage failure, coastal inundation and storm surge are different mechanisms. They require different data and different responses. This screening did not assess Chennai’s local drainage, cyclone, coastal-inundation or storm-surge exposure. Its narrower finding is still useful: among the indicators measured here, rainfall intensity and mapped riverine extent tell different stories.

## What the pilot demonstrates about climate-hazard screening

The pilot study demonstrates what changes when climate evidence is brought closer to the scale and structure of an infrastructure decision.

- **Administrative granularity changes the picture.** Nearby locations within the same state or commercial cluster can show materially different hazard pressures. District and block evidence can reveal contrasts hidden by larger levels of aggregation.
- **Different hazards draw different geographies.** The Western Ghats divide humid heat and rainfall differently; NCR’s gradient crosses state boundaries; Chennai’s rainfall indicators and mapped riverine extent tell different stories. A single hazard label or weighted total would hide those mechanisms.
- **Physical metrics make the next question clearer.** Days above a wet-bulb threshold, millimetres of one-day rainfall and percentage of block area within a mapped flood footprint connect more directly to cooling, drainage, access, substation and water-system investigations than an unexplained score.
- **Screening makes due diligence proportionate.** It does not determine whether a campus is safe or viable. It identifies where site coordinates, engineering design, utilities, exposure and vulnerability need closer examination.

This is the role IRT can play: turn a general concern about future climate into a location- and mechanism-specific agenda for further assessment.

## What the portfolio suggests—and what it does not

Across the measured indicators, MMR appears compound-exposed: high humid heat, material mapped riverine flood extent and the strongest extreme-rainfall values in the portfolio. Chennai combines high humid heat with comparatively high mapped riverine extent, while its selected rainfall indicators are not the portfolio’s highest. Eastern NCR combines higher humid-heat pressure within its cluster with the highest mapped RP-100 flood extent in the screened set.

Pune City, Bengaluru and Hyderabad remain outside the leading exposure tier on the measured indicators. Hyderabad illustrates the care needed even here: Shabad and Shaikpet show 0% mapped RP-100 flood extent, while Gandipet records 15%. Its profile is comparatively favourable, but it is not uniformly flood-free. Further, this analysis says nothing about its water sources, grid, access, drainage, urban heat-island effects or facility design.

There is no overall “best” or “worst” location in this analysis because we did not invent a weighting rule. Infrastructure does not fail because a composite score is high. It can fail when cooling headroom shrinks, a substation or access route floods, drainage is overwhelmed, or water supplies become constrained. Such preliminary screening identifies which mechanisms require further analysis and deeper investigation.

> **Visual 4 — Different hazards, different stories.** Cluster mechanism table showing projected 24-model ensemble-mean mid-century days at or above 28°C wet-bulb, present-day mapped RP-100 flood extent as a percentage of block area, and projected 24-model ensemble-mean mid-century Rx1day. No weighted total; show ranges and missing values explicitly. Block level; SSP5-8.5, 2040–2060 for heat and rainfall; present-day snapshot for flood; heat coverage: 18 blocks, rainfall and flood coverage: 22 blocks.

## Putting the first screen into infrastructure planning

Data centres are the pilot, but the principle applies to industrial parks, logistics hubs, hospitals, energy assets, transport corridors, urban expansion and any other major public investment. Climate-hazard information becomes useful when it enters ordinary decisions about where to build, what to design for and which questions must be resolved before capital is committed.

Climate-hazard screening should happen before land acquisition and design lock-in. State and local incentive packages should require evidence that relevant hazard pressures have been screened and material mechanisms identified. Lenders and investors should ask for the same evidence during due diligence. Utilities should consider the climate-sensitive power and water dependencies created by concentrated infrastructure loads. Land-use planning should use district and block-level evidence to inform where new clusters are encouraged and what safeguards they require.

Where public incentives or notified-facility frameworks support new data-centre capacity, climate-hazard screening could provide a proportionate first gate before eligibility, siting or supporting utility investments are finalised. Long-term fiscal certainty should be matched by evidence that the infrastructure it helps mobilise is being planned for the physical conditions it will face.

It is a way to ask better questions earlier. If a screening flags humid heat, the next step may be a cooling-system and water-source assessment. If it flags mapped riverine extent, the next step may be site-scale hydrology, drainage, substation and access-route analysis. Coastal locations may require cyclone, storm-surge and compound rainfall–tide modelling that this case study did not provide.

The screening does not make the investment decision. It makes the investigation proportionate to the hazard pressures identified in present-day and projected evidence. Mainstreaming that first screen would help move infrastructure planning from general climate awareness toward proactive, location-specific due diligence.

## The first filter, not the final answer

IRT is designed to make climate hazard pressure visible at the administrative scales where decisions are made. It closes an important information gap, but it is not a replacement for precise site coordinates, surveys, engineering studies, hydrological modelling, local utility data, water-source analysis, exposure and vulnerability assessment or community consultation.

Its value is that it can change the first conversation. Instead of asking only whether a location has land, fibre, power and incentives, planners can also ask: which climate-scale mechanisms are visible here, how do nearby alternatives differ, and what needs deeper investigation before we commit?

The data-centre pilot shows what this first filter can do: reveal local contrasts, separate hazard mechanisms and direct the next stage of investigation. Climate-hazard screening will not decide every infrastructure question. But it should decide what questions get asked before the next generation of infrastructure is locked into place.

Explore the [India Resilience Tool](https://irt.resilience.org.in) and contact Resilience Actions to request a demo.

---

## Sources and notes

1. India Resilience Tool, including the in-app Technical Guidance Note: [irt.resilience.org.in](https://irt.resilience.org.in). Case-study values come from development-deployment portfolio exports dated 30 July 2026 and were checked against the documented export tables before publication. The Technical Guidance Note documents IRT’s 24-model ensemble implementation and interpretation limits.
2. Reserve Bank of India, *Report on Currency and Finance 2022–23: Towards a Greener Cleaner India*, Chapter III stakeholder survey. The survey reported that roughly 95% of responding financial institutions lacked adequate climate-risk data and recorded demand for geographically disaggregated physical-hazard scenarios.
3. Cushman & Wakefield, *India Data Centre Update H1 2025*: [stable report page](https://www.cushmanwakefield.com/en/india/insights/india-data-centre-update). Market-capacity figures refer to the report’s H1 2025 snapshot; announced or upcoming capacity is not treated as operational.
4. Press Information Bureau, Government of India, *Budget 2026–27 Sets the Stage for India as a Global Hub for Cloud and AI Infrastructure*, 14 February 2026: [PIB backgrounder](https://www.pib.gov.in/PressReleasePage.aspx?PRID=2227953&reg=48&lang=2). The tax holiday through 2047 is described as a Budget proposal subject to a defined eligibility framework.
5. Ministry of Electronics and Information Technology, Government of India, Rajya Sabha response, 13 March 2026: [PIB release](https://www.pib.gov.in/PressReleasePage.aspx?PRID=2239616&reg=48&lang=2). The release reports capacity of about 375 MW in 2020 and around 1,500 MW by 2025, and separately estimates electricity demand from data centres at 13.56 GW by 2031–32. Electricity demand and installed data-centre capacity are related but not identical measures.
6. Projected heat and rainfall indicators use the downscaled, bias-corrected NASA NEX-GDDP-CMIP6 climate dataset: [NASA Center for Climate Simulation](https://www.nccs.nasa.gov/data-collections/nex-gddp-cmip6/). IRT’s 24-model selection and aggregation method are documented in the IRT Technical Guidance Note rather than by NASA.
7. RP-100 riverine flood is a present-day/static screening layer derived from Baugh et al. (2024), *Global river flood hazard maps, Version 2.1*, Joint Research Centre: [dataset DOI](https://doi.org/10.2905/JRC.VD32YWG). Mapped extent is the percentage of a block intersecting the modelled RP-100 inundation footprint; it is not flood depth at a campus.
