# Blog Structure — India Resilience Tool Data-Centre Case Study

**Working title:** What climate-hazard screening reveals about India's data-centre geography

**Format:** LinkedIn newsletter / long-form blog post, approximately 2,000-2,500 words.

**Primary audience:** Resilience Actions LinkedIn audience: climate-resilience, sustainability, policy, finance, infrastructure, and research practitioners, including readers from organisations such as CEEW, WRI, and adjacent institutions.

**Primary call to action:** Visit the India Resilience Tool at https://irt.resilience.org.in and request a demo.

---

## Central Thesis

Climate-hazard screening should become a standard first gate for infrastructure planning in India. The India Resilience Tool shows why: across India's data-centre geography, some of the most consequential differences appear within metropolitan regions and between nearby blocks, while different physical hazards tell fundamentally different stories.

Use "climate-hazard screening" as the main phrase. IRT supplies hazard-pressure evidence, not a complete IPCC-style risk assessment with exposure, vulnerability, adaptive capacity, engineering design, and operating controls.

---

## Editorial Frame

The post should read as an editorial research note, not a product announcement. It should be human, direct, evidence-led, and punchy, with no visible technical scaffolding unless it helps the reader trust a claim.

The narrative arc:

1. Open with one concrete finding that makes the tool's purpose obvious.
2. Explain the missing decision layer IRT was built to provide.
3. Use data centres as a live case study of why hazard-specific screening matters.
4. Show how block-level physical metrics change the interpretation of familiar city/cluster labels.
5. Close by arguing that climate-hazard screening should become normal infrastructure due diligence.

Avoid writing as if the tool gives final site verdicts. It gives an early screening layer that should trigger deeper investigation.

---

## Section 1 — A 150-Kilometre Climate Divide

**Purpose:** Open with the strongest finding, not with background. The reader should immediately see why block-level climate-hazard screening matters.

**Core opening fact:** Mumbai Suburban and Pune City are roughly 150 km apart and operate within the same state. Under SSP5-8.5 at mid-century, the block containing the Mumbai sites records about 69 days per year at or above 28 degC wet-bulb, compared with about 9 days in Pune City. That is roughly a 7.5x difference.

**Suggested opening paragraph direction:**

Start with the Mumbai-Pune contrast, then widen to the infrastructure-planning point. The argument is not "Mumbai bad, Pune good"; it is that long-lived infrastructure decisions are often made using city, state, land, fibre, power, and market logic, while climate hazard pressure can shift sharply over short distances.

**Claims to include:**

- The difference is within one state, so it avoids the common problem of comparing state-normalised composite scores across state boundaries.
- This is not a micro-site verdict. It is a block-level signal that should change the questions asked during siting and due diligence.
- The comparison is especially useful because data centres are long-lived assets whose cooling, power, water, drainage, and access dependencies are climate-sensitive.

**Potential line:**

That 150-kilometre difference is the case for making climate-hazard screening an ordinary first gate in infrastructure planning.

**Visual candidate:**

A simple two-bar chart: Mumbai Suburban vs Pune City, mid-century days >=28 degC wet-bulb. Use values from `FINDINGS.md` table 4a.

---

## Section 2 — The Missing First Gate

**Purpose:** Introduce IRT and the gap it addresses, borrowing the framing from the Technical Guidance Note but keeping the prose blog-friendly.

**Core idea:** India plans, finances, and permits adaptation-relevant infrastructure at subnational scales: states, districts, and increasingly blocks. But climate information often reaches decision-makers as national maps, coarse grids, generic vulnerability labels, or opaque composite scores.

**IRT positioning:**

- IRT turns downscaled CMIP6 climate projections and selected hazard layers into district- and block-level hazard-pressure evidence.
- It is designed for comparison across places, hazards, scenarios, and time horizons.
- It helps planners and infrastructure decision-makers ask better first questions: where is hazard pressure high, what mechanism is driving it, and where should deeper investigation begin?
- It supplies the hazard-pressure layer within a broader risk frame. It does not claim to estimate realised loss, business interruption, asset vulnerability, social vulnerability, adaptive capacity, or engineering resilience.

**Important wording:**

- Say "hazard-pressure" when describing IRT outputs.
- Say "screening" or "first gate," not "final assessment."
- Say "climate Risk" only where referring to the wider policy problem or common terminology, and clarify that IRT's numeric outputs are hazard-pressure indicators.

**Potential line:**

IRT is not the last word on infrastructure risk. It is the first serious filter: a way to see where climate pressure is already visible before capital, land, and policy lock in the next generation of infrastructure.

**Visual candidate:**

Screenshot or graphic from IRT landing/dashboard showing district/block hazard exploration, ideally with a physical metric or portfolio output rather than a cross-state composite score.

---

## Section 3 — Why Data Centres Are A Useful Test

**Purpose:** Explain why this sector is a good demonstration case without turning the post into a full market report.

**Core idea:** Data centres are usually discussed as digital infrastructure, but they are also physical infrastructure. They require reliable electricity, cooling headroom, water and/or heat-rejection capacity, drainage, land, access roads, backup fuel logistics, and resilient substations. These dependencies are climate-sensitive.

**Claims to include:**

- India's data-centre market is expanding quickly and is concentrated in a handful of clusters: MMR, Chennai, Hyderabad, Bengaluru, Delhi NCR, Pune, and a few emerging locations.
- Siting decisions are rationally driven by fibre connectivity, power access, land, customers, cloud regions, and state incentives.
- The missing layer is systematic climate-hazard screening at the geography where facilities actually sit.
- The point is not to argue that current clusters were irrational. The point is that climate hazard pressure should now be added to the siting scorecard before future capacity is locked in.

**Capacity claims:**

Use only citation-backed capacity claims. Candidate claims from the deep research report should be checked and cited in "Sources and notes" before publication:

- National operational capacity around 1,280 MW in H1 2025 from Cushman & Wakefield.
- Mumbai's share around 46% of operational capacity and 41% of upcoming supply from Cushman & Wakefield.
- CBRE September 2025 estimate around 1,530 MW, if used as a second estimate.
- Ministry of Power / CEEW electricity-demand figures, if the post needs a national-growth frame.

**Avoid:**

- Naming specific operators or campuses in the main text.
- Presenting announced/unbuilt sites as operating assets.
- Overloading the reader with market statistics.

---

## Section 4 — How We Screened The Portfolio

**Purpose:** Give readers enough method detail to trust the case study, while keeping technical implementation out of the main narrative.

**Core facts:**

- The operating portfolio comprised 33 campuses represented by 28 unique locality-level coordinate points.
- Coordinates were uploaded to the IRT portfolio workflow.
- IRT resolved the points to block-level administrative units.
- National riverine-flood and extreme-rainfall runs resolved the operating points to 22 distinct blocks.
- Humid-heat analysis covered 18 blocks across seven states.
- The post should aggregate findings to clusters such as MMR, Chennai, NCR, Hyderabad, Bengaluru, and Pune, not named sites or companies.

**Hazards and evidence bases:**

- Humid heat: projected climate metrics under SSP5-8.5, especially days per year >=28 degC wet-bulb and >=30 degC wet-bulb, using baseline 1990-2010 and future periods such as 2040-2060.
- Extreme rainfall / flash flood: projected metrics under SSP5-8.5, including Rx1day, R20mm, and CWD.
- Riverine flood: present-day RP-100 snapshot from JRC flood layers, especially mapped flood extent as share of block inundated.

**Composite-score constraint:**

Composite scores should not be used for the cross-state case-study findings. The findings should rely on physical metrics only: days per year, degC, mm of rainfall, and mapped flood extent. This is because dashboard composites are cohort-normalised and can be misleading when read across states or periods without the right context.

**Method honesty box:**

Use a boxed note or visually distinct paragraph:

> What this screening covers: The operating portfolio comprised 33 campuses represented by 28 unique locality-level coordinate points. The national rainfall and riverine-flood runs resolved these points to 22 blocks; humid-heat analysis covered 18 blocks across seven states. Composite scores were excluded. Heat and extreme rainfall use projected climate indicators, while RP-100 riverine flood is a present-day snapshot. Results describe block-level climate pressure, not campus-level flood depth or engineering risk. Water sources, grid resilience, access routes, drainage, coastal inundation, cyclone exposure, storm surge, and campus design were not assessed.

**Visual candidate:**

Use one or more portfolio heatmaps downloaded from IRT, but ensure they show physical metrics or metric sheets, not invalid cross-state composite tabs.

---

## Section 5 — Three Ways Geography Changes The Story

**Purpose:** Present the strongest evidence as three narrative findings. Each should show what IRT reveals that a city label or generic risk score would hide.

### 5.1 Maharashtra: The Western Ghats Divide

**Core finding:** Maharashtra splits sharply between coastal MMR and the Pune/Nashik side of the Western Ghats.

**Evidence to use:**

- Mumbai Suburban: about 69.1 mid-century days >=28 degC wet-bulb.
- Thane: about 68.6.
- Panvel: about 67.2.
- Pune City: about 9.2.
- Mulshi and Nashik: about 6.8.
- Mid-century Rx1day: Thane around 101.8 mm, Mumbai Suburban around 99.4 mm, Panvel around 98.5 mm, Mulshi around 66.8 mm, Pune City around 46.1 mm.

**Interpretation:**

- Coastal MMR is high on humid heat and rainfall intensity.
- Pune City, east of the Ghats in the rain shadow, has much lower humid heat and the lowest measured Rx1day in the screened portfolio.
- Mulshi is an important nuance: it behaves more like the plateau on humid heat but more like the Ghats crest on rainfall. This reinforces why district-level or city-label analysis can blur the story.

**Careful wording:**

Say "blocks containing operating sites" and "measured indicators," not "sites are safe/unsafe."

### 5.2 Delhi NCR: One Market, Different Hazard Pressure

**Core finding:** Delhi NCR is not one climate-hazard object. The screened blocks show a west-to-east humid-heat gradient, with eastern NCR showing greater measured humid-heat pressure and mapped riverine flood extent.

**Evidence to use:**

- Gurgaon: about 51.5 mid-century days >=28 degC wet-bulb.
- New Delhi: about 59.0.
- South East Delhi: about 63.7.
- Bisrakh: about 65.4.
- Dankaur: about 66.8.
- Days >=30 degC wet-bulb also rise eastward: about 12.0 in Gurgaon to about 17.7 in Dankaur.
- RP-100 mapped flood extent is highest in Bisrakh and Dankaur among the screened blocks: about 0.53 and 0.39 respectively.

**Interpretation:**

- This is a roughly 15-day spread in mid-century humid-heat pressure across the broader NCR geography.
- Eastern NCR takes both measured hits in this portfolio: higher humid-heat pressure and greater mapped riverine flood extent.
- The finding is cluster-scale and block-scale, not a claim about individual facilities, substations, or access roads.

**Careful wording:**

Use "eastern NCR shows greater measured humid-heat pressure and mapped riverine flood extent," not "eastern NCR performs worse" without qualification.

### 5.3 Chennai: Riverine Exposure Is Not The Same As Rainfall Intensity

**Core finding:** Chennai's screened blocks show high humid heat and high mapped riverine flood extent, but Chennai is not exceptional among the screened blocks on the selected regional rainfall indicators.

**Evidence to use:**

- Chennai and Thiruporur are in the high humid-heat tier: about 74.5 and 64.2 mid-century days >=28 degC wet-bulb.
- Mapped RP-100 flood extent: Chennai around 0.35 and Thiruporur around 0.32.
- Mid-century Rx1day: Chennai around 64.1 mm and Thiruporur around 63.5 mm, below MMR and around/near the NCR range in the screened set.
- R20mm for Chennai is about 16.2 days, much lower than MMR's roughly 48 days in the screened blocks.

**Interpretation:**

- This is the cleanest example of why mechanism separation matters.
- A combined "flood risk" label can hide whether the dominant signal is rainfall intensity, mapped riverine extent, drainage failure, coastal exposure, or storm surge.
- IRT cannot fully assess Chennai's local pluvial drainage, coastal inundation, cyclone, or storm-surge risk in this pass. The point is narrower: among the measured indicators, riverine extent and rainfall intensity tell different stories.

**Careful wording:**

Use "selected regional rainfall indicators" and avoid implying there is no local pluvial or drainage risk.

---

## Section 6 — What The Portfolio Suggests, And What It Does Not

**Purpose:** Synthesize the portfolio-level view without creating an implicit composite ranking.

**Key synthesis points:**

- MMR is compound-exposed across the measured indicators: high humid heat, material mapped riverine flood extent in screened blocks, and the strongest extreme-rainfall indicators in the portfolio.
- Chennai is highly exposed on humid heat and mapped riverine extent, while its selected rainfall-intensity indicators are not the portfolio's highest.
- Eastern NCR combines higher measured humid-heat pressure with the highest mapped RP-100 flood extent among screened blocks.
- Pune City, Bengaluru, and Hyderabad avoid the leading exposure tier on the measured indicators.
- Hyderabad shows one of the strongest all-round profiles on the three measured indicators, including zero mapped RP-100 flood extent in the resolved Hyderabad blocks. This must be caveated because water, grid, access, drainage, heat-island effects, and facility design were not assessed.

**Important caution:**

Do not declare an overall "best" or "worst" location unless a formal weighting rule is introduced. The post should avoid inventing a composite. It can say "compound-exposed," "stronger all-round profile on the measured indicators," and "requires deeper investigation."

**Potential line:**

Infrastructure does not fail because a composite score is high. It fails through physical mechanisms: lost cooling headroom, inundated substations, inaccessible roads, overwhelmed drainage, or constrained water supplies. Screening helps identify which mechanisms require deeper investigation.

**Visual candidates:**

- Three-axis cluster table: humid heat days >=28 degC, mapped RP-100 flood extent, Rx1day mid-century.
- Optional ranked dot plot by cluster for each metric, avoiding a single weighted total.

---

## Section 7 — From Data Centres To Infrastructure Policy

**Purpose:** Move from the case study to the larger argument: climate-hazard screening should become a routine first gate for infrastructure planning.

**Core idea:** Data centres are the example, but the lesson applies to long-lived infrastructure more broadly: industrial parks, logistics hubs, hospitals, energy assets, transport corridors, urban expansion, and large public investments.

**Policy and practice implications:**

- Siting: hazard-pressure screening should happen before land acquisition and design lock-in.
- Incentives: state and local incentives should require evidence that climate hazard pressure has been screened and material mechanisms identified.
- Finance: lenders and investors should require hazard-screening evidence as part of due diligence.
- Utilities: power and water planning should consider the climate-sensitive dependencies of concentrated infrastructure loads.
- Land-use planning: block-level and district-level hazard evidence should inform where infrastructure clusters are encouraged or discouraged.
- Due diligence: screening should identify what needs deeper assessment: hydrology, drainage, water sources, substations, access roads, backup systems, and community impacts.

**Tone:**

This section should not sound punitive. Emphasise better questions, earlier screening, and avoiding lock-in. Current data-centre clusters were sited for rational market reasons; the argument is that climate hazard pressure now needs to sit alongside those reasons.

---

## Section 8 — The First Filter, Not The Final Answer

**Purpose:** Close with humility and action.

**Core closing points:**

- IRT is designed to make climate hazard pressure visible at the administrative scales where decisions are made.
- It is not a replacement for site surveys, engineering studies, hydrological modelling, local utility data, water-source analysis, or community consultation.
- Its value is that it can change the first conversation. Instead of asking only whether a location has land, fibre, power, and incentives, infrastructure planners can ask what climate mechanisms are already visible and what requires deeper investigation.

**Call to action:**

Invite readers to explore IRT at https://irt.resilience.org.in and request a demo from Resilience Actions.

**Potential closing line:**

Climate-hazard screening will not decide every infrastructure question. But it should decide what questions get asked before the next generation of infrastructure is locked into place.

---

## Claims And Wording Guardrails

- Prefer "climate-hazard screening" over "climate-risk screening."
- Prefer "hazard-pressure" over "risk score" when describing IRT outputs.
- Prefer "blocks containing operating sites" over "sites experience."
- Prefer "mapped RP-100 flood extent" over "flooding at the campus."
- Prefer "one of the strongest all-round profiles on the measured indicators" over "best combined profile."
- Prefer "compound-exposed across the measured indicators" over "worst location."
- Distinguish projected metrics from snapshot metrics:
  - Humid heat and extreme rainfall are projected under SSP5-8.5.
  - RP-100 riverine flood is a present-day/static snapshot.
- Do not use cross-state composite scores for findings.
- Do not imply water risk was assessed in this data-centre pass.
- Do not imply coordinate precision is campus-footprint precision.

---

## Visual Plan

1. **Lead chart:** Mumbai Suburban vs Pune City, mid-century days >=28 degC wet-bulb.
2. **Portfolio heatmap:** downloaded from IRT, physical metric view only.
3. **Maharashtra split chart:** MMR blocks vs Pune/Nashik blocks for humid heat and optionally Rx1day.
4. **NCR gradient chart:** west-to-east dot/slope chart for days >=28 degC and/or days >=30 degC wet-bulb.
5. **Cluster mechanism table:** humid heat, mapped RP-100 flood extent, and Rx1day by cluster, with no weighted total.
6. **Optional Chennai callout:** high mapped riverine extent but not highest selected rainfall-intensity indicators.

---

## Sources And Notes To Prepare Before Publication

Use a short "Sources and notes" section at the end of the post, rather than inline academic-style citations.

Candidate source groups:

- IRT Technical Guidance Note / in-app Read the Docs: framing, hazard-pressure scope, district/block resolution, data sources, and interpretation limits.
- Case-study findings: `case_study/data_centres/analysis/FINDINGS.md`.
- Site-list provenance: `case_study/data_centres/README.md`.
- Data-centre market capacity and pipeline: citation-backed sources from the deep research report, especially Cushman & Wakefield, CBRE, CEEW, Ministry of Power, and WRI where used.
- Climate data: NASA-NEX GDDP-CMIP6 / CMIP6 and JRC RP-100 flood data, if the post includes dataset provenance.

Before drafting the final blog, verify every market-capacity statistic against its citation and decide which claims are necessary. Keep the public post lean: one or two capacity statistics are enough.
