# Data-centre case study — site list

Site coordinates for the "where should India's next data centre *not* go?" case
study. Act 1 stress-tests the operating footprint; Act 2 screens for new ground.

## Files

| File | Contents |
|------|----------|
| `upload/dc_sites_operational.csv` | 33 operating campuses as 28 unique points — the Act 1 upload set |
| `upload/dc_sites_announced.csv` | 4 announced/under-construction sites — Act 2 only |
| `upload/by_state/<state>.csv` | Operating sites split by state (11 files) |
| `reference/dc_sites_all.csv` | All 37 sites with operator, district, precision and source |
| `build_site_csvs.py` | Regenerates every CSV above from one embedded table |

Coverage: 37 sites, 21 districts, 11 states.

## Upload format

Everything under `upload/` matches the dashboard's coordinate-upload contract
exactly, as defined by `sample_coordinates.xlsx`:

```csv
id,custom_name,lat,long
1,Yotta - NM1 (Yotta Datacenter Park),18.91347,73.19478
```

`id` restarts at 1 in each file, because each file is uploaded on its own.
`custom_name` is `operator - facility`, held to ASCII and never starting with a
character a spreadsheet would treat as a formula.

**Coordinates are unique within every file.** The dashboard rejects repeated
lat/long pairs, so co-located campuses are merged into a single point whose
label names every operator at it, rather than being dropped:

```csv
13,"SIPCOT IT Park, Siruseri - STT GDC India, Sify, AdaniConneX",12.82358,80.22215
```

Four such merges exist — SIPCOT Siruseri (3 campuses), Ambattur (2), Electronic
City (2) and Greater Kailash I (2) — so 33 operating sites upload as 28 points.
When reading results back, remember that a merged point carries the risk score
for a *cluster*, not one operator's campus.

### Delhi NCT

Delhi NCT proper hosts very little commercial colocation capacity. STT GDC is
effectively the only operator inside the territory — STT Delhi 1 at Videsh
Sanchar Bhavan (Bangla Sahib Road, New Delhi district) and STT Delhi 2 and 3
sharing the Greater Kailash I complex (South East district), together about
12 MW. Everything else branded "Delhi" sits outside NCT: CtrlS Delhi is roughly
350 m from CtrlS Noida, and Nxtra, Sify, NTT, Yotta and AdaniConneX are all in
Noida, Greater Noida or Manesar.

Note also that "Okhla" in operator listings usually means **Noida** — New Okhla
Industrial Development Authority — not the Okhla area of Delhi. There is no
data-centre cluster in Okhla proper.

That format has nowhere to carry provenance, which is why `reference/` exists.
Join the two on coordinates, or on the row order within a file — the reference
table is emitted in the same order.

State files contain **operating sites only**. The upload format has no `status`
column, so an announced site mixed into a state file would be
indistinguishable from a live one. Andhra Pradesh therefore has no state file:
its only site (Google Visakhapatnam) is announced.

## How the coordinates were produced

Two stages, because data-centre aggregators paywall their coordinate datasets:

1. **Locality verified from a public source** — operator location pages (STT GDC,
   Sify, CtrlS, Yotta, Nxtra, NTT, Equinix, AdaniConneX, Web Werks, ESDS),
   DataCenterDynamics and TechCrunch for announced sites, and aggregator listings
   (datacentermap, datacenters.com, baxtel, PeeringDB, datacenterHawk) for street
   addresses. The `source` column records which.
2. **Coordinates geocoded from that locality** via Nominatim / OpenStreetMap
   (data © OpenStreetMap contributors, ODbL). The geocoder's own district
   attribution was retained as an independent cross-check and is what populates
   the `district` column.

No coordinate is estimated or inferred by hand.

## Caveats — read before using these in a demo

- **Locality precision, not campus precision.** The `precision` column is
  `locality` for 35 sites and `city` for 2 (DC32 Jamnagar, DC34 Lucknow). This is
  adequate to resolve a site to its district; it is not a building footprint.
  Sites near a district boundary are the ones to check — the Navi Mumbai cluster
  straddles Thane/Raigad.
- **Seven sites share three coordinate points.** Ambattur (DC12, DC16), Siruseri
  (DC13, DC14, DC15) and Electronic City (DC18, DC19) are genuinely co-located in
  shared industrial parks — an artefact of locality-level geocoding, but also a
  real feature of how Indian data centres cluster into a handful of IT parks.
  `upload/` merges them; `reference/` keeps all seven as separate rows.
- **Ambattur has a district ambiguity.** The municipality falls inside Greater
  Chennai Corporation while the revenue district is Tiruvallur. Both OSM and LGD
  should say Tiruvallur; if the tool returns Chennai, that is a boundary-vintage
  difference, not an error in this file.
- **Announced sites are not operating assets.** Keep DC22, DC32, DC33 and DC34
  visually distinct from the operating footprint — attaching a risk score to an
  unbuilt facility invites a fair objection.
- **The `district` column is provenance, not input.** It appears only in
  `reference/`. Because the uploads are coordinates, the tool resolves districts
  by point-in-polygon; nothing here depends on district *names* matching the LGD
  roster. Use `district` to check the tool's answer, not to produce it.

## Regenerating

```bash
python case_study/data_centres/build_site_csvs.py
```

The verified coordinates are embedded in the script, so this is deterministic and
needs no network access.
