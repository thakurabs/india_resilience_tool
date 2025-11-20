Here’s a complete `README.md` you can copy-paste as-is and tweak later if needed:

````markdown
# IRT Climate Pipeline (Prototype)

This repository contains the code used to:

1. **Download** downscaled daily climate model data from NASA NEX-GDDP.
2. **Pre-process** the downloaded NetCDF data for a region of interest (e.g. India / Telangana).
3. **Compute climate indices** and post-process them into district-level CSV files.
4. **Visualize** the results using a Streamlit dashboard.

> **Note:** This repo is intentionally **code-only**. Raw and processed data
> (e.g. NetCDF files, large CSVs) live outside the repo and are not tracked by Git.

---

## 1. Environment

- Python: **3.10**
- Conda environment name: **`irt`**

Create the environment using the provided `irt_env.yml`:

```bash
conda env create -f irt_env.yml -n irt
conda activate irt
````

If the environment already exists:

```bash
conda activate irt
```

---

## 2. Data locations (on disk)

These paths are **local** to the primary development machine and should be
adapted as needed on other systems. A typical layout is:

* Raw NEX-GDDP data (subset for India / region):

  * `D:\projects\irt\r1i1p1f1\`
* Processed outputs (intermediate and final CSVs):

  * `D:\projects\irt\processed\...`
* District polygons / GeoJSON:

  * `D:\projects\irt\districts_4326.geojson` (or equivalent path)

These directories are **not** part of the Git repo and should not be added to Git.

---

## 3. Main scripts

* `nex_india_subset_download_s3_v1.py`
  Downloads and subsets NASA NEX-GDDP data for the region of interest.
  Writes NetCDF files into the raw data directory (e.g. `D:\projects\irt\r1i1p1f1\`).

* `compute_indices_v1.py`
  Reads the downloaded NetCDF files, computes climate indices, and writes
  district-level CSVs to the processed directory (e.g. `D:\projects\irt\processed\...`).

* `avg_days_above_32degC.py`
  Utility script for computing “days above 32°C” metrics, typically at district level.

* `build_master_metrics.py`
  Post-processes index CSVs into master tables that are consumed by the dashboard.

* `build_all_csv.ps1`
  PowerShell helper to run a sequence of CSV-building steps (optional convenience).

* `climdex_dashboard_v2.py`
  Streamlit app to explore the computed indices via an interactive dashboard.

---

## 4. Typical workflow

From the repository root (e.g. `D:\projects\irt_v1\`):

### 4.1 Download NEX-GDDP data

```bash
conda activate irt
python nex_india_subset_download_s3_v1.py
```

This populates the raw data directory (e.g. `D:\projects\irt\r1i1p1f1\`).

### 4.2 Compute climate indices

```bash
python compute_indices_v1.py
```

This reads the NetCDF data and writes district-level CSVs into
`D:\projects\irt\processed\...` (or equivalent).

### 4.3 Build master metrics CSVs

Either run the PowerShell helper:

```powershell
.\build_all_csv.ps1
```

or directly call:

```bash
python build_master_metrics.py
```

This step produces aggregated “master” CSV files that the dashboard will read.

### 4.4 Run the Streamlit dashboard

```bash
streamlit run climdex_dashboard_v2.py --server.address 0.0.0.0 --server.port 8501
```

On the same machine you can open:

```text
http://localhost:8501
```

From another machine on the same network, use:

```text
http://<your-ipv4-address>:8501
```

---

## 5. Repository layout

A minimal layout for this repo is:

```text
.
├─ climdex_dashboard_v2.py
├─ compute_indices_v1.py
├─ avg_days_above_32degC.py
├─ nex_india_subset_download_s3_v1.py
├─ build_master_metrics.py
├─ build_all_csv.ps1
├─ irt_env.yml
├─ districts_4326.geojson        # if version-controlled; optional
├─ README.md
└─ .gitignore
```

Large data directories such as `r1i1p1f1\` and `processed\` are intentionally
kept **outside** this repository.

---

## 6. Notes & future work

* Replace hard-coded paths (e.g. `D:\projects\irt\...`) with a small config file
  so different users/machines can point to their own data directories.
* Add simple consistency checks for the generated CSVs (e.g. row counts, basic
  validity checks) and turn them into lightweight tests.
* Add a basic GitHub Actions workflow to:

  * set up Python 3.10
  * create a minimal environment
  * run import checks and very fast tests that do not require large data downloads.

```
```
