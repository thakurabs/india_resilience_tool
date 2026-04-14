#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DEFAULT_DATA_DIR="$(cd "${REPO_ROOT}/.." && pwd)/irt_data"

CONDA_ENV_NAME="${CONDA_ENV_NAME:-irt}"
IRT_DATA_DIR="${IRT_DATA_DIR:-${DEFAULT_DATA_DIR}}"

INDIA_SOUTH="${INDIA_SOUTH:-6}"
INDIA_NORTH="${INDIA_NORTH:-38}"
INDIA_WEST="${INDIA_WEST:-68}"
INDIA_EAST="${INDIA_EAST:-98}"

RUN_NEX="${RUN_NEX:-1}"
RUN_ERA5="${RUN_ERA5:-1}"
RUN_ERA5_HURS="${RUN_ERA5_HURS:-1}"
STAGE_ERA5_FOR_PIPELINE="${STAGE_ERA5_FOR_PIPELINE:-0}"
ERA5_YEARS="${ERA5_YEARS:-1951-2025}"
PYTHON_BIN="${PYTHON_BIN:-}"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

fail() {
  printf '[ERROR] %s\n' "$*" >&2
  exit 1
}

append_python_candidate() {
  local candidate="$1"
  local -n target_ref="$2"
  if [[ -n "${candidate}" ]]; then
    target_ref+=("${candidate}")
  fi
}

python_supports_download_stack() {
  local candidate="$1"
  "${candidate}" - <<'PY' >/dev/null 2>&1
import importlib
required = ["boto3", "xarray", "h5netcdf", "s3fs"]
for mod in required:
    importlib.import_module(mod)
PY
}

resolve_windows_path_to_wsl() {
  local raw_path="$1"
  if [[ -z "${raw_path}" ]]; then
    return 1
  fi
  if [[ "${raw_path}" =~ ^[A-Za-z]:\\ ]] && command -v wslpath >/dev/null 2>&1; then
    wslpath -u "${raw_path}"
    return 0
  fi
  printf '%s\n' "${raw_path}"
}

resolve_windows_python_from_conda_prefix() {
  local prefix="${CONDA_PREFIX:-}"
  local prefix_unix=""

  if [[ -z "${prefix}" ]]; then
    return 1
  fi

  if [[ "${prefix}" =~ ^[A-Za-z]:\\ ]] && command -v wslpath >/dev/null 2>&1; then
    prefix_unix="$(wslpath -u "${prefix}")"
  else
    prefix_unix="${prefix}"
  fi

  if [[ -x "${prefix_unix}/python.exe" ]]; then
    PYTHON_BIN="${prefix_unix}/python.exe"
    return 0
  fi

  if [[ -x "${prefix_unix}/bin/python" ]]; then
    PYTHON_BIN="${prefix_unix}/bin/python"
    return 0
  fi

  return 1
}

discover_python_bin() {
  local candidate=""
  local candidate_unix=""
  local userprofile_unix=""
  local localappdata_unix=""
  local -a candidates=()

  append_python_candidate "${PYTHON_BIN:-}" candidates

  if resolve_windows_python_from_conda_prefix; then
    append_python_candidate "${PYTHON_BIN}" candidates
  fi

  if [[ -n "${LOCALAPPDATA:-}" ]]; then
    localappdata_unix="$(resolve_windows_path_to_wsl "${LOCALAPPDATA}")"
    append_python_candidate "${localappdata_unix}/miniconda3/envs/${CONDA_ENV_NAME}/python.exe" candidates
    append_python_candidate "${localappdata_unix}/anaconda3/envs/${CONDA_ENV_NAME}/python.exe" candidates
  fi

  if [[ -n "${USERPROFILE:-}" ]]; then
    userprofile_unix="$(resolve_windows_path_to_wsl "${USERPROFILE}")"
    append_python_candidate "${userprofile_unix}/AppData/Local/miniconda3/envs/${CONDA_ENV_NAME}/python.exe" candidates
    append_python_candidate "${userprofile_unix}/AppData/Local/anaconda3/envs/${CONDA_ENV_NAME}/python.exe" candidates
    append_python_candidate "${userprofile_unix}/miniconda3/envs/${CONDA_ENV_NAME}/python.exe" candidates
    append_python_candidate "${userprofile_unix}/anaconda3/envs/${CONDA_ENV_NAME}/python.exe" candidates
  fi

  append_python_candidate "/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/${CONDA_ENV_NAME}/python.exe" candidates
  append_python_candidate "${HOME}/miniconda3/envs/${CONDA_ENV_NAME}/bin/python" candidates
  append_python_candidate "${HOME}/anaconda3/envs/${CONDA_ENV_NAME}/bin/python" candidates
  append_python_candidate "$(command -v python.exe 2>/dev/null || true)" candidates
  append_python_candidate "$(command -v python 2>/dev/null || true)" candidates
  append_python_candidate "$(command -v python3 2>/dev/null || true)" candidates

  for candidate in "${candidates[@]}"; do
    [[ -z "${candidate}" ]] && continue
    candidate_unix="$(resolve_windows_path_to_wsl "${candidate}")"
    if [[ -x "${candidate_unix}" ]] && python_supports_download_stack "${candidate_unix}"; then
      PYTHON_BIN="${candidate_unix}"
      return 0
    fi
    if command -v "${candidate}" >/dev/null 2>&1 && python_supports_download_stack "${candidate}"; then
      PYTHON_BIN="${candidate}"
      return 0
    fi
  done

  return 1
}

activate_conda_env() {
  local conda_sh=""
  local -a candidates=()

  if discover_python_bin; then
    log "Using python interpreter: ${PYTHON_BIN}"
    return 0
  fi

  if [[ -n "${CONDA_EXE:-}" ]]; then
    candidates+=("$(cd "$(dirname "${CONDA_EXE}")/../etc/profile.d" && pwd)/conda.sh")
  fi

  if command -v conda >/dev/null 2>&1; then
    local conda_base
    conda_base="$(conda info --base 2>/dev/null || true)"
    if [[ -n "${conda_base}" ]]; then
      candidates+=("${conda_base}/etc/profile.d/conda.sh")
    fi
  fi

  candidates+=(
    "${HOME}/miniconda3/etc/profile.d/conda.sh"
    "${HOME}/anaconda3/etc/profile.d/conda.sh"
    "/opt/conda/etc/profile.d/conda.sh"
    "/mnt/c/Users/22015611/AppData/Local/miniconda3/etc/profile.d/conda.sh"
  )

  for candidate in "${candidates[@]}"; do
    if [[ -f "${candidate}" ]]; then
      conda_sh="${candidate}"
      break
    fi
  done

  if [[ -z "${conda_sh}" ]]; then
    fail "Could not find a usable Python or conda.sh. Activate '${CONDA_ENV_NAME}' first, or set PYTHON_BIN explicitly."
  fi

  if [[ "${conda_sh}" == /mnt/c/* ]]; then
    fail "Found only Windows conda.sh at '${conda_sh}', which is not sourceable from WSL bash. Activate '${CONDA_ENV_NAME}' in the parent shell and rerun, or set PYTHON_BIN explicitly."
  fi

  # shellcheck disable=SC1090
  source "${conda_sh}"
  conda activate "${CONDA_ENV_NAME}"
  PYTHON_BIN="$(command -v python)"
}

check_python() {
  if [[ -n "${PYTHON_BIN}" ]] && { [[ -x "${PYTHON_BIN}" ]] || command -v "${PYTHON_BIN}" >/dev/null 2>&1; }; then
    return 0
  fi
  fail "python is not available for '${CONDA_ENV_NAME}'. Activate the env first, or set PYTHON_BIN explicitly."
}

check_cds_credentials() {
  if [[ -n "${CDSAPI_URL:-}" && -n "${CDSAPI_KEY:-}" ]]; then
    return 0
  fi
  if [[ -f "${HOME}/.cdsapirc" ]]; then
    return 0
  fi
  fail "ERA5 download requires CDS credentials. Set CDSAPI_URL/CDSAPI_KEY or create ~/.cdsapirc first."
}

prepare_dirs() {
  mkdir -p "${IRT_DATA_DIR}" "${IRT_DATA_DIR}/era5"
}

run_nex_download() {
  local variable="$1"
  log "Downloading NEX-GDDP-CMIP6 variable '${variable}' for the India bbox into ${IRT_DATA_DIR}"
  "${PYTHON_BIN}" - "${variable}" "${IRT_DATA_DIR}" "${INDIA_SOUTH}" "${INDIA_NORTH}" "${INDIA_WEST}" "${INDIA_EAST}" <<'PY'
import sys

import tools.data_acquisition.nex_india_subset_download_s3_v1 as m

variable = sys.argv[1]
m.OUTDIR = sys.argv[2]
m.LAT_MIN = float(sys.argv[3])
m.LAT_MAX = float(sys.argv[4])
m.LON_MIN = float(sys.argv[5])
m.LON_MAX = float(sys.argv[6])
sys.argv = ["nex_india_subset_download_s3_v1.py", variable]
m.main()
PY
}

run_all_nex_downloads() {
  local -a variables=("pr" "tas" "tasmax" "tasmin" "hurs")
  local variable
  for variable in "${variables[@]}"; do
    run_nex_download "${variable}"
  done
}

run_era5_downloads() {
  log "Downloading ERA5 daily statistics for the India bbox into ${IRT_DATA_DIR}/era5"
  "${PYTHON_BIN}" - "${IRT_DATA_DIR}" "${ERA5_YEARS}" "${INDIA_SOUTH}" "${INDIA_NORTH}" "${INDIA_WEST}" "${INDIA_EAST}" <<'PY'
from pathlib import Path
import sys

from tools.data_acquisition.download_era5_daily_stats_structured import (
    VariableConfig,
    download_era5_daily_stats_monthly_structured,
)

data_dir = sys.argv[1]
years_env = sys.argv[2].strip()
india_south = float(sys.argv[3])
india_north = float(sys.argv[4])
india_west = float(sys.argv[5])
india_east = float(sys.argv[6])

years = []
for part in years_env.split(","):
    part = part.strip()
    if not part:
        continue
    if "-" in part:
        start_s, end_s = part.split("-", 1)
        years.extend(range(int(start_s), int(end_s) + 1))
    else:
        years.append(int(part))
years = sorted(set(years))
if not years:
    raise ValueError("ERA5_YEARS did not resolve to any years.")

out_dir = Path(data_dir) / "era5"
area = [
    india_north,
    india_west,
    india_south,
    india_east,
]
variable_settings = {
    "pr": VariableConfig("total_precipitation", "daily_sum", "1_hourly"),
    "tas": VariableConfig("2m_temperature", "daily_mean", "6_hourly"),
    "tasmax": VariableConfig("2m_temperature", "daily_max", "1_hourly"),
    "tasmin": VariableConfig("2m_temperature", "daily_min", "1_hourly"),
    "tdps": VariableConfig("2m_dewpoint_temperature", "daily_mean", "6_hourly"),
}

download_era5_daily_stats_monthly_structured(
    out_dir=out_dir,
    years=years,
    variable_settings=variable_settings,
    area=area,
    time_zone="utc+00:00",
    delete_zip_after_extract=False,
)
PY
}

run_era5_hurs_derivation() {
  log "Deriving ERA5 hurs monthly files from tas + tdps"
  "${PYTHON_BIN}" - "${IRT_DATA_DIR}" <<'PY'
from pathlib import Path
import sys

from tools.data_prep.derive_hurs_from_era5_tas_tdps import derive_hurs_monthly

nc_dir = Path(sys.argv[1]) / "era5" / "nc"
derive_hurs_monthly(nc_dir=nc_dir, overwrite=False)
PY
}

stage_era5_for_pipeline() {
  log "Staging ERA5 precipitation into pipeline layout under IRT_DATA_DIR/r1i1p1f1"
  "${PYTHON_BIN}" -m tools.data_prep.prepare_reanalysis_for_pipeline \
    --years "${ERA5_YEARS}" \
    --bbox "${INDIA_SOUTH},${INDIA_NORTH},${INDIA_WEST},${INDIA_EAST}" \
    --era5-nc-dir "${IRT_DATA_DIR}/era5/nc" \
    --skip-imd
}

print_summary() {
  cat <<EOF

Completed pan-India raw data workflow.

Data root: ${IRT_DATA_DIR}
India bbox: south=${INDIA_SOUTH}, north=${INDIA_NORTH}, west=${INDIA_WEST}, east=${INDIA_EAST}
NEX run: ${RUN_NEX}
ERA5 run: ${RUN_ERA5}
ERA5 hurs derivation: ${RUN_ERA5_HURS}
ERA5 pipeline staging: ${STAGE_ERA5_FOR_PIPELINE}

Expected outputs:
- NEX raw annual NetCDFs under: ${IRT_DATA_DIR}/r1i1p1f1/
- ERA5 raw monthly NetCDFs under: ${IRT_DATA_DIR}/era5/nc/
- ERA5 raw zip payloads under: ${IRT_DATA_DIR}/era5/zips/
EOF
}

main() {
  cd "${REPO_ROOT}"

  log "Repo root: ${REPO_ROOT}"
  log "IRT_DATA_DIR: ${IRT_DATA_DIR}"
  log "Pan-India bbox: ${INDIA_SOUTH},${INDIA_NORTH},${INDIA_WEST},${INDIA_EAST}"

  activate_conda_env
  check_python
  prepare_dirs

  export IRT_DATA_DIR INDIA_SOUTH INDIA_NORTH INDIA_WEST INDIA_EAST ERA5_YEARS

  if [[ "${RUN_NEX}" == "1" ]]; then
    run_all_nex_downloads
  else
    log "Skipping NEX downloads because RUN_NEX=${RUN_NEX}"
  fi

  if [[ "${RUN_ERA5}" == "1" ]]; then
    check_cds_credentials
    run_era5_downloads
  else
    log "Skipping ERA5 downloads because RUN_ERA5=${RUN_ERA5}"
  fi

  if [[ "${RUN_ERA5_HURS}" == "1" ]]; then
    run_era5_hurs_derivation
  else
    log "Skipping ERA5 hurs derivation because RUN_ERA5_HURS=${RUN_ERA5_HURS}"
  fi

  if [[ "${STAGE_ERA5_FOR_PIPELINE}" == "1" ]]; then
    stage_era5_for_pipeline
  else
    log "Skipping pipeline staging because STAGE_ERA5_FOR_PIPELINE=${STAGE_ERA5_FOR_PIPELINE}"
  fi

  print_summary
}

main "$@"
