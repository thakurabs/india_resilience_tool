#!/usr/bin/env python3
import os
import sys
import time
import argparse
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import xarray as xr
import numpy as np

S3_BUCKET = "nex-gddp-cmip6"
S3_PREFIX = "NEX-GDDP-CMIP6"

# Configuration (keep DEFAULT_MEMBER if you want to override in code)
DEFAULT_MEMBER = "r1i1p1f1"
EXPERIMENTS = ["historical", "ssp245", "ssp585"]
LAT_MIN, LAT_MAX = 17.0, 22.0
LON_MIN, LON_MAX = 78.0, 85.0
OUTDIR = r"D:\projects\irt\aspirational_districts_data"
SLEEP_BETWEEN = 0.5

def s3_client():
    return boto3.client("s3", region_name="us-west-2",
                        config=Config(signature_version=UNSIGNED))

def years_for_experiment_fixed(exp):
    """
    Returns list of years according to the requested policy:
    - historical => 1990-2010
    - ssp245, ssp585 => 2020-2060
    """
    if exp == "historical":
        return list(range(1990, 2010 + 1))
    elif exp in ("ssp245", "ssp585"):
        return list(range(2020, 2060 + 1))
    else:
        return []

def list_available_members(model, exp, var):
    """List all available ensemble members for a given model/exp/var."""
    cli = s3_client()
    members = set()
    folder = f"{S3_PREFIX}/{model}/{exp}/"
    paginator = cli.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=folder, Delimiter="/"):
        for pref in page.get("CommonPrefixes", []):
            member = pref["Prefix"].rstrip("/").split("/")[-1]
            if member.startswith("r") and "i" in member and "p" in member:
                members.add(member)
    return sorted(members)

def list_remote_year_keys(model, exp, member, var, year):
    """Return (key, tail) tuples for var/model/exp/member/year."""
    cli = s3_client()
    out = []
    folder = f"{S3_PREFIX}/{model}/{exp}/{member}/{var}/"
    paginator = cli.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=folder):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            tail = k.rsplit("/", 1)[-1]
            if tail.startswith(f"{var}_day_{model}_{exp}_{member}_") and tail.endswith(f"_{year}.nc"):
                out.append((k, tail))
    return out

def detect_lat_lon_vars(ds):
    # Robust but simple detection of lat/lon coordinate names
    lat_var = None
    lon_var = None
    # Check coords first
    for c in ds.coords:
        lc = c.lower()
        if lc in ("lat", "latitude") and lat_var is None:
            lat_var = c
        if lc in ("lon", "longitude") and lon_var is None:
            lon_var = c
    # If not found, check data variables for names containing lat/lon
    if lat_var is None:
        for c in list(ds.coords) + list(ds.data_vars):
            if "lat" in c.lower():
                lat_var = c
                break
    if lon_var is None:
        for c in list(ds.coords) + list(ds.data_vars):
            if "lon" in c.lower():
                lon_var = c
                break
    return lat_var, lon_var

def download_and_subset_from_s3(s3_key, out_path, lat_min, lat_max, lon_min, lon_max):
    """Download from S3 and subset to region of interest."""
    try:
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"
        # Try direct open with fsspec/s3fs via xarray
        try:
            ds = xr.open_dataset(
                s3_url,
                engine='h5netcdf',
                storage_options={'anon': True}
            )
        except Exception as e_direct:
            # Fallback: download temporary local file with boto3 then open
            import tempfile
            cli = s3_client()
            tmpf = tempfile.NamedTemporaryFile(delete=False, suffix=".nc")
            tmpf.close()
            with open(tmpf.name, "wb") as fh:
                cli.download_fileobj(S3_BUCKET, s3_key, fh)
            ds = xr.open_dataset(tmpf.name, engine='h5netcdf')
            # remove temp file after opening (we'll keep ds in memory / lazy)
            try:
                os.remove(tmpf.name)
            except Exception:
                pass

        lat_var, lon_var = detect_lat_lon_vars(ds)
        if lat_var is None or lon_var is None:
            raise RuntimeError(f"Could not detect lat/lon in dataset coords: {list(ds.coords)}")

        ds_subset = ds.sel({lat_var: slice(lat_min, lat_max),
                            lon_var: slice(lon_min, lon_max)})

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        ds_subset.to_netcdf(out_path)
        ds.close()
        ds_subset.close()
        return True

    except Exception as e:
        print(f"[ERROR] Failed to process {s3_key}: {e}", file=sys.stderr)
        return False

def list_models_from_s3():
    """List model folders under NEX-GDDP-CMIP6/"""
    cli = s3_client()
    models = set()
    paginator = cli.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET,
                                   Prefix=f"{S3_PREFIX}/",
                                   Delimiter="/"):
        for pref in page.get("CommonPrefixes", []):
            models.add(pref["Prefix"].split("/", 1)[1].strip("/"))
    return sorted(models)

def main():
    ap = argparse.ArgumentParser(
        description="Download and subset NEX-GDDP-CMIP6 data directly from S3.\n"
                    "Usage: nex_india_subset_download.py <variable>\n"
                    "Example: python nex_india_subset_download.py pr"
    )
    ap.add_argument("variable", help="Variable to download (e.g. pr, tas, huss)")
    args = ap.parse_args()
    variable = args.variable

    # Check minimal dependencies
    try:
        import xarray
        import h5netcdf
    except ImportError as e:
        print(f"[ERROR] Missing required package: {e}", file=sys.stderr)
        print("Install with: pip install xarray h5netcdf s3fs fsspec", file=sys.stderr)
        sys.exit(1)

    os.makedirs(OUTDIR, exist_ok=True)

    # Discover models (default: all available)
    print("Discovering all available models from S3...")
    models = list_models_from_s3()
    if not models:
        print("No models discovered. Check connectivity.", file=sys.stderr)
        sys.exit(2)
    print(f"Discovered {len(models)} models. Downloading from all.")

    for exp in EXPERIMENTS:
        yrs = years_for_experiment_fixed(exp)
        if not yrs:
            print(f"[SKIP] {exp}: no years configured.")
            continue

        for model in models:
            # Use DEFAULT_MEMBER (can change in code), or you can set to 'auto' here
            member = DEFAULT_MEMBER
            if member.lower() == 'auto':
                available_members = list_available_members(model, exp, variable)
                if not available_members:
                    print(f"[SKIP] {model}: no ensemble members found for {exp}/{variable}")
                    continue
                member = available_members[0]

            for year in yrs:
                found = list_remote_year_keys(model, exp, member, variable, year)
                if not found:
                    print(f"[MISS ] {model}/{exp}/{variable}/{year} (member: {member})")
                    continue

                s3_key, tail = found[0]
                out_path = os.path.join(
                    OUTDIR, member, exp, variable, model,
                    f"{year}.nc"
                )

                if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                    print(f"[SKIP] exists: {out_path}")
                    continue

                print(f"[GET ] s3://{S3_BUCKET}/{s3_key}")
                ok = download_and_subset_from_s3(
                    s3_key, out_path, LAT_MIN, LAT_MAX, LON_MIN, LON_MAX
                )
                if ok:
                    print(f"[SAVE] {out_path}")

                time.sleep(SLEEP_BETWEEN)

if __name__ == "__main__":
    main()
