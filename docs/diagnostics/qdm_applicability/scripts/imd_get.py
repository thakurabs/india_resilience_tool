"""Download IMD gridded daily data (.grd) for the requested years into irt_data/imd/<var>.

Usage: imd_get.py Y0 Y1 [rain|tmax|tmin]   (default rain)
rain is 0.25 deg (135 x 129); tmax/tmin are 1.0 deg (31 x 31). New files only; retries with
backoff; validates size = nlat*nlon*ndays*4 bytes."""
import sys, os, time, calendar
import imdlib as imd
DIR = "D:/projects/irt_data/imd"
GRID = {"rain": (135, 129), "tmax": (31, 31), "tmin": (31, 31)}
y0, y1 = int(sys.argv[1]), int(sys.argv[2])
var = sys.argv[3] if len(sys.argv) > 3 else "rain"
nlat, nlon = GRID[var]
bad = []
for y in range(y0, y1 + 1):
    path = f"{DIR}/{var}/{y}.GRD" if var != "rain" else f"{DIR}/rain/{y}.grd"
    want = nlat * nlon * (366 if calendar.isleap(y) else 365) * 4
    if os.path.exists(path) and os.path.getsize(path) == want:
        print(var, y, "ok (exists)", flush=True); continue
    if os.path.exists(path):
        print(var, y, "exists with unexpected size", os.path.getsize(path), "- left untouched", flush=True)
        bad.append(y); continue
    for attempt in range(1, 7):
        try:
            t = time.time()
            imd.get_data(var, y, y, fn_format="yearwise", file_dir=DIR)
            size = os.path.getsize(path) if os.path.exists(path) else -1
            if size == want:
                print(var, y, "downloaded", size, f"{time.time()-t:.0f}s attempt {attempt}", flush=True); break
            print(var, y, "bad size", size, "want", want, flush=True)
            if os.path.exists(path): os.remove(path)   # only ever removes a file this run just wrote
        except Exception as e:
            print(var, y, "attempt", attempt, "failed:", type(e).__name__, flush=True)
        time.sleep(30 * attempt)
    else:
        bad.append(y)
print(var, "FAILED YEARS:", bad, flush=True)
