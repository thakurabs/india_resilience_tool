"""Download IMD 0.25 deg gridded daily rainfall (.grd) for the requested years into irt_data/imd/rain.
New files only; retries with backoff; validates size = 135*129*ndays*4 bytes."""
import sys, os, time, calendar
import imdlib as imd
DIR = "D:/projects/irt_data/imd"
y0, y1 = int(sys.argv[1]), int(sys.argv[2])
bad = []
for y in range(y0, y1 + 1):
    path = f"{DIR}/rain/{y}.grd"
    want = 135 * 129 * (366 if calendar.isleap(y) else 365) * 4
    if os.path.exists(path) and os.path.getsize(path) == want:
        print(y, "ok (exists)", flush=True); continue
    for attempt in range(1, 7):
        try:
            t = time.time()
            imd.get_data("rain", y, y, fn_format="yearwise", file_dir=DIR)
            size = os.path.getsize(path) if os.path.exists(path) else -1
            if size == want:
                print(y, "downloaded", size, f"{time.time()-t:.0f}s attempt {attempt}", flush=True); break
            print(y, "bad size", size, "want", want, flush=True)
            if os.path.exists(path): os.remove(path)   # only ever removes a file this run just wrote
        except Exception as e:
            print(y, "attempt", attempt, "failed:", type(e).__name__, flush=True)
        time.sleep(30 * attempt)
    else:
        bad.append(y)
print("FAILED YEARS:", bad, flush=True)
