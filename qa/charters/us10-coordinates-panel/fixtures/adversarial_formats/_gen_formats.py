#!/usr/bin/env python3
"""Build adversarial .xlsx and .zip-shapefile upload fixtures by mutating the
app's OWN valid sample files (downloaded from the live app). Stdlib only.

Sources (the app's genuine accepted samples):
  qa/runs/export-capture/coord_xlsx__sample_coordinates.xlsx
  qa/runs/export-capture/coord_zip__sample_coordinates.zip  (shapefile)
"""
import io
import os
import re
import struct
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
QA = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))  # -> qa/
XLSX_SRC = os.path.join(QA, "runs", "export-capture", "coord_xlsx__sample_coordinates.xlsx")
ZIP_SRC = os.path.join(QA, "runs", "export-capture", "coord_zip__sample_coordinates.zip")
SHEET = "xl/worksheets/sheet1.xml"


def read_members(path):
    with zipfile.ZipFile(path) as z:
        return [(i, z.read(i.filename)) for i in z.infolist()]


def write_xlsx(name, sheet_bytes):
    members = read_members(XLSX_SRC)
    out = os.path.join(HERE, name)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for info, data in members:
            z.writestr(info, sheet_bytes if info.filename == SHEET else data)
    print(f"  wrote {name} ({os.path.getsize(out)} b)")


def sheet():
    with zipfile.ZipFile(XLSX_SRC) as z:
        return z.read(SHEET).decode("utf-8")


S = sheet()

# x01 — formula injection in a custom_name cell (B2)
write_xlsx("x01_xlsx_formula.xlsx",
           S.replace('<c r="B2" t="str"><v>Site A</v></c>',
                     '<c r="B2" t="str"><v>=cmd|\' /C calc\'!A1</v></c>').encode())

# x02 — out-of-range coords (C2 lat 999, D3 long -500)
write_xlsx("x02_xlsx_outofrange.xlsx",
           S.replace('<c r="C2"><v>17.385</v></c>', '<c r="C2"><v>999</v></c>')
            .replace('<c r="D3"><v>78.3489</v></c>', '<c r="D3"><v>-500</v></c>').encode())

# x03 — non-numeric in a coordinate cell (C2 = "abc")
write_xlsx("x03_xlsx_nonnumeric.xlsx",
           S.replace('<c r="C2"><v>17.385</v></c>', '<c r="C2" t="str"><v>abc</v></c>').encode())

# x04 — documented (spec) schema headers Latitude/Longitude/Label (drift test on xlsx path)
write_xlsx("x04_xlsx_docschema.xlsx",
           S.replace('<v>custom_name</v>', '<v>Label</v>')
            .replace('<v>lat</v>', '<v>Latitude</v>')
            .replace('<v>long</v>', '<v>Longitude</v>').encode())

# x05 — empty coordinate cells (blank C2/D2)
write_xlsx("x05_xlsx_empty_coords.xlsx",
           S.replace('<c r="C2"><v>17.385</v></c>', '<c r="C2" t="str"><v></v></c>')
            .replace('<c r="D2"><v>78.4867</v></c>', '<c r="D2" t="str"><v></v></c>').encode())


# ---------- shapefile (.zip) fixtures ----------
def shp_members():
    with zipfile.ZipFile(ZIP_SRC) as z:
        return {i.filename: z.read(i.filename) for i in z.infolist()}


M = shp_members()
BASE = os.path.splitext(next(k for k in M if k.endswith(".shp")))[0]


def edit_dbf_customname(dbf: bytes, new_value: str) -> bytes:
    """Overwrite record 0's custom_name (field 'custom_nam', 50 chars) in place."""
    b = bytearray(dbf)
    header_size = int.from_bytes(b[8:10], "little")
    # Walk 32-byte field descriptors from offset 32 until 0x0D terminator.
    off, rec_off, fields = 32, 1, []  # rec_off starts after 1-byte deletion flag
    while b[off] != 0x0D:
        fname = bytes(b[off:off + 11]).split(b"\x00")[0].decode("latin1")
        flen = b[off + 16]
        fields.append((fname, rec_off, flen))
        rec_off += flen
        off += 32
    target = next(f for f in fields if f[0].startswith("custom_nam"))
    start = header_size + target[1]
    val = new_value.encode("latin1", "replace")[:target[2]].ljust(target[2], b" ")
    b[start:start + target[2]] = val
    return bytes(b)


def write_zip(name, files: dict):
    out = os.path.join(HERE, name)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for fn, data in files.items():
            z.writestr(fn, data)
    print(f"  wrote {name} ({os.path.getsize(out)} b, members={list(files)})")


# z01 — baseline: repack the app's own sample UNCHANGED (control: must be accepted)
write_zip("z01_shp_baseline.zip", dict(M))

# z02 — formula injection in the .dbf attribute custom_name
m2 = dict(M)
m2[f"{BASE}.dbf"] = edit_dbf_customname(M[f"{BASE}.dbf"], "=cmd|' /C calc'!A1")
write_zip("z02_shp_formula_dbf.zip", m2)

# z03 — missing the required .shp geometry component (partial shapefile)
write_zip("z03_shp_missing_shp.zip", {k: v for k, v in M.items() if not k.endswith(".shp")})

# z04 — only the .dbf (no geometry at all)
write_zip("z04_shp_only_dbf.zip", {f"{BASE}.dbf": M[f"{BASE}.dbf"]})

# z05 — overlong custom_name (fills the 50-char field)
m5 = dict(M)
m5[f"{BASE}.dbf"] = edit_dbf_customname(M[f"{BASE}.dbf"], "A" * 50)
write_zip("z05_shp_longname_dbf.zip", m5)

# z06 — valid shapefile + a large junk member (structure-plus-noise)
m6 = dict(M)
m6["junk_payload.txt"] = b"X" * 200000
write_zip("z06_shp_plus_junk.zip", m6)


def edit_shp_points(shp: bytes, points):
    """Rewrite each Point record's (X, Y) doubles in place and recompute the
    bbox in the 100-byte main-file header. `points` is a list of (lon, lat)
    the same length as the record count. Record sizes are unchanged (fixed-size
    Point = 28 bytes), so record offsets and the .shx index stay valid."""
    b = bytearray(shp)
    off, i = 100, 0
    while off < len(b) and i < len(points):
        recnum, clen = struct.unpack(">ii", b[off:off + 8])
        st = struct.unpack("<i", b[off + 8:off + 12])[0]
        if st != 1:
            raise ValueError(f"record {recnum} is not a Point (type {st})")
        lon, lat = points[i]
        struct.pack_into("<2d", b, off + 12, lon, lat)  # X at content+4, Y at content+12
        off += 8 + clen * 2
        i += 1
    if i != len(points):
        raise ValueError(f"expected {len(points)} point records, rewrote {i}")
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    struct.pack_into("<4d", b, 36, min(xs), min(ys), max(xs), max(ys))  # header bbox
    return bytes(b)


def edit_shx_bbox(shx: bytes, points):
    """Mirror the recomputed bbox into the .shx index header (offset 36)."""
    b = bytearray(shx)
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    struct.pack_into("<4d", b, 36, min(xs), min(ys), max(xs), max(ys))
    return bytes(b)


# z07 — out-of-India geometry: move the 3 points to London / mid-Pacific /
# Null Island. Valid shapefile structure; only the coordinates are foreign.
OUT_OF_INDIA = [(-0.1276, 51.5074), (-160.0, -10.0), (0.0, 0.0)]
m7 = dict(M)
m7[f"{BASE}.shp"] = edit_shp_points(M[f"{BASE}.shp"], OUT_OF_INDIA)
m7[f"{BASE}.shx"] = edit_shx_bbox(M[f"{BASE}.shx"], OUT_OF_INDIA)
write_zip("z07_shp_outofindia.zip", m7)

print("done.")
