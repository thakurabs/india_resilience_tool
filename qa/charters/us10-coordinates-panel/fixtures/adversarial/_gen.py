#!/usr/bin/env python3
"""Generate an adversarial upload-coordinate fixture matrix for US 10.

All value-poison files use the app's OWN accepted schema (id,custom_name,lat,long)
so they pass structure/type gates and stress *value* validation. File-level cases
stress size limits, empty/malformed parsing, and the .zip shapefile path.
"""
import csv
import io
import os
import struct
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))


def w(name, text, mode="w", encoding="utf-8"):
    path = os.path.join(HERE, name)
    if mode == "wb":
        with open(path, "wb") as f:
            f.write(text)
    else:
        with open(path, mode, encoding=encoding, newline="") as f:
            f.write(text)
    print(f"  wrote {name} ({os.path.getsize(path)} bytes)")


HEADER = "id,custom_name,lat,long\n"

# 1. Out-of-range coordinates (valid schema, impossible values)
w("v01_outofrange.csv", HEADER + "1,BadLat,999,79.2\n2,BadLong,17.3,-500\n3,NaNish,1e9,1e9\n")

# 2. Non-numeric in numeric cells
w("v02_nonnumeric.csv", HEADER + '1,Alpha,abc,def\n2,Sym,#$%,@!\n3,Blank,, \n')

# 3. Empty lat/long cells
w("v03_empty_coords.csv", HEADER + "1,NoCoords,,\n2,HalfCoord,17.3,\n")

# 4. In-schema but far outside India coverage
w("v04_outofindia.csv", HEADER + "1,London,51.5074,-0.1278\n2,Pacific,0.0,-160.0\n3,NullIsland,0,0\n")

# 5. CSV formula / spreadsheet injection in custom_name
w(
    "v05_formula_injection.csv",
    HEADER
    + '1,"=cmd|\' /C calc\'!A1",17.38,78.48\n'
    + '2,"@SUM(1+9)*cmd|\'/C calc\'!A0",17.44,78.34\n'
    + '3,"+1+1",16.50,80.64\n'
    + '4,"-2+3",17.20,78.60\n',
)

# 6. Overlong custom_name (5000 chars) + extreme precision coords
longname = "A" * 5000
w("v06_longname.csv", HEADER + f'1,"{longname}",17.385000000000000123456789,78.486700000000001\n')

# 7. Unicode / emoji / RTL / control chars in name
w("v07_unicode.csv", HEADER + '1,"🔥💧 <script>alert(1)</script>",17.38,78.48\n2,"مرحبا‮evil",17.44,78.34\n')

# 8. Duplicate + negative-zero + whitespace-padded numeric
w("v08_weird_numbers.csv", HEADER + "1,Dup,17.38,78.48\n1,Dup,17.38,78.48\n2,Pad,  17.44 , 78.34 \n3,NegZero,-0.0,-0.0\n")

# 9. Over 1 MB file (UI states 'max 1 MB'): ~60k valid rows
buf = io.StringIO()
buf.write(HEADER)
for i in range(60000):
    buf.write(f"{i},Site{i},{17.0 + (i % 100) / 100.0},{78.0 + (i % 100) / 100.0}\n")
w("f01_over1mb.csv", buf.getvalue())

# 10. Empty (0-byte) file
w("f02_empty.csv", "")

# 11. Header only, no data rows
w("f03_header_only.csv", HEADER)

# 12. Malformed CSV: unclosed quote, ragged columns, embedded newlines
w(
    "f04_malformed.csv",
    HEADER
    + '1,"unterminated,17.38,78.48\n'
    + "2,too,many,columns,here,17.44,78.34\n"
    + "3\n"
    + '4,"line\nbreak inside",16.5,80.6\n',
)

# 13. Binary content masquerading as .csv (PNG magic bytes)
png_magic = b"\x89PNG\r\n\x1a\n" + struct.pack(">I", 0xDEADBEEF) + b"garbage\x00\xff" * 50
w("f05_binary_as.csv", png_magic, mode="wb")

# 14. Bogus .zip (valid zip, but NOT a shapefile) for the shapefile upload path
zpath = os.path.join(HERE, "f06_bogus_shapefile.zip")
with zipfile.ZipFile(zpath, "w") as z:
    z.writestr("readme.txt", "this zip has no .shp/.shx/.dbf")
    z.writestr("data.csv", HEADER + "1,X,17.3,78.4\n")
print(f"  wrote f06_bogus_shapefile.zip ({os.path.getsize(zpath)} bytes)")

# 15. Wrong extension: a real CSV named .xlsx (exercises xlsx branch with bad content)
w("f07_csv_named.xlsx", HEADER + "1,FakeXlsx,17.3,78.4\n")

print("done.")
