# NCL colour tables

Verbatim colour tables from the NCAR Command Language gallery
(<https://www.ncl.ucar.edu/Document/Graphics/color_table_gallery.shtml>), kept
here so the pilot's choropleths do not depend on a network fetch or on an extra
package. Format is NCL's own: an `ncolors=` header, a `# r g b` comment line,
then one 0-255 triple per line.

- `WhiteBlueGreenYellowRed.rgb` — 254 colours, white -> blue -> green -> yellow
  -> red. Monotonic in lightness for most of its span, so it reads as an ordered
  scale.
- `matlab_hsv.rgb` — 64 colours, MATLAB's HSV wheel. **Cyclic**: it starts and
  ends on red, so the top and bottom of a score scale render in nearly the same
  hue. Included because it was asked for; it is not a safe default for an
  ordered quantity.

Loaded by `_load_cmap` in `heat_risk_national_ruler_pilot.py`; any file dropped
in this directory becomes available to `--map-cmap` by its stem.
