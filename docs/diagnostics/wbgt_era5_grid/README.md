# Gridded ERA5 WBGT: IRT deployed vs Lemke & Kjellstrom reference methods

**Local days (IST):** 2024-05-21  
**Cells:** 289 at 0.25 deg  
**Driver:** ERA5 hourly single levels, CDS (`scratch\wbgt_era5_grid`)  
**References:** Liljegren et al. (2008) outdoor; Bernard et al. (1999) indoor.

All four methods share one hourly driver, so driver error cancels and what
remains is method error.

## Scores

`bias` is candidate minus reference, averaged over cells.

| comparison | n | ref mean C | cand mean C | bias C | min bias | max bias | RMSE C | r |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| shade, peak vs peak (formula only) | 289 | 30.62 | 30.69 | +0.07 | -0.02 | +0.12 | 0.07 | 0.999 |
| shade, mean vs mean (formula only) | 289 | 28.37 | 28.34 | -0.02 | -0.06 | +0.02 | 0.03 | 1.000 |
| shade, AS DEPLOYED vs reference peak | 289 | 30.62 | 28.53 | -2.09 | -4.20 | -1.18 | 2.14 | 0.854 |
| outdoor, peak vs peak (formula only) | 289 | 34.76 | 36.69 | +1.94 | -0.88 | +4.94 | 2.21 | 0.600 |
| outdoor, mean vs mean (formula only) | 289 | 29.75 | 34.15 | +4.40 | +3.55 | +5.53 | 4.42 | 0.965 |
| outdoor, AS DEPLOYED vs reference peak | 289 | 34.76 | 34.45 | -0.31 | -3.35 | +1.63 | 1.24 | 0.450 |
| shipped shade vs outdoor reference | 289 | 34.76 | 28.53 | -6.23 | -9.41 | -4.47 | 6.33 | 0.454 |

## Reading this table

- `peak vs peak` isolates **formula** error: both sides see the same hourly
  series and the same reduction.
- `AS DEPLOYED vs reference peak` adds **aggregation** error, because the IRT
  side is the formula evaluated on daily-mean Ta and RH, which is what the
  pipeline ships.
- The spread between `min bias` and `max bias` is the part that matters for a
  frozen national CDF ruler: a uniform offset barely moves ranks, a spatially
  varying one reorders districts.
