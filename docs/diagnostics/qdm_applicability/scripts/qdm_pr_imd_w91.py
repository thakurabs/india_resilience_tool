"""Sensitivity: identical to qdm_pr_imd.py but QDM grouping window 91 days instead of 31 (CarbonPlan default)."""
import sys
sys.path.insert(0, "docs/diagnostics/qdm_applicability/scripts"); sys.path.insert(0, ".")
import qdm_applicability as Q
Q.KW = dict(nquantiles=Q.NQUANTILES, window=91)
import qdm_pr_imd as P
from pathlib import Path
P.OUT = Path(".scratch/qdm_applicability/out_imd_w91"); P.OUT.mkdir(parents=True, exist_ok=True)
P.main()
