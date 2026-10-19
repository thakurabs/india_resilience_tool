"""Small scientific-contract checks for the multicity comparison (no downloads)."""
import unittest
import numpy as np
import pandas as pd
from compare_multicity import summarize, city_cells

class ComparisonContracts(unittest.TestCase):
    def frame(self, cp, w1):
        return pd.DataFrame({'city':'Test', 'year':2005, 'date':pd.date_range('2005-01-01',periods=len(cp)).strftime('%Y-%m-%d'), 'carbonplan_wbgt_c':cp, 'w1_area_mean_wbgt_c':w1})

    def test_threshold_inclusive_and_disagreement_identity(self):
        a,m,t=summarize(self.frame(np.full(365,30.),np.full(365,32.)))
        x=t.set_index('threshold_c')
        self.assertEqual(x.loc[30,'both_exceed_days'],365)
        self.assertEqual(x.loc[32,'w1_only_days_on_matched_dates'],365)
        self.assertEqual(x.loc[32,'carbonplan_days_complete_year'],0)
        self.assertEqual(m.w1_days_ge_32.sum(),365)

    def test_missing_day_does_not_become_zero_or_complete_count(self):
        w=np.full(365,30.);w[0]=np.nan
        a,m,t=summarize(self.frame(np.full(365,30.),w))
        self.assertTrue(np.isnan(a.w1_annual_mean_c.iloc[0]))
        self.assertTrue(t.w1_days_complete_year.isna().all())
        self.assertTrue((t.matched_days==364).all())
        self.assertTrue(np.isnan(m.w1_days_ge_30.iloc[0]))

    def test_all_missing_and_single_point(self):
        for n in (1,365):
            a,m,t=summarize(self.frame(np.full(n,np.nan),np.full(n,np.nan)))
            self.assertTrue(a.w1_annual_mean_c.isna().all())
            self.assertTrue(t.w1_days_complete_year.isna().all())
            self.assertTrue((t.matched_days==0).all())

    def test_empty(self):
        self.assertTrue(all(x.empty for x in summarize(self.frame([],[]))))

    def test_single_cell_geometry_and_extreme_thresholds(self):
        feature={'geometry':{'type':'Polygon','coordinates':[[[77,10],[77.25,10],[77.25,10.25],[77,10.25],[77,10]]]}}
        cells,_,area=city_cells(feature,np.array([10.125]),np.array([77.125]))
        self.assertAlmostEqual(cells.overlap_m2.sum()/area,1.)
        _,_,t=summarize(self.frame(np.full(365,-50.),np.full(365,60.)))
        self.assertTrue((t.w1_only_days_on_matched_dates==365).all())

if __name__=='__main__':
    unittest.main()
