"""Validate multicity daily evidence and render comparable seasonal summaries."""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main() -> None:
    """Check identities and write city summaries and publication-ready PNG figures."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out-dir', type=Path, required=True)
    args = parser.parse_args()
    out = args.out_dir
    daily = pd.read_csv(out / 'paired_daily.csv')
    annual = pd.read_csv(out / 'annual_summary.csv')
    counts = pd.read_csv(out / 'threshold_summary.csv')
    monthly = pd.read_csv(out / 'monthly_summary.csv')
    assert len(daily) == 5*3*365
    assert not daily.duplicated(['city', 'date']).any()
    assert len(annual) == 15 and len(counts) == 45 and len(monthly) == 180
    for (city, year), d in daily.groupby(['city', 'year']):
        expected = pd.date_range(f'{year}-01-01', f'{year}-12-31').strftime('%Y-%m-%d')
        assert list(d.sort_values('date').date) == list(expected)
        a = annual[(annual.city == city) & (annual.year == year)].iloc[0]
        c = d.carbonplan_wbgt_c.to_numpy(); w = d.w1_area_mean_wbgt_c.to_numpy()
        valid = np.isfinite(c) & np.isfinite(w)
        assert valid.all(), f'Incomplete city-year {city} {year}; cannot pool annual counts'
        np.testing.assert_allclose([c.mean(), w.mean()], [a.carbonplan_annual_mean_c, a.w1_annual_mean_c], atol=1e-10, rtol=0)
        for t in (28,30,32):
            row = counts[(counts.city == city) & (counts.year == year) & (counts.threshold_c == t)].iloc[0]
            assert (c>=t).sum() == row.carbonplan_days_complete_year
            assert (w>=t).sum() == row.w1_days_complete_year
            assert row.both_exceed_days + row.cp_only_days_on_matched_dates == (c>=t).sum()
            assert row.both_exceed_days + row.w1_only_days_on_matched_dates == (w>=t).sum()
            assert row.both_exceed_days + row.neither_exceed_days + row.cp_only_days_on_matched_dates + row.w1_only_days_on_matched_dates == 365
            months = monthly[(monthly.city == city) & (monthly.year == year)]
            assert months[f'carbonplan_days_ge_{t}'].sum() == (c>=t).sum()
            assert months[f'w1_days_ge_{t}'].sum() == (w>=t).sum()
    rows=[]
    cities = ['Kochi','Bikaner','Shimla','Hyderabad','Kolkata']
    for city in cities:
        a = annual[annual.city==city]
        row={'city':city, 'years':len(a), 'carbonplan_mean_c':a.carbonplan_annual_mean_c.mean(), 'w1_mean_c':a.w1_annual_mean_c.mean(), 'mean_difference_c':a.w1_minus_cp_annual_mean_c.mean()}
        for t in (28,30,32):
            c = counts[(counts.city==city)&(counts.threshold_c==t)]
            delta = c.w1_days_complete_year-c.carbonplan_days_complete_year
            row.update({f'cp_days_ge_{t}_per_year':c.carbonplan_days_complete_year.mean(), f'w1_days_ge_{t}_per_year':c.w1_days_complete_year.mean(), f'delta_days_ge_{t}_per_year':delta.mean(), f'delta_days_ge_{t}_min':delta.min(), f'delta_days_ge_{t}_max':delta.max(), f'w1_only_ge_{t}_per_year':c.w1_only_days_on_matched_dates.mean(), f'cp_only_ge_{t}_per_year':c.cp_only_days_on_matched_dates.mean()})
        rows.append(row)
    summary=pd.DataFrame(rows);summary.to_csv(out/'city_summary.csv',index=False)
    fig,axes=plt.subplots(3,2,figsize=(11,10),sharex=True)
    for ax,city in zip(axes.flat,cities):
        m=monthly[monthly.city==city].groupby('month').mean(numeric_only=True)
        ax.plot(m.index,m.carbonplan_mean_c,label='CarbonPlan',color='#3465a4')
        ax.plot(m.index,m.w1_mean_c,label='W1',color='#d95f02')
        ax.set_title(city);ax.set_ylabel('Mean daily maximum WBGT (°C)');ax.grid(alpha=.2)
        ax.set_xticks(range(1,13));ax.set_xlabel('Month');ax.tick_params(labelbottom=True)
    axes.flat[-1].axis('off');axes.flat[0].legend()
    fig.suptitle('Published CarbonPlan vs W1 — ACCESS-CM2, 2005/2007/2009\nMatched city footprints; different spatial weighting and correction methods')
    fig.tight_layout(rect=(0,0,1,.94));fig.savefig(out/'seasonal_comparison.png',dpi=160);plt.close(fig)
    fig,ax=plt.subplots(figsize=(9,4))
    x=np.arange(len(cities))
    for i,t in enumerate((28,30,32)):
        means=summary[f'delta_days_ge_{t}_per_year'].to_numpy()
        lower=means-summary[f'delta_days_ge_{t}_min'].to_numpy()
        upper=summary[f'delta_days_ge_{t}_max'].to_numpy()-means
        ax.bar(x+(i-1)*.25,means,width=.24,label=f'≥{t}°C',yerr=np.stack([lower,upper]),capsize=3)
    ax.axhline(0,color='black',lw=.7);ax.set_xticks(x,cities);ax.set_ylabel('W1 − CarbonPlan (days/year)');ax.legend()
    ax.set_title('Threshold differences: three-year mean; whiskers show year range')
    fig.tight_layout();fig.savefig(out/'threshold_differences.png',dpi=160);plt.close(fig)
    print(summary[['city','mean_difference_c','delta_days_ge_28_per_year','delta_days_ge_30_per_year','delta_days_ge_32_per_year']].to_string(index=False))
    print('Validated 5,475 dates, 15 annual summaries, 45 threshold rows and 180 monthly rows.')

if __name__ == '__main__':
    main()
