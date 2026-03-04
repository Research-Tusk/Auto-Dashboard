'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import { OEMTrendChart } from '@/components/charts/OEMTrendChart';
import { PowertrainMixChart } from '@/components/charts/PowertrainMixChart';

export default function OEMDeepDivePage() {
  const params = useParams();
  const ticker = (params?.ticker as string)?.toUpperCase() ?? '';

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!ticker) return;
    fetch(`/api/oem?ticker=${ticker}`)
      .then(r => r.json())
      .then(d => {
        if (d.error) { setError(d.error); }
        else { setData(d); }
        setLoading(false);
      })
      .catch(err => { setError(err.message); setLoading(false); });
  }, [ticker]);

  if (loading) return <div className="p-8"><div className="skeleton h-8 w-48 mb-4" /><div className="skeleton h-64 w-full" /></div>;
  if (error) return <div className="p-8 text-red-600">Error: {error}</div>;
  if (!data?.monthly?.length) return <div className="p-8 text-slate-500">No data found for {ticker}</div>;

  const oemName = data.monthly[0]?.oem_name ?? ticker;

  // Aggregate by month for trend chart
  const monthlyBySegment: Record<string, Record<string, any>> = {};
  for (const row of data.monthly) {
    const key = row.month_key;
    if (!monthlyBySegment[key]) monthlyBySegment[key] = { month_key: key };
    const segKey = `${row.segment_code}_${row.fuel_bucket}`;
    monthlyBySegment[key][segKey] = (monthlyBySegment[key][segKey] ?? 0) + row.total_units;
  }
  const trendData = Object.values(monthlyBySegment)
    .sort((a, b) => a.month_key.localeCompare(b.month_key))
    .slice(-13);

  // Powertrain mix (current month)
  const currentMonth = data.monthly[0]?.month_key;
  const currentRows = data.monthly.filter((r: any) => r.month_key === currentMonth);
  const powertrainData = currentRows.map((r: any) => ({
    name: `${r.segment_code} ${r.fuel_bucket}`,
    value: r.total_units,
  }));

  // Revenue rows
  const revenueRows = data.revenue?.slice(0, 8) ?? [];

  // Market share
  const shareRows = data.share?.slice(0, 6) ?? [];

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">{oemName}</h1>
        <p className="text-slate-500 text-sm mt-1">NSE: {ticker}</p>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-3 gap-4">
        <div className="kpi-card">
          <div className="text-2xl font-bold text-slate-900">
            {currentRows.reduce((s: number, r: any) => s + r.total_units, 0).toLocaleString()}
          </div>
          <div className="text-sm text-slate-500 mt-1">MTD Units</div>
        </div>
        <div className="kpi-card">
          <div className="text-2xl font-bold text-brand-600">
            {shareRows[0]?.market_share_pct?.toFixed(1) ?? '—'}%
          </div>
          <div className="text-sm text-slate-500 mt-1">Market Share</div>
        </div>
        <div className="kpi-card">
          <div className="text-2xl font-bold text-emerald-600">
            {revenueRows[0]?.revenue_retail_cr != null
              ? `₹${revenueRows[0].revenue_retail_cr.toLocaleString()} Cr`
              : '—'
            }
          </div>
          <div className="text-sm text-slate-500 mt-1">
            Revenue Proxy ({revenueRows[0]?.fy_quarter ?? ''})
          </div>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-2 gap-6">
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-4">Monthly Units Trend</h2>
          <OEMTrendChart data={trendData} />
        </div>
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-4">Powertrain Mix</h2>
          <PowertrainMixChart data={powertrainData} />
        </div>
      </div>

      {/* Revenue table */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <h2 className="text-base font-semibold text-slate-900 mb-4">Quarterly Revenue Proxy</h2>
        <p className="disclaimer-banner mb-4">
          ⚠️ Demand-based proxy: registrations × ASP assumption. NOT accounting revenue.
        </p>
        <table className="data-table">
          <thead>
            <tr>
              <th>Quarter</th>
              <th>Units (Retail)</th>
              <th>ASP (L)</th>
              <th>Revenue Est. (Cr)</th>
              <th>Completeness</th>
            </tr>
          </thead>
          <tbody>
            {revenueRows.map((r: any) => (
              <tr key={r.fy_quarter}>
                <td className="font-medium">{r.fy_quarter}</td>
                <td>{r.units_retail?.toLocaleString() ?? '—'}</td>
                <td>{r.asp_used?.toFixed(2) ?? '—'}</td>
                <td className="font-medium">₹{r.revenue_retail_cr?.toLocaleString() ?? '—'} Cr</td>
                <td>{r.data_completeness != null ? `${(r.data_completeness * 100).toFixed(0)}%` : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
