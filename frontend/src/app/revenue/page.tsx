'use client';

import { useEffect, useState } from 'react';
import { RevenueBarChart } from '@/components/charts/RevenueBarChart';

const DISCLAIMER =
  'Revenue figures are demand-based proxies (registrations \u00d7 ASP assumption). ' +
  'NOT accounting revenue. Not for investment decisions.';

export default function RevenuePage() {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/revenue')
      .then(r => r.json())
      .then(d => { setData(d.data ?? []); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  // Group by quarter for chart
  const byQuarter: Record<string, { quarter: string; total_cr: number; count: number }> = {};
  for (const row of data) {
    if (!byQuarter[row.fy_quarter]) {
      byQuarter[row.fy_quarter] = { quarter: row.fy_quarter, total_cr: 0, count: 0 };
    }
    byQuarter[row.fy_quarter].total_cr += row.revenue_retail_cr ?? 0;
    byQuarter[row.fy_quarter].count += 1;
  }
  const chartData = Object.values(byQuarter)
    .sort((a, b) => a.quarter.localeCompare(b.quarter))
    .slice(-8);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-slate-900">Revenue Estimator</h1>

      <div className="disclaimer-banner">{DISCLAIMER}</div>

      {loading ? (
        <div className="skeleton h-64 w-full rounded-xl" />
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-4">
            Quarterly Demand Proxy — All OEMs (INR Crore)
          </h2>
          <RevenueBarChart data={chartData} />
        </div>
      )}

      {/* Detail table */}
      <div className="bg-white rounded-xl border border-slate-200 p-6 overflow-auto">
        <h2 className="text-base font-semibold text-slate-900 mb-4">Detail</h2>
        <table className="data-table">
          <thead>
            <tr>
              <th>Quarter</th>
              <th>Units (Retail)</th>
              <th>ASP Used (L)</th>
              <th>Revenue Est. (Cr)</th>
              <th>Data Completeness</th>
            </tr>
          </thead>
          <tbody>
            {data.slice(0, 40).map((row, i) => (
              <tr key={i}>
                <td className="font-medium">{row.fy_quarter}</td>
                <td>{row.units_retail?.toLocaleString() ?? '—'}</td>
                <td>{row.asp_used?.toFixed(2) ?? '—'}</td>
                <td className="font-medium">₹{row.revenue_retail_cr?.toLocaleString() ?? '—'} Cr</td>
                <td>{row.data_completeness != null ? `${(row.data_completeness * 100).toFixed(0)}%` : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
