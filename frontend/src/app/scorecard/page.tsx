'use client';

import { useEffect, useState } from 'react';

const QUARTERS_DISPLAYED = 8;

export default function ScorecardPage() {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/scorecard')
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-4">
        <div className="skeleton h-8 w-48" />
        <div className="skeleton h-64 w-full rounded-xl" />
      </div>
    );
  }

  const oems: any[] = data?.oems ?? [];
  const revenue: any[] = data?.revenue ?? [];

  // Get distinct quarters (sorted desc)
  const quarters = [...new Set(revenue.map((r: any) => r.fy_quarter))]
    .sort((a, b) => b.localeCompare(a))
    .slice(0, QUARTERS_DISPLAYED);

  // Build pivot: oem_id → quarter → revenue_cr
  const pivot: Record<number, Record<string, number>> = {};
  for (const row of revenue) {
    if (!pivot[row.oem_id]) pivot[row.oem_id] = {};
    pivot[row.oem_id][row.fy_quarter] =
      (pivot[row.oem_id][row.fy_quarter] ?? 0) + (row.revenue_retail_cr ?? 0);
  }

  const inScopeOems = oems.filter(o => o.nse_ticker);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-slate-900">Quarterly Scorecard</h1>

      <div className="disclaimer-banner">
        ⚠️ Revenue figures are demand-based proxies. NOT accounting revenue.
        Compare to official OEM quarterly disclosures for investment decisions.
      </div>

      <div className="bg-white rounded-xl border border-slate-200 overflow-auto">
        <table className="data-table min-w-full">
          <thead>
            <tr>
              <th className="pl-6">OEM</th>
              <th>Ticker</th>
              {quarters.map(q => (
                <th key={q} className="text-right">{q}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {inScopeOems.map(oem => (
              <tr key={oem.oem_id}>
                <td className="pl-6 font-medium">{oem.oem_name}</td>
                <td className="text-slate-500 font-mono text-xs">{oem.nse_ticker}</td>
                {quarters.map(q => {
                  const val = pivot[oem.oem_id]?.[q];
                  return (
                    <td key={q} className="text-right">
                      {val != null ? `₹${val.toLocaleString(undefined, { maximumFractionDigits: 0 })} Cr` : '—'}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
