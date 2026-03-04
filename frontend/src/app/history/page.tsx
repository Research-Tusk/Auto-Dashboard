'use client';

import { useEffect, useState } from 'react';
import { HistoryChart } from '@/components/charts/HistoryChart';

const SEGMENTS = ['PV', 'CV', '2W'] as const;
const CURRENT_YEAR = new Date().getFullYear();

export default function HistoryPage() {
  const [activeSegment, setActiveSegment] = useState<'PV' | 'CV' | '2W'>('PV');
  const [fromYear, setFromYear] = useState(2020);
  const [toYear, setToYear] = useState(CURRENT_YEAR);
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  const fetchData = () => {
    setLoading(true);
    fetch(`/api/history?segment=${activeSegment}&from_year=${fromYear}&to_year=${toYear}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  };

  useEffect(() => { fetchData(); }, [activeSegment, fromYear, toYear]);

  // Compute annual TIV for bar chart
  const annualTIV: Record<string, number> = {};
  for (const row of data?.tiv ?? []) {
    const year = row.month_key?.slice(0, 4);
    if (year) annualTIV[year] = (annualTIV[year] ?? 0) + (row.tiv_units ?? 0);
  }
  const annualData = Object.entries(annualTIV)
    .map(([year, total]) => ({ year, total }))
    .sort((a, b) => a.year.localeCompare(b.year));

  // Top OEMs by share in most recent year
  const latestYearRows = data?.share?.filter(
    (r: any) => r.month_key?.startsWith(String(toYear))
  ) ?? [];
  const oemShare: Record<string, number> = {};
  for (const r of latestYearRows) {
    oemShare[r.oem_name] = (oemShare[r.oem_name] ?? 0) + (r.oem_units ?? 0);
  }
  const totalLatest = Object.values(oemShare).reduce((s, v) => s + v, 0);
  const topOems = Object.entries(oemShare)
    .map(([name, units]) => ({ name, units, share: units / (totalLatest || 1) }))
    .sort((a, b) => b.units - a.units)
    .slice(0, 10);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-slate-900">Historical Explorer</h1>

        {/* Filters */}
        <div className="flex items-center gap-3">
          <div className="flex gap-1">
            {SEGMENTS.map(seg => (
              <button
                key={seg}
                onClick={() => setActiveSegment(seg)}
                className={`px-3 py-1 rounded text-sm font-medium transition-colors ${
                  activeSegment === seg
                    ? 'bg-brand-600 text-white'
                    : 'bg-white text-slate-600 border border-slate-200'
                }`}
              >
                {seg}
              </button>
            ))}
          </div>
          <select
            value={fromYear}
            onChange={e => setFromYear(Number(e.target.value))}
            className="text-sm border border-slate-200 rounded px-2 py-1"
          >
            {Array.from({ length: CURRENT_YEAR - 2016 + 1 }, (_, i) => 2016 + i).map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
          <span className="text-slate-400 text-sm">–</span>
          <select
            value={toYear}
            onChange={e => setToYear(Number(e.target.value))}
            className="text-sm border border-slate-200 rounded px-2 py-1"
          >
            {Array.from({ length: CURRENT_YEAR - 2016 + 1 }, (_, i) => 2016 + i).map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </div>
      </div>

      {loading ? (
        <div className="skeleton h-64 w-full rounded-xl" />
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-4">
            Annual TIV — {activeSegment} (Units)
          </h2>
          <HistoryChart data={annualData} />
        </div>
      )}

      {/* OEM share table */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <h2 className="text-base font-semibold text-slate-900 mb-4">
          Top OEMs by Volume — {activeSegment} ({toYear})
        </h2>
        <table className="data-table">
          <thead>
            <tr>
              <th>OEM</th>
              <th className="text-right">Units</th>
              <th className="text-right">Share</th>
            </tr>
          </thead>
          <tbody>
            {topOems.map((oem, i) => (
              <tr key={oem.name}>
                <td className="font-medium">{i + 1}. {oem.name}</td>
                <td className="text-right">{oem.units.toLocaleString()}</td>
                <td className="text-right">{(oem.share * 100).toFixed(1)}%</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
