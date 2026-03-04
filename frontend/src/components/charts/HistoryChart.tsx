'use client';

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

interface HistoryDataPoint {
  year: string;
  total: number;
}

interface HistoryChartProps {
  data: HistoryDataPoint[];
}

export function HistoryChart({ data }: HistoryChartProps) {
  if (!data.length) {
    return <div className="h-64 flex items-center justify-center text-slate-400">No data</div>;
  }

  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={data} margin={{ top: 5, right: 20, left: 40, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis
          dataKey="year"
          tick={{ fontSize: 12, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          tickFormatter={(v) => `${(v / 1_000_000).toFixed(1)}M`}
          tick={{ fontSize: 12, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value: number) => [value.toLocaleString(), 'Units']}
          contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0' }}
        />
        <Bar dataKey="total" fill="#6366f1" radius={[4, 4, 0, 0]} name="Annual TIV" />
      </BarChart>
    </ResponsiveContainer>
  );
}
