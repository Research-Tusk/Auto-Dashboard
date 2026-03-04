'use client';

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

interface RevenueDataPoint {
  quarter: string;
  total_cr: number;
}

interface RevenueBarChartProps {
  data: RevenueDataPoint[];
}

export function RevenueBarChart({ data }: RevenueBarChartProps) {
  if (!data.length) {
    return <div className="h-64 flex items-center justify-center text-slate-400">No data</div>;
  }

  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={data} margin={{ top: 5, right: 20, left: 40, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis
          dataKey="quarter"
          tick={{ fontSize: 12, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis
          tickFormatter={(v) => `₹${(v / 1000).toFixed(0)}K Cr`}
          tick={{ fontSize: 12, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value: number) => [`₹${value.toLocaleString()} Cr`, 'Revenue Proxy']}
          contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0' }}
        />
        <Bar
          dataKey="total_cr"
          fill="#6366f1"
          radius={[4, 4, 0, 0]}
          name="Revenue Proxy (Cr)"
        />
      </BarChart>
    </ResponsiveContainer>
  );
}
