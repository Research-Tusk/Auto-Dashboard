'use client';

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

interface TIVDataPoint {
  month_key: string;
  tiv_units: number;
  ev_units: number;
  ev_penetration_pct?: number;
}

interface SegmentTrendChartProps {
  data: TIVDataPoint[];
  showEV?: boolean;
}

export function SegmentTrendChart({ data, showEV = true }: SegmentTrendChartProps) {
  if (!data.length) {
    return (
      <div className="h-64 flex items-center justify-center text-slate-400">
        No data available
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={data} margin={{ top: 5, right: 20, left: 40, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
        <XAxis
          dataKey="month_key"
          tickFormatter={(v) => {
            // Format as 'MMM YY'
            if (!v) return '';
            const d = new Date(v);
            return d.toLocaleDateString('en-IN', { month: 'short', year: '2-digit' });
          }}
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
          interval={1}
        />
        <YAxis
          tickFormatter={(v) =>
            v >= 1_000_000
              ? `${(v / 1_000_000).toFixed(1)}M`
              : `${(v / 1_000).toFixed(0)}K`
          }
          tick={{ fontSize: 11, fill: '#94a3b8' }}
          axisLine={false}
          tickLine={false}
        />
        <Tooltip
          formatter={(value: number, name: string) => [
            value.toLocaleString(),
            name,
          ]}
          labelFormatter={(label) => {
            if (!label) return '';
            const d = new Date(label);
            return d.toLocaleDateString('en-IN', { month: 'long', year: 'numeric' });
          }}
          contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0' }}
        />
        <Legend />
        <Line
          type="monotone"
          dataKey="tiv_units"
          stroke="#3b82f6"
          strokeWidth={2}
          dot={false}
          name="Total Units"
        />
        {showEV && (
          <Line
            type="monotone"
            dataKey="ev_units"
            stroke="#8b5cf6"
            strokeWidth={2}
            dot={false}
            strokeDasharray="4 2"
            name="EV Units"
          />
        )}
      </LineChart>
    </ResponsiveContainer>
  );
}

// Alias for backward compat
export const TIVLineChart = SegmentTrendChart;
