interface KPICardProps {
  label: string;
  value: string | number;
  sub?: string;
  trend?: number;       // positive = up, negative = down
  trendLabel?: string;
  loading?: boolean;
  color?: 'default' | 'green' | 'violet' | 'amber' | 'red';
}

const COLOR_CLASSES = {
  default: 'text-slate-900',
  green:   'text-emerald-600',
  violet:  'text-violet-600',
  amber:   'text-amber-600',
  red:     'text-red-600',
};

export function KPICard({
  label, value, sub, trend, trendLabel, loading = false, color = 'default'
}: KPICardProps) {
  if (loading) {
    return (
      <div className="kpi-card">
        <div className="skeleton h-8 w-24 mb-2" />
        <div className="skeleton h-4 w-16" />
      </div>
    );
  }

  const trendColor = trend == null ? '' : trend >= 0 ? 'text-emerald-500' : 'text-red-500';
  const trendArrow = trend == null ? '' : trend >= 0 ? '↑' : '↓';

  return (
    <div className="kpi-card">
      <div className={`text-2xl font-bold ${COLOR_CLASSES[color]}`}>{value}</div>
      {sub && <div className="text-sm text-slate-400 mt-0.5">{sub}</div>}
      <div className="text-sm text-slate-500 mt-1">{label}</div>
      {trend != null && (
        <div className={`text-xs mt-1 ${trendColor}`}>
          {trendArrow} {Math.abs(trend).toFixed(1)}%
          {trendLabel && <span className="text-slate-400 ml-1">{trendLabel}</span>}
        </div>
      )}
    </div>
  );
}
