import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';
import type { ReactNode } from 'react';

type Accent = 'brand' | 'positive' | 'negative' | 'neutral';

interface KPICardProps {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  accent?: Accent;
  loading?: boolean;
}

const ACCENT_CLASSES: Record<Accent, string> = {
  brand: 'text-brand-700',
  positive: 'text-emerald-600',
  negative: 'text-red-500',
  neutral: 'text-slate-700',
};

export function KPICard({ label, value, sub, accent = 'neutral', loading = false }: KPICardProps) {
  return (
    <div className={twMerge('section-card flex flex-col gap-1 min-w-0', loading ? 'animate-pulse' : '')}>
      <p className="text-xs font-medium text-slate-500 truncate">{label}</p>
      {loading ? (
        <div className="h-7 bg-slate-100 rounded w-2/3" />
      ) : (
        <p className={clsx('text-2xl font-bold tabular-nums tracking-tight truncate', ACCENT_CLASSES[accent])}>
          {value}
        </p>
      )}
      {sub && !loading && (
        <p className="text-xs text-slate-400 truncate">{sub}</p>
      )}
    </div>
  );
}
